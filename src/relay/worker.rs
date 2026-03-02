//! Relay worker process — manages the MQTT relay as a standalone process.
//!
//! Entry point for `hcom relay-worker`. Handles PID file management,
//! signal handling, auto-exit watchdog, and relay lifecycle.
//!
//! Auto-spawn: `maybe_auto_spawn()` checks config, PID, and instance count
//! before spawning a new relay-worker process.

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::config::HcomConfig;
use crate::db::HcomDb;
use crate::log;
use crate::relay::client::RelayCommand;

// ── PID file helpers ────────────────────────────────────────────────

fn pid_file_path() -> PathBuf {
    crate::paths::hcom_dir().join(".tmp").join("relay.pid")
}

/// Write current PID to relay.pid atomically.
fn write_pid_file() {
    let pid = std::process::id().to_string();
    crate::paths::atomic_write(&pid_file_path(), &pid);
}

/// Read PID from relay.pid and validate process is alive.
fn read_pid_file() -> Option<u32> {
    let path = pid_file_path();
    let content = std::fs::read_to_string(&path).ok()?;
    let pid: u32 = content.trim().parse().ok()?;
    if is_process_alive(pid) {
        Some(pid)
    } else {
        // Stale PID file — clean up
        let _ = std::fs::remove_file(&path);
        None
    }
}

/// Remove PID file.
fn remove_pid_file() {
    let _ = std::fs::remove_file(pid_file_path());
}

/// Check if a process is alive (kill -0).
fn is_process_alive(pid: u32) -> bool {
    // SAFETY: kill(pid, 0) is a no-op signal that checks process existence.
    let ret = unsafe { libc::kill(pid as i32, 0) };
    ret == 0
}

/// Check if a relay-worker process is currently running.
pub fn is_relay_worker_running() -> bool {
    read_pid_file().is_some()
}

// ── Drop guard for PID file cleanup ─────────────────────────────────

struct PidFileGuard;

impl Drop for PidFileGuard {
    fn drop(&mut self) {
        remove_pid_file();
    }
}

// ── Worker entry point ──────────────────────────────────────────────

/// Run the relay-worker process. Called from router dispatch.
pub fn run() -> i32 {
    // Check if already running
    if let Some(existing_pid) = read_pid_file() {
        eprintln!("relay-worker already running (PID {})", existing_pid);
        return 1;
    }

    // Write PID file (guard removes on exit)
    write_pid_file();
    let _pid_guard = PidFileGuard;

    log::log_info(
        "relay",
        "relay_worker.start",
        &format!("pid={}", std::process::id()),
    );

    // Load config
    let config = match HcomConfig::load(None) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: Failed to load config: {e}");
            return 1;
        }
    };

    if !super::is_relay_enabled(&config) {
        eprintln!("Error: Relay not configured or disabled");
        return 1;
    }

    // Connect to MQTT
    let (relay, connection, cmd_tx) = match super::client::MqttRelay::connect(&config) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error: Failed to connect: {e}");
            return 1;
        }
    };

    // Bind TCP notify listener for CLI → daemon push wake.
    // CLI callers (hcom send, hooks) connect to trigger immediate push.
    let notify_port = setup_notify_listener(&cmd_tx);

    // Install signal handlers via signal-hook (sets AtomicBool on SIGTERM/SIGINT).
    // The watchdog thread checks this flag — no separate signal-polling thread needed.
    let shutdown = Arc::new(AtomicBool::new(false));
    let _ = signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&shutdown));
    let _ = signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&shutdown));

    // Spawn auto-exit watchdog thread (also monitors shutdown flag)
    let cmd_tx_watchdog = cmd_tx;
    std::thread::spawn(move || {
        auto_exit_watchdog(cmd_tx_watchdog, shutdown);
    });

    // Run relay event loop (blocks until shutdown)
    relay.run(connection);

    // Clear notify port so CLI callers stop trying to connect
    if notify_port.is_some() {
        if let Ok(db) = HcomDb::open() {
            super::safe_kv_set(&db, "relay_daemon_port", None);
        }
    }

    log::log_info("relay", "relay_worker.stop", "exited cleanly");
    0
}

/// Bind TCP listener on random port for CLI→daemon push notifications.
/// Stores port in KV `relay_daemon_port`. Returns port on success.
fn setup_notify_listener(cmd_tx: &std::sync::mpsc::Sender<RelayCommand>) -> Option<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").ok()?;
    let port = listener.local_addr().ok()?.port();

    // Store port in DB so CLI callers can find us
    if let Ok(db) = HcomDb::open() {
        super::safe_kv_set(&db, "relay_daemon_port", Some(&port.to_string()));
    }

    log::log_info(
        "relay",
        "relay_worker.notify_listen",
        &format!("port={}", port),
    );

    // Spawn thread to accept connections and send Push commands.
    // Each incoming TCP connection (no data, just connect+close) triggers a push.
    let cmd_tx = cmd_tx.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(conn) => {
                    drop(conn); // Close immediately — connection itself is the signal
                    if cmd_tx.send(RelayCommand::Push).is_err() {
                        break; // Relay shut down
                    }
                }
                Err(_) => break,
            }
        }
    });

    Some(port)
}

/// Auto-exit watchdog: every 30s, check if any local instances exist.
/// If none for 2 consecutive checks, or shutdown signal received, send Shutdown.
fn auto_exit_watchdog(cmd_tx: std::sync::mpsc::Sender<RelayCommand>, shutdown: Arc<AtomicBool>) {
    let mut consecutive_empty = 0u32;
    let mut db = HcomDb::open().ok();

    loop {
        std::thread::sleep(Duration::from_secs(30));

        if shutdown.load(Ordering::Relaxed) {
            let _ = cmd_tx.send(RelayCommand::Shutdown);
            return;
        }

        // Re-open DB if previous connection failed
        if db.is_none() {
            db = HcomDb::open().ok();
        }

        let count = match &db {
            Some(d) => local_instance_count(d),
            None => {
                consecutive_empty = 0;
                continue;
            }
        };

        if count == 0 {
            consecutive_empty += 1;
            if consecutive_empty >= 2 {
                log::log_info(
                    "relay",
                    "relay_worker.auto_exit",
                    "no local instances for 2 checks",
                );
                let _ = cmd_tx.send(RelayCommand::Shutdown);
                return;
            }
        } else {
            consecutive_empty = 0;
        }
    }
}

/// Count local (non-remote) instances.
fn local_instance_count(db: &HcomDb) -> i64 {
    db.conn()
        .query_row(
            "SELECT COUNT(*) FROM instances WHERE COALESCE(origin_device_id, '') = ''",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0)
}

// ── Auto-spawn ──────────────────────────────────────────────────────

/// Spawn relay-worker if relay is enabled, not already running, and instances exist.
/// Safe to call from any context (hooks, send, TUI). No-op if conditions aren't met.
pub fn maybe_auto_spawn() {
    // Load config
    let config = match HcomConfig::load(None) {
        Ok(c) => c,
        Err(_) => return,
    };

    if !super::is_relay_enabled(&config) {
        return;
    }

    // Already running?
    if is_relay_worker_running() {
        return;
    }

    // Any local instances alive?
    let db = match HcomDb::open() {
        Ok(db) => db,
        Err(_) => return,
    };

    let count: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM instances \
             WHERE COALESCE(origin_device_id, '') = '' \
             AND status NOT IN ('stopped', 'dead')",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);

    if count == 0 {
        return;
    }

    // Spawn relay-worker process
    let binary = match std::env::current_exe() {
        Ok(b) => b,
        Err(_) => return,
    };

    let mut cmd = Command::new(&binary);
    cmd.arg("relay-worker")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    // Detach into own session so it survives parent terminal close (no SIGHUP)
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        unsafe {
            cmd.pre_exec(|| {
                libc::setsid();
                Ok(())
            });
        }
    }

    match cmd.spawn() {
        Ok(child) => {
            log::log_info(
                "relay",
                "relay_worker.spawned",
                &format!("pid={}", child.id()),
            );
        }
        Err(e) => {
            log::log_warn(
                "relay",
                "relay_worker.spawn_err",
                &format!("{}", e),
            );
        }
    }
}

/// Stop a running relay-worker by sending SIGTERM to the PID from PID file.
pub fn stop_relay_worker() -> bool {
    if let Some(pid) = read_pid_file() {
        // SAFETY: Sending SIGTERM to a known PID.
        let ret = unsafe { libc::kill(pid as i32, libc::SIGTERM) };
        if ret == 0 {
            log::log_info(
                "relay",
                "relay_worker.stopped",
                &format!("pid={}", pid),
            );
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pid_file_path() {
        crate::config::Config::init();
        let path = pid_file_path();
        assert!(path.to_string_lossy().contains("relay.pid"));
    }

    #[test]
    fn test_is_process_alive_self() {
        assert!(is_process_alive(std::process::id()));
    }

    #[test]
    fn test_is_process_alive_invalid() {
        // PID 0 is the kernel scheduler, shouldn't be accessible
        // PID 99999999 almost certainly doesn't exist
        assert!(!is_process_alive(99999999));
    }
}
