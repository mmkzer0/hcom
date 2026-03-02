//! Relay daemon process management.
//!
//! Accessed via `hcom relay daemon [start|stop|restart|status]`.
//! Manages the `hcom relay-worker` background process for MQTT relay.

use std::fs;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use crate::paths::hcom_dir;

fn pid_path() -> PathBuf {
    hcom_dir().join(".tmp").join("relay.pid")
}

fn read_pid() -> Option<u32> {
    fs::read_to_string(pid_path())
        .ok()
        .and_then(|s| s.trim().parse().ok())
}

fn is_alive(pid: u32) -> bool {
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

pub(crate) fn daemon_status() -> i32 {
    let pid = match read_pid() {
        Some(p) => p,
        None => {
            println!("Daemon not running");
            return 0;
        }
    };

    if !is_alive(pid) {
        println!("Daemon: stale PID file (process not running)");
        return 0;
    }

    println!("Daemon: running (PID {pid})");
    0
}

pub(crate) fn daemon_start() -> i32 {
    let pp = pid_path();

    // Check if already running
    if let Some(pid) = read_pid() {
        if is_alive(pid) {
            println!("Daemon already running (PID {pid})");
            return 0;
        }
        let _ = fs::remove_file(&pp);
    }

    // Ensure .tmp dir exists for relay.pid
    let _ = fs::create_dir_all(hcom_dir().join(".tmp"));

    // Find our own binary path
    let hcom_bin = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("hcom"));

    // Start relay-worker as background process
    let result = std::process::Command::new(&hcom_bin)
        .arg("relay-worker")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();

    match result {
        Ok(_child) => {
            // Wait for relay worker to write its own PID file
            for _ in 0..20 {
                thread::sleep(Duration::from_millis(100));
                if let Some(pid) = read_pid() {
                    println!("Daemon started (PID {pid})");
                    return 0;
                }
            }
            println!("Daemon started (waiting for relay worker)");
            0
        }
        Err(e) => {
            eprintln!("Failed to start daemon: {e}");
            1
        }
    }
}

pub(crate) fn daemon_stop() -> i32 {
    let pp = pid_path();

    let pid = match read_pid() {
        Some(p) => p,
        None => {
            println!("Daemon not running");
            return 0;
        }
    };

    if !is_alive(pid) {
        println!("Daemon not running (stale PID file)");
        let _ = fs::remove_file(&pp);
        return 0;
    }

    unsafe { libc::kill(pid as i32, libc::SIGTERM); }
    println!("Sent SIGTERM to daemon (PID {pid})");

    for _ in 0..50 {
        thread::sleep(Duration::from_millis(100));
        if !is_alive(pid) {
            println!("Daemon stopped");
            let _ = fs::remove_file(&pp);
            return 0;
        }
    }

    println!("Daemon did not respond to SIGTERM, escalating to SIGKILL");
    unsafe { libc::kill(pid as i32, libc::SIGKILL); }
    println!("Daemon killed (SIGKILL)");
    let _ = fs::remove_file(&pp);
    0
}

pub fn cmd_daemon(argv: &[String]) -> i32 {
    let subcmd = argv.first().map(|s| s.as_str()).unwrap_or("status");

    match subcmd {
        "status" => daemon_status(),
        "start" => daemon_start(),
        "stop" => daemon_stop(),
        "restart" => {
            daemon_stop();
            thread::sleep(Duration::from_millis(500));
            daemon_start()
        }
        other => {
            eprintln!("Unknown daemon subcommand: {other}");
            eprintln!("Usage: hcom relay daemon [status|start|stop|restart]");
            1
        }
    }
}
