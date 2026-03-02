//! Relay MQTT roundtrip integration test.
//!
//! Two real hcom instances (separate HCOM_DIR), each with their own
//! daemon, talking through a real public MQTT broker.
//! Zero mocking, zero fake payloads.
//!
//! Phases:
//! 1. Device A: hcom relay new → daemon connects to broker
//! 2. Device A: hcom send → event pushed to broker
//! 3. Device B: hcom relay connect <token> → daemon connects, pulls
//! 4. Verify: Device B sees Device A's event in hcom events (namespaced)
//! 5. Verify: Device A sees Device B as remote device in relay status
//! 6. Cleanup: relay off, daemon stop, remove temp dirs
//!
//! Requires:
//! - hcom installed
//! - Network access to public MQTT brokers
//!
//! Run:
//!     cargo test -p hcom --test test_relay_roundtrip -- --ignored --nocapture

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;
use std::time::{Duration, Instant};

// ── Helpers ────────────────────────────────────────────────────────────

fn hcom_with_dir(cmd: &str, hcom_dir: &str) -> Output {
    Command::new("hcom")
        .args(shell_words::split(cmd).unwrap())
        .env("HCOM_DIR", hcom_dir)
        .output()
        .expect("failed to execute hcom")
}

fn check(label: &str, cmd: &str, hcom_dir: &str) -> String {
    let out = hcom_with_dir(cmd, hcom_dir);
    assert!(
        out.status.success(),
        "Device {label}: hcom {cmd}\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    String::from_utf8_lossy(&out.stdout).to_string()
}

fn poll_until<T>(
    mut f: impl FnMut() -> Option<T>,
    description: &str,
    timeout: Duration,
    interval: Duration,
) -> T {
    let start = Instant::now();
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert!(
            start.elapsed() < timeout,
            "Timeout ({timeout:?}): {description}"
        );
        thread::sleep(interval);
    }
}

fn parse_token(output: &str) -> Option<String> {
    for line in output.lines() {
        if let Some(rest) = line.strip_prefix("  hcom relay connect ") {
            return Some(rest.trim().to_string());
        }
        if line.contains("hcom relay connect ") {
            if let Some(pos) = line.find("hcom relay connect ") {
                let after = &line[pos + "hcom relay connect ".len()..];
                return Some(after.trim().to_string());
            }
        }
    }
    None
}

fn parse_device_id(status_output: &str) -> Option<String> {
    for line in status_output.lines() {
        if let Some(rest) = line.strip_prefix("  Device ID: ") {
            return Some(rest.trim().to_string());
        }
        if line.contains("Device ID:") {
            if let Some(pos) = line.find("Device ID:") {
                let after = &line[pos + "Device ID:".len()..];
                return Some(after.trim().to_string());
            }
        }
    }
    None
}

fn read_device_uuid(hcom_dir: &str) -> Option<String> {
    let path = Path::new(hcom_dir).join(".tmp/device_id");
    fs::read_to_string(path).ok().and_then(|s| {
        let trimmed = s.trim().to_string();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    })
}

fn kill_daemon(hcom_dir: &str) {
    let pid_path = Path::new(hcom_dir).join("hcomd.pid");
    if let Ok(content) = fs::read_to_string(&pid_path) {
        if let Ok(pid) = content.trim().parse::<i32>() {
            unsafe {
                libc::kill(pid, libc::SIGTERM);
            }
            // Wait up to 3s
            for _ in 0..30 {
                thread::sleep(Duration::from_millis(100));
                if unsafe { libc::kill(pid, 0) } != 0 {
                    return;
                }
            }
            // Still alive — SIGKILL
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
        }
    }
}

// ── Cleanup guard ──────────────────────────────────────────────────────

struct RelayGuard {
    dir_a: Option<PathBuf>,
    dir_b: Option<PathBuf>,
}

impl Drop for RelayGuard {
    fn drop(&mut self) {
        for dir in [&self.dir_a, &self.dir_b] {
            if let Some(d) = dir {
                let d_str = d.to_string_lossy();
                let _ = hcom_with_dir("relay off", &d_str);
                let _ = hcom_with_dir("daemon stop", &d_str);
                kill_daemon(&d_str);
                let _ = fs::remove_dir_all(d);
            }
        }
    }
}

// ── Main test ──────────────────────────────────────────────────────────

#[test]
#[ignore]
fn test_relay_roundtrip() {
    let dir_a = tempfile::tempdir().expect("failed to create temp dir A");
    let dir_b = tempfile::tempdir().expect("failed to create temp dir B");

    // Prevent tempfile from auto-deleting — we manage cleanup via guard
    let dir_a_path = dir_a.keep();
    let dir_b_path = dir_b.keep();

    let _guard = RelayGuard {
        dir_a: Some(dir_a_path.clone()),
        dir_b: Some(dir_b_path.clone()),
    };

    let path_a = dir_a_path.to_string_lossy().to_string();
    let path_b = dir_b_path.to_string_lossy().to_string();

    eprintln!("{}", "=".repeat(60));
    eprintln!("Relay Roundtrip: two real hcom instances via MQTT");
    eprintln!("{}", "=".repeat(60));
    eprintln!("\n  Device A: {path_a}");
    eprintln!("  Device B: {path_b}");

    // ── Phase 1: Device A creates relay group ────────────────────
    eprintln!("\n[Phase 1] Device A: relay new...");

    let output = check("A", "relay new", &path_a);
    eprint!("{}", output.trim_end());
    eprintln!();

    let token = parse_token(&output).expect("Could not parse token from relay new output");
    eprintln!("  OK: Token: {}...", &token[..token.len().min(24)]);

    // Start daemon so relay actually connects to broker
    check("A", "daemon start", &path_a);
    eprintln!("  OK: Device A daemon started");

    // Wait for connected
    poll_until(
        || {
            let out = hcom_with_dir("relay status", &path_a);
            if out.status.success() {
                let stdout = String::from_utf8_lossy(&out.stdout).to_lowercase();
                if stdout.contains("connected") {
                    return Some(());
                }
            }
            None
        },
        "Device A relay connected",
        Duration::from_secs(20),
        Duration::from_secs(1),
    );
    eprintln!("  OK: Device A connected to broker");

    let status_a = String::from_utf8_lossy(
        &hcom_with_dir("relay status", &path_a).stdout,
    )
    .to_string();
    let short_a = parse_device_id(&status_a).expect("Could not parse Device A short ID");
    eprintln!("  OK: Device A short ID: {short_a}");

    // ── Phase 2: Device A sends test message ─────────────────────
    eprintln!("\n[Phase 2] Device A: sending test message...");

    let marker = format!("relay-rt-{}", &uuid::Uuid::new_v4().to_string()[..8]);
    check("A", &format!("send --from relaytest -- \"{marker}\""), &path_a);
    eprintln!("  OK: Sent: {marker}");

    poll_until(
        || {
            let out = hcom_with_dir("relay status", &path_a);
            if out.status.success() {
                let stdout = String::from_utf8_lossy(&out.stdout);
                let lower = stdout.to_lowercase();
                if lower.contains("up to date") {
                    return Some(());
                }
                eprintln!("    relay status: {}", stdout.lines().find(|l| l.to_lowercase().contains("queue")).unwrap_or("(no queue line)"));
            }
            None
        },
        "Device A push queue drained",
        Duration::from_secs(30),
        Duration::from_secs(2),
    );
    eprintln!("  OK: Device A: pushed to broker");

    // ── Phase 3: Device B joins ──────────────────────────────────
    eprintln!("\n[Phase 3] Device B: relay connect...");

    let output = check("B", &format!("relay connect {token}"), &path_b);
    eprint!("{}", output.trim_end());
    eprintln!();

    // Start daemon so relay actually connects to broker
    check("B", "daemon start", &path_b);
    eprintln!("  OK: Device B daemon started");

    poll_until(
        || {
            let out = hcom_with_dir("relay status", &path_b);
            if out.status.success() {
                let stdout = String::from_utf8_lossy(&out.stdout).to_lowercase();
                if stdout.contains("connected") {
                    return Some(());
                }
            }
            None
        },
        "Device B relay connected",
        Duration::from_secs(20),
        Duration::from_secs(1),
    );
    eprintln!("  OK: Device B connected to broker");

    // ── Phase 4: Device B sees relayed event ─────────────────────
    eprintln!("\n[Phase 4] Device B: checking for relayed event...");

    let (ev, data) = poll_until(
        || {
            let out = hcom_with_dir("events --last 50", &path_b);
            if !out.status.success() {
                eprintln!("    events cmd failed: {}", String::from_utf8_lossy(&out.stderr));
                return None;
            }
            let stdout = String::from_utf8_lossy(&out.stdout);
            let line_count = stdout.lines().count();
            if line_count > 0 {
                eprintln!("    Device B has {line_count} events");
            }
            for line in stdout.lines() {
                let line = line.trim();
                if let Ok(ev) = serde_json::from_str::<serde_json::Value>(line) {
                    let mut data = ev["data"].clone();
                    // data may be string-encoded JSON
                    if let Some(s) = data.as_str() {
                        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(s) {
                            data = parsed;
                        }
                    }
                    if let Some(text) = data["text"].as_str() {
                        if text.contains(&marker) {
                            return Some((ev, data));
                        }
                    }
                }
            }
            None
        },
        &format!("Device B sees '{marker}'"),
        Duration::from_secs(30),
        Duration::from_secs(2),
    );
    eprintln!("  OK: Event received: type={}", ev["type"]);

    // Verify sender namespaced with Device A's short ID
    let expected_from = format!("relaytest:{short_a}");
    let actual_from = data["from"].as_str().unwrap_or("");
    assert_eq!(
        actual_from, expected_from,
        "from={actual_from}, expected {expected_from}"
    );
    eprintln!("  OK: from namespaced: {actual_from}");

    // Verify _relay marker points back to Device A
    let actual_uuid_a = read_device_uuid(&path_a).expect("Could not read Device A UUID");
    let relay_marker = &data["_relay"];
    assert_eq!(
        relay_marker["device"].as_str().unwrap_or(""),
        actual_uuid_a,
        "_relay.device={}, expected {actual_uuid_a}",
        relay_marker["device"]
    );
    eprintln!(
        "  OK: _relay.device = Device A ({}...)",
        &actual_uuid_a[..actual_uuid_a.len().min(8)]
    );

    assert_eq!(
        relay_marker["short"].as_str().unwrap_or(""),
        short_a,
        "_relay.short={}, expected {short_a}",
        relay_marker["short"]
    );
    eprintln!("  OK: _relay.short = {short_a}");

    // ── Phase 5: Device A sees Device B as remote ────────────────
    eprintln!("\n[Phase 5] Device A: checking for Device B as remote...");

    let status_b = String::from_utf8_lossy(
        &hcom_with_dir("relay status", &path_b).stdout,
    )
    .to_string();
    let short_b = parse_device_id(&status_b).expect("Could not parse Device B short ID");
    eprintln!("  OK: Device B short ID: {short_b}");

    let remote_line: String = poll_until(
        || {
            let out = hcom_with_dir("relay status", &path_a);
            if !out.status.success() {
                return None;
            }
            let stdout = String::from_utf8_lossy(&out.stdout);
            for line in stdout.lines() {
                if line.contains("Remote devices:") && line.contains(&short_b) {
                    return Some(line.trim().to_string());
                }
            }
            // Debug: show what relay status says about remote devices
            let remote_line = stdout.lines().find(|l| l.contains("Remote") || l.contains("other devices"));
            eprintln!("    relay status: {}", remote_line.unwrap_or("(no remote line)"));
            None
        },
        &format!("Device A sees {short_b} in remote devices"),
        Duration::from_secs(30),
        Duration::from_secs(2),
    );
    eprintln!("  OK: {remote_line}");

    // Cleanup handled by guard Drop
    eprintln!("\n{}", "=".repeat(60));
    eprintln!("ALL PHASES PASSED");
    eprintln!("{}", "=".repeat(60));
}
