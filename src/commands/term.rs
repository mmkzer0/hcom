//! `hcom term` command — terminal admin: screen queries, text injection, debug logging.
//!
//!
//! Talks to PTY instances via their TCP inject ports.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::time::Duration;

use crate::db::HcomDb;

/// Parsed arguments for `hcom term`.
#[derive(clap::Parser, Debug)]
#[command(name = "term", about = "Terminal admin: screen query, injection, debug")]
pub struct TermArgs {
    /// Subcommand and arguments
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub args: Vec<String>,
}
use crate::instances::resolve_display_name;
use crate::paths::hcom_dir;
use crate::shared::CommandContext;

/// PTY debug flag file path.
fn flag_path() -> PathBuf {
    hcom_dir().join(".tmp").join("pty_debug_on")
}

/// Look up inject port for an instance from notify_endpoints table.
fn get_inject_port(db: &HcomDb, instance_name: &str) -> Option<i32> {
    db.conn()
        .query_row(
            "SELECT port FROM notify_endpoints WHERE instance = ?1 AND kind = 'inject'",
            rusqlite::params![instance_name],
            |row| row.get(0),
        )
        .ok()
}

/// Get all instances that have inject ports registered.
fn get_pty_instances(db: &HcomDb) -> Vec<(String, i32)> {
    let mut stmt = match db.conn().prepare(
        "SELECT instance, port FROM notify_endpoints WHERE kind = 'inject'",
    ) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };

    stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i32>(1)?))
    })
    .ok()
    .map(|rows| rows.filter_map(|r| r.ok()).collect())
    .unwrap_or_default()
}

/// Send data on a single TCP connection.
fn inject_raw(port: i32, data: &[u8]) -> Result<(), String> {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .map_err(|e| format!("connect: {e}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .ok();
    stream.write_all(data).map_err(|e| format!("write: {e}"))?;
    Ok(())
}

/// Inject text into PTY via inject port.
fn inject_text(db: &HcomDb, name: &str, text: &str, enter: bool) -> i32 {
    let port = match get_inject_port(db, name) {
        Some(p) => p,
        None => {
            println!("No inject port for '{name}'.");
            return 1;
        }
    };

    if !text.is_empty() {
        if let Err(e) = inject_raw(port, text.as_bytes()) {
            println!("Failed to inject to '{name}' (port {port}): {e}");
            return 1;
        }
    }

    if enter {
        if !text.is_empty() {
            std::thread::sleep(Duration::from_millis(100));
        }
        if let Err(e) = inject_raw(port, b"\r") {
            println!("Failed to inject enter to '{name}' (port {port}): {e}");
            return 1;
        }
    }

    let label = match (text.is_empty(), enter) {
        (false, true) => format!("{} chars + enter", text.len()),
        (false, false) => format!("{} chars", text.len()),
        (true, _) => "enter".to_string(),
    };
    println!("Injected {label} to {name}");
    0
}

/// Send screen query to inject port, get back parsed JSON.
fn query_screen(port: i32) -> Option<serde_json::Value> {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(2))).ok();
    stream.write_all(b"\x00SCREEN\n").ok()?;
    stream.shutdown(std::net::Shutdown::Write).ok()?;

    let mut data = Vec::new();
    stream.read_to_end(&mut data).ok()?;
    if data.is_empty() {
        return None;
    }
    serde_json::from_slice(&data).ok()
}

/// Format screen JSON as readable text.
fn format_screen(data: &serde_json::Value) -> String {
    let lines = data["lines"].as_array();
    let cursor = data["cursor"].as_array();
    let size = data["size"].as_array();

    let (rows, cols) = size
        .map(|s| {
            (
                s.first().and_then(|v| v.as_i64()).unwrap_or(0),
                s.get(1).and_then(|v| v.as_i64()).unwrap_or(0),
            )
        })
        .unwrap_or((0, 0));

    let (cr, cc) = cursor
        .map(|c| {
            (
                c.first().and_then(|v| v.as_i64()).unwrap_or(0),
                c.get(1).and_then(|v| v.as_i64()).unwrap_or(0),
            )
        })
        .unwrap_or((0, 0));

    let ready = data.get("ready");
    let prompt_empty = data.get("prompt_empty");
    let input_text = data.get("input_text");

    let mut out = Vec::new();
    out.push(format!("Screen {rows}x{cols}  cursor ({cr},{cc})"));
    out.push(format!(
        "ready={ready}  prompt_empty={prompt_empty}  input_text={input_text}",
        ready = ready.map(|v| v.to_string()).unwrap_or("null".into()),
        prompt_empty = prompt_empty.map(|v| v.to_string()).unwrap_or("null".into()),
        input_text = input_text
            .map(|v| match v.as_str() {
                Some(s) => format!("\"{}\"", s),
                None => v.to_string(),
            })
            .unwrap_or("null".into()),
    ));
    out.push(String::new());

    if let Some(lines) = lines {
        for (i, line) in lines.iter().enumerate() {
            let text = line.as_str().unwrap_or("");
            if !text.is_empty() {
                out.push(format!("  {i:3}: {text}"));
            }
        }
    }

    out.join("\n")
}

/// Handle: hcom term debug on|off|logs
fn handle_debug(argv: &[String]) -> i32 {
    let sub = argv.first().map(|s| s.as_str());

    match sub {
        Some("on") => {
            let path = flag_path();
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let _ = std::fs::File::create(&path);
            println!("PTY debug logging enabled. Running instances pick up within ~10s.");
            0
        }
        Some("off") => {
            let _ = std::fs::remove_file(flag_path());
            println!("PTY debug logging disabled.");
            0
        }
        Some("logs") => list_logs(),
        _ => {
            let status = if flag_path().exists() { "on" } else { "off" };
            println!("PTY debug logging is {status}. Usage: hcom term debug on|off|logs");
            0
        }
    }
}

/// List PTY debug log files.
fn list_logs() -> i32 {
    let debug_dir = hcom_dir().join(".tmp").join("logs").join("pty_debug");
    if !debug_dir.exists() {
        println!("No PTY debug logs found.");
        return 0;
    }

    let mut logs: Vec<(PathBuf, u64)> = std::fs::read_dir(&debug_dir)
        .ok()
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter(|e| {
                    e.path()
                        .extension()
                        .and_then(|ext| ext.to_str())
                        == Some("log")
                })
                .filter_map(|e| {
                    let size = e.metadata().ok()?.len();
                    Some((e.path(), size))
                })
                .collect()
        })
        .unwrap_or_default();

    if logs.is_empty() {
        println!("No PTY debug logs found.");
        return 0;
    }

    // Sort by modification time, newest first
    logs.sort_by(|a, b| {
        let a_time = std::fs::metadata(&a.0)
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        let b_time = std::fs::metadata(&b.0)
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        b_time.cmp(&a_time)
    });

    let enabled = flag_path().exists();
    println!("Debug logging: {}", if enabled { "ON" } else { "OFF" });
    println!("Log dir: {}", debug_dir.display());
    for (path, size) in &logs {
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("?");
        println!("  {name}  ({size} bytes)");
    }
    0
}

/// Handle screen query: hcom term [name] [--json]
fn handle_screen(db: &HcomDb, argv: &[String]) -> i32 {
    let raw_json = argv.iter().any(|a| a == "--json");
    let args: Vec<&str> = argv
        .iter()
        .filter(|a| a.as_str() != "--json")
        .map(|s| s.as_str())
        .collect();
    let name = args.first().copied();

    // Resolve display name if provided
    let name = name.map(|n| resolve_display_name(db, n).unwrap_or_else(|| n.to_string()));

    if let Some(ref name) = name {
        let port = match get_inject_port(db, name) {
            Some(p) => p,
            None => {
                println!("No inject port for '{name}'. Instance not running or not PTY-managed.");
                return 1;
            }
        };
        match query_screen(port) {
            Some(result) => {
                if raw_json {
                    println!("{}", serde_json::to_string(&result).unwrap_or_default());
                } else {
                    println!("{}", format_screen(&result));
                }
                0
            }
            None => {
                println!("No response from '{name}' (port {port}).");
                1
            }
        }
    } else {
        // No name — query all PTY instances
        let instances = get_pty_instances(db);
        if instances.is_empty() {
            println!("No PTY instances found.");
            return 1;
        }

        let mut found = false;
        for (inst_name, port) in &instances {
            if let Some(result) = query_screen(*port) {
                if found {
                    println!();
                }
                if raw_json {
                    let mut merged = result.clone();
                    merged["name"] = serde_json::json!(inst_name);
                    println!("{}", serde_json::to_string(&merged).unwrap_or_default());
                } else {
                    println!("[{inst_name}]");
                    println!("{}", format_screen(&result));
                }
                found = true;
            } else {
                println!("[{inst_name}] not responding (port {port})");
            }
        }

        if found { 0 } else { 1 }
    }
}

pub fn cmd_term(db: &HcomDb, args: &TermArgs, _ctx: Option<&CommandContext>) -> i32 {
    let argv = &args.args;
    let sub = argv.first().map(|s| s.as_str());

    if sub == Some("--help") || sub == Some("-h") {
        println!(
            "hcom term - Terminal admin: screen query, text injection, debug logging\n\n\
             Usage:\n  \
             hcom term                  Query all PTY screens\n  \
             hcom term <name>           Query specific instance screen\n  \
             hcom term <name> --json    JSON output\n  \
             hcom term inject <name> [text] [--enter]   Inject text/enter\n  \
             hcom term debug on|off|logs                 PTY debug logging"
        );
        return 0;
    }

    if sub == Some("inject") {
        let enter = argv.iter().any(|a| a == "--enter");
        let args: Vec<&str> = argv[1..]
            .iter()
            .filter(|a| a.as_str() != "--enter")
            .map(|s| s.as_str())
            .collect();
        if args.is_empty() {
            println!("Usage: hcom term inject <name> [text] [--enter]");
            return 1;
        }
        let name = resolve_display_name(db, args[0]).unwrap_or_else(|| args[0].to_string());
        let text = if args.len() > 1 {
            args[1..].join(" ")
        } else {
            String::new()
        };
        if text.is_empty() && !enter {
            println!("Nothing to inject (provide text or --enter)");
            return 1;
        }
        return inject_text(db, &name, &text, enter);
    }

    if sub == Some("debug") {
        return handle_debug(&argv[1..]);
    }

    // Screen query
    handle_screen(db, argv)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_screen() {
        let data = serde_json::json!({
            "lines": ["hello", "", "world"],
            "cursor": [2, 5],
            "size": [24, 80],
            "ready": true,
            "prompt_empty": false,
            "input_text": "test",
        });
        let result = format_screen(&data);
        assert!(result.contains("Screen 24x80"));
        assert!(result.contains("cursor (2,5)"));
        assert!(result.contains("hello"));
        assert!(result.contains("world"));
    }

    #[test]
    fn test_flag_path() {
        // Just verify it returns something sensible
        let path = flag_path();
        assert!(path.to_string_lossy().contains("pty_debug_on"));
    }
}
