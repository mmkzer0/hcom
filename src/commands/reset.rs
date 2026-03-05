//! `hcom reset` command — archive and clear conversation, optionally hooks/config.
//!
//!
//! Modes:
//!   hcom reset              Clear database (archive conversation)
//!   hcom reset hooks        Remove hooks
//!   hcom reset all          Stop all + clear db + remove hooks + reset config

use std::fs;

use crate::db::HcomDb;
use crate::paths::{hcom_dir, ARCHIVE_DIR, FLAGS_DIR, LOGS_DIR, LAUNCH_DIR};
use crate::shared::{CommandContext, is_inside_ai_tool, shorten_path};

/// Get timestamp for archive directory names.
fn get_archive_timestamp() -> String {
    chrono::Local::now().format("%Y-%m-%d_%H%M%S").to_string()
}

/// Archive the current database to ~/.hcom/archive/session-{timestamp}/.
fn archive_and_clear_db() -> Result<Option<String>, String> {
    let base = hcom_dir();
    let db_file = base.join("hcom.db");
    let db_wal = base.join("hcom.db-wal");
    let db_shm = base.join("hcom.db-shm");

    if !db_file.exists() {
        return Ok(None);
    }

    // Check if DB has content worth archiving
    let has_content = {
        let conn = rusqlite::Connection::open(&db_file).map_err(|e| e.to_string())?;
        let event_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM events", [], |r| r.get(0))
            .unwrap_or(0);
        let instance_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM instances", [], |r| r.get(0))
            .unwrap_or(0);
        event_count > 0 || instance_count > 0
    };

    if !has_content {
        // Empty DB — just delete
        let _ = fs::remove_file(&db_file);
        let _ = fs::remove_file(&db_wal);
        let _ = fs::remove_file(&db_shm);
        return Ok(None);
    }

    // PASSIVE checkpoint: flush WAL into main DB
    if let Ok(conn) = rusqlite::Connection::open(&db_file) {
        let _ = conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE)");
    }

    let timestamp = get_archive_timestamp();
    let session_archive = base.join(ARCHIVE_DIR).join(format!("session-{timestamp}"));
    fs::create_dir_all(&session_archive).map_err(|e| e.to_string())?;

    // Copy DB files to archive
    fs::copy(&db_file, session_archive.join("hcom.db")).map_err(|e| e.to_string())?;
    if db_wal.exists() {
        let _ = fs::copy(&db_wal, session_archive.join("hcom.db-wal"));
    }
    if db_shm.exists() {
        let _ = fs::copy(&db_shm, session_archive.join("hcom.db-shm"));
    }

    // Delete main DB files
    let _ = fs::remove_file(&db_file);
    let _ = fs::remove_file(&db_wal);
    let _ = fs::remove_file(&db_shm);

    Ok(Some(session_archive.to_string_lossy().to_string()))
}

/// Clean temp files (launch scripts, prompts, old logs).
fn clean_temp_files() {
    let base = hcom_dir();
    let cutoff_24h = crate::shared::constants::now_epoch_f64() - 86400.0;
    let cutoff_30d = crate::shared::constants::now_epoch_f64() - 30.0 * 86400.0;

    // Clean launch scripts >24h
    let launch_dir = base.join(LAUNCH_DIR);
    if launch_dir.exists() {
        if let Ok(rd) = fs::read_dir(&launch_dir) {
            for entry in rd.filter_map(|e| e.ok()) {
                if entry.path().is_file() {
                    if let Ok(meta) = entry.metadata() {
                        if let Ok(mtime) = meta.modified() {
                            let secs = crate::shared::system_time_to_epoch_f64(mtime);
                            if secs < cutoff_24h {
                                let _ = fs::remove_file(entry.path());
                            }
                        }
                    }
                }
            }
        }
    }

    // Clean prompt temp files >24h
    let prompts_dir = base.join(".tmp").join("prompts");
    if prompts_dir.exists() {
        if let Ok(rd) = fs::read_dir(&prompts_dir) {
            for entry in rd.filter_map(|e| e.ok()) {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) == Some("md") {
                    if let Ok(meta) = entry.metadata() {
                        if let Ok(mtime) = meta.modified() {
                            let secs = crate::shared::system_time_to_epoch_f64(mtime);
                            if secs < cutoff_24h {
                                let _ = fs::remove_file(path);
                            }
                        }
                    }
                }
            }
        }
    }

    // Clean background logs >30d
    let logs_dir = base.join(LOGS_DIR);
    if logs_dir.exists() {
        if let Ok(rd) = fs::read_dir(&logs_dir) {
            for entry in rd.filter_map(|e| e.ok()) {
                let path = entry.path();
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name.starts_with("background_") && name.ends_with(".log") {
                    if let Ok(meta) = entry.metadata() {
                        if let Ok(mtime) = meta.modified() {
                            let secs = crate::shared::system_time_to_epoch_f64(mtime);
                            if secs < cutoff_30d {
                                let _ = fs::remove_file(path);
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Archive and reset config files.
pub fn reset_config() -> i32 {
    let base = hcom_dir();
    let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S").to_string();
    let archive_config_dir = base.join(ARCHIVE_DIR).join("config");

    let mut archived = false;

    let toml_path = base.join("config.toml");
    if toml_path.exists() {
        let _ = fs::create_dir_all(&archive_config_dir);
        if fs::copy(&toml_path, archive_config_dir.join(format!("config.toml.{timestamp}"))).is_ok()
        {
            let _ = fs::remove_file(&toml_path);
            println!("Config archived to archive/config/config.toml.{timestamp}");
            archived = true;
        }
    }

    let env_path = base.join("config.env");
    if env_path.exists() {
        let _ = fs::create_dir_all(&archive_config_dir);
        if fs::copy(&env_path, archive_config_dir.join(format!("env.{timestamp}"))).is_ok() {
            let _ = fs::remove_file(&env_path);
            if !archived {
                println!("Env archived to archive/config/env.{timestamp}");
            }
        }
    }

    if !archived {
        println!("No config file to reset");
    }
    0
}

/// Print reset preview for AI tools (shows what will be destroyed).
fn print_reset_preview(target: Option<&str>, db: &HcomDb) {
    let hcom_cmd = "hcom";

    let (instance_count, event_count, local_instances) = {
        let ec: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM events", [], |r| r.get(0))
            .unwrap_or(0);
        let mut names = Vec::new();
        if let Ok(mut stmt) = db.conn().prepare(
            "SELECT name FROM instances WHERE origin_device_id IS NULL OR origin_device_id = ''",
        ) {
            if let Ok(rows) = stmt.query_map([], |r| r.get::<_, String>(0)) {
                for name in rows.filter_map(|r| r.ok()) {
                    names.push(name);
                }
            }
        }
        let ic = names.len();
        (ic, ec, names)
    };

    let names_display = if local_instances.is_empty() {
        "(none)".to_string()
    } else {
        let shown: Vec<&str> = local_instances.iter().take(5).map(|s| s.as_str()).collect();
        let suffix = if local_instances.len() > 5 { " ..." } else { "" };
        format!("{}{suffix}", shown.join(", "))
    };
    let plural = if instance_count != 1 { "s" } else { "" };

    match target {
        Some("hooks") => {
            println!(
                "\n== RESET HOOKS PREVIEW ==\n\
                 This will remove hcom hooks from tool configs.\n\n\
                 Actions:\n  \
                 \u{2022} Remove hooks from Claude Code settings (~/.claude/settings.json)\n  \
                 \u{2022} Remove hooks from Gemini CLI settings (~/.gemini/settings.json)\n  \
                 \u{2022} Remove hooks from Codex config (~/.codex/)\n\n\
                 To reinstall: hcom hooks add\n\n\
                 Add --go flag and run again to proceed:\n  \
                 {hcom_cmd} --go reset hooks\n"
            );
        }
        Some("all") => {
            println!(
                "\n== RESET ALL PREVIEW ==\n\
                 This will stop all instances, archive the database, remove hooks, and reset config.\n\n\
                 Current state:\n  \
                 \u{2022} {instance_count} local instance{plural}: {names_display}\n  \
                 \u{2022} {event_count} events in database\n\n\
                 Actions:\n  \
                 1. Stop all {instance_count} local instances (kills processes, logs snapshots)\n  \
                 2. Archive database to ~/.hcom/archive/session-<timestamp>/\n  \
                 3. Delete database (hcom.db)\n  \
                 4. Remove hooks from Claude/Gemini/Codex configs\n  \
                 5. Archive and delete config.toml + env\n  \
                 6. Clear device identity (new UUID on next relay)\n\n\
                 Add --go flag and run again to proceed:\n  \
                 {hcom_cmd} --go reset all\n"
            );
        }
        _ => {
            println!(
                "\n== RESET PREVIEW ==\n\
                 This will archive and clear the current hcom session.\n\n\
                 Current state:\n  \
                 \u{2022} {instance_count} instance{plural}: {names_display}\n  \
                 \u{2022} {event_count} events in database\n\n\
                 Actions:\n  \
                 1. Archive database to ~/.hcom/archive/session-<timestamp>/\n  \
                 2. Delete database (hcom.db, hcom.db-wal, hcom.db-shm)\n  \
                 3. Log reset event to fresh database\n  \
                 4. Sync with relay (push reset, pull fresh state)\n\n\
                 Note: Instance rows are deleted but snapshots preserved in archive.\n      \
                 Query archived sessions with: {hcom_cmd} archive\n\n\
                 Add --go flag and run again to proceed:\n  \
                 {hcom_cmd} --go reset\n"
            );
        }
    }
}

pub fn cmd_reset(db: &HcomDb, argv: &[String], ctx: Option<&CommandContext>) -> i32 {
    // Handle --help
    if argv.iter().any(|a| a == "--help" || a == "-h") {
        println!(
            "hcom reset - Reset hcom components\n\n\
             Usage:\n  \
             hcom reset              Clear database (archive conversation)\n  \
             hcom reset hooks        Remove hooks\n  \
             hcom reset all          Stop all + clear db + remove hooks + reset config\n\n\
             Note: Hooks are auto-installed on any hcom command. Use 'reset hooks' to remove."
        );
        return 0;
    }

    // Parse subcommand — flags after target are errors, not silently skipped
    let mut target: Option<&str> = None;
    for arg in argv {
        if target.is_none() && !arg.starts_with("-") {
            target = Some(arg.as_str());
        } else {
            eprintln!("Unknown argument: {arg}\n");
            eprintln!(
                "hcom reset - Reset hcom components\n\n\
                 Usage:\n  \
                 hcom reset              Clear database (archive conversation)\n  \
                 hcom reset hooks        Remove hooks\n  \
                 hcom reset all          Stop all + clear db + remove hooks + reset config"
            );
            return 1;
        }
    }

    // Validate
    if let Some(t) = target {
        if t != "hooks" && t != "all" {
            eprintln!("Error: Unknown target: {t}");
            return 1;
        }
    }

    // Confirmation gate: inside AI tools, require --go
    if is_inside_ai_tool() && !ctx.map(|c| c.go).unwrap_or(false) {
        print_reset_preview(target, db);
        return 0;
    }

    let mut exit_codes = Vec::new();

    // hooks: remove hooks from all locations
    if target == Some("hooks") {
        return super::hooks::cmd_hooks_remove(&["all"]);
    }

    // Stop all instances before clearing database
    let stop_args = crate::commands::stop::StopArgs { targets: vec!["all".into()] };
    exit_codes.push(crate::commands::stop::cmd_stop(db, &stop_args, ctx));

    // Stop relay daemon if running before clear
    let _ = crate::commands::daemon::daemon_stop();

    // Clean temp files
    clean_temp_files();

    // Archive and clear database
    match archive_and_clear_db() {
        Ok(Some(path)) => {
            println!("Archived to {}/", shorten_path(&path));
            println!("Started fresh HCOM conversation");
        }
        Ok(None) => {
            // DB didn't exist
            println!("No HCOM conversation to clear");
        }
        Err(e) => {
            eprintln!("Error: Failed to archive: {e}");
            exit_codes.push(1);
        }
    }

    // For reset all: clear pidtrack before recovery can trigger
    if target == Some("all") {
        let pidtrack = hcom_dir().join(".tmp").join("launched_pids.json");
        let _ = fs::remove_file(pidtrack);
    }

    // Log reset event to fresh DB
    if let Ok(fresh_db) = HcomDb::open() {
        let _ = fresh_db.init_db();
        let _ = fresh_db.log_reset_event();
    }

    // Push reset event to relay so remote devices see it immediately
    crate::relay::trigger_push();

    // all: also remove hooks, reset config, clear device identity
    if target == Some("all") {
        // Clear device identity
        let device_id_file = hcom_dir().join(".tmp").join("device_id");
        let _ = fs::remove_file(&device_id_file);

        // Clear instance counter
        let instance_count_file = hcom_dir().join(FLAGS_DIR).join("instance_count");
        let _ = fs::remove_file(&instance_count_file);

        // Remove hooks
        if super::hooks::cmd_hooks_remove(&["all"]) != 0 {
            exit_codes.push(1);
        }

        // Reset config
        exit_codes.push(reset_config());
    }

    exit_codes.into_iter().max().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_archive_timestamp_format() {
        let ts = get_archive_timestamp();
        // Should be in format YYYY-MM-DD_HHMMSS
        assert!(ts.len() >= 15);
        assert!(ts.contains('-'));
        assert!(ts.contains('_'));
    }

    #[test]
    fn test_shorten_path() {
        assert_eq!(shorten_path("/usr/local"), "/usr/local");
    }
}
