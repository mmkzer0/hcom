//! Kill command: `hcom kill <name(s)|all|tag:X>`
//!
//!
//! Sends SIGTERM to process groups and optionally closes terminal panes.

use std::collections::HashSet;

use anyhow::{Result, bail};

use crate::db::HcomDb;
use crate::hooks::common::stop_instance;
use crate::identity;
use crate::instances;
use crate::log::log_info;
use crate::paths;
use crate::pidtrack;
use crate::router::GlobalFlags;
use crate::terminal;

/// Parsed arguments for `hcom kill`.
#[derive(clap::Parser, Debug)]
#[command(name = "kill", about = "Kill agent processes")]
pub struct KillArgs {
    /// Targets to kill (names, "all", or "tag:X")
    pub targets: Vec<String>,
}

/// Resolve who initiated the kill
fn resolve_initiator(db: &HcomDb, explicit_name: Option<&str>) -> String {
    if let Some(name) = explicit_name {
        return name.to_string();
    }
    match identity::resolve_identity(db, None, None, None, None, None, None) {
        Ok(id) if matches!(id.kind, crate::shared::SenderKind::Instance) => id.name,
        _ => "cli".to_string(),
    }
}

/// Run the kill command.
pub fn run(argv: &[String], flags: &GlobalFlags) -> Result<i32> {
    // Filter out global flags already consumed by the router
    let mut filtered = vec!["kill".to_string()];
    let mut skip_next = false;
    for arg in argv {
        if skip_next { skip_next = false; continue; }
        match arg.as_str() {
            "kill" | "--go" => continue,
            "--name" => { skip_next = true; continue; }
            _ => filtered.push(arg.clone()),
        }
    }

    use clap::Parser;
    let kill_args = match KillArgs::try_parse_from(&filtered) {
        Ok(a) => a,
        Err(e) => { e.print().ok(); return Ok(if e.use_stderr() { 1 } else { 0 }); }
    };

    let targets = kill_args.targets;
    if targets.is_empty() {
        eprintln!("Error: no target specified\n\nUsage: kill <TARGET>...\n\nFor more information, try '--help'.");
        return Ok(1);
    }
    let explicit_name = flags.name.clone();

    let db = HcomDb::open()?;
    let hcom_dir = paths::hcom_dir();
    let initiator = resolve_initiator(&db, explicit_name.as_deref());

    // If any target is "all", just kill all
    if targets.iter().any(|t| t == "all") {
        return kill_all(&db, &hcom_dir, &initiator);
    }

    let mut worst_exit = 0;
    for target in &targets {
        let exit = if let Some(tag) = target.strip_prefix("tag:") {
            kill_by_tag(&db, &hcom_dir, tag, &initiator)?
        } else {
            kill_single(&db, &hcom_dir, target, &initiator)?
        };
        if exit > worst_exit {
            worst_exit = exit;
        }
    }
    Ok(worst_exit)
}

/// Format pane close info
fn pane_info_str(pane_closed: bool, preset_name: &str, pane_id: &str) -> String {
    if pane_closed {
        if !pane_id.is_empty() {
            format!(" (closed {} pane {})", preset_name, pane_id)
        } else if !preset_name.is_empty() {
            format!(" (closed {} pane)", preset_name)
        } else {
            String::new()
        }
    } else if !preset_name.is_empty() {
        format!(" (pane close failed for {})", preset_name)
    } else {
        String::new()
    }
}

/// Kill all instances.
fn kill_all(db: &HcomDb, hcom_dir: &std::path::Path, initiator: &str) -> Result<i32> {
    let instances = db.iter_instances_full()?;
    let mut killed = 0;
    let mut failed = 0;

    // Collect active PIDs for orphan filtering
    let mut active_pids = HashSet::new();

    for inst in &instances {
        // Skip remote instances
        if inst.origin_device_id.is_some() {
            continue;
        }

        if let Some(pid) = inst.pid {
            active_pids.insert(pid as u32);
            let is_headless = inst.background != 0;
            let (result, pane_closed, preset_name, pane_id) =
                kill_instance(db, &inst.name, pid as u32, &inst.launch_context, is_headless);
            let pane_info = pane_info_str(pane_closed, &preset_name, &pane_id);
            match result {
                terminal::KillResult::Sent => {
                    println!("Sent SIGTERM to process group {} for '{}'{}", pid, inst.name, pane_info);
                    killed += 1;
                }
                terminal::KillResult::AlreadyDead => {
                    println!("Process group {} not found for '{}' (already terminated){}", pid, inst.name, pane_info);
                    killed += 1;
                }
                terminal::KillResult::PermissionDenied => {
                    eprintln!("Permission denied to kill process group {} for '{}'", pid, inst.name);
                    failed += 1;
                }
            }
            // Clean up instance
            stop_instance(db, &inst.name, initiator, "killed");
            println!("  To resume: hcom r {}", inst.name);
        } else {
            // No PID tracked — just clean up
            stop_instance(db, &inst.name, initiator, "killed");
        }
    }

    // Kill orphans too
    let orphans = pidtrack::get_orphan_processes(hcom_dir, Some(&active_pids));
    for orphan in &orphans {
        let (result, pane_closed) = terminal::kill_process(
            orphan.pid, &orphan.terminal_preset, &orphan.pane_id,
            &orphan.process_id, &orphan.kitty_listen_on, &orphan.terminal_id,
        );
        let names = orphan.names.join(", ");
        let pane_info = pane_info_str(pane_closed, &orphan.terminal_preset, &orphan.pane_id);
        let label = if !names.is_empty() || !pane_info.is_empty() {
            format!(" ({}{})", names, pane_info)
        } else {
            String::new()
        };
        match result {
            terminal::KillResult::Sent => {
                println!("Sent SIGTERM to orphan process group {}{}", orphan.pid, label);
                killed += 1;
            }
            terminal::KillResult::AlreadyDead => {
                println!("Orphan process group {} already terminated{}", orphan.pid, label);
            }
            terminal::KillResult::PermissionDenied => {
                failed += 1;
            }
        }
        pidtrack::remove_pid(hcom_dir, orphan.pid);
    }

    if killed == 0 && failed == 0 {
        println!("No processes with tracked PIDs found");
    } else {
        if failed > 0 {
            println!("Killed {}, {} failed", killed, failed);
        } else {
            println!("Killed {}", killed);
        }
    }

    Ok(if failed > 0 { 1 } else { 0 })
}

/// Kill instances by tag.
fn kill_by_tag(db: &HcomDb, hcom_dir: &std::path::Path, tag: &str, initiator: &str) -> Result<i32> {
    let instances = db.iter_instances_full()?;
    let tagged: Vec<_> = instances
        .iter()
        .filter(|inst| {
            inst.tag.as_deref() == Some(tag) && inst.origin_device_id.is_none()
        })
        .collect();

    let mut killed = 0;
    let mut failed = 0;

    // Kill active instances with this tag
    for inst in &tagged {
        if let Some(pid) = inst.pid {
            let is_headless = inst.background != 0;
            let (result, pane_closed, preset_name, pane_id) =
                kill_instance(db, &inst.name, pid as u32, &inst.launch_context, is_headless);
            let pane_info = pane_info_str(pane_closed, &preset_name, &pane_id);
            match result {
                terminal::KillResult::Sent => {
                    println!("Sent SIGTERM to process group {} for '{}'{}", pid, inst.name, pane_info);
                    killed += 1;
                }
                terminal::KillResult::AlreadyDead => {
                    println!("Process group {} already terminated for '{}'", pid, inst.name);
                }
                terminal::KillResult::PermissionDenied => {
                    eprintln!("Permission denied to kill process group {} for '{}'", pid, inst.name);
                    failed += 1;
                }
            }
            stop_instance(db, &inst.name, initiator, "killed");
        } else {
            // No PID tracked — print error, don't print resume tip
            eprintln!("Cannot kill {} - no tracked process. Use hcom stop instead.", inst.name);
            failed += 1;
            stop_instance(db, &inst.name, initiator, "killed");
        }
    }

    // Also kill orphan processes with this tag (stopped but still running)
    let active_pids: HashSet<u32> = tagged.iter().filter_map(|i| i.pid.map(|p| p as u32)).collect();
    let orphans = pidtrack::get_orphan_processes(hcom_dir, Some(&active_pids));
    let tagged_orphans: Vec<_> = orphans.iter().filter(|o| o.tag == tag).collect();
    for orphan in &tagged_orphans {
        let names = orphan.names.join(", ");
        let (result, pane_closed) = terminal::kill_process(
            orphan.pid, &orphan.terminal_preset, &orphan.pane_id,
            &orphan.process_id, &orphan.kitty_listen_on, &orphan.terminal_id,
        );
        let pane_info = pane_info_str(pane_closed, &orphan.terminal_preset, &orphan.pane_id);
        match result {
            terminal::KillResult::Sent => {
                println!("Sent SIGTERM to stopped process group {} for '{}'{}", orphan.pid, names, pane_info);
                killed += 1;
            }
            terminal::KillResult::AlreadyDead => {
                println!("Process group {} already terminated for '{}'", orphan.pid, names);
            }
            terminal::KillResult::PermissionDenied => {
                eprintln!("Permission denied to kill process group {}", orphan.pid);
                failed += 1;
            }
        }
        pidtrack::remove_pid(hcom_dir, orphan.pid);
    }

    if tagged.is_empty() && tagged_orphans.is_empty() {
        eprintln!("No agents with tag '{}'", tag);
        return Ok(1);
    }

    println!("Killed {} (tag:{})", killed, tag);
    Ok(if failed > 0 { 1 } else { 0 })
}

/// Kill a single instance by name.
fn kill_single(db: &HcomDb, hcom_dir: &std::path::Path, target: &str, initiator: &str) -> Result<i32> {
    // Resolve display name
    let name = instances::resolve_display_name(db, target)
        .unwrap_or_else(|| target.to_string());

    let inst = match db.get_instance_full(&name)? {
        Some(inst) => inst,
        None => {
            // Check orphans
            let orphans = pidtrack::get_orphan_processes(hcom_dir, None);
            // Also match by PID number (TUI sends kill by PID for orphans)
            let target_pid = target.parse::<u32>().ok();
            if let Some(orphan) = orphans.iter().find(|o| {
                o.names.contains(&target.to_string())
                    || o.process_id == target
                    || target_pid == Some(o.pid)
            }) {
                let (result, pane_closed) = terminal::kill_process(
                    orphan.pid, &orphan.terminal_preset, &orphan.pane_id,
                    &orphan.process_id, &orphan.kitty_listen_on, &orphan.terminal_id,
                );
                let pane_info = pane_info_str(pane_closed, &orphan.terminal_preset, &orphan.pane_id);
                match result {
                    terminal::KillResult::Sent => {
                        println!("Sent SIGTERM to process group {} for stopped instance '{}'{}", orphan.pid, target, pane_info);
                    }
                    terminal::KillResult::AlreadyDead => {
                        println!("Process group {} not found for '{}' (already terminated){}", orphan.pid, target, pane_info);
                    }
                    terminal::KillResult::PermissionDenied => {
                        eprintln!("Permission denied to kill process group {}", orphan.pid);
                        return Ok(1);
                    }
                }
                pidtrack::remove_pid(hcom_dir, orphan.pid);
                return Ok(0);
            }
            bail!("Agent '{}' not found", target);
        }
    };

    let pid = match inst.pid {
        Some(pid) => pid as u32,
        None => bail!("No tracked PID for '{}' — use 'hcom stop {}' instead", name, name),
    };

    let is_headless = inst.background != 0;
    let (result, pane_closed, preset_name, pane_id) =
        kill_instance(db, &name, pid, &inst.launch_context, is_headless);
    stop_instance(db, &name, initiator, "killed");

    let pane_info = pane_info_str(pane_closed, &preset_name, &pane_id);
    match result {
        terminal::KillResult::Sent => {
            println!("Sent SIGTERM to process group {} for '{}'{}", pid, name, pane_info);
            println!("  To resume: hcom r {}", name);
            Ok(0)
        }
        terminal::KillResult::AlreadyDead => {
            println!("Process group {} not found for '{}' (already terminated){}", pid, name, pane_info);
            println!("  To resume: hcom r {}", name);
            Ok(0)
        }
        terminal::KillResult::PermissionDenied => {
            eprintln!("Permission denied to kill process group {} for '{}'", pid, name);
            Ok(1)
        }
    }
}

/// Kill a process and close its terminal pane.
/// Returns (KillResult, pane_closed, preset_name, pane_id).
fn kill_instance(
    _db: &HcomDb,
    name: &str,
    pid: u32,
    launch_context: &Option<String>,
    is_headless: bool,
) -> (terminal::KillResult, bool, String, String) {
    // Headless instances have no terminal pane — skip pane close
    if is_headless {
        let (result, pane_closed) = terminal::kill_process(pid, "", "", "", "", "");
        log_info(
            "kill",
            "lifecycle.kill",
            &format!("name={} pid={} result={:?} pane_closed={} headless=true", name, pid, result, pane_closed),
        );
        return (result, pane_closed, String::new(), String::new());
    }

    let ti = launch_context.as_deref()
        .map(terminal::resolve_terminal_info_from_launch_context)
        .unwrap_or_default();

    let (result, pane_closed) = terminal::kill_process(
        pid,
        &ti.preset_name,
        &ti.pane_id,
        &ti.process_id,
        &ti.kitty_listen_on,
        &ti.terminal_id,
    );

    log_info(
        "kill",
        "lifecycle.kill",
        &format!("name={} pid={} result={:?} pane_closed={}", name, pid, result, pane_closed),
    );

    (result, pane_closed, ti.preset_name.clone(), ti.pane_id.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kill_no_target_fails() {
        // Missing required target → clap error → exit code 1
        let flags = GlobalFlags::default();
        let argv = vec!["kill".to_string()];
        let result = run(&argv, &flags).unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn test_kill_args_parse_single() {
        use clap::Parser;
        let args = KillArgs::try_parse_from(["kill", "myagent"]).unwrap();
        assert_eq!(args.targets, vec!["myagent"]);
    }

    #[test]
    fn test_kill_args_parse_multiple() {
        use clap::Parser;
        let args = KillArgs::try_parse_from(["kill", "nozu", "zelu"]).unwrap();
        assert_eq!(args.targets, vec!["nozu", "zelu"]);
    }

    #[test]
    fn test_kill_args_no_target_is_empty_vec() {
        use clap::Parser;
        let args = KillArgs::try_parse_from(["kill"]).unwrap();
        assert!(args.targets.is_empty());
    }
}
