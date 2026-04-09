//! Resume command: `hcom r <name> [tool-args...]`
//!
//!
//! Loads a stopped instance's snapshot and relaunches with --resume session_id.

use anyhow::{Result, bail};
use serde_json::json;

use crate::db::HcomDb;
use crate::commands::launch::{
    LaunchOutputContext, LaunchPreview, extract_launch_flags, is_background_from_args,
    load_hcom_config, print_launch_feedback, print_launch_preview, resolve_launcher_name,
};
use crate::hooks::claude_args;
use crate::launcher::{self, LaunchParams};
use crate::log::log_info;
use crate::router::GlobalFlags;
use crate::tools::{codex_args, gemini_args};

/// Run the resume command. `argv` is the full argv[1..].
pub fn run(argv: &[String], flags: &GlobalFlags) -> Result<i32> {
    let (name, extra_args) = parse_resume_argv(argv, "r")?;

    do_resume(&name, false, &extra_args, flags)
}

/// Parse resume/fork argv: `r|f <name> [extra-args...]`
pub fn parse_resume_argv(argv: &[String], cmd: &str) -> Result<(String, Vec<String>)> {
    let mut i = 0;

    // Skip command name and global flags
    while i < argv.len() {
        match argv[i].as_str() {
            s if s == cmd || s == "resume" || s == "fork" || s == "f" => {
                i += 1;
            }
            "--name" => {
                i += 2;
            }
            "--go" => {
                i += 1;
            }
            _ => break,
        }
    }

    if i >= argv.len() {
        bail!("Usage: hcom {} <name> [tool-args...]", cmd);
    }

    let name = argv[i].clone();
    let extra_args = argv[i + 1..].to_vec();

    Ok((name, extra_args))
}

/// Core resume/fork logic.
pub fn do_resume(
    name: &str,
    fork: bool,
    extra_args: &[String],
    flags: &GlobalFlags,
) -> Result<i32> {
    let db = HcomDb::open()?;
    let name = crate::instances::resolve_display_name_or_stopped(&db, name)
        .unwrap_or_else(|| name.to_string());

    // For resume (not fork): reject if instance is still active
    if !fork {
        if let Ok(Some(_)) = db.get_instance_full(&name) {
            bail!("'{}' is still active — run hcom stop {} first", name, name);
        }
    }

    // Load snapshot: from active instance (fork) or stopped event (resume)
    let (tool, session_id, launch_args_str, tag, background, last_event_id, snapshot_dir) = if fork
    {
        load_instance_data(&db, &name)?
    } else {
        load_stopped_snapshot(&db, &name)?
    };

    if session_id.is_empty() {
        bail!(
            "No session ID found for '{}' — cannot {}",
            name,
            if fork { "fork" } else { "resume" }
        );
    }

    validate_resume_operation(&tool, fork)?;

    let hcom_config = load_hcom_config();
    let inherited_tag = if tag.is_empty() {
        None
    } else {
        Some(tag.clone())
    };

    // Extract hcom-level flags from extra args before tool parsing.
    let (dir_override, launch_flags, clean_extra) = extract_resume_flags(extra_args);

    // Determine effective working directory:
    // - Explicit --dir flag wins (validated and canonicalized)
    // - For resume: use snapshot directory (continue where you left off)
    // - For fork: use current directory (start fresh in new context)
    let effective_cwd = if let Some(ref dir) = dir_override {
        let path = std::path::Path::new(dir);
        if !path.is_dir() {
            bail!("--dir path does not exist or is not a directory: {}", dir);
        }
        path.canonicalize()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|_| dir.clone())
    } else if fork {
        std::env::current_dir()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|_| ".".to_string())
    } else if !snapshot_dir.is_empty() && std::path::Path::new(&snapshot_dir).is_dir() {
        snapshot_dir.clone()
    } else {
        if !snapshot_dir.is_empty() {
            eprintln!(
                "Warning: original directory '{}' no longer exists, using current directory",
                snapshot_dir
            );
        }
        std::env::current_dir()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|_| ".".to_string())
    };

    let should_preview = should_preview_resume(&launch_flags, &clean_extra);

    let mut cli_tool_args = build_resume_args(&tool, &session_id, fork);
    cli_tool_args.extend(clean_extra);

    // Merge with original launch args
    let original_args: Vec<String> = if !launch_args_str.is_empty() {
        serde_json::from_str(&launch_args_str).unwrap_or_default()
    } else {
        Vec::new()
    };

    // For resume, merge original args with new args (new overrides)
    let merged_cli_args = if !original_args.is_empty() {
        merge_resume_args(&tool, &original_args, &cli_tool_args)
    } else {
        cli_tool_args
    };

    let mut merged_args = merged_cli_args.clone();

    if launch_flags.headless && tool != "claude" {
        bail!("--headless is only supported for Claude resume/fork launches");
    }

    let is_headless = launch_flags.headless || is_background_from_args(&tool, &merged_args) || background;
    let use_pty = tool == "claude" && !is_headless && cfg!(unix);

    if tool == "claude" && is_headless {
        let spec = claude_args::resolve_claude_args(Some(&merged_args), None);
        let updated = claude_args::add_background_defaults(&spec);
        merged_args = updated.rebuild_tokens(true);
    }

    let ctx = crate::shared::HcomContext::from_os();
    if ctx.is_inside_ai_tool() && !flags.go && should_preview {
        let action = if fork { "fork" } else { "resume" };
        let identity_note = if fork {
            format!("Fork source: {} (new identity)", name)
        } else {
            format!("Resume target: {} (same identity)", name)
        };
        let cwd_note = format!("Directory source: {}", effective_cwd);
        let notes = [identity_note.as_str(), cwd_note.as_str()];
        print_launch_preview(LaunchPreview {
            action,
            tool: &tool,
            count: 1,
            background: is_headless,
            args: &merged_cli_args,
            tag: launch_flags.tag.as_deref().or(inherited_tag.as_deref()),
            cwd: Some(&effective_cwd),
            terminal: launch_flags.terminal.as_deref(),
            config: &hcom_config,
            show_config_args: false,
            notes: &notes,
        });
        return Ok(0);
    }

    let launcher_name =
        resolve_launcher_name(&db, flags, std::env::var("HCOM_PROCESS_ID").ok().as_deref());
    let launcher_name_for_output = launcher_name.clone();
    let fork_child_name = if fork {
        let (alive_names, taken_names) = crate::instance_names::collect_taken_names(&db)?;
        let candidate = crate::instance_names::allocate_name(
            &|n| taken_names.contains(n) || db.get_instance_full(n).ok().flatten().is_some(),
            &alive_names,
            200,
            1200,
            900.0,
        )?;
        Some(candidate)
    } else {
        None
    };
    let effective_tag = launch_flags.tag.clone().or(inherited_tag.clone());
    let fork_initial_prompt = if fork && tool == "codex" {
        let child_name = fork_child_name
            .as_deref()
            .expect("fork child name should be generated");
        let display_name = effective_tag
            .as_deref()
            .map(|tag| format!("{tag}-{child_name}"))
            .unwrap_or_else(|| child_name.to_string());
        let identity_reset = format!(
            "You are a fork of {name}, but your new hcom identity is now {display_name}.\n\
             Your hcom name is {child_name}.\n\
             Do not use {name}'s hcom identity anymore, even if it appears in inherited thread history.\n\
             Use [hcom:{child_name}] in your first response only.\n\
             Use `hcom ... --name {child_name}` for all hcom commands.\n\
             If asked about your identity, answer exactly: {display_name}"
        );
        Some(match launch_flags.initial_prompt.as_deref() {
            Some(user_prompt) if !user_prompt.trim().is_empty() => {
                format!("{identity_reset}\n\n{user_prompt}")
            }
            _ => identity_reset,
        })
    } else {
        launch_flags.initial_prompt.clone()
    };
    let output_tag = effective_tag.clone();
    let launch_tag = effective_tag.clone();
    let base_system_prompt = resume_system_prompt(&tool, &name, fork);
    let effective_system_prompt = match launch_flags.system_prompt.as_deref() {
        Some(custom) if !custom.trim().is_empty() => format!("{base_system_prompt}\n\n{custom}"),
        _ => base_system_prompt,
    };
    let output = LaunchOutputContext {
        action: if fork { "fork" } else { "resume" },
        tool: &tool,
        requested_count: 1,
        tag: output_tag.as_deref(),
        launcher_name: &launcher_name_for_output,
        terminal: launch_flags.terminal.as_deref(),
        background: is_headless,
        run_here: launch_flags.run_here,
        hcom_config: &hcom_config,
    };

    // Launch
    let result = launcher::launch(
        &db,
        LaunchParams {
            tool: tool.clone(),
            count: 1,
            args: merged_args,
            tag: launch_tag,
            system_prompt: Some(effective_system_prompt),
            pty: use_pty,
            background: is_headless,
            cwd: Some(effective_cwd),
            env: None,
            launcher: Some(launcher_name),
            run_here: launch_flags.run_here,
            initial_prompt: fork_initial_prompt,
            batch_id: launch_flags.batch_id.clone(),
            name: if fork { fork_child_name } else { Some(name.clone()) },
            skip_validation: false,
            terminal: launch_flags.terminal.clone(),
            append_reply_handoff: !(fork && tool == "codex"),
        },
    )?;

    // For resume: restore cursor so pending messages are delivered
    if !fork && last_event_id > 0 {
        crate::instances::update_instance_position(
            &db,
            &name,
            &serde_json::Map::from_iter([("last_event_id".to_string(), json!(last_event_id))]),
        );
    }

    print_launch_feedback(&db, &result, &output)?;

    log_info(
        if fork { "fork" } else { "resume" },
        &format!("cmd.{}", if fork { "fork" } else { "resume" }),
        &format!(
            "name={} tool={} session={} launched={}",
            name, tool, session_id, result.launched
        ),
    );

    Ok(if result.launched > 0 { 0 } else { 1 })
}

/// Extract resume-only flags, then reuse the shared launch flag parser.
fn extract_resume_flags(
    args: &[String],
) -> (Option<String>, crate::commands::launch::HcomLaunchFlags, Vec<String>) {
    let mut dir = None;
    let mut filtered = Vec::new();
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--" {
            filtered.extend_from_slice(&args[i..]);
            break;
        } else if args[i] == "--dir" && i + 1 < args.len() {
            dir = Some(args[i + 1].clone());
            i += 2;
        } else if args[i].starts_with("--dir=") {
            dir = Some(args[i][6..].to_string());
            i += 1;
        } else {
            filtered.push(args[i].clone());
            i += 1;
        }
    }
    let (flags, remaining) = extract_launch_flags(&filtered);
    (dir, flags, remaining)
}

fn should_preview_resume(
    launch_flags: &crate::commands::launch::HcomLaunchFlags,
    tool_args: &[String],
) -> bool {
    !tool_args.is_empty() || *launch_flags != crate::commands::launch::HcomLaunchFlags::default()
}

fn validate_resume_operation(tool: &str, fork: bool) -> Result<()> {
    if fork && tool == "gemini" {
        bail!("Gemini does not support session forking (hcom f)");
    }
    Ok(())
}

fn resume_system_prompt(tool: &str, name: &str, fork: bool) -> String {
    if fork {
        if tool == "codex" {
            format!(
                "YOU ARE A FORK of agent '{}'. \
                 You have the same session history but are a NEW agent with an already-assigned hcom identity. \
                 Use that assigned identity for all hcom commands.",
                name
            )
        } else {
            format!(
                "YOU ARE A FORK of agent '{}'. \
                 You have the same session history but are a NEW agent. \
                 Run hcom start to get your own identity.",
                name
            )
        }
    } else {
        format!("YOUR SESSION HAS BEEN RESUMED! You are still '{}'.", name)
    }
}

/// Load data from an active or stopped instance.
fn load_instance_data(
    db: &HcomDb,
    name: &str,
) -> Result<(String, String, String, String, bool, i64, String)> {
    // Try active instance first
    if let Ok(Some(inst)) = db.get_instance_full(name) {
        return Ok((
            inst.tool.clone(),
            inst.session_id.as_deref().unwrap_or("").to_string(),
            inst.launch_args.as_deref().unwrap_or("").to_string(),
            inst.tag.as_deref().unwrap_or("").to_string(),
            inst.background != 0,
            inst.last_event_id,
            inst.directory.clone(),
        ));
    }

    // Fall back to stopped snapshot
    load_stopped_snapshot(db, name)
}

/// Load stopped snapshot from life events.
fn load_stopped_snapshot(
    db: &HcomDb,
    name: &str,
) -> Result<(String, String, String, String, bool, i64, String)> {
    // Query the latest "stopped" life event for this instance
    let mut stmt = db.conn().prepare(
        "SELECT data FROM events WHERE type='life' AND instance=? ORDER BY id DESC LIMIT 10",
    )?;

    let rows: Vec<String> = stmt
        .query_map(rusqlite::params![name], |row| row.get::<_, String>(0))?
        .filter_map(|r| r.ok())
        .collect();

    for data_str in &rows {
        if let Ok(data) = serde_json::from_str::<serde_json::Value>(data_str) {
            if data.get("action").and_then(|v| v.as_str()) == Some("stopped") {
                if let Some(snapshot) = data.get("snapshot") {
                    let tool = snapshot
                        .get("tool")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let session_id = snapshot
                        .get("session_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let launch_args = snapshot
                        .get("launch_args")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let tag = snapshot
                        .get("tag")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let background = snapshot
                        .get("background")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0)
                        != 0;
                    let last_event_id = snapshot
                        .get("last_event_id")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    let directory = snapshot
                        .get("directory")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();

                    return Ok((
                        tool,
                        session_id,
                        launch_args,
                        tag,
                        background,
                        last_event_id,
                        directory,
                    ));
                }
            }
        }
    }

    bail!("No stopped snapshot found for '{}'", name)
}

/// Build tool-specific resume/fork args.
fn build_resume_args(tool: &str, session_id: &str, fork: bool) -> Vec<String> {
    match tool {
        "claude" | "claude-pty" => {
            let mut args = vec!["--resume".to_string(), session_id.to_string()];
            if fork {
                args.push("--fork-session".to_string());
            }
            args
        }
        "gemini" => {
            vec!["--resume".to_string(), session_id.to_string()]
        }
        "codex" => {
            let subcmd = if fork { "fork" } else { "resume" };
            vec![subcmd.to_string(), session_id.to_string()]
        }
        "opencode" => {
            let mut args = vec!["--session".to_string(), session_id.to_string()];
            if fork {
                args.push("--fork".to_string());
            }
            args
        }
        _ => Vec::new(),
    }
}

/// Merge original launch args with resume-specific args.
fn merge_resume_args(tool: &str, original: &[String], resume: &[String]) -> Vec<String> {
    // Resume args take precedence. We strip --resume/--session from original
    // and prepend resume args.
    match tool {
        "claude" | "claude-pty" => {
            let orig_spec = claude_args::resolve_claude_args(Some(original), None);
            let resume_spec = claude_args::resolve_claude_args(Some(resume), None);
            let merged = claude_args::merge_claude_args(&orig_spec, &resume_spec);
            merged.rebuild_tokens(true)
        }
        "gemini" => {
            let orig_spec = gemini_args::resolve_gemini_args(Some(original), None);
            let resume_spec = gemini_args::resolve_gemini_args(Some(resume), None);
            let merged = gemini_args::merge_gemini_args(&orig_spec, &resume_spec);
            merged.rebuild_tokens(true, true)
        }
        "codex" => {
            let stripped_original =
                crate::tools::codex_preprocessing::strip_codex_developer_instructions(original);
            let orig_spec = codex_args::resolve_codex_args(Some(&stripped_original), None);
            let resume_spec = codex_args::resolve_codex_args(Some(resume), None);
            let merged = codex_args::merge_codex_args(&orig_spec, &resume_spec);
            merged.rebuild_tokens(true, true)
        }
        "opencode" => merge_opencode_args(original, resume),
        _ => {
            // For unknown tools: resume args only.
            resume.to_vec()
        }
    }
}

fn merge_opencode_args(original: &[String], resume: &[String]) -> Vec<String> {
    let mut preserved = Vec::new();
    let mut i = 0;

    while i < original.len() {
        let token = &original[i];

        if token == "--session" || token == "--prompt" {
            i += 2;
            continue;
        }
        if token == "--fork" || token.starts_with("--session=") || token.starts_with("--prompt=") {
            i += 1;
            continue;
        }

        preserved.push(token.clone());
        i += 1;
    }

    let mut merged = resume.to_vec();
    merged.extend(preserved);
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(items: &[&str]) -> Vec<String> {
        items.iter().map(|i| i.to_string()).collect()
    }

    #[test]
    fn test_parse_resume_argv() {
        let (name, extra) = parse_resume_argv(&s(&["r", "luna"]), "r").unwrap();
        assert_eq!(name, "luna");
        assert!(extra.is_empty());
    }

    #[test]
    fn test_parse_resume_argv_with_extra() {
        let (name, extra) = parse_resume_argv(&s(&["r", "luna", "--model", "opus"]), "r").unwrap();
        assert_eq!(name, "luna");
        assert_eq!(extra, s(&["--model", "opus"]));
    }

    #[test]
    fn test_parse_resume_argv_empty_fails() {
        assert!(parse_resume_argv(&s(&["r"]), "r").is_err());
    }

    #[test]
    fn test_build_resume_args_claude() {
        let args = build_resume_args("claude", "sess-123", false);
        assert_eq!(args, s(&["--resume", "sess-123"]));
    }

    #[test]
    fn test_build_resume_args_claude_fork() {
        let args = build_resume_args("claude", "sess-123", true);
        assert_eq!(args, s(&["--resume", "sess-123", "--fork-session"]));
    }

    #[test]
    fn test_build_resume_args_codex_resume() {
        let args = build_resume_args("codex", "sess-456", false);
        assert_eq!(args, s(&["resume", "sess-456"]));
    }

    #[test]
    fn test_build_resume_args_codex_fork() {
        let args = build_resume_args("codex", "sess-456", true);
        assert_eq!(args, s(&["fork", "sess-456"]));
    }

    #[test]
    fn test_build_resume_args_gemini() {
        let args = build_resume_args("gemini", "sess-789", false);
        assert_eq!(args, s(&["--resume", "sess-789"]));
    }

    #[test]
    fn test_validate_resume_operation_rejects_gemini_fork() {
        let err = validate_resume_operation("gemini", true).unwrap_err().to_string();
        assert_eq!(err, "Gemini does not support session forking (hcom f)");
    }

    #[test]
    fn test_validate_resume_operation_allows_gemini_resume() {
        assert!(validate_resume_operation("gemini", false).is_ok());
    }

    #[test]
    fn test_merge_resume_args_opencode_preserves_non_session_flags() {
        let merged = merge_resume_args(
            "opencode",
            &s(&[
                "--model",
                "openai/gpt-5.4",
                "--session",
                "old-sess",
                "--prompt",
                "old prompt",
                "--approval-mode",
                "on-request",
            ]),
            &s(&["--session", "new-sess", "--fork"]),
        );
        assert_eq!(
            merged,
            s(&[
                "--session",
                "new-sess",
                "--fork",
                "--model",
                "openai/gpt-5.4",
                "--approval-mode",
                "on-request",
            ])
        );
    }

    #[test]
    fn test_merge_resume_args_opencode_strips_equals_form_session_and_prompt() {
        let merged = merge_resume_args(
            "opencode",
            &s(&[
                "--session=old-sess",
                "--prompt=old prompt",
                "--model",
                "anthropic/claude-sonnet-4-6",
            ]),
            &s(&["--session", "new-sess"]),
        );
        assert_eq!(
            merged,
            s(&[
                "--session",
                "new-sess",
                "--model",
                "anthropic/claude-sonnet-4-6",
            ])
        );
    }

    #[test]
    fn test_build_resume_args_opencode_fork() {
        let args = build_resume_args("opencode", "sess-000", true);
        assert_eq!(args, s(&["--session", "sess-000", "--fork"]));
    }

    #[test]
    fn test_extract_resume_flags_terminal() {
        let (dir, flags, remaining) = extract_resume_flags(&s(&["--terminal", "alacritty", "--model", "opus"]));
        assert_eq!(dir, None);
        assert_eq!(flags.terminal, Some("alacritty".to_string()));
        assert_eq!(remaining, s(&["--model", "opus"]));
    }

    #[test]
    fn test_extract_resume_flags_tag_and_terminal() {
        let (dir, flags, remaining) = extract_resume_flags(&s(&["--tag", "test", "--terminal", "kitty"]));
        assert_eq!(dir, None);
        assert_eq!(flags.tag, Some("test".to_string()));
        assert_eq!(flags.terminal, Some("kitty".to_string()));
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_extract_resume_flags_equals_form() {
        let (dir, flags, remaining) = extract_resume_flags(&s(&["--tag=test", "--terminal=alacritty"]));
        assert_eq!(dir, None);
        assert_eq!(flags.tag, Some("test".to_string()));
        assert_eq!(flags.terminal, Some("alacritty".to_string()));
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_extract_resume_flags_none() {
        let (dir, flags, remaining) = extract_resume_flags(&s(&["--model", "opus"]));
        assert_eq!(dir, None);
        assert_eq!(flags.tag, None);
        assert_eq!(flags.terminal, None);
        assert_eq!(remaining, s(&["--model", "opus"]));
    }

    #[test]
    fn test_extract_resume_flags_dir() {
        let (dir, flags, remaining) = extract_resume_flags(&s(&["--dir", "/tmp/test", "--model", "opus"]));
        assert_eq!(dir, Some("/tmp/test".to_string()));
        assert_eq!(flags.tag, None);
        assert_eq!(flags.terminal, None);
        assert_eq!(remaining, s(&["--model", "opus"]));
    }

    #[test]
    fn test_extract_resume_flags_shared_launch_flags() {
        let (dir, flags, remaining) = extract_resume_flags(&s(&[
            "--dir=/tmp/test",
            "--headless",
            "--batch-id",
            "batch-1",
            "--run-here",
            "--hcom-prompt",
            "hi",
            "--hcom-system-prompt",
            "sys",
            "--model",
            "opus",
        ]));
        assert_eq!(dir, Some("/tmp/test".to_string()));
        assert!(flags.headless);
        assert_eq!(flags.batch_id, Some("batch-1".to_string()));
        assert_eq!(flags.run_here, Some(true));
        assert_eq!(flags.initial_prompt, Some("hi".to_string()));
        assert_eq!(flags.system_prompt, Some("sys".to_string()));
        assert_eq!(remaining, s(&["--model", "opus"]));
    }

    #[test]
    fn test_extract_resume_flags_stops_at_double_dash() {
        let (dir, flags, remaining) = extract_resume_flags(&s(&[
            "--dir",
            "/tmp/test",
            "--",
            "--dir",
            "tool-dir",
            "--model",
            "opus",
        ]));
        assert_eq!(dir, Some("/tmp/test".to_string()));
        assert_eq!(flags, crate::commands::launch::HcomLaunchFlags::default());
        assert_eq!(remaining, s(&["--dir", "tool-dir", "--model", "opus"]));
    }

    #[test]
    fn test_should_preview_resume_false_for_plain_resume() {
        assert!(!should_preview_resume(
            &crate::commands::launch::HcomLaunchFlags::default(),
            &[]
        ));
    }

    #[test]
    fn test_should_preview_resume_true_for_tool_args() {
        assert!(should_preview_resume(
            &crate::commands::launch::HcomLaunchFlags::default(),
            &s(&["--model", "opus"])
        ));
    }

    #[test]
    fn test_should_preview_resume_true_for_hcom_only_flags() {
        let flags = crate::commands::launch::HcomLaunchFlags {
            terminal: Some("kitty".to_string()),
            ..Default::default()
        };
        assert!(should_preview_resume(&flags, &[]));
    }

    #[test]
    fn test_resume_system_prompt_codex_fork_does_not_tell_agent_to_rebind() {
        let prompt = resume_system_prompt("codex", "luna", true);
        assert!(prompt.contains("already-assigned hcom identity"));
        assert!(!prompt.contains("Run hcom start"));
    }

    #[test]
    fn test_resume_system_prompt_non_codex_fork_still_uses_start_guidance() {
        let prompt = resume_system_prompt("claude", "luna", true);
        assert!(prompt.contains("Run hcom start"));
    }
}
