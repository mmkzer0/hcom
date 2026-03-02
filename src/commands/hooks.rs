//! `hcom hooks` command — add/remove/status for tool hooks.
//!
//!
//! Manages hook installation across Claude, Gemini, Codex, and OpenCode.

use crate::db::HcomDb;
use crate::shared::CommandContext;

/// Parsed arguments for `hcom hooks`.
#[derive(clap::Parser, Debug)]
#[command(name = "hooks", about = "Manage tool hooks")]
pub struct HooksArgs {
    /// Subcommand and arguments (status/add/remove [tool])
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub args: Vec<String>,
}

/// Valid tool names for hooks management.
const HOOK_TOOLS: &[&str] = &["claude", "gemini", "codex", "opencode"];

/// Get hook installation status for each tool.
fn get_tool_status() -> Vec<(&'static str, bool, String)> {
    let claude_installed = crate::hooks::claude::verify_claude_hooks_installed(None, false);
    let claude_path = crate::hooks::claude::get_claude_settings_path()
        .to_string_lossy()
        .to_string();

    let gemini_installed = crate::hooks::gemini::verify_gemini_hooks_installed(false);
    let gemini_path = crate::hooks::gemini::get_gemini_settings_path()
        .to_string_lossy()
        .to_string();

    let codex_installed = crate::hooks::codex::verify_codex_hooks_installed(false);
    let codex_path = crate::hooks::codex::get_codex_config_path()
        .to_string_lossy()
        .to_string();

    let opencode_installed = crate::hooks::opencode::verify_opencode_plugin_installed();
    let opencode_path = crate::hooks::opencode::get_opencode_plugin_path()
        .to_string_lossy()
        .to_string();

    vec![
        ("claude", claude_installed, claude_path),
        ("gemini", gemini_installed, gemini_path),
        ("codex", codex_installed, codex_path),
        ("opencode", opencode_installed, opencode_path),
    ]
}

/// Show hook installation status for all tools.
fn cmd_hooks_status() -> i32 {
    let status = get_tool_status();
    for (tool, installed, path) in &status {
        if *installed {
            println!("{tool}:  installed    ({path})");
        } else {
            println!("{tool}:  not installed");
        }
    }
    0
}

/// Add hooks for specified tool(s).
fn cmd_hooks_add(argv: &[&str]) -> i32 {
    // Get auto_approve from config
    let include_permissions = crate::config::load_config_snapshot().core.auto_approve;

    // Determine which tools to install
    let tools: Vec<&str> = if argv.is_empty() {
        // Auto-detect current tool
        let current = detect_current_tool();
        if HOOK_TOOLS.contains(&current) {
            vec![current]
        } else {
            HOOK_TOOLS.to_vec()
        }
    } else if argv[0] == "all" {
        HOOK_TOOLS.to_vec()
    } else if HOOK_TOOLS.contains(&argv[0]) {
        vec![argv[0]]
    } else {
        eprintln!("Error: Unknown tool: {}", argv[0]);
        eprintln!("Valid options: claude, gemini, codex, opencode, all");
        return 1;
    };

    // Install hooks — propagate error detail where available
    let mut results: Vec<(&str, bool, Option<String>)> = Vec::new();
    for tool in &tools {
        let (success, err) = match *tool {
            "claude" => (crate::hooks::claude::setup_claude_hooks(include_permissions), None),
            "gemini" => (crate::hooks::gemini::setup_gemini_hooks(include_permissions), None),
            "codex" => (crate::hooks::codex::setup_codex_hooks(include_permissions), None),
            "opencode" => match crate::hooks::opencode::install_opencode_plugin() {
                Ok(v) => (v, None),
                Err(e) => (false, Some(e.to_string())),
            },
            _ => (false, None),
        };
        results.push((tool, success, err));
    }

    // Report results
    let success_count = results.iter().filter(|(_, ok, _)| *ok).count();
    let fail_count = results.len() - success_count;

    let status = get_tool_status();
    for (tool, success, err) in &results {
        let path = status
            .iter()
            .find(|(t, _, _)| t == tool)
            .map(|(_, _, p)| p.as_str())
            .unwrap_or("");
        if *success {
            println!("Added {tool} hooks  ({path})");
        } else if let Some(e) = err {
            eprintln!("Failed to add {tool} hooks: {e}");
        } else {
            eprintln!("Failed to add {tool} hooks");
        }
    }

    if success_count > 0 {
        println!();
        if tools.len() == 1 {
            let tool_name = match tools[0] {
                "claude" => "Claude Code",
                "gemini" => "Gemini CLI",
                "codex" => "Codex",
                "opencode" => "OpenCode",
                other => other,
            };
            println!("Restart {tool_name} to activate hooks.");
        } else {
            println!("Restart the tool(s) to activate hooks.");
        }
    }

    if fail_count > 0 { 1 } else { 0 }
}

/// Remove hooks for specified tool(s). Called from both `hcom hooks remove` and `hcom reset hooks`.
pub fn cmd_hooks_remove(argv: &[&str]) -> i32 {
    // Determine which tools to remove
    let tools: Vec<&str> = if argv.is_empty() || (argv.len() == 1 && argv[0] == "all") {
        HOOK_TOOLS.to_vec()
    } else if HOOK_TOOLS.contains(&argv[0]) {
        vec![argv[0]]
    } else {
        eprintln!("Error: Unknown tool: {}", argv[0]);
        eprintln!("Valid options: claude, gemini, codex, opencode, all");
        return 1;
    };

    // Remove hooks — propagate error detail where available
    let mut results: Vec<(&str, bool, Option<String>)> = Vec::new();
    for tool in &tools {
        let (success, err) = match *tool {
            "claude" => (crate::hooks::claude::remove_claude_hooks(), None),
            "gemini" => (crate::hooks::gemini::remove_gemini_hooks(), None),
            "codex" => (crate::hooks::codex::remove_codex_hooks(), None),
            "opencode" => match crate::hooks::opencode::remove_opencode_plugin() {
                Ok(()) => (true, None),
                Err(e) => (false, Some(e.to_string())),
            },
            _ => (false, None),
        };
        results.push((tool, success, err));
    }

    // Report results
    for (tool, success, err) in &results {
        if *success {
            println!("Removed {tool} hooks");
        } else if let Some(e) = err {
            eprintln!("Failed to remove {tool} hooks: {e}");
        } else {
            eprintln!("Failed to remove {tool} hooks");
        }
    }

    let fail_count = results.iter().filter(|(_, ok, _)| !*ok).count();
    if fail_count > 0 { 1 } else { 0 }
}

/// Detect current AI tool from environment.
fn detect_current_tool() -> &'static str {
    if std::env::var("CLAUDE_CODE_ENTRYPOINT").is_ok() {
        "claude"
    } else if std::env::var("GEMINI_CLI_ENTRYPOINT").is_ok() {
        "gemini"
    } else if std::env::var("CODEX_CLI_ENTRYPOINT").is_ok() {
        "codex"
    } else if std::env::var("OPENCODE").ok().as_deref() == Some("1") {
        "opencode"
    } else {
        "adhoc"
    }
}

pub fn cmd_hooks(_db: &HcomDb, args: &HooksArgs, _ctx: Option<&CommandContext>) -> i32 {
    let argv = &args.args;
    if argv.is_empty() {
        // No args = show status
        return cmd_hooks_status();
    }

    let first = argv[0].as_str();

    if first == "--help" || first == "-h" {
        println!(
            "hcom hooks - Manage tool hooks for hcom integration\n\n\
             Hooks enable automatic message delivery and status tracking. Without hooks,\n\
             you can still use hcom in ad-hoc mode (run hcom start in any ai tool).\n\n\
             Usage:\n  \
             hcom hooks                  Show hook status for all tools\n  \
             hcom hooks status           Same as above\n  \
             hcom hooks add [tool]       Add hooks (claude|gemini|codex|opencode|all)\n  \
             hcom hooks remove [tool]    Remove hooks (claude|gemini|codex|opencode|all)\n\n\
             Examples:\n  \
             hcom hooks add claude       Add Claude Code hooks only\n  \
             hcom hooks add              Auto-detect tool or add all\n  \
             hcom hooks remove all       Remove all hooks\n\n\
             After adding, restart the tool to activate hooks."
        );
        return 0;
    }

    let sub_argv: Vec<&str> = argv[1..].iter().map(|s| s.as_str()).collect();

    match first {
        "status" => cmd_hooks_status(),
        "add" | "install" => cmd_hooks_add(&sub_argv),
        "remove" | "uninstall" => cmd_hooks_remove(&sub_argv),
        _ => {
            eprintln!("Error: Unknown hooks subcommand: {first}");
            eprintln!("Usage: hcom hooks [status|add|remove] [tool]");
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_current_tool_default() {
        // In test env, none of the AI tool vars should be set
        // (unless running inside one, which is fine — it'll detect it)
        let tool = detect_current_tool();
        assert!(
            ["claude", "gemini", "codex", "opencode", "adhoc"].contains(&tool),
            "unexpected tool: {tool}"
        );
    }
}
