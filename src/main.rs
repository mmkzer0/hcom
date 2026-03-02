//! hcom: High-performance PTY wrapper, daemon client, and TUI
//!
//! Routing (handled by router module):
//!   hcom pty <tool> [args...]     - PTY wrapper mode
//!   hcom <hook> [payload]         - Hook handler (Claude/Gemini/Codex/OpenCode)
//!   hcom <command> [args...]      - CLI command (send, list, events, ...)
//!   hcom [N] <tool> [args...]     - Launch tool (claude, gemini, codex, opencode)
//!   hcom --new-terminal           - Open TUI in new terminal window
//!   hcom                          - TUI mode
//!
//! PTY mode outputs on startup:
//!   INJECT_PORT=<port>   - TCP port for text injection
//!   STATE_PORT=<port>    - TCP port for state queries
//!   READY                - Signal that PTY is ready for use

mod bootstrap;
mod cli_context;
pub mod commands;
mod config;
pub mod core;
mod db;
mod delivery;
pub mod hooks;
pub mod identity;
pub mod launcher;
mod instances;
mod log;
pub mod messages;
mod notify;
mod paths;
mod pidtrack;
pub mod scripts;
pub mod relay;
mod pty;
pub mod router;
pub mod shared;
pub mod terminal;
mod tool;
pub mod tools;
mod transcript;
mod tui;

use anyhow::{Context, Result, bail};
use std::panic;
use std::str::FromStr;

fn main() -> Result<()> {
    // Initialize global config from environment variables
    config::Config::init();

    // Set custom panic hook to log to file instead of stderr (prevents TUI corruption)
    panic::set_hook(Box::new(|panic_info| {
        let location = panic_info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "unknown".to_string());
        let message = if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "unknown panic".to_string()
        };
        log::log_error("native", "panic", &format!("{} at {}", message, location));
    }));

    // Dispatch via router (replaces manual MainAction matching)
    router::dispatch()
}

/// Run PTY wrapper mode.
pub fn run_pty(args: &[String]) -> Result<()> {
    if args.is_empty() || args[0] == "--help" || args[0] == "-h" {
        eprintln!("hcom pty - PTY wrapper for hcom");
        eprintln!();
        eprintln!("Usage: hcom pty <tool> [args...]");
        eprintln!();
        eprintln!("Tools: claude, gemini, codex");
        eprintln!();
        eprintln!("The PTY wrapper provides:");
        eprintln!("  - Text injection via TCP port (INJECT_PORT)");
        eprintln!("  - State queries via TCP port (STATE_PORT)");
        eprintln!("  - Ready detection for tool startup");
        eprintln!();
        eprintln!("Environment:");
        eprintln!("  HCOM_INSTANCE_NAME    Instance name for logging");
        eprintln!("  HCOM_DIR              Custom hcom directory");
        if args.is_empty() {
            bail!("Tool name required");
        }
        return Ok(());
    }

    let tool_str = &args[0];
    let tool_args: Vec<&str> = args[1..].iter().map(|s| s.as_str()).collect();

    // Parse tool - use enum for known tools, raw string for testing arbitrary commands
    let (ready_pattern, tool_name) = match tool::Tool::from_str(tool_str) {
        Ok(tool) => (tool.ready_pattern().to_vec(), tool_str.to_string()),
        Err(_) => (vec![], tool_str.to_string()), // Allow arbitrary commands for testing
    };

    let instance_name = config::Config::get().instance_name;

    // Build command (use original string for execve)
    let command = tool_str.as_str();

    // Create and run PTY
    let mut proxy = pty::Proxy::spawn(
        command,
        &tool_args,
        pty::ProxyConfig {
            ready_pattern,
            instance_name,
            tool: tool_name,
        },
    )
    .context("Failed to spawn PTY")?;

    let exit_code = proxy.run().context("PTY run failed")?;

    // Drop proxy to run cleanup (join delivery thread, which does DB cleanup)
    drop(proxy);

    std::process::exit(exit_code);
}

#[cfg(test)]
mod tests {
    use crate::router::{self, Action};

    fn args(s: &[&str]) -> Vec<String> {
        s.iter().map(|s| s.to_string()).collect()
    }

    /// Test that no args runs Rust TUI
    #[test]
    fn test_no_args_runs_rust_tui() {
        let action = router::resolve_action(&[]);
        assert_eq!(action, Action::Tui);
    }

    /// Test that PTY mode is correctly identified
    #[test]
    fn test_pty_mode() {
        let action = router::resolve_action(&args(&["pty", "claude"]));
        assert_eq!(
            action,
            Action::Pty {
                args: args(&["claude"])
            }
        );
    }

    /// Test that client mode is correctly identified for non-pty commands
    #[test]
    fn test_client_mode() {
        let action = router::resolve_action(&args(&["list"]));
        match action {
            Action::Command { cmd, .. } => assert_eq!(cmd, "list"),
            _ => panic!("Expected Command action, got {:?}", action),
        }
    }

    /// Test PTY mode with multiple args
    #[test]
    fn test_pty_mode_with_args() {
        let action = router::resolve_action(&args(&["pty", "claude", "--arg1", "--arg2"]));
        assert_eq!(
            action,
            Action::Pty {
                args: args(&["claude", "--arg1", "--arg2"])
            }
        );
    }
}
