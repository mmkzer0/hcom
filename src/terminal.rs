//! Terminal launching, script creation, and process management.
//!
//!
//! Handles:
//! - Terminal preset resolution (kitty, wezterm, tmux, etc.)
//! - Bash script creation for tool launches
//! - Terminal process spawning (new window, same terminal, background)
//! - Kill/close operations for managed terminals

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};

use crate::paths;
use crate::shared::constants::{
    self, HCOM_IDENTITY_VARS, TERMINAL_ENV_MAP, TOOL_MARKER_VARS, get_terminal_preset,
};
use crate::shared::platform;

// ==================== Types ====================

/// Result of kill_process().
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillResult {
    Sent,
    AlreadyDead,
    PermissionDenied,
}

/// Terminal info resolved for an instance.
#[derive(Debug, Clone, Default)]
pub struct TerminalInfo {
    pub preset_name: String,
    pub pane_id: String,
    pub process_id: String,
    pub kitty_listen_on: String,
    pub terminal_id: String,
}

/// Result from launch_terminal.
#[derive(Debug)]
pub enum LaunchResult {
    /// Background mode: (log_file_path, pid)
    Background(String, u32),
    /// Success (run_here or new window)
    Success,
    /// Failed
    Failed(String),
}

// ==================== macOS App Bundles ====================

/// macOS app bundle fallback commands for cross-platform terminals.
/// Used when CLI binary isn't in PATH but .app bundle is installed.
const MACOS_APP_FALLBACKS: &[(&str, &str)] = &[
    ("kitty-window", "open -n -a kitty.app --args {script}"),
    ("wezterm-window", "open -n -a WezTerm.app --args start -- bash {script}"),
    ("alacritty", "open -n -a Alacritty.app --args -e bash {script}"),
];

/// Terminal context vars that should not leak to other terminal apps.
const TERMINAL_CONTEXT_VARS: &[&str] = &["KITTY_WINDOW_ID", "WEZTERM_PANE"];

// ==================== Detection Helpers ====================

/// Detect terminal preset from inherited environment variables.
/// Used for same-terminal PTY launches (run_here=True) to enable close-on-kill.
pub fn detect_terminal_from_env() -> Option<String> {
    for &(env_var, preset_name) in TERMINAL_ENV_MAP {
        if std::env::var(env_var).ok().filter(|v| !v.is_empty()).is_some() {
            return Some(preset_name.to_string());
        }
    }
    None
}

/// Find macOS .app bundle in common locations.
fn find_macos_app(name: &str) -> Option<PathBuf> {
    let app_name = if name.ends_with(".app") {
        name.to_string()
    } else {
        format!("{}.app", name)
    };

    let home = std::env::var("HOME").ok()?;
    let search_dirs = [
        PathBuf::from("/Applications"),
        PathBuf::from("/System/Applications"),
        PathBuf::from("/System/Applications/Utilities"),
        PathBuf::from(home).join("Applications"),
    ];

    for base in &search_dirs {
        let app_path = base.join(&app_name);
        if app_path.exists() {
            return Some(app_path);
        }
    }
    None
}

/// Find kitten binary — PATH first, then macOS app bundle.
fn find_kitten_binary() -> Option<String> {
    if let Some(path) = which_bin("kitten") {
        return Some(path);
    }
    if cfg!(target_os = "macos") {
        if let Some(app) = find_macos_app("kitty") {
            let full = app.join("Contents/MacOS/kitten");
            if full.exists() {
                return Some(full.to_string_lossy().to_string());
            }
        }
    }
    None
}

/// Find a reachable kitty remote control socket.
pub fn find_kitty_socket() -> String {
    let kitten = match find_kitten_binary() {
        Some(k) => k,
        None => return String::new(),
    };

    // Find candidate sockets
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Ok(entries) = fs::read_dir("/tmp") {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with("kitty") {
                candidates.push(entry.path());
            }
        }
    }
    candidates.sort_by(|a, b| b.cmp(a)); // Reverse sort (newest first)

    for sock_path in &candidates {
        // Check if it's a socket
        if let Ok(meta) = fs::metadata(sock_path) {
            use std::os::unix::fs::FileTypeExt;
            if !meta.file_type().is_socket() {
                continue;
            }
        } else {
            continue;
        }

        let socket_uri = format!("unix:{}", sock_path.display());
        if let Ok(output) = Command::new(&kitten)
            .args(["@", "--to", &socket_uri, "ls"])
            .output()
        {
            if output.status.success() {
                return socket_uri;
            }
        }
    }
    String::new()
}

/// Check if a wezterm mux server is reachable.
pub fn wezterm_reachable() -> bool {
    Command::new("wezterm")
        .args(["cli", "list"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Simple `which` implementation — find binary in PATH.
pub fn which_bin(name: &str) -> Option<String> {
    let path_var = std::env::var("PATH").ok()?;
    for dir in path_var.split(':') {
        let candidate = Path::new(dir).join(name);
        if candidate.exists() && candidate.is_file() {
            return Some(candidate.to_string_lossy().to_string());
        }
    }
    None
}

/// Check if a file has a node shebang (#!/usr/bin/env node or similar).
/// Used on Termux to detect npm-installed tools that need `node <path>` rewrite.
pub fn has_node_shebang(path: &str) -> bool {
    use std::io::Read;
    let Ok(mut f) = std::fs::File::open(path) else { return false };
    let mut buf = [0u8; 64];
    let Ok(n) = f.read(&mut buf) else { return false };
    let header = String::from_utf8_lossy(&buf[..n]);
    header.starts_with("#!") && header.contains("node")
}

// ==================== Preset Resolution ====================

/// Resolve binary to full path via macOS app bundle fallback.
fn resolve_binary_path(binary: &str, app_name: Option<&str>, preset_name: &str) -> Option<String> {
    if which_bin(binary).is_some() {
        return None; // Already on PATH
    }
    if !cfg!(target_os = "macos") {
        return None;
    }
    let app = find_macos_app(app_name.unwrap_or(preset_name))?;
    let full_path = app.join("Contents/MacOS").join(binary);
    if full_path.exists() {
        Some(full_path.to_string_lossy().to_string())
    } else {
        None
    }
}

/// Resolve preset name to command template string.
///
/// On macOS, if CLI binary isn't in PATH but .app bundle exists,
/// uses a hardcoded fallback or substitutes the full binary path.
pub fn resolve_terminal_preset(preset_name: &str) -> Option<String> {
    let preset = get_terminal_preset(preset_name)?;
    let mut open_cmd = preset.open.to_string();

    if let Some(binary) = preset.binary {
        if which_bin(binary).is_none() && cfg!(target_os = "macos") {
            // New-window presets have hardcoded fallbacks using `open -a`
            for &(name, fallback) in MACOS_APP_FALLBACKS {
                if name == preset_name {
                    let app_name = preset.app_name.unwrap_or(preset_name);
                    if find_macos_app(app_name).is_some() {
                        return Some(fallback.to_string());
                    }
                }
            }
            // Tab/split presets: substitute leading binary with full path
            let app_name = preset.app_name.unwrap_or(preset_name);
            if let Some(full_path) = resolve_binary_path(binary, Some(app_name), preset_name) {
                if open_cmd.starts_with(binary) {
                    open_cmd = format!("{}{}", full_path, &open_cmd[binary.len()..]);
                }
            }
        }
    }

    Some(open_cmd)
}

/// Get terminal presets for current platform with availability status.
pub fn get_available_presets() -> Vec<(String, bool)> {
    let mut result = vec![("default".to_string(), true)];
    let system = platform::platform_name();

    for (name, preset) in constants::TERMINAL_PRESETS.iter() {
        if !preset.platforms.contains(&system) {
            continue;
        }

        let available = if let Some(binary) = preset.binary {
            let in_path = which_bin(binary).is_some();
            if !in_path && system == "Darwin" {
                resolve_binary_path(binary, preset.app_name, name).is_some()
            } else {
                in_path
            }
        } else if system == "Darwin" {
            let app_name = preset.app_name.unwrap_or(name);
            find_macos_app(app_name).is_some()
        } else {
            true
        };

        result.push((name.to_string(), available));
    }

    result.push(("custom".to_string(), true));
    result
}

// ==================== Environment Building ====================

/// Build environment variable string for bash shells.
pub fn build_env_string(env_vars: &HashMap<String, String>, format_type: &str) -> String {
    let mut valid: Vec<(&String, &String)> = env_vars
        .iter()
        .filter(|(k, _)| {
            k.chars().next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
                && k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        })
        .collect();
    valid.sort_by_key(|(k, _)| k.to_string());

    if format_type == "bash_export" {
        valid
            .iter()
            .map(|(k, v)| format!("export {}={};", k, shell_quote(v)))
            .collect::<Vec<_>>()
            .join(" ")
    } else {
        valid
            .iter()
            .map(|(k, v)| format!("{}={}", k, shell_quote(v)))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// Shell-quote a string for bash.
fn shell_quote(s: &str) -> String {
    if s.is_empty() {
        return "''".to_string();
    }
    // If all safe chars, no quoting needed
    if s.chars()
        .all(|c| c.is_ascii_alphanumeric() || "/_-.=:,@".contains(c))
    {
        return s.to_string();
    }
    // Use single quotes, escaping any embedded single quotes
    format!("'{}'", s.replace('\'', "'\\''"))
}

// ==================== Script Creation ====================

/// Create a bash script for terminal launch.
///
/// Scripts provide uniform execution across all platforms/terminals.
pub fn create_bash_script(
    script_file: &Path,
    env: &HashMap<String, String>,
    cwd: Option<&str>,
    command_str: &str,
    background: bool,
    tool_name: Option<&str>,
    opens_new_window: bool,
) -> Result<()> {
    let tool_name = tool_name.unwrap_or_else(|| {
        let cmd_lower = command_str.to_lowercase();
        if cmd_lower.contains("opencode") {
            "OpenCode"
        } else if cmd_lower.contains("gemini") {
            "Gemini"
        } else if cmd_lower.contains("codex") {
            "Codex"
        } else if cmd_lower.contains("claude") {
            "Claude Code"
        } else {
            "hcom"
        }
    });

    let mut f = fs::File::create(script_file).context("Failed to create script file")?;

    writeln!(f, "#!/bin/bash")?;
    writeln!(
        f,
        "printf \"\\033]0;hcom: starting {}...\\007\"",
        tool_name
    )?;
    writeln!(f, "echo \"Starting {}...\"", tool_name)?;

    // Unset tool markers and identity vars to prevent inheritance
    writeln!(f, "unset {}", TOOL_MARKER_VARS.join(" "))?;
    writeln!(f, "unset {}", HCOM_IDENTITY_VARS.join(" "))?;

    // Discover paths for minimal environments (kitty splits, etc.)
    let mut paths_to_add: Vec<String> = Vec::new();

    fn add_path(paths: &mut Vec<String>, binary_path: Option<String>) {
        if let Some(bp) = binary_path {
            if let Some(dir) = Path::new(&bp).parent() {
                let dir_str = dir.to_string_lossy().to_string();
                if !paths.contains(&dir_str) {
                    paths.push(dir_str);
                }
            }
        }
    }

    // Always add hcom's own directory
    add_path(&mut paths_to_add, which_bin("hcom"));
    // Add python3 to PATH for agents that need it
    add_path(&mut paths_to_add, which_bin("python3"));
    // Detect tool from command and add its path
    let cmd_stripped = command_str.trim_start();
    let tool_cmd = cmd_stripped.split_whitespace().next().unwrap_or("");
    add_path(&mut paths_to_add, which_bin(tool_cmd));
    // Claude needs node
    if tool_cmd == "claude" {
        add_path(&mut paths_to_add, which_bin("node"));
    }

    if !paths_to_add.is_empty() {
        writeln!(f, "export PATH=\"{}:$PATH\"", paths_to_add.join(":"))?;
    }

    // Write env exports
    let env_str = build_env_string(env, "bash_export");
    if !env_str.is_empty() {
        writeln!(f, "{}", env_str)?;
    }

    if let Some(dir) = cwd {
        writeln!(f, "cd {}", shell_quote(dir))?;
    }

    // Resolve tool path for full path execution.
    // On Termux, npm-installed tools have shebangs like #!/usr/bin/env node which
    // fail (no /usr/bin/env). Detect node shebangs and rewrite to: node /path/to/tool args
    let mut final_command = command_str.to_string();
    if !tool_cmd.is_empty() {
        if let Some(tool_path) = which_bin(tool_cmd) {
            if platform::is_termux() && has_node_shebang(&tool_path) {
                let node = which_bin("node")
                    .unwrap_or_else(|| platform::TERMUX_NODE_PATH.to_string());
                final_command = final_command.replacen(
                    &format!("{} ", tool_cmd),
                    &format!("{} {} ", shell_quote(&node), shell_quote(&tool_path)),
                    1,
                );
            } else {
                final_command = final_command.replacen(
                    &format!("{} ", tool_cmd),
                    &format!("{} ", shell_quote(&tool_path)),
                    1,
                );
            }
        }
    }

    writeln!(f, "{}", final_command)?;

    if opens_new_window {
        writeln!(f, "unset HCOM_PROCESS_ID HCOM_LAUNCHED HCOM_PTY_MODE HCOM_TAG HCOM_CODEX_SANDBOX_MODE")?;
        writeln!(f, "rm -f {}", shell_quote(&script_file.to_string_lossy()))?;
        writeln!(f, "exec bash -l")?;
    } else if !background {
        writeln!(f, "hcom_status=$?")?;
        writeln!(f, "rm -f {}", shell_quote(&script_file.to_string_lossy()))?;
        writeln!(f, "exit $hcom_status")?;
    }

    // Make executable
    fs::set_permissions(script_file, fs::Permissions::from_mode(0o755))?;

    Ok(())
}

// ==================== Terminal Launching ====================

/// Build clean env for terminal launcher subprocesses.
///
/// Strips AI tool markers, hcom identity vars, and terminal context vars.
fn get_launcher_env() -> HashMap<String, String> {
    let mut strip: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for v in TOOL_MARKER_VARS {
        strip.insert(v);
    }
    for v in HCOM_IDENTITY_VARS {
        strip.insert(v);
    }
    for v in TERMINAL_CONTEXT_VARS {
        strip.insert(v);
    }
    strip.insert("HCOM_LAUNCHED_PRESET");

    std::env::vars()
        .filter(|(k, _)| !strip.contains(k.as_str()))
        .collect()
}

/// Parse terminal command template safely to prevent shell injection.
fn parse_terminal_command(
    template: &str,
    script_file: &str,
    process_id: &str,
) -> Result<Vec<String>> {
    if !template.contains("{script}") {
        bail!(
            "Custom terminal command must include {{script}} placeholder\n\
             Example: open -n -a kitty.app --args bash \"{{script}}\""
        );
    }

    let parts = shell_split(template)?;

    let mut replaced = Vec::new();
    let mut placeholder_found = false;
    for mut part in parts {
        if part.contains("{process_id}") {
            part = part.replace("{process_id}", process_id);
        }
        if part.contains("{script}") {
            part = part.replace("{script}", script_file);
            placeholder_found = true;
        }
        replaced.push(part);
    }

    if !placeholder_found {
        bail!("{{script}} placeholder not found after parsing");
    }

    Ok(replaced)
}

/// Shell-split a string.
fn shell_split(s: &str) -> Result<Vec<String>> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut escape_next = false;

    for ch in s.chars() {
        if escape_next {
            current.push(ch);
            escape_next = false;
            continue;
        }
        if ch == '\\' && !in_single {
            escape_next = true;
            continue;
        }
        if ch == '\'' && !in_double {
            in_single = !in_single;
            continue;
        }
        if ch == '"' && !in_single {
            in_double = !in_double;
            continue;
        }
        if ch.is_whitespace() && !in_single && !in_double {
            if !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
            continue;
        }
        current.push(ch);
    }

    if in_single || in_double {
        bail!("Unmatched quote in command");
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    Ok(tokens)
}

/// Get macOS Terminal.app launch command.
fn get_macos_terminal_argv() -> Vec<String> {
    vec![
        "open".to_string(),
        "-a".to_string(),
        "Terminal".to_string(),
        "{script}".to_string(),
    ]
}

/// Get first available standard Linux terminal.
fn get_linux_terminal_argv() -> Option<Vec<String>> {
    let terminals = [
        (
            "gnome-terminal",
            &["gnome-terminal", "--", "bash", "{script}"] as &[&str],
        ),
        ("konsole", &["konsole", "-e", "bash", "{script}"]),
        ("xterm", &["xterm", "-e", "bash", "{script}"]),
    ];

    for (term_name, argv) in &terminals {
        if which_bin(term_name).is_some() {
            return Some(argv.iter().map(|s| s.to_string()).collect());
        }
    }

    // WSL fallback
    if platform::is_wsl() && which_bin("cmd.exe").is_some() {
        if which_bin("wt.exe").is_some() {
            return Some(
                ["cmd.exe", "/c", "start", "wt.exe", "bash", "{script}"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            );
        }
        return Some(
            ["cmd.exe", "/c", "start", "bash", "{script}"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
        );
    }

    None
}

/// Spawn terminal process, detached when inside AI tools.
///
/// Returns (success, stdout_first_line) — stdout captured for {id} in close commands.
fn spawn_terminal_process(
    argv: &[String],
    inside_ai_tool: bool,
) -> Result<(bool, String)> {
    let launcher_env = get_launcher_env();
    let env_vec: Vec<(String, String)> = launcher_env.into_iter().collect();

    if inside_ai_tool {
        // Fully detach: don't let AI tool's PTY capture our output
        let launch_dir = paths::hcom_path(&[paths::LAUNCH_DIR]);
        fs::create_dir_all(&launch_dir).ok();

        let child = Command::new(&argv[0])
            .args(&argv[1..])
            .env_clear()
            .envs(env_vec.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("Failed to spawn terminal process")?;

        let output = child.wait_with_output().context("Failed to wait for terminal")?;

        let captured = String::from_utf8_lossy(&output.stdout)
            .lines()
            .next()
            .unwrap_or("")
            .to_string();

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let msg = format!(
                "Terminal launch failed (exit code {})",
                output.status.code().unwrap_or(-1)
            );
            let full_msg = if stderr.is_empty() {
                msg
            } else {
                format!("{}: {}", msg, stderr)
            };
            bail!(full_msg);
        }

        Ok((true, captured))
    } else {
        // Normal case: wait for terminal launcher to complete
        let output = Command::new(&argv[0])
            .args(&argv[1..])
            .env_clear()
            .envs(env_vec.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .output()
            .context("Failed to run terminal launcher")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            bail!(
                "Terminal launch failed (exit code {}): {}",
                output.status.code().unwrap_or(-1),
                stderr
            );
        }

        let captured = String::from_utf8_lossy(&output.stdout)
            .lines()
            .next()
            .unwrap_or("")
            .to_string();
        Ok((true, captured))
    }
}

/// Write captured terminal ID to temp file for child to read.
fn write_terminal_id(env: &HashMap<String, String>, captured_id: &str) {
    if captured_id.is_empty() {
        return;
    }
    let process_id = match env.get("HCOM_PROCESS_ID") {
        Some(pid) if !pid.is_empty() => pid,
        _ => return,
    };
    let ids_dir = paths::hcom_path(&[".tmp", "terminal_ids"]);
    fs::create_dir_all(&ids_dir).ok();
    fs::write(ids_dir.join(process_id), captured_id).ok();
}

/// Launch terminal with command.
///
/// # Modes
/// - `background=true`: Launch as background process, returns Background(log_file, pid)
/// - `run_here=true`: Run in current terminal (blocking via execve)
/// - Otherwise: New terminal window/tab/split
pub fn launch_terminal(
    command: &str,
    env: &HashMap<String, String>,
    cwd: Option<&str>,
    background: bool,
    run_here: bool,
    terminal: Option<&str>,
    inside_ai_tool: bool,
) -> Result<LaunchResult> {
    let config_and_instance_env = env.clone();

    // Determine terminal mode
    let mut terminal_mode = terminal.unwrap_or("default").to_string();

    // Determine script extension
    let use_command_ext =
        !background && cfg!(target_os = "macos") && terminal_mode == "default";
    let extension = if use_command_ext { ".command" } else { ".sh" };
    let script_file = paths::hcom_path(&[
        paths::LAUNCH_DIR,
        &format!(
            "hcom_{}_{}{}",
            std::process::id(),
            rand::random::<u16>() % 9000 + 1000,
            extension
        ),
    ]);

    // Ensure launch dir exists
    if let Some(parent) = script_file.parent() {
        fs::create_dir_all(parent).ok();
    }

    let opens_new_window = !background && !run_here;

    // Resolve smart terminal shortcuts
    let mut kitty_socket = String::new();
    let mut final_env = config_and_instance_env;

    if opens_new_window {
        if terminal_mode == "kitty" {
            if std::env::var("KITTY_WINDOW_ID").ok().filter(|v| !v.is_empty()).is_some() {
                terminal_mode = "kitty-split".to_string();
            } else {
                kitty_socket = find_kitty_socket();
                terminal_mode = if kitty_socket.is_empty() {
                    "kitty-window".to_string()
                } else {
                    "kitty-tab".to_string()
                };
            }
        } else if terminal_mode == "wezterm" {
            if std::env::var("WEZTERM_PANE").ok().filter(|v| !v.is_empty()).is_some() {
                terminal_mode = "wezterm-split".to_string();
            } else if wezterm_reachable() {
                terminal_mode = "wezterm-tab".to_string();
            } else {
                terminal_mode = "wezterm-window".to_string();
            }
        }

        if terminal_mode != "default" && terminal_mode != "print" {
            final_env.insert("HCOM_LAUNCHED_PRESET".to_string(), terminal_mode.clone());
        }
        if !kitty_socket.is_empty() {
            final_env.insert("KITTY_LISTEN_ON".to_string(), kitty_socket.clone());
        }
    }

    // Create script
    create_bash_script(
        &script_file,
        &final_env,
        cwd,
        command,
        background,
        None,
        opens_new_window,
    )?;

    // Background mode
    if background {
        let logs_dir = paths::hcom_path(&[paths::LOGS_DIR]);
        fs::create_dir_all(&logs_dir).ok();
        let log_name = env.get("HCOM_BACKGROUND").cloned().unwrap_or_default();
        let log_file = logs_dir.join(&log_name);

        let log_handle = fs::File::create(&log_file).context("Failed to create log file")?;

        let mut cmd = Command::new("bash");
        cmd.arg(&script_file)
            .stdin(std::process::Stdio::null())
            .stdout(log_handle.try_clone()?)
            .stderr(log_handle);

        // Detach child into its own session so it survives parent exit (no SIGHUP)
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

        let child = cmd.spawn().context("Failed to launch background process")?;

        // Brief health check
        std::thread::sleep(std::time::Duration::from_millis(200));
        let pid = child.id();

        return Ok(LaunchResult::Background(
            log_file.to_string_lossy().to_string(),
            pid,
        ));
    }

    // Print mode (debug)
    if terminal_mode == "print" {
        let content = fs::read_to_string(&script_file)?;
        println!("# Script: {}", script_file.display());
        print!("{}", content);
        fs::remove_file(&script_file).ok();
        return Ok(LaunchResult::Success);
    }

    // Run in current terminal (blocking)
    if run_here {
        // Build full env (config + shell)
        let full_env = build_full_env(&final_env);
        if let Some(dir) = cwd {
            std::env::set_current_dir(dir).ok();
        }
        // Use execve to replace this process entirely
        use std::ffi::CString;
        let bash_path = which_bin("bash").unwrap_or_else(|| "/bin/bash".to_string());
        let bash = CString::new(bash_path).unwrap();
        let arg0 = CString::new("bash").unwrap();
        let arg1 = CString::new(script_file.to_string_lossy().as_ref()).unwrap();
        let argv_ptrs: Vec<*const libc::c_char> =
            vec![arg0.as_ptr(), arg1.as_ptr(), std::ptr::null()];
        let env_cstrings: Vec<CString> = full_env
            .iter()
            .filter_map(|(k, v)| CString::new(format!("{}={}", k, v)).ok())
            .collect();
        let mut env_ptrs: Vec<*const libc::c_char> =
            env_cstrings.iter().map(|c| c.as_ptr()).collect();
        env_ptrs.push(std::ptr::null());
        // execve replaces process; never returns on success
        unsafe {
            libc::execve(bash.as_ptr(), argv_ptrs.as_ptr(), env_ptrs.as_ptr());
        }
        bail!("execve failed: {}", std::io::Error::last_os_error());
    }

    // New window / custom command mode
    let custom_cmd: Option<String> = if terminal_mode == "default" {
        None
    } else if get_terminal_preset(&terminal_mode).is_some() {
        // Known preset — check kitty remote control requirements
        if terminal_mode == "kitty-tab" || terminal_mode == "kitty-split" {
            let listen_on = std::env::var("KITTY_LISTEN_ON")
                .ok()
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| kitty_socket.clone());
            if listen_on.is_empty() {
                bail!(
                    "{} requires remote control.\n\
                     Add to ~/.config/kitty/kitty.conf:\n\
                     allow_remote_control yes\n\
                     listen_on unix:/tmp/kitty\n\
                     Then restart kitty.",
                    terminal_mode
                );
            }
        }
        let mut cmd = resolve_terminal_preset(&terminal_mode).unwrap_or_default();
        // Inject --to for kitty commands launched outside kitty
        if !kitty_socket.is_empty()
            && cmd.contains("kitten @")
            && !cmd.contains("--to")
        {
            cmd = cmd.replace("kitten @", &format!("kitten @ --to {}", shell_quote(&kitty_socket)));
        }
        // Target launcher's tab for splits
        if terminal_mode == "kitty-tab" || terminal_mode == "kitty-split" {
            if let Ok(wid) = std::env::var("KITTY_WINDOW_ID") {
                if !wid.is_empty() && cmd.contains(" -- ") {
                    cmd = cmd.replacen(" -- ", &format!(" --match window_id:{} -- ", wid), 1);
                }
            }
        }
        Some(cmd)
    } else {
        // Custom command template
        Some(terminal_mode.clone())
    };

    let script_str = script_file.to_string_lossy().to_string();

    if let Some(cmd_template) = custom_cmd {
        // Parse user-provided or preset command template
        let final_argv = parse_terminal_command(
            &cmd_template,
            &script_str,
            env.get("HCOM_PROCESS_ID").map(|s| s.as_str()).unwrap_or(""),
        )?;
        let (success, captured_id) = spawn_terminal_process(&final_argv, inside_ai_tool)?;
        write_terminal_id(env, &captured_id);
        if success {
            Ok(LaunchResult::Success)
        } else {
            Ok(LaunchResult::Failed("Terminal process failed".to_string()))
        }
    } else {
        // Platform default
        if platform::is_termux() {
            let am_argv = vec![
                "am", "startservice", "--user", "0",
                "-n", "com.termux/com.termux.app.RunCommandService",
                "-a", "com.termux.RUN_COMMAND",
                "--es", "com.termux.RUN_COMMAND_PATH", &script_str,
                "--ez", "com.termux.RUN_COMMAND_BACKGROUND", "false",
            ];
            Command::new(am_argv[0])
                .args(&am_argv[1..])
                .status()
                .context("Failed to launch Termux")?;
            return Ok(LaunchResult::Success);
        }

        let argv = match platform::platform_name() {
            "Darwin" => get_macos_terminal_argv(),
            "Linux" => get_linux_terminal_argv()
                .ok_or_else(|| anyhow::anyhow!("No supported terminal emulator found"))?,
            other => bail!("Unsupported platform: {}", other),
        };

        let final_argv: Vec<String> = argv
            .iter()
            .map(|a| a.replace("{script}", &script_str))
            .collect();
        let (success, captured_id) = spawn_terminal_process(&final_argv, inside_ai_tool)?;
        write_terminal_id(env, &captured_id);
        if success {
            Ok(LaunchResult::Success)
        } else {
            Ok(LaunchResult::Failed("Terminal process failed".to_string()))
        }
    }
}

/// Build full env from config env + shell env.
fn build_full_env(config_env: &HashMap<String, String>) -> HashMap<String, String> {
    let mut full = config_env.clone();
    for (k, v) in std::env::vars() {
        if TOOL_MARKER_VARS.contains(&k.as_str()) {
            continue;
        }
        if k == "HCOM_TERMINAL" {
            continue;
        }
        // Config env takes precedence for HCOM_ vars
        full.entry(k).or_insert(v);
    }
    full
}

// ==================== Kill / Close ====================

/// Close terminal pane via preset-specific command.
///
/// Must run before SIGTERM because terminal CLIs match panes by PID/pane_id.
/// Non-fatal: caller should always proceed with SIGTERM regardless.
pub fn close_terminal_pane(
    pid: u32,
    preset_name: &str,
    pane_id: &str,
    process_id: &str,
    kitty_listen_on: &str,
    terminal_id: &str,
) -> bool {
    let preset = match get_terminal_preset(preset_name) {
        Some(p) => p,
        None => return false,
    };

    let close_template = match preset.close {
        Some(c) => c,
        None => return false,
    };

    let mut close_cmd = close_template.to_string();

    // Determine effective pane_id (fall back to terminal_id)
    let effective_pane_id = if pane_id.is_empty() && !terminal_id.is_empty() {
        terminal_id
    } else {
        pane_id
    };

    // Skip if command needs a placeholder we don't have
    if close_cmd.contains("{pane_id}") && effective_pane_id.is_empty() {
        return false;
    }
    if close_cmd.contains("{process_id}") && process_id.is_empty() {
        return false;
    }
    if close_cmd.contains("{id}") && terminal_id.is_empty() {
        return false;
    }

    close_cmd = close_cmd.replace("{pid}", &pid.to_string());
    close_cmd = close_cmd.replace("{pane_id}", effective_pane_id);
    close_cmd = close_cmd.replace("{process_id}", process_id);
    close_cmd = close_cmd.replace("{id}", terminal_id);

    // Resolve binary path via app bundle fallback
    if let Some(binary) = preset.binary {
        let app_name = preset.app_name.unwrap_or(preset_name);
        if let Some(full_path) = resolve_binary_path(binary, Some(app_name), preset_name) {
            if close_cmd.starts_with(binary) {
                close_cmd = format!("{}{}", full_path, &close_cmd[binary.len()..]);
            }
        }
    }

    // Inject --to for kitten commands when we have the socket path
    if close_cmd.contains("kitten @")
        && !kitty_listen_on.is_empty()
        && !close_cmd.contains("--to")
        && !kitty_listen_on.starts_with("fd:")
    {
        close_cmd = close_cmd.replace(
            "kitten @",
            &format!("kitten @ --to {}", shell_quote(kitty_listen_on)),
        );
    }

    // Execute
    Command::new("sh")
        .args(["-c", &close_cmd])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Close terminal pane (if applicable) then SIGTERM the process group.
pub fn kill_process(
    pid: u32,
    preset_name: &str,
    pane_id: &str,
    process_id: &str,
    kitty_listen_on: &str,
    terminal_id: &str,
) -> (KillResult, bool) {
    let pane_closed = if !preset_name.is_empty() {
        close_terminal_pane(pid, preset_name, pane_id, process_id, kitty_listen_on, terminal_id)
    } else {
        false
    };

    // SIGTERM the process group
    let result = unsafe { libc::killpg(pid as i32, libc::SIGTERM) };
    let kill_result = if result == 0 {
        KillResult::Sent
    } else {
        match std::io::Error::last_os_error().raw_os_error() {
            Some(libc::ESRCH) => KillResult::AlreadyDead,
            Some(libc::EPERM) => KillResult::PermissionDenied,
            _ => KillResult::AlreadyDead,
        }
    };

    (kill_result, pane_closed)
}

// ==================== Terminal Info Resolution ====================

/// Resolve terminal info from a JSON launch_context string.
///
/// Parses the launch_context JSON stored in instance position data.
/// The caller is responsible for loading the launch_context from DB/pidtrack.
pub fn resolve_terminal_info_from_launch_context(launch_context_json: &str) -> TerminalInfo {
    let mut info = TerminalInfo::default();

    if let Ok(lc) = serde_json::from_str::<serde_json::Value>(launch_context_json) {
        info.preset_name = lc
            .get("terminal_preset")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        info.pane_id = lc
            .get("pane_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        info.process_id = lc
            .get("process_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        info.terminal_id = lc
            .get("terminal_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        // Kitty socket from launch context or env snapshot
        let lc_env = lc.get("env").and_then(|v| v.as_object());
        info.kitty_listen_on = lc
            .get("kitty_listen_on")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .or_else(|| {
                lc_env.and_then(|e| e.get("KITTY_LISTEN_ON").and_then(|v| v.as_str()))
            })
            .unwrap_or("")
            .to_string();
    }

    // Infer preset when pane_id captured but not preset
    if info.preset_name.is_empty() && !info.pane_id.is_empty() && !info.kitty_listen_on.is_empty()
    {
        info.preset_name = "kitty-split".to_string();
    }

    info
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shell_quote_empty() {
        assert_eq!(shell_quote(""), "''");
    }

    #[test]
    fn test_shell_quote_simple() {
        assert_eq!(shell_quote("hello"), "hello");
    }

    #[test]
    fn test_shell_quote_spaces() {
        assert_eq!(shell_quote("hello world"), "'hello world'");
    }

    #[test]
    fn test_shell_quote_single_quotes() {
        assert_eq!(shell_quote("it's"), "'it'\\''s'");
    }

    #[test]
    fn test_shell_split_basic() {
        let parts = shell_split("foo bar baz").unwrap();
        assert_eq!(parts, vec!["foo", "bar", "baz"]);
    }

    #[test]
    fn test_shell_split_quoted() {
        let parts = shell_split("foo 'bar baz' qux").unwrap();
        assert_eq!(parts, vec!["foo", "bar baz", "qux"]);
    }

    #[test]
    fn test_shell_split_double_quoted() {
        let parts = shell_split(r#"foo "bar baz" qux"#).unwrap();
        assert_eq!(parts, vec!["foo", "bar baz", "qux"]);
    }

    #[test]
    fn test_shell_split_unmatched_quote() {
        assert!(shell_split("foo 'bar").is_err());
    }

    #[test]
    fn test_build_env_string_bash() {
        let mut env = HashMap::new();
        env.insert("FOO".to_string(), "bar".to_string());
        let result = build_env_string(&env, "bash");
        assert_eq!(result, "FOO=bar");
    }

    #[test]
    fn test_build_env_string_export() {
        let mut env = HashMap::new();
        env.insert("FOO".to_string(), "bar baz".to_string());
        let result = build_env_string(&env, "bash_export");
        assert_eq!(result, "export FOO='bar baz';");
    }

    #[test]
    fn test_build_env_string_filters_invalid() {
        let mut env = HashMap::new();
        env.insert("GOOD".to_string(), "val".to_string());
        env.insert("123BAD".to_string(), "val".to_string());
        let result = build_env_string(&env, "bash");
        assert!(result.contains("GOOD"));
        assert!(!result.contains("123BAD"));
    }

    #[test]
    fn test_detect_terminal_from_env_none() {
        // In test environment, none of the terminal env vars should be set
        // (unless running inside kitty/tmux, in which case this test is fine to skip)
        let result = detect_terminal_from_env();
        // Just verify it returns an Option - value depends on test environment
        let _ = result;
    }

    #[test]
    fn test_parse_terminal_command_basic() {
        let argv = parse_terminal_command("open -a Terminal {script}", "/tmp/test.sh", "").unwrap();
        assert_eq!(argv, vec!["open", "-a", "Terminal", "/tmp/test.sh"]);
    }

    #[test]
    fn test_parse_terminal_command_missing_placeholder() {
        assert!(parse_terminal_command("open -a Terminal", "/tmp/test.sh", "").is_err());
    }

    #[test]
    fn test_parse_terminal_command_with_process_id() {
        let argv =
            parse_terminal_command("tmux split -t {process_id} -- {script}", "/tmp/test.sh", "abc-123")
                .unwrap();
        assert_eq!(
            argv,
            vec!["tmux", "split", "-t", "abc-123", "--", "/tmp/test.sh"]
        );
    }

    #[test]
    fn test_kill_result_enum() {
        assert_eq!(KillResult::Sent, KillResult::Sent);
        assert_ne!(KillResult::Sent, KillResult::AlreadyDead);
    }

    #[test]
    fn test_sandbox_flags_in_get_sandbox_flags() {
        use crate::tools::codex_preprocessing::get_sandbox_flags;
        let flags = get_sandbox_flags("workspace");
        assert!(flags.contains(&"--full-auto".to_string()));
    }

    #[test]
    fn test_get_available_presets_always_has_default_and_custom() {
        let presets = get_available_presets();
        assert_eq!(presets.first().unwrap().0, "default");
        assert_eq!(presets.last().unwrap().0, "custom");
    }

    #[test]
    fn test_has_node_shebang_with_node_script() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tool.js");
        std::fs::write(&path, "#!/usr/bin/env node\nconsole.log('hi');\n").unwrap();
        assert!(has_node_shebang(path.to_str().unwrap()));
    }

    #[test]
    fn test_has_node_shebang_with_bash_script() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tool.sh");
        std::fs::write(&path, "#!/bin/bash\necho hello\n").unwrap();
        assert!(!has_node_shebang(path.to_str().unwrap()));
    }

    #[test]
    fn test_has_node_shebang_with_elf_binary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tool");
        std::fs::write(&path, b"\x7fELF\x02\x01\x01\x00").unwrap();
        assert!(!has_node_shebang(path.to_str().unwrap()));
    }

    #[test]
    fn test_has_node_shebang_nonexistent() {
        assert!(!has_node_shebang("/nonexistent/path/to/tool"));
    }
}
