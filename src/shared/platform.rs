//! Platform detection utilities.
//!

use std::sync::LazyLock;

/// Cached WSL detection result.
static IS_WSL: LazyLock<bool> = LazyLock::new(detect_wsl);

/// Cached Termux detection result.
static IS_TERMUX: LazyLock<bool> = LazyLock::new(detect_termux);

/// Whether running in WSL (Windows Subsystem for Linux).
pub fn is_wsl() -> bool {
    *IS_WSL
}

/// Whether running in Termux on Android.
pub fn is_termux() -> bool {
    *IS_TERMUX
}

/// Detect WSL by checking /proc/version for "microsoft".
fn detect_wsl() -> bool {
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/version")
            .map(|content| content.to_lowercase().contains("microsoft"))
            .unwrap_or(false)
    }
}

/// Detect Termux by checking env vars and paths.
fn detect_termux() -> bool {
    use std::env;
    use std::path::Path;

    // Primary: TERMUX_VERSION (works all versions)
    if env::var("TERMUX_VERSION").is_ok() {
        return true;
    }
    // Modern: TERMUX__ROOTFS (v0.119.0+)
    if env::var("TERMUX__ROOTFS").is_ok() {
        return true;
    }
    // Fallback: path check
    if Path::new("/data/data/com.termux").exists() {
        return true;
    }
    // Fallback: PREFIX check
    if let Ok(prefix) = env::var("PREFIX") {
        if prefix.contains("com.termux") {
            return true;
        }
    }
    false
}

/// Default Termux node path for shebang bypass.
pub const TERMUX_NODE_PATH: &str = "/data/data/com.termux/files/usr/bin/node";

/// Replace HOME prefix with ~ for display.
pub fn shorten_path(path: &str) -> String {
    if let Ok(home) = std::env::var("HOME") {
        if path == home {
            return "~".to_string();
        }
        if let Some(rest) = path.strip_prefix(home.as_str()) {
            if rest.starts_with('/') {
                return format!("~{rest}");
            }
        }
    }
    path.to_string()
}

/// Shorten path and truncate to max_width, keeping the trailing portion visible.
///
/// Example: "/very/long/path/to/some/project" with max 30 → ".../path/to/some/project"
pub fn shorten_path_max(path: &str, max_width: usize) -> String {
    let shortened = shorten_path(path);
    if shortened.len() <= max_width || max_width < 4 {
        return shortened;
    }
    // Keep the rightmost portion (more useful than left)
    let keep = max_width - 3; // "..."
    let start = shortened.len() - keep;
    // Find a char boundary (UTF-8 safe)
    let start = (start..shortened.len()).find(|&i| shortened.is_char_boundary(i)).unwrap_or(shortened.len());
    // Find a clean break at a '/' if possible
    if let Some(slash) = shortened[start..].find('/') {
        let pos = start + slash;
        if shortened.len() - pos <= max_width - 3 {
            return format!("...{}", &shortened[pos..]);
        }
    }
    format!("...{}", &shortened[start..])
}

/// Whether running inside any AI tool (env-var check, no HcomContext needed).
///
/// Uses the same env vars as HcomContext::from_env() for tool detection.
/// For code that already has an HcomContext, prefer `ctx.is_inside_ai_tool()`.
pub fn is_inside_ai_tool() -> bool {
    use std::env;
    let is_set = |k: &str| env::var(k).is_ok();
    let is_eq = |k: &str, v: &str| env::var(k).ok().as_deref() == Some(v);
    let nonempty = |k: &str| env::var(k).ok().filter(|v| !v.is_empty()).is_some();
    // Claude (matches HcomContext: CLAUDECODE=1 || CLAUDE_ENV_FILE non-empty)
    is_eq("CLAUDECODE", "1") || nonempty("CLAUDE_ENV_FILE")
        // Gemini
        || is_eq("GEMINI_CLI", "1")
        // Codex (all 5 markers from HcomContext)
        || is_set("CODEX_SANDBOX")
        || is_set("CODEX_SANDBOX_NETWORK_DISABLED")
        || is_set("CODEX_MANAGED_BY_NPM")
        || is_set("CODEX_MANAGED_BY_BUN")
        || is_set("CODEX_THREAD_ID")
        // OpenCode
        || is_eq("OPENCODE", "1")
        // hcom-launched
        || is_eq("HCOM_LAUNCHED", "1")
}

/// Detect current AI tool from environment (no HcomContext needed).
///
/// Uses the same env vars as HcomContext::from_env() for tool detection.
pub fn detect_current_tool_from_env() -> &'static str {
    use std::env;
    let is_set = |k: &str| env::var(k).is_ok();
    let is_eq = |k: &str, v: &str| env::var(k).ok().as_deref() == Some(v);
    let nonempty = |k: &str| env::var(k).ok().filter(|v| !v.is_empty()).is_some();
    if is_eq("CLAUDECODE", "1") || nonempty("CLAUDE_ENV_FILE") {
        "claude"
    } else if is_eq("GEMINI_CLI", "1") {
        "gemini"
    } else if is_set("CODEX_SANDBOX")
        || is_set("CODEX_SANDBOX_NETWORK_DISABLED")
        || is_set("CODEX_MANAGED_BY_NPM")
        || is_set("CODEX_MANAGED_BY_BUN")
        || is_set("CODEX_THREAD_ID")
    {
        "codex"
    } else if is_eq("OPENCODE", "1") {
        "opencode"
    } else {
        "adhoc"
    }
}

/// Current platform name
pub fn platform_name() -> &'static str {
    if cfg!(target_os = "macos") {
        "Darwin"
    } else if cfg!(target_os = "linux") || cfg!(target_os = "android") {
        "Linux"
    } else if cfg!(target_os = "windows") {
        "Windows"
    } else {
        "Unknown"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_platform_name_is_known() {
        let name = platform_name();
        assert!(
            ["Darwin", "Linux", "Windows"].contains(&name),
            "unexpected platform: {name}"
        );
    }

    #[test]
    fn test_is_wsl_returns_bool() {
        // Just ensure it doesn't panic.
        let _ = is_wsl();
    }

    #[test]
    fn test_is_termux_returns_bool() {
        let _ = is_termux();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn test_not_wsl_on_macos() {
        assert!(!is_wsl());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn test_platform_is_darwin() {
        assert_eq!(platform_name(), "Darwin");
    }
}
