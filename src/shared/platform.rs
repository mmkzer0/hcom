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

/// Current platform name
pub fn platform_name() -> &'static str {
    if cfg!(target_os = "macos") {
        "Darwin"
    } else if cfg!(target_os = "linux") {
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
