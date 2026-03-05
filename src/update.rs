//! Auto-update checker — checks GitHub Releases API once daily.
//!
//! Port of the Python `get_update_info()` / `get_update_notice()` pattern:
//! - Cache file: `~/.hcom/.tmp/flags/update_check` (version string or empty)
//! - Uses file mtime for 24h TTL (no timestamp parsing)
//! - Silent on network failure (never blocks CLI)
//! - Background check: spawns detached curl process to avoid blocking (<5ms)
//! - Detects install method from binary path to suggest correct update command

use crate::paths::{atomic_write, hcom_path, FLAGS_DIR};
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

const CHECK_INTERVAL: Duration = Duration::from_secs(86400); // 24 hours

fn flag_path() -> PathBuf {
    hcom_path(&[FLAGS_DIR, "update_check"])
}

/// Parse version string "x.y.z" into comparable tuple.
fn parse_version(v: &str) -> Option<(u32, u32, u32)> {
    let parts: Vec<&str> = v.trim().trim_start_matches('v').split('.').collect();
    if parts.len() >= 3 {
        Some((
            parts[0].parse().ok()?,
            parts[1].parse().ok()?,
            parts[2].parse().ok()?,
        ))
    } else {
        None
    }
}

/// Spawn a detached background process to fetch latest version and write the cache file.
/// Returns immediately — result shows up on next command.
fn spawn_background_check(flag: &PathBuf, current: &str) {
    let flag_str = flag.to_string_lossy().to_string();
    let current = current.to_string();

    // Shell script that: fetches GitHub API, extracts tag, compares versions, writes cache.
    // Runs completely detached — parent doesn't wait.
    let script = format!(
        r#"
TAG=$(curl -fsSL --max-time 5 https://api.github.com/repos/aannoo/hcom/releases/latest 2>/dev/null | grep '"tag_name"' | head -1 | cut -d'"' -f4)
VER="${{TAG#v}}"
if [ -n "$VER" ]; then
    # Compare: if remote > current, write version; else write empty
    REMOTE=$(echo "$VER" | awk -F. '{{printf "%d%06d%06d", $1, $2, $3}}')
    LOCAL=$(echo "{current}" | awk -F. '{{printf "%d%06d%06d", $1, $2, $3}}')
    if [ "$REMOTE" -gt "$LOCAL" ] 2>/dev/null; then
        printf '%s' "$VER" > "{flag_str}"
    else
        printf '' > "{flag_str}"
    fi
else
    printf '' > "{flag_str}"
fi
"#
    );

    // Fire and forget — detach from parent process
    let _ = std::process::Command::new("sh")
        .args(["-c", &script])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
}

/// Detect install method and return appropriate update command.
fn get_update_cmd() -> &'static str {
    let exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(_) => {
            return "curl -fsSL https://raw.githubusercontent.com/aannoo/hcom/main/install.sh | sh"
        }
    };

    let path_str = exe.to_string_lossy();

    // Dev build
    if path_str.contains("/hook-comms/") || path_str.contains("/target/") || path_str.contains("/.hcom-build/") {
        return "./build.sh";
    }

    // uv tool install
    if path_str.contains("/uv/") || path_str.contains("/.local/share/uv/") {
        return "uv tool upgrade hcom";
    }

    // pip install (venv or site-packages)
    if path_str.contains("/site-packages/") || path_str.contains("/venv/") {
        return "pip install -U hcom";
    }

    // Default: curl installer
    "curl -fsSL https://raw.githubusercontent.com/aannoo/hcom/main/install.sh | sh"
}

/// Check for updates (once daily cached). Returns (latest_version, update_cmd) or None.
///
/// Never blocks: if the cache is stale, spawns a background process to refresh it
/// and returns the current (possibly stale) cached result.
pub fn get_update_info() -> Option<(String, &'static str)> {
    let flag = flag_path();
    let current = env!("CARGO_PKG_VERSION");

    // Check if cache is stale and needs refresh
    let should_check = if flag.exists() {
        match flag.metadata().and_then(|m| m.modified()) {
            Ok(mtime) => SystemTime::now()
                .duration_since(mtime)
                .unwrap_or(Duration::ZERO)
                > CHECK_INTERVAL,
            Err(_) => true,
        }
    } else {
        true
    };

    if should_check {
        // Non-blocking: spawn background check, result appears on next command
        spawn_background_check(&flag, current);
    }

    // Read cached result (may be from a previous check)
    let latest = fs::read_to_string(&flag).ok()?.trim().to_string();
    if latest.is_empty() {
        return None;
    }

    // Double-check (handles manual upgrades)
    if parse_version(current) >= parse_version(&latest) {
        atomic_write(&flag, "");
        return None;
    }

    Some((latest, get_update_cmd()))
}

/// Return update notice string for stderr, or None if up to date.
pub fn get_update_notice() -> Option<String> {
    let (latest, cmd) = get_update_info()?;
    Some(format!("→ Update available: hcom v{latest} ({cmd})"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_version() {
        assert_eq!(parse_version("0.7.0"), Some((0, 7, 0)));
        assert_eq!(parse_version("v1.2.3"), Some((1, 2, 3)));
        assert_eq!(parse_version("bad"), None);
        assert_eq!(parse_version("1.2"), None);
    }

    #[test]
    fn test_version_comparison() {
        assert!(parse_version("0.8.0") > parse_version("0.7.0"));
        assert!(parse_version("1.0.0") > parse_version("0.99.99"));
        assert!(parse_version("0.7.0") == parse_version("0.7.0"));
    }

    #[test]
    fn test_get_update_cmd_default() {
        // In test context, binary is in target/debug, which contains /target/
        assert_eq!(get_update_cmd(), "./build.sh");
    }
}
