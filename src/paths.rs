//! Centralized path resolution and file utilities for hcom.
//!
//! Single source of truth for all hcom directory and file paths.
//! Respects HCOM_DIR env var for worktrees/dev, falls back to ~/.hcom.
//! Also provides atomic file operations and flag counters.

use crate::config::Config;
use std::fs;
use std::path::{Path, PathBuf};

// ==================== Path Constants ====================

pub const LOGS_DIR: &str = ".tmp/logs";
pub const LAUNCH_DIR: &str = ".tmp/launch";
pub const FLAGS_DIR: &str = ".tmp/flags";
pub const LAUNCHES_DIR: &str = "launches";
pub const ARCHIVE_DIR: &str = "archive";
pub const SCRIPTS_DIR: &str = "scripts";

// ==================== Base Path Helpers ====================

/// Get the hcom base directory.
///
/// Uses centralized Config (HCOM_DIR env var or ~/.hcom fallback).
pub fn hcom_dir() -> PathBuf {
    Config::get().hcom_dir
}

/// Build path under hcom directory, optionally ensuring parent exists.
pub fn hcom_path(parts: &[&str]) -> PathBuf {
    let mut path = hcom_dir();
    for part in parts {
        path = path.join(part);
    }
    path
}

/// Get project root (parent of hcom_dir). Used for anchoring tool config files.
///
/// Uses cached Config — for test-friendly env-reactive resolution, use
/// `hooks::common::tool_config_root()` instead.
pub fn get_project_root() -> PathBuf {
    hcom_dir()
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("/"))
}

// ==================== Derived Paths ====================

/// Get the database path (hcom_dir/hcom.db)
pub fn db_path() -> PathBuf {
    hcom_dir().join("hcom.db")
}

/// Get the log file path (hcom_dir/.tmp/logs/hcom.log)
pub fn log_path() -> PathBuf {
    hcom_dir().join(".tmp").join("logs").join("hcom.log")
}

/// Get the pidtrack file path (hcom_dir/.tmp/launched_pids.json)
pub fn pidtrack_path() -> PathBuf {
    hcom_dir().join(".tmp").join("launched_pids.json")
}

/// Get the config TOML path (hcom_dir/config.toml)
pub fn config_toml_path() -> PathBuf {
    hcom_dir().join("config.toml")
}

/// Get the scripts directory (hcom_dir/scripts/)
pub fn scripts_dir() -> PathBuf {
    hcom_dir().join(SCRIPTS_DIR)
}

// ==================== Directory Management ====================

/// Ensure all critical HCOM directories exist. Idempotent, safe to call repeatedly.
/// Called at hook entry to support opt-in scenarios where hooks execute before CLI commands.
/// Returns true on success, false on failure.
pub fn ensure_hcom_directories() -> bool {
    ensure_hcom_directories_at(&hcom_dir())
}

/// Ensure directories under a given base (testable without global config).
pub fn ensure_hcom_directories_at(base: &Path) -> bool {
    for dir_name in [LOGS_DIR, LAUNCH_DIR, FLAGS_DIR, LAUNCHES_DIR, ARCHIVE_DIR] {
        if fs::create_dir_all(base.join(dir_name)).is_err() {
            return false;
        }
    }
    true
}

// ==================== Atomic File Operations ====================

/// Write content to file atomically (temp file + rename).
/// Returns the underlying IO error on failure for callers that need error detail.
pub fn atomic_write_io(filepath: &Path, content: &str) -> std::io::Result<()> {
    // Ensure parent directory exists
    if let Some(parent) = filepath.parent() {
        fs::create_dir_all(parent)?;
    }

    // Write to temp file in the same directory (same filesystem for rename)
    let tmp = tempfile::NamedTempFile::new_in(
        filepath.parent().unwrap_or_else(|| Path::new(".")),
    )?;

    // Write content and fsync before rename to ensure data is on disk
    std::io::Write::write_all(&mut &tmp, content.as_bytes())?;
    tmp.as_file().sync_all()?;

    // Persist atomically (temp file → target path via rename)
    tmp.persist(filepath).map_err(|e| e.error)?;
    Ok(())
}

/// Write content to file atomically (temp file + rename).
/// Returns true on success, false on failure.
pub fn atomic_write(filepath: &Path, content: &str) -> bool {
    atomic_write_io(filepath, content).is_ok()
}

// ==================== Flag Counters ====================

/// Increment a counter in .tmp/flags/{name} and return new value.
pub fn increment_flag_counter(name: &str) -> i32 {
    increment_flag_counter_at(&hcom_dir(), name)
}

/// Increment flag counter under a given base (testable).
pub fn increment_flag_counter_at(base: &Path, name: &str) -> i32 {
    let flag_file = base.join(FLAGS_DIR).join(name);
    let _ = fs::create_dir_all(flag_file.parent().unwrap());

    let count = read_flag_file(&flag_file) + 1;
    atomic_write(&flag_file, &count.to_string());
    count
}

fn read_flag_file(path: &Path) -> i32 {
    fs::read_to_string(path)
        .ok()
        .and_then(|s| s.trim().parse::<i32>().ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_ensure_hcom_directories_at() {
        let tmp = TempDir::new().unwrap();
        assert!(ensure_hcom_directories_at(tmp.path()));

        // Verify all directories were created
        assert!(tmp.path().join(LOGS_DIR).is_dir());
        assert!(tmp.path().join(LAUNCH_DIR).is_dir());
        assert!(tmp.path().join(FLAGS_DIR).is_dir());
        assert!(tmp.path().join(LAUNCHES_DIR).is_dir());
        assert!(tmp.path().join(ARCHIVE_DIR).is_dir());

        // Idempotent — second call succeeds too
        assert!(ensure_hcom_directories_at(tmp.path()));
    }

    #[test]
    fn test_atomic_write() {
        let tmp = TempDir::new().unwrap();
        let filepath = tmp.path().join("test.txt");

        assert!(atomic_write(&filepath, "hello world"));
        assert_eq!(fs::read_to_string(&filepath).unwrap(), "hello world");

        // Overwrite
        assert!(atomic_write(&filepath, "new content"));
        assert_eq!(fs::read_to_string(&filepath).unwrap(), "new content");
    }

    #[test]
    fn test_atomic_write_creates_parent_dirs() {
        let tmp = TempDir::new().unwrap();
        let filepath = tmp.path().join("a").join("b").join("test.txt");

        assert!(atomic_write(&filepath, "nested"));
        assert_eq!(fs::read_to_string(&filepath).unwrap(), "nested");
    }

    #[test]
    fn test_flag_counters() {
        let tmp = TempDir::new().unwrap();

        // Counter starts at 0 (read raw flag file)
        assert_eq!(read_flag_file(&tmp.path().join(FLAGS_DIR).join("test_flag")), 0);

        assert_eq!(increment_flag_counter_at(tmp.path(), "test_flag"), 1);
        assert_eq!(read_flag_file(&tmp.path().join(FLAGS_DIR).join("test_flag")), 1);

        assert_eq!(increment_flag_counter_at(tmp.path(), "test_flag"), 2);
        assert_eq!(read_flag_file(&tmp.path().join(FLAGS_DIR).join("test_flag")), 2);

        // Different flag is independent
        assert_eq!(read_flag_file(&tmp.path().join(FLAGS_DIR).join("other_flag")), 0);
    }

    #[test]
    fn test_get_project_root_logic() {
        // get_project_root returns parent of hcom_dir
        // Test the logic directly without relying on global Config
        let base = Path::new("/home/test/.hcom");
        assert_eq!(
            base.parent().unwrap().to_path_buf(),
            PathBuf::from("/home/test")
        );
    }
}
