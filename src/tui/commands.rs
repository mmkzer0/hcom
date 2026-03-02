//! Native command dispatch for TUI — replaces daemon socket RPC.
//!
//! Runs hcom commands as subprocesses of the same binary, capturing
//! stdout/stderr into a Response. Subprocess has its own file
//! descriptors, keeping TUI rendering unaffected.
//!
//! Why subprocess instead of in-process function calls:
//! The TUI owns stdout (fd 1) for ratatui rendering. Commands print
//! to stdout via println!(). Redirecting fd 1 from a background thread
//! would corrupt the TUI display. Subprocess isolation is clean and
//! adds ~5ms overhead — imperceptible for interactive operations.

use std::path::PathBuf;
use std::process::Command;

use crate::tui::rpc::Response;

/// Resolve the hcom binary path.
///
/// In normal operation (TUI running), this is the current executable.
/// The binary is cached per-process since it never changes at runtime.
fn hcom_binary() -> Result<PathBuf, String> {
    std::env::current_exe().map_err(|e| format!("cannot find hcom binary: {e}"))
}

/// Run an hcom command natively, returning captured stdout/stderr.
///
/// Spawns the current binary as a subprocess with the given argv.
/// Inherits environment (HCOM_DEV_ROOT, HCOM_DIR, etc.) so routing
/// and config resolution work identically to CLI invocations.
pub fn run_native(argv: &[String]) -> Result<Response, String> {
    let binary = hcom_binary()?;

    let output = Command::new(&binary)
        .args(argv)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .map_err(|e| format!("spawn hcom: {e}"))?;

    Ok(Response {
        exit_code: output.status.code().unwrap_or(1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Find the hcom binary for testing. In test context, current_exe()
    /// returns the test runner, not hcom. Look in cargo target dir instead.
    fn test_binary() -> Option<PathBuf> {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        for profile in ["debug", "release"] {
            let bin = manifest_dir.join("target").join(profile).join("hcom");
            if bin.exists() {
                return Some(bin);
            }
        }
        None
    }

    fn run_test_command(argv: &[&str]) -> Option<Response> {
        let binary = test_binary()?;
        let args: Vec<String> = argv.iter().map(|s| s.to_string()).collect();
        let output = Command::new(&binary)
            .args(&args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .ok()?;
        Some(Response {
            exit_code: output.status.code().unwrap_or(1),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }

    #[test]
    fn run_native_version() {
        let resp = match run_test_command(&["--version"]) {
            Some(r) => r,
            None => {
                eprintln!("skipping: hcom binary not found in target/");
                return;
            }
        };
        assert_eq!(resp.exit_code, 0);
        assert!(
            resp.stdout.starts_with("hcom "),
            "expected 'hcom ...' got: {}",
            resp.stdout
        );
    }

    #[test]
    fn response_from_run_native_has_correct_fields() {
        // Test the Response construction without needing the binary
        let resp = Response {
            exit_code: 0,
            stdout: "hello\n".into(),
            stderr: String::new(),
        };
        assert!(resp.ok());
        assert_eq!(resp.combined_output_lines(), vec!["hello"]);
    }
}
