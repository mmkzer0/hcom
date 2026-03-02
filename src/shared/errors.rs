//! Error type hierarchy for hcom.
//!
//! Three error types for different contexts:
//! - `HcomError`: User-facing operation failures (shown to humans)
//! - `HookError`: Hook handler returns (exit codes + JSON output)
//! - `CLIError`: Argument validation (printed to stderr, exit 1)
//!
//! Internal propagation uses `anyhow::Result` — these types are for
//! the boundaries where errors become user-visible output.

use thiserror::Error;

/// User-facing hcom operation error.
///
/// Displayed directly to the user (e.g., "instance not found", "send failed").
#[derive(Debug, Error)]
pub enum HcomError {
    #[error("{0}")]
    NotFound(String),

    #[error("{0}")]
    InvalidInput(String),

    #[error("{0}")]
    IdentityRequired(String),

    #[error("{0}")]
    SendFailed(String),

    #[error("{0}")]
    DatabaseError(String),

    #[error("{0}")]
    ConfigError(String),

    #[error("{0}")]
    Other(String),
}

/// Hook handler error — determines exit code and JSON output.
///
/// Hook handlers return `Result<HookResult, HookError>` where:
/// - `Ok(result)` → exit 0, print result JSON to stdout
/// - `Err(HookError)` → exit code + optional output
///
#[derive(Debug, Error)]
pub enum HookError {
    /// Hook pre-gate: no instances, fast exit 0 with empty stdout.
    #[error("pre-gate: no instances")]
    PreGate,

    /// Hook block decision (exit 2 with block JSON).
    /// Used by Claude Stop hook when messages are pending.
    #[error("block: {reason}")]
    Block { reason: String },

    /// Hook internal failure (exit 1, log error).
    #[error("hook error: {0}")]
    Internal(String),

    /// Identity resolution failed (exit 0, empty output — not fatal for hooks).
    #[error("identity not resolved: {0}")]
    IdentityNotResolved(String),
}

impl HookError {
    /// Exit code for this hook error.
    pub fn exit_code(&self) -> i32 {
        match self {
            HookError::PreGate => 0,
            HookError::Block { .. } => 2,
            HookError::Internal(_) => 1,
            HookError::IdentityNotResolved(_) => 0,
        }
    }
}

/// CLI argument validation error.
///
/// Printed to stderr, exits with code 1.
#[derive(Debug, Error)]
pub enum CLIError {
    #[error("{0}")]
    InvalidArgument(String),

    #[error("unknown command: {0}")]
    UnknownCommand(String),

    #[error("{0}")]
    MissingArgument(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hcom_error_display() {
        let e = HcomError::NotFound("instance 'luna' not found".into());
        assert_eq!(e.to_string(), "instance 'luna' not found");
    }

    #[test]
    fn test_hook_error_exit_codes() {
        assert_eq!(HookError::PreGate.exit_code(), 0);
        assert_eq!(
            HookError::Block {
                reason: "pending".into()
            }
            .exit_code(),
            2
        );
        assert_eq!(HookError::Internal("boom".into()).exit_code(), 1);
        assert_eq!(
            HookError::IdentityNotResolved("no binding".into()).exit_code(),
            0
        );
    }

    #[test]
    fn test_cli_error_display() {
        let e = CLIError::UnknownCommand("frobnicate".into());
        assert_eq!(e.to_string(), "unknown command: frobnicate");
    }

    #[test]
    fn test_hcom_error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<HcomError>();
        assert_send_sync::<HookError>();
        assert_send_sync::<CLIError>();
    }
}
