//! Shared types, constants, and utilities for hcom.
//!
//!
//! ## Modules
//! - `errors`: Error type hierarchy (HcomError, HookError, CLIError)
//! - `constants`: Status values, ANSI codes, message patterns, terminal presets, SenderIdentity
//! - `platform`: WSL/Termux detection
//! - `context`: HcomContext per-request bag-of-state
//!
//! ## Ops inlining (0H.3)
//! - `cleanup_instance_subscriptions` → instances or db
//! - `auto_subscribe_defaults` → instances (already called from there)
//! - `load_stopped_snapshot` → instances or resume module
//! - `resume_system_prompt` → resume module
//! - `op_send` (pure passthrough) → callers inline directly
//! - `op_stop` (5 lines of validation) → callers inline directly

pub mod constants;
pub mod context;
pub mod errors;
pub mod platform;

// Re-export key types at module level for convenience.
pub use constants::{
    CommandContext, SenderIdentity, SenderKind, TerminalPreset,
    // Status constants
    ST_ACTIVE, ST_BLOCKED, ST_ERROR, ST_INACTIVE, ST_LAUNCHING, ST_LISTENING,
    // Message constants
    SENDER, SYSTEM_SENDER, MAX_MESSAGES_PER_DELIVERY, MAX_MESSAGE_SIZE,
    // Patterns
    MENTION_PATTERN, BIND_MARKER_RE,
    // Functions
    extract_mentions, status_icon, status_fg, status_bg, get_terminal_preset,
    // Time helpers
    now_epoch_f64, now_epoch_i64,
};
pub use context::{HcomContext, ToolType};
pub use errors::{CLIError, HcomError, HookError};
pub use platform::{is_termux, is_wsl, platform_name};
