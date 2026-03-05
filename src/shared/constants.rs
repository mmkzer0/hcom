//! Shared constants for hcom — single source of truth.
//!
//! message patterns, terminal presets, and sender identity types.

use regex::Regex;
use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};

// ===== Time helpers =====

/// Current time as f64 seconds since epoch (for REAL columns like updated_at, created_at).
pub fn now_epoch_f64() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Convert a SystemTime to f64 seconds since epoch (for file mtimes, etc).
pub fn system_time_to_epoch_f64(t: SystemTime) -> f64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Current time as i64 seconds since epoch (for INTEGER columns like last_stop, heartbeat).
pub fn now_epoch_i64() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Current time as ISO 8601 string with microsecond precision (for TEXT timestamp columns).
pub fn now_iso() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.6f+00:00")
        .to_string()
}

// ===== Version =====
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

// ===== Message Constants =====

/// CLI sender identity (the human operator).
pub const SENDER: &str = "bigboss";

/// System notification identity (launcher, watchdog, subscriptions).
pub const SYSTEM_SENDER: &str = "hcom";

/// Max messages delivered in a single hook response.
pub const MAX_MESSAGES_PER_DELIVERY: usize = 50;

/// Max message body size (1MB).
pub const MAX_MESSAGE_SIZE: usize = 1_048_576;

/// Stop hook polling interval in seconds.
pub const STOP_HOOK_POLL_INTERVAL_SECS: f64 = 0.1;

// ===== Message Patterns =====

/// @mention regex — matches `@name` but not `email@domain` or `path.@name`.
///
/// Rejects @mentions preceded by [a-zA-Z0-9._-] to prevent matching:
/// - email: user@domain.com
/// - paths: /file.@test
/// - identifiers: var_@name, some-id@mention
///
/// Capture group 2 is the name. Includes `:` for remote names (@luna:BOXE).
/// Uses boundary match instead of lookbehind (Rust regex doesn't support lookbehind).
pub static MENTION_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?:^|[^a-zA-Z0-9._\-])@([a-zA-Z0-9][\w:\-]*)").unwrap());

/// Extract all @mention names from text.
pub fn extract_mentions(text: &str) -> Vec<String> {
    MENTION_PATTERN
        .captures_iter(text)
        .map(|c| c[1].to_string())
        .collect()
}

/// Binding marker for vanilla sessions: [hcom:<name>] or legacy [HCOM:BIND:<name>].
pub static BIND_MARKER_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\[(?:hcom|HCOM:BIND):([a-z0-9_]+)\]").unwrap());

// ===== Release Configuration =====

/// Tools available for launch.
pub const RELEASED_TOOLS: &[&str] = &["claude", "gemini", "codex", "opencode"];

/// Tools that support background/headless mode.
pub const RELEASED_BACKGROUND: &[&str] = &["claude"];

// ===== Environment Variable Constants =====

/// Tool detection markers — set by AI tools, cleared to prevent inheritance.
pub const TOOL_MARKER_VARS: &[&str] = &[
    "CLAUDECODE",
    "GEMINI_CLI",
    "GEMINI_SYSTEM_MD",
    "CODEX_SANDBOX",
    "CODEX_SANDBOX_NETWORK_DISABLED",
    "CODEX_MANAGED_BY_NPM",
    "CODEX_MANAGED_BY_BUN",
    "CODEX_THREAD_ID",
    "OPENCODE",
];

/// HCOM identity vars — set per-instance, cleared to prevent parent identity leakage.
pub const HCOM_IDENTITY_VARS: &[&str] = &[
    "HCOM_PROCESS_ID",
    "HCOM_LAUNCHED",
    // HCOM_LAUNCHED_PRESET excluded — must survive into Rust binary for hook forwarding
    "HCOM_PTY_MODE",
    "HCOM_BACKGROUND",
    "HCOM_LAUNCHED_BY",
    "HCOM_LAUNCH_BATCH_ID",
    "HCOM_LAUNCH_EVENT_ID",
];

// ===== Status Constants =====

pub const ST_ACTIVE: &str = "active";
pub const ST_LISTENING: &str = "listening";
pub const ST_BLOCKED: &str = "blocked";
pub const ST_INACTIVE: &str = "inactive";
pub const ST_LAUNCHING: &str = "launching";
pub const ST_ERROR: &str = "error";

/// Valid status values (ordered for display priority).
pub const STATUS_ORDER: &[&str] = &[
    ST_ACTIVE,
    ST_LISTENING,
    ST_BLOCKED,
    ST_ERROR,
    ST_LAUNCHING,
    ST_INACTIVE,
];

/// Status icons (unicode).
pub fn status_icon(status: &str) -> &'static str {
    match status {
        ST_ACTIVE => "\u{25b6}",    // ▶
        ST_LISTENING => "\u{25c9}", // ◉
        ST_BLOCKED => "\u{25a0}",   // ■
        ST_LAUNCHING => "\u{25ce}", // ◎
        ST_ERROR => "\u{2717}",     // ✗
        "stopped" => "\u{2298}",    // ⊘
        ST_INACTIVE => "\u{25cb}",  // ○
        _ => "\u{25cb}",            // ○
    }
}

/// Adhoc instance icon (neutral — not claiming alive or dead).
pub const ADHOC_ICON: &str = "\u{25e6}"; // ◦

/// Status foreground ANSI color.
pub fn status_fg(status: &str) -> &'static str {
    match status {
        ST_ACTIVE => FG_GREEN,
        ST_LISTENING => FG_BLUE,
        ST_BLOCKED => FG_RED,
        ST_LAUNCHING => FG_YELLOW,
        ST_ERROR => FG_RED,
        ST_INACTIVE => FG_GRAY,
        _ => FG_GRAY,
    }
}

/// Status background ANSI color.
pub fn status_bg(status: &str) -> &'static str {
    match status {
        ST_ACTIVE => BG_GREEN,
        ST_LISTENING => BG_BLUE,
        ST_BLOCKED => BG_RED,
        ST_LAUNCHING => BG_YELLOW,
        ST_ERROR => BG_RED,
        ST_INACTIVE => BG_GRAY,
        _ => BG_GRAY,
    }
}

// ===== ANSI Color Codes =====

// Core
pub const RESET: &str = "\x1b[0m";
pub const DIM: &str = "\x1b[2m";
pub const BOLD: &str = "\x1b[1m";
pub const REVERSE: &str = "\x1b[7m";

// Foreground
pub const FG_GREEN: &str = "\x1b[32m";
pub const FG_CYAN: &str = "\x1b[36m";
pub const FG_WHITE: &str = "\x1b[37m";
pub const FG_BLACK: &str = "\x1b[30m";
pub const FG_GRAY: &str = "\x1b[38;5;242m";
pub const FG_YELLOW: &str = "\x1b[33m";
pub const FG_RED: &str = "\x1b[31m";
pub const FG_BLUE: &str = "\x1b[38;5;75m";

// TUI-specific foreground
pub const FG_ORANGE: &str = "\x1b[38;5;208m";
pub const FG_GOLD: &str = "\x1b[38;5;220m";
pub const FG_LIGHTGRAY: &str = "\x1b[38;5;250m";
pub const FG_DELIVER: &str = "\x1b[38;5;156m";
pub const FG_STALE: &str = "\x1b[38;5;137m";

// Background
pub const BG_BLUE: &str = "\x1b[48;5;69m";
pub const BG_GREEN: &str = "\x1b[42m";
pub const BG_CYAN: &str = "\x1b[46m";
pub const BG_YELLOW: &str = "\x1b[43m";
pub const BG_RED: &str = "\x1b[41m";
pub const BG_GRAY: &str = "\x1b[100m";
pub const BG_STALE: &str = "\x1b[48;5;137m";
pub const BG_ORANGE: &str = "\x1b[48;5;208m";
pub const BG_CHARCOAL: &str = "\x1b[48;5;236m";
pub const BG_GOLD: &str = "\x1b[48;5;220m";

// Terminal control
pub const CLEAR_SCREEN: &str = "\x1b[2J";
pub const CURSOR_HOME: &str = "\x1b[H";
pub const HIDE_CURSOR: &str = "\x1b[?25l";
pub const SHOW_CURSOR: &str = "\x1b[?25h";

// Box drawing
pub const BOX_H: &str = "\u{2500}"; // ─

// ===== Sender Identity =====

/// Sender identity for message routing.
///
#[derive(Debug, Clone)]
pub struct SenderIdentity {
    /// Identity type: determines routing behavior.
    pub kind: SenderKind,
    /// Display name stored in events.instance column.
    pub name: String,
    /// Full instance data from DB (for kind=Instance only).
    pub instance_data: Option<serde_json::Value>,
    /// Claude session ID for transcript binding.
    pub session_id: Option<String>,
}

/// Sender identity kind — determines routing rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SenderKind {
    /// Registered hcom participant (full routing rules apply).
    Instance,
    /// External sender via --from flag (broadcasts to all).
    External,
    /// System-generated message (broadcasts to all).
    System,
}

impl SenderIdentity {
    /// External and system senders broadcast to everyone.
    pub fn broadcasts(&self) -> bool {
        matches!(self.kind, SenderKind::External | SenderKind::System)
    }

    /// Group session ID for routing (session-based group membership).
    ///
    /// For subagents: uses parent_session_id (groups them with parent).
    /// For parents: uses own session_id.
    ///
    pub fn group_id(&self) -> Option<&str> {
        let data = self.instance_data.as_ref()?;
        // Subagent — use parent_session_id
        if let Some(parent_sid) = data.get("parent_session_id").and_then(|v| v.as_str()) {
            if !parent_sid.is_empty() {
                return Some(parent_sid);
            }
        }
        // Parent — use own session_id
        data.get("session_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
    }
}

/// Resolved identity context for a single CLI invocation.
#[derive(Debug, Clone)]
pub struct CommandContext {
    /// Raw `--name` value (if provided).
    pub explicit_name: Option<String>,
    /// Resolved instance identity (best-effort; may be None).
    pub identity: Option<SenderIdentity>,
    /// Whether --go flag was provided.
    pub go: bool,
}

// ===== Terminal Presets =====

/// Terminal preset configuration.
#[derive(Debug, Clone)]
pub struct TerminalPreset {
    /// Binary to check for availability (None = check app bundle).
    pub binary: Option<&'static str>,
    /// App name for macOS bundle detection (e.g., "kitty", "WezTerm").
    pub app_name: Option<&'static str>,
    /// Command template with {script} placeholder.
    pub open: &'static str,
    /// Close command template with {pane_id} placeholder (None = no close API).
    pub close: Option<&'static str>,
    /// Env var that contains the pane ID.
    pub pane_id_env: Option<&'static str>,
    /// Supported platforms.
    pub platforms: &'static [&'static str],
}

/// All 23 terminal presets.
pub static TERMINAL_PRESETS: LazyLock<Vec<(&'static str, TerminalPreset)>> = LazyLock::new(|| {
    vec![
        // macOS
        (
            "Terminal.app",
            TerminalPreset {
                binary: None,
                app_name: None,
                open: "open -a Terminal {script}",
                close: None,
                pane_id_env: None,
                platforms: &["Darwin"],
            },
        ),
        (
            "iTerm",
            TerminalPreset {
                binary: None,
                app_name: None,
                open: "open -a iTerm {script}",
                close: None,
                pane_id_env: None,
                platforms: &["Darwin"],
            },
        ),
        (
            "Ghostty",
            TerminalPreset {
                binary: None,
                app_name: None,
                open: "open -na Ghostty.app --args -e bash {script}",
                close: None,
                pane_id_env: None,
                platforms: &["Darwin"],
            },
        ),
        // Cross-platform (smart presets: auto-detect split/tab/window)
        (
            "kitty",
            TerminalPreset {
                binary: Some("kitty"),
                app_name: Some("kitty"),
                open: "kitty --env HCOM_PROCESS_ID={process_id} {script}",
                close: Some("kitten @ close-window --match id:{pane_id}"),
                pane_id_env: None,
                platforms: &["Darwin", "Linux"],
            },
        ),
        (
            "kitty-window",
            TerminalPreset {
                binary: Some("kitty"),
                app_name: Some("kitty"),
                open: "kitty --env HCOM_PROCESS_ID={process_id} {script}",
                close: Some("kitten @ close-window --match id:{pane_id}"),
                pane_id_env: None,
                platforms: &["Darwin", "Linux"],
            },
        ),
        (
            "wezterm",
            TerminalPreset {
                binary: Some("wezterm"),
                app_name: Some("WezTerm"),
                open: "wezterm start -- bash {script}",
                close: Some("wezterm cli kill-pane --pane-id {pane_id}"),
                pane_id_env: Some("WEZTERM_PANE"),
                platforms: &["Darwin", "Linux", "Windows"],
            },
        ),
        (
            "wezterm-window",
            TerminalPreset {
                binary: Some("wezterm"),
                app_name: Some("WezTerm"),
                open: "wezterm start -- bash {script}",
                close: Some("wezterm cli kill-pane --pane-id {pane_id}"),
                pane_id_env: Some("WEZTERM_PANE"),
                platforms: &["Darwin", "Linux", "Windows"],
            },
        ),
        (
            "alacritty",
            TerminalPreset {
                binary: Some("alacritty"),
                app_name: Some("Alacritty"),
                open: "alacritty -e bash {script}",
                close: None,
                pane_id_env: None,
                platforms: &["Darwin", "Linux", "Windows"],
            },
        ),
        // Tab utilities
        (
            "ttab",
            TerminalPreset {
                binary: Some("ttab"),
                app_name: None,
                open: "ttab {script}",
                close: None,
                pane_id_env: None,
                platforms: &["Darwin"],
            },
        ),
        (
            "wttab",
            TerminalPreset {
                binary: Some("wttab"),
                app_name: None,
                open: "wttab {script}",
                close: None,
                pane_id_env: None,
                platforms: &["Windows"],
            },
        ),
        // Linux terminals
        (
            "gnome-terminal",
            TerminalPreset {
                binary: Some("gnome-terminal"),
                app_name: None,
                open: "gnome-terminal --window -- bash {script}",
                close: None,
                pane_id_env: None,
                platforms: &["Linux"],
            },
        ),
        (
            "konsole",
            TerminalPreset {
                binary: Some("konsole"),
                app_name: None,
                open: "konsole -e bash {script}",
                close: None,
                pane_id_env: None,
                platforms: &["Linux"],
            },
        ),
        (
            "xterm",
            TerminalPreset {
                binary: Some("xterm"),
                app_name: None,
                open: "xterm -e bash {script}",
                close: None,
                pane_id_env: None,
                platforms: &["Linux"],
            },
        ),
        (
            "tilix",
            TerminalPreset {
                binary: Some("tilix"),
                app_name: None,
                open: "tilix -e bash {script}",
                close: None,
                pane_id_env: None,
                platforms: &["Linux"],
            },
        ),
        (
            "terminator",
            TerminalPreset {
                binary: Some("terminator"),
                app_name: None,
                open: "terminator -x bash {script}",
                close: None,
                pane_id_env: None,
                platforms: &["Linux"],
            },
        ),
        // Windows
        (
            "Windows Terminal",
            TerminalPreset {
                binary: Some("wt"),
                app_name: None,
                open: "wt bash {script}",
                close: None,
                pane_id_env: None,
                platforms: &["Windows"],
            },
        ),
        (
            "mintty",
            TerminalPreset {
                binary: Some("mintty"),
                app_name: None,
                open: "mintty bash {script}",
                close: None,
                pane_id_env: None,
                platforms: &["Windows"],
            },
        ),
        // Within-terminal splits/tabs
        (
            "tmux",
            TerminalPreset {
                binary: Some("tmux"),
                app_name: None,
                open: "tmux new-session -d bash {script}",
                close: Some("tmux kill-pane -t {pane_id}"),
                pane_id_env: Some("TMUX_PANE"),
                platforms: &["Darwin", "Linux"],
            },
        ),
        (
            "tmux-split",
            TerminalPreset {
                binary: Some("tmux"),
                app_name: None,
                open: "tmux split-window -h {script}",
                close: Some("tmux kill-pane -t {pane_id}"),
                pane_id_env: Some("TMUX_PANE"),
                platforms: &["Darwin", "Linux"],
            },
        ),
        (
            "wezterm-tab",
            TerminalPreset {
                binary: Some("wezterm"),
                app_name: Some("WezTerm"),
                open: "wezterm cli spawn -- bash {script}",
                close: Some("wezterm cli kill-pane --pane-id {pane_id}"),
                pane_id_env: Some("WEZTERM_PANE"),
                platforms: &["Darwin", "Linux", "Windows"],
            },
        ),
        (
            "wezterm-split",
            TerminalPreset {
                binary: Some("wezterm"),
                app_name: Some("WezTerm"),
                open: "wezterm cli split-pane --top-level --right -- bash {script}",
                close: Some("wezterm cli kill-pane --pane-id {pane_id}"),
                pane_id_env: Some("WEZTERM_PANE"),
                platforms: &["Darwin", "Linux", "Windows"],
            },
        ),
        (
            "kitty-tab",
            TerminalPreset {
                binary: Some("kitten"),
                app_name: Some("kitty"),
                open: "kitten @ launch --type=tab --env HCOM_PROCESS_ID={process_id} -- bash {script}",
                close: Some("kitten @ close-tab --match id:{pane_id}"),
                pane_id_env: None,
                platforms: &["Darwin", "Linux"],
            },
        ),
        (
            "kitty-split",
            TerminalPreset {
                binary: Some("kitten"),
                app_name: Some("kitty"),
                open: "kitten @ launch --type=window --env HCOM_PROCESS_ID={process_id} -- bash {script}",
                close: Some("kitten @ close-window --match id:{pane_id}"),
                pane_id_env: None,
                platforms: &["Darwin", "Linux"],
            },
        ),
    ]
});

/// Look up a terminal preset by name (case-sensitive).
pub fn get_terminal_preset(name: &str) -> Option<&TerminalPreset> {
    TERMINAL_PRESETS
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, p)| p)
}

/// Map environment variables to terminal presets for auto-detection.
/// Used for same-terminal PTY launches to enable close-on-kill.
pub const TERMINAL_ENV_MAP: &[(&str, &str)] = &[
    ("TMUX_PANE", "tmux-split"),
    ("WEZTERM_PANE", "wezterm-split"),
    ("KITTY_WINDOW_ID", "kitty-split"),
    ("ZELLIJ_PANE_ID", "zellij"),
    ("WAVETERM_BLOCKID", "waveterm"),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mention_pattern_basic() {
        assert_eq!(extract_mentions("hello @luna and @nova"), vec!["luna", "nova"]);
    }

    #[test]
    fn test_mention_pattern_rejects_email() {
        assert!(extract_mentions("email user@domain.com").is_empty());
    }

    #[test]
    fn test_mention_pattern_remote_name() {
        assert_eq!(extract_mentions("send to @luna:BOXE"), vec!["luna:BOXE"]);
    }

    #[test]
    fn test_mention_pattern_rejects_underscore_prefix() {
        assert!(
            extract_mentions("var_@name").is_empty(),
            "should reject underscore-prefixed mention"
        );
    }

    #[test]
    fn test_mention_pattern_rejects_dot_prefix() {
        assert!(
            extract_mentions("file.@name").is_empty(),
            "should reject dot-prefixed mention"
        );
    }

    #[test]
    fn test_mention_pattern_start_of_string() {
        assert_eq!(extract_mentions("@luna hello"), vec!["luna"]);
    }

    #[test]
    fn test_bind_marker() {
        let caps = BIND_MARKER_RE.captures("[hcom:luna]");
        assert_eq!(caps.unwrap()[1].to_string(), "luna");
    }

    #[test]
    fn test_bind_marker_legacy() {
        let caps = BIND_MARKER_RE.captures("[HCOM:BIND:test_name]");
        assert_eq!(caps.unwrap()[1].to_string(), "test_name");
    }

    #[test]
    fn test_status_icons() {
        assert_eq!(status_icon(ST_ACTIVE), "▶");
        assert_eq!(status_icon(ST_LISTENING), "◉");
        assert_eq!(status_icon(ST_BLOCKED), "■");
        assert_eq!(status_icon(ST_LAUNCHING), "◎");
        assert_eq!(status_icon(ST_ERROR), "✗");
        assert_eq!(status_icon(ST_INACTIVE), "○");
    }

    #[test]
    fn test_terminal_presets_count() {
        assert_eq!(TERMINAL_PRESETS.len(), 23);
    }

    #[test]
    fn test_terminal_preset_lookup() {
        let preset = get_terminal_preset("kitty").unwrap();
        assert_eq!(preset.binary, Some("kitty"));
        assert!(preset.close.is_some());

        assert!(get_terminal_preset("nonexistent").is_none());
    }

    #[test]
    fn test_sender_identity_broadcasts() {
        let instance = SenderIdentity {
            kind: SenderKind::Instance,
            name: "luna".into(),
            instance_data: None,
            session_id: None,
        };
        assert!(!instance.broadcasts());

        let external = SenderIdentity {
            kind: SenderKind::External,
            name: "user".into(),
            instance_data: None,
            session_id: None,
        };
        assert!(external.broadcasts());

        let system = SenderIdentity {
            kind: SenderKind::System,
            name: "hcom".into(),
            instance_data: None,
            session_id: None,
        };
        assert!(system.broadcasts());
    }

    #[test]
    fn test_sender_identity_group_id() {
        // Parent — uses own session_id
        let parent = SenderIdentity {
            kind: SenderKind::Instance,
            name: "luna".into(),
            instance_data: Some(serde_json::json!({"session_id": "sess-123"})),
            session_id: None,
        };
        assert_eq!(parent.group_id(), Some("sess-123"));

        // Subagent — uses parent_session_id (not own session_id)
        let subagent = SenderIdentity {
            kind: SenderKind::Instance,
            name: "sub1".into(),
            instance_data: Some(serde_json::json!({
                "session_id": "sub-sess",
                "parent_session_id": "parent-sess"
            })),
            session_id: None,
        };
        assert_eq!(subagent.group_id(), Some("parent-sess"));

        // No instance data
        let no_data = SenderIdentity {
            kind: SenderKind::Instance,
            name: "luna".into(),
            instance_data: None,
            session_id: None,
        };
        assert_eq!(no_data.group_id(), None);

        // Empty instance data
        let empty = SenderIdentity {
            kind: SenderKind::Instance,
            name: "luna".into(),
            instance_data: Some(serde_json::json!({})),
            session_id: None,
        };
        assert_eq!(empty.group_id(), None);
    }

    #[test]
    fn test_released_tools() {
        assert!(RELEASED_TOOLS.contains(&"claude"));
        assert!(RELEASED_TOOLS.contains(&"gemini"));
        assert!(RELEASED_TOOLS.contains(&"codex"));
        assert!(RELEASED_TOOLS.contains(&"opencode"));
        assert_eq!(RELEASED_TOOLS.len(), 4);
    }

    #[test]
    fn test_status_order() {
        assert_eq!(STATUS_ORDER.len(), 6);
        assert_eq!(STATUS_ORDER[0], ST_ACTIVE);
        assert_eq!(STATUS_ORDER[5], ST_INACTIVE);
    }
}
