//! Transcript reading: per-tool parsers and a unified read API.
//!
//! Adapters for Claude (.jsonl), Gemini (.json), Codex (.jsonl), and OpenCode
//! (SQLite). All return a tool-agnostic `Vec<Exchange>` so callers can format
//! and project without caring about the source tool.

pub mod claude;
pub mod codex;
pub mod copilot;
pub mod cursor;
pub mod gemini;
pub mod kimi;
pub mod opencode;
pub mod pi;
pub mod shared;

use std::path::Path;

use serde_json::{Value, json};

pub use shared::{Exchange, ToolUse, format_exchanges, summarize_action};

pub(crate) use opencode::{
    get_kilo_db_path, get_opencode_db_path, search_kilo_sessions, search_opencode_sessions,
};

/// Which tool produced the transcript (replaces stringly-typed agent codes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolKind {
    Claude,
    Antigravity,
    Gemini,
    Codex,
    OpenCode,
    Cursor,
    Kimi,
    Copilot,
    Pi,
}

/// Options for reading a transcript.
pub struct ReadOptions {
    pub last: usize,
    pub detailed: bool,
    /// Required by OpenCode (SQLite) parsers.
    pub session_id: Option<String>,
    /// Codex-only: short retry when the rollout JSONL has not yet been flushed
    /// past the user turn.
    pub allow_codex_retry: bool,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            last: 10,
            detailed: false,
            session_id: None,
            allow_codex_retry: true,
        }
    }
}

/// Read exchanges from a transcript at `path` for the given tool.
pub fn read(path: &Path, kind: ToolKind, opts: &ReadOptions) -> Result<Vec<Exchange>, String> {
    if !path.exists() {
        return Err(format!("Transcript not found: {}", path.display()));
    }

    let mut exchanges = match kind {
        ToolKind::Claude => claude::parse_claude_jsonl(path, opts.last, opts.detailed),
        ToolKind::Antigravity => claude::parse_claude_jsonl(path, opts.last, opts.detailed),
        ToolKind::Gemini => gemini::parse_gemini_json(path, opts.last),
        ToolKind::Codex => codex::parse_codex_jsonl(path, opts.last, opts.detailed),
        ToolKind::Cursor => cursor::parse_cursor_jsonl(path, opts.last, opts.detailed),
        ToolKind::Kimi => kimi::parse_kimi_wire_jsonl(path, opts.last, opts.detailed),
        ToolKind::Copilot => copilot::parse_copilot_jsonl(path, opts.last, opts.detailed),
        ToolKind::Pi => pi::parse_pi_jsonl(path, opts.last, opts.detailed),
        ToolKind::OpenCode => {
            let sid = opts.session_id.as_deref().unwrap_or("");
            if sid.is_empty() {
                return Err("OpenCode transcript requires a session_id".to_string());
            }
            opencode::parse_opencode_sqlite(path, sid, opts.last)
        }
    }?;

    if matches!(kind, ToolKind::Codex)
        && opts.allow_codex_retry
        && codex::should_retry_codex_transcript(&exchanges)
    {
        // Codex rollout JSONL can briefly contain the user turn before the
        // assistant text for that same turn lands. Local transcript reads do a
        // short retry; RPC handlers opt out so they do not block the relay
        // reader thread.
        exchanges = codex::retry_codex_transcript(path, opts.last, opts.detailed, exchanges)?;
    }

    Ok(exchanges)
}

/// Detect tool kind from a transcript path. Returns `None` for ambiguous
/// `.jsonl`/extensionless cases; callers must not assign a known parser merely
/// because detection failed.
pub fn detect_kind_from_path(path: &str) -> Option<ToolKind> {
    if path.ends_with(".json") {
        Some(ToolKind::Gemini)
    } else if path.ends_with(".db") {
        Some(ToolKind::OpenCode)
    } else if path.contains("agent-transcripts") {
        // Cursor's `.jsonl` would otherwise fall through to Claude. Key off the
        // cursor-unique `agent-transcripts` segment (not the `.jsonl` extension,
        // which is shared with Claude/Codex).
        Some(ToolKind::Cursor)
    } else if path.contains("session-state") && path.ends_with("events.jsonl") {
        Some(ToolKind::Copilot)
    } else if path.ends_with(".jsonl")
        && (path.contains(".pi/agent/sessions")
            || path.contains(".pi/sessions")
            || path.to_lowercase().contains("pi_coding_agent_session"))
    {
        Some(ToolKind::Pi)
    } else {
        None
    }
}

/// Map the agent string used elsewhere to a `ToolKind`, with path inference as
/// a compatibility aid when the agent value is absent or unknown.
///
/// Unknown identities and ambiguous paths are errors rather than silently
/// selecting Claude's parser.
pub fn kind_from_agent_or_path(agent: &str, path: &str) -> Result<ToolKind, String> {
    let kind = match agent {
        // `claude-pty` is a legacy persisted launch-surface alias for claude.
        "claude" | "claude-pty" => Some(ToolKind::Claude),
        "antigravity" | "agy" => Some(ToolKind::Antigravity),
        "gemini" => Some(ToolKind::Gemini),
        "codex" => Some(ToolKind::Codex),
        "opencode" | "kilo" => Some(ToolKind::OpenCode),
        "cursor" | "cursor-agent" => Some(ToolKind::Cursor),
        "kimi" => Some(ToolKind::Kimi),
        "copilot" => Some(ToolKind::Copilot),
        "pi" | "pi-agent" => Some(ToolKind::Pi),
        _ => detect_kind_from_path(path),
    };

    kind.ok_or_else(|| {
        format!(
            "Unable to determine transcript parser for agent '{}' and path '{}'",
            agent, path
        )
    })
}

// ── Public API for other commands (bundle) ──────────────────────────────

/// Options for querying and formatting transcript exchanges.
pub struct TranscriptQuery<'a> {
    pub path: &'a str,
    pub agent: &'a str,
    pub last: usize,
    pub detailed: bool,
    pub session_id: Option<&'a str>,
}

/// Public wrapper for read (used by bundle prepare/cat).
///
/// Returns a JSON projection that intentionally drops tools/edits/errors/
/// ended_on_error — bundle consumers only read user/action/files/timestamp.
pub fn get_exchanges_pub(q: &TranscriptQuery) -> Result<Vec<Value>, String> {
    let kind = kind_from_agent_or_path(q.agent, q.path)?;
    let opts = ReadOptions {
        last: q.last,
        detailed: q.detailed,
        session_id: q.session_id.map(|s| s.to_string()),
        allow_codex_retry: true,
    };
    let exchanges = read(Path::new(q.path), kind, &opts)?;
    Ok(exchanges
        .iter()
        .map(|ex| {
            json!({
                "position": ex.position,
                "user": ex.user,
                "action": ex.action,
                "files": ex.files,
                "timestamp": ex.timestamp,
            })
        })
        .collect())
}

/// Public wrapper for format_exchanges (used by bundle cat).
pub fn format_exchanges_pub(
    q: &TranscriptQuery,
    instance: &str,
    full: bool,
) -> Result<String, String> {
    let kind = kind_from_agent_or_path(q.agent, q.path)?;
    let opts = ReadOptions {
        last: q.last,
        detailed: q.detailed,
        session_id: q.session_id.map(|s| s.to_string()),
        allow_codex_retry: true,
    };
    let exchanges = read(Path::new(q.path), kind, &opts)?;
    Ok(format_exchanges(&exchanges, instance, full, q.detailed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_kind_from_path_routes_cursor_jsonl_via_agent_transcripts() {
        // Cursor `.jsonl` keys off the agent-transcripts segment, not the
        // extension (shared with Claude/Codex), so it no longer falls through
        // to Claude.
        assert_eq!(
            detect_kind_from_path("/h/.cursor/projects/r/agent-transcripts/u/u.jsonl"),
            Some(ToolKind::Cursor)
        );
        // A plain Claude `.jsonl` stays ambiguous.
        assert_eq!(detect_kind_from_path("/h/.claude/projects/r/u.jsonl"), None);
        assert_eq!(
            detect_kind_from_path("/x/session.json"),
            Some(ToolKind::Gemini)
        );
        assert_eq!(
            detect_kind_from_path("/x/opencode.db"),
            Some(ToolKind::OpenCode)
        );
    }

    #[test]
    fn unknown_agent_and_ambiguous_path_is_an_error() {
        let err = kind_from_agent_or_path("future-tool", "/tmp/session.jsonl").unwrap_err();
        assert!(err.contains("future-tool"));
        assert!(err.contains("session.jsonl"));
    }

    #[test]
    fn legacy_claude_pty_agent_maps_to_claude() {
        assert_eq!(
            kind_from_agent_or_path("claude-pty", "/h/.claude/projects/r/u.jsonl").unwrap(),
            ToolKind::Claude
        );
    }

    #[test]
    fn unknown_agent_can_use_unambiguous_path_detection() {
        assert_eq!(
            kind_from_agent_or_path("future-tool", "/tmp/session.json").unwrap(),
            ToolKind::Gemini
        );
    }
}
