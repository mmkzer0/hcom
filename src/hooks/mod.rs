//! Shared hook infrastructure for all tools (Claude, Gemini, Codex, OpenCode).
//!
//! - Hook payload normalization (per-tool → unified `HookPayload`)
//! - Hook result types (allow/block/error with structured output)
//! - Shared functions: deliver, poll, bootstrap inject, identity init, finalize
//!
//! Tool-specific dispatchers (Phase 1B/1C/1D) call into these shared functions.

pub mod claude;
pub mod claude_args;
pub mod codex;
pub mod common;
pub mod family;
pub mod gemini;
pub mod opencode;
pub mod utils;

use serde_json::Value;

/// Shared test helpers for hook test modules (claude, codex, gemini).
#[cfg(test)]
pub mod test_helpers {
    use std::path::PathBuf;

    /// RAII guard that saves/restores HCOM_DIR and HOME env vars, and resets Config.
    pub struct EnvGuard {
        saved_hcom: Option<String>,
        saved_home: Option<String>,
    }

    impl EnvGuard {
        pub fn new() -> Self {
            Self {
                saved_hcom: std::env::var("HCOM_DIR").ok(),
                saved_home: std::env::var("HOME").ok(),
            }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            unsafe {
                match &self.saved_hcom {
                    Some(v) => std::env::set_var("HCOM_DIR", v),
                    None => std::env::remove_var("HCOM_DIR"),
                }
                match &self.saved_home {
                    Some(v) => std::env::set_var("HOME", v),
                    None => std::env::remove_var("HOME"),
                }
            }
            crate::config::Config::reset();
            crate::config::Config::init();
        }
    }

    /// Create an isolated test env: tempdir with .hcom dir, env vars set.
    /// Returns (tempdir, hcom_dir, test_home, guard).
    pub fn isolated_test_env() -> (tempfile::TempDir, PathBuf, PathBuf, EnvGuard) {
        let guard = EnvGuard::new();
        let dir = tempfile::tempdir().unwrap();
        let test_home = dir.path().to_path_buf();
        let hcom_dir = test_home.join(".hcom");
        std::fs::create_dir_all(&hcom_dir).unwrap();
        unsafe {
            std::env::set_var("HCOM_DIR", &hcom_dir);
            std::env::set_var("HOME", &test_home);
        }
        crate::config::Config::reset();
        crate::config::Config::init();
        (dir, hcom_dir, test_home, guard)
    }
}

// Re-export key types.
pub use common::{
    deliver_pending_messages, find_last_bind_marker, finalize_session, get_pending_instances,
    init_hook_context, inject_bootstrap_once, poll_messages, stop_instance,
};
pub use family::{bind_vanilla_instance, extract_tool_detail};
pub use utils::{HookCategory, HookInfo, HOOK_REGISTRY};

// ==================== Hook Payload ====================

/// Normalized hook payload — unified across all tools.
///
/// Each tool's raw hook JSON is different. Factory methods normalize into
/// this common struct so shared functions work identically across tools.
///
#[derive(Debug, Clone)]
pub struct HookPayload {
    /// Claude/Gemini session ID, Codex thread ID.
    pub session_id: String,
    /// Path to tool's JSONL transcript (Claude) or conversation log.
    pub transcript_path: String,
    /// Hook name (e.g., "Stop", "PostToolUse", "PreToolUse").
    pub hook_name: String,
    /// Tool type string ("claude", "gemini", "codex", "opencode").
    pub tool: String,
    /// Tool name from hook (e.g., "Bash", "Write" for PostToolUse).
    pub tool_name: String,
    /// Tool input dict (for extract_tool_detail).
    pub tool_input: Value,
    /// Tool result/response (for AfterTool/PostToolUse hooks).
    pub tool_result: String,
    /// Notification type (for Notification hooks, e.g., "ToolPermission").
    pub notification_type: String,
    /// Raw hook payload for tool-specific access.
    pub raw: Value,
}

impl HookPayload {
    /// Return session_id as Option (None if empty).
    pub fn session_id_opt(&self) -> Option<&str> {
        if self.session_id.is_empty() { None } else { Some(&self.session_id) }
    }

    /// Return transcript_path as Option (None if empty).
    pub fn transcript_path_opt(&self) -> Option<&str> {
        if self.transcript_path.is_empty() { None } else { Some(&self.transcript_path) }
    }

    /// Build from Claude hook JSON.
    ///
    /// Claude hook stdin format (all keys at root level):
    ///   { "session_id", "transcript_path", "tool_name", "tool_input",
    ///     "tool_response", "agent_id", "agent_type" }
    pub fn from_claude(raw: &Value) -> Self {
        // Tool result can be string or dict with stdout/stderr
        let tool_result = match raw.get("tool_response") {
            Some(Value::Object(obj)) => obj
                .get("stdout")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            Some(Value::String(s)) => s.clone(),
            _ => String::new(),
        };

        Self {
            session_id: raw
                .get("session_id")
                .or_else(|| raw.get("sessionId"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            transcript_path: raw
                .get("transcript_path")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            hook_name: raw
                .get("hook_name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            tool: "claude".to_string(),
            tool_name: raw
                .get("tool_name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            tool_input: raw
                .get("tool_input")
                .cloned()
                .unwrap_or(Value::Object(Default::default())),
            tool_result,
            notification_type: String::new(),
            raw: raw.clone(),
        }
    }

    /// Build from Gemini hook JSON.
    ///
    /// Gemini hook stdin format (all keys at root level):
    ///   { "session_id"/"sessionId", "transcript_path"/"session_path",
    ///     "tool_name"/"toolName", "tool_input"/"toolInput",
    ///     "tool_response", "notification_type" }
    pub fn from_gemini(raw: &Value) -> Self {
        // Tool response format varies: dict with llmContent/output, or string
        let tool_result = match raw.get("tool_response") {
            Some(Value::Object(obj)) => obj
                .get("llmContent")
                .or_else(|| obj.get("output"))
                .or_else(|| {
                    obj.get("response")
                        .and_then(|r| r.get("output"))
                })
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            Some(v) => v.as_str().map(|s| s.to_string()).unwrap_or_else(|| v.to_string()),
            None => String::new(),
        };

        // Gemini may use toolInput or tool_input
        let tool_input = raw
            .get("tool_input")
            .or_else(|| raw.get("toolInput"))
            .cloned()
            .unwrap_or(Value::Object(Default::default()));

        Self {
            session_id: raw
                .get("session_id")
                .or_else(|| raw.get("sessionId"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            transcript_path: raw
                .get("transcript_path")
                .or_else(|| raw.get("session_path"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            hook_name: raw
                .get("hook_name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            tool: "gemini".to_string(),
            tool_name: raw
                .get("tool_name")
                .or_else(|| raw.get("toolName"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            tool_input,
            tool_result,
            notification_type: raw
                .get("notification_type")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            raw: raw.clone(),
        }
    }

    /// Build from Codex hook JSON.
    ///
    /// Codex notify payload (passed as argv[2]):
    ///   { "type": "agent-turn-complete", "thread-id": "uuid",
    ///     "turn-id", "cwd", "input-messages", "last-assistant-message" }
    /// Note: Codex has no tool_name/tool_input — only event_type.
    pub fn from_codex(raw: &Value) -> Self {
        let thread_id = raw
            .get("thread-id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        Self {
            session_id: thread_id,
            transcript_path: raw
                .get("transcript_path")
                .or_else(|| raw.get("session_path"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            hook_name: raw
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            tool: "codex".to_string(),
            tool_name: String::new(),
            tool_input: Value::Object(Default::default()),
            tool_result: String::new(),
            notification_type: String::new(),
            raw: raw.clone(),
        }
    }

    /// Build from OpenCode hook JSON.
    ///
    /// OpenCode hooks: session_id from env, minimal tool info.
    pub fn from_opencode(raw: &Value) -> Self {
        Self {
            session_id: raw
                .get("session_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            transcript_path: raw
                .get("transcript_path")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            hook_name: raw
                .get("hook_name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            tool: "opencode".to_string(),
            tool_name: raw
                .get("tool_name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            tool_input: raw
                .get("tool_input")
                .cloned()
                .unwrap_or(Value::Object(Default::default())),
            tool_result: String::new(),
            notification_type: String::new(),
            raw: raw.clone(),
        }
    }
}

// ==================== Hook Result ====================

/// Hook handler result — determines exit code and stdout output.
///
/// the dispatcher into exit codes + JSON output.
#[derive(Debug, Clone)]
pub enum HookResult {
    /// Allow the operation (exit 0, optional additionalContext/systemMessage).
    Allow {
        /// Additional context injected into the model's context window.
        additional_context: Option<String>,
        /// System message update (Claude-specific).
        system_message: Option<String>,
    },

    /// Block the operation (exit 2, with reason for blocking).
    /// Used by Stop hook to deliver messages.
    Block {
        /// Reason text (formatted messages for delivery).
        reason: String,
    },

    /// Update the tool input before execution (exit 0, updatedInput field).
    /// Used by PreToolUse to modify tool arguments.
    UpdateInput {
        /// Modified tool input JSON.
        updated_input: Value,
    },
}

impl HookResult {
    /// Exit code for this result.
    pub fn exit_code(&self) -> i32 {
        match self {
            HookResult::Allow { .. } => 0,
            HookResult::Block { .. } => 2,
            HookResult::UpdateInput { .. } => 0,
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hook_payload_from_claude() {
        // Matches actual Claude hook stdin: all keys at root level
        let raw = serde_json::json!({
            "session_id": "sess-123",
            "transcript_path": "/tmp/transcript.jsonl",
            "hook_name": "PostToolUse",
            "tool_name": "Bash",
            "tool_input": {"command": "ls"}
        });
        let payload = HookPayload::from_claude(&raw);
        assert_eq!(payload.session_id, "sess-123");
        assert_eq!(payload.transcript_path, "/tmp/transcript.jsonl");
        assert_eq!(payload.hook_name, "PostToolUse");
        assert_eq!(payload.tool, "claude");
        assert_eq!(payload.tool_name, "Bash");
    }

    #[test]
    fn test_hook_payload_from_gemini() {
        // Matches actual Gemini hook stdin: tool_name/tool_input at root
        let raw = serde_json::json!({
            "session_id": "gem-456",
            "hook_name": "after_tool_call",
            "tool_name": "run_shell_command",
            "tool_input": {"command": "echo hi"}
        });
        let payload = HookPayload::from_gemini(&raw);
        assert_eq!(payload.session_id, "gem-456");
        assert_eq!(payload.tool, "gemini");
        assert_eq!(payload.tool_name, "run_shell_command");
        assert_eq!(payload.tool_input["command"], "echo hi");
    }

    #[test]
    fn test_hook_payload_from_codex() {
        // Matches actual Codex notify payload: thread-id (hyphen), no tool_name
        let raw = serde_json::json!({
            "thread-id": "thread-789",
            "type": "agent-turn-complete",
            "cwd": "/tmp/project"
        });
        let payload = HookPayload::from_codex(&raw);
        assert_eq!(payload.session_id, "thread-789");
        assert_eq!(payload.tool, "codex");
        assert_eq!(payload.hook_name, "agent-turn-complete");
        assert_eq!(payload.tool_name, "");
    }

    #[test]
    fn test_hook_payload_from_opencode() {
        let raw = serde_json::json!({
            "session_id": "oc-111",
            "hook_name": "PostToolUse",
            "tool_name": "bash",
            "tool_input": {"command": "pwd"}
        });
        let payload = HookPayload::from_opencode(&raw);
        assert_eq!(payload.session_id, "oc-111");
        assert_eq!(payload.tool, "opencode");
        assert_eq!(payload.tool_name, "bash");
    }

    #[test]
    fn test_hook_payload_missing_fields() {
        let raw = serde_json::json!({});
        let payload = HookPayload::from_claude(&raw);
        assert_eq!(payload.session_id, "");
        assert_eq!(payload.transcript_path, "");
        assert_eq!(payload.tool_name, "");
    }

    #[test]
    fn test_hook_payload_from_gemini_camelcase_fallbacks() {
        // sessionId fallback
        let raw = serde_json::json!({
            "sessionId": "gem-camel",
            "session_path": "/tmp/gemini/chat.json",
            "hook_name": "BeforeAgent"
        });
        let payload = HookPayload::from_gemini(&raw);
        assert_eq!(payload.session_id, "gem-camel");
        assert_eq!(payload.transcript_path, "/tmp/gemini/chat.json");
    }

    #[test]
    fn test_hook_payload_from_gemini_tool_response_string() {
        // String tool_response should not be JSON-quoted
        let raw = serde_json::json!({
            "session_id": "gem-1",
            "tool_response": "plain text output"
        });
        let payload = HookPayload::from_gemini(&raw);
        assert_eq!(payload.tool_result, "plain text output");
    }

    #[test]
    fn test_hook_result_allow() {
        let result = HookResult::Allow {
            additional_context: Some("bootstrap text".into()),
            system_message: None,
        };
        assert_eq!(result.exit_code(), 0);
        match &result {
            HookResult::Allow { additional_context, system_message } => {
                assert_eq!(additional_context.as_deref(), Some("bootstrap text"));
                assert!(system_message.is_none());
            }
            _ => panic!("expected Allow"),
        }
    }

    #[test]
    fn test_hook_result_allow_empty() {
        let result = HookResult::Allow {
            additional_context: None,
            system_message: None,
        };
        assert_eq!(result.exit_code(), 0);
        match &result {
            HookResult::Allow { additional_context, system_message } => {
                assert!(additional_context.is_none());
                assert!(system_message.is_none());
            }
            _ => panic!("expected Allow"),
        }
    }

    #[test]
    fn test_hook_result_block() {
        let result = HookResult::Block {
            reason: "<hcom>message here</hcom>".into(),
        };
        assert_eq!(result.exit_code(), 2);
        match &result {
            HookResult::Block { reason } => {
                assert_eq!(reason, "<hcom>message here</hcom>");
            }
            _ => panic!("expected Block"),
        }
    }

    #[test]
    fn test_hook_result_update_input() {
        let result = HookResult::UpdateInput {
            updated_input: serde_json::json!({"command": "echo modified"}),
        };
        assert_eq!(result.exit_code(), 0);
        match &result {
            HookResult::UpdateInput { updated_input } => {
                assert_eq!(updated_input["command"], "echo modified");
            }
            _ => panic!("expected UpdateInput"),
        }
    }
}
