//! OpenCode launch preprocessing.
//!
//! Port of OpenCode-specific env setup from launcher.py lines 800-827.
//!
//! Minimal preprocessing: sets environment variables for hcom integration.
//! Plugin management is handled separately in hooks/opencode.rs.

use std::collections::HashMap;

/// Auto-approve JSON for hcom bash commands.
/// Value for OPENCODE_PERMISSION env var.
const OPENCODE_PERMISSION_JSON: &str = r#"{"bash":{"hcom *":"allow"}}"#;

/// Preprocess environment variables for OpenCode launch.
///
/// Sets:
/// - `OPENCODE_PERMISSION`: Auto-approve all `hcom *` bash commands
/// - `HCOM_NAME`: Instance name for plugin diagnostics (set before identity binding)
pub fn preprocess_opencode_env(env: &mut HashMap<String, String>, instance_name: &str) {
    env.insert(
        "OPENCODE_PERMISSION".to_string(),
        OPENCODE_PERMISSION_JSON.to_string(),
    );
    env.insert("HCOM_NAME".to_string(), instance_name.to_string());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_preprocess_sets_permission() {
        let mut env = HashMap::new();
        preprocess_opencode_env(&mut env, "luna");
        let perm = env.get("OPENCODE_PERMISSION").unwrap();
        assert!(perm.contains("hcom *"));
        assert!(perm.contains("allow"));
    }

    #[test]
    fn test_preprocess_sets_hcom_name() {
        let mut env = HashMap::new();
        preprocess_opencode_env(&mut env, "nova");
        assert_eq!(env.get("HCOM_NAME").unwrap(), "nova");
    }

    #[test]
    fn test_preprocess_overwrites_existing() {
        let mut env = HashMap::new();
        env.insert("HCOM_NAME".to_string(), "old".to_string());
        preprocess_opencode_env(&mut env, "nova");
        assert_eq!(env.get("HCOM_NAME").unwrap(), "nova");
    }

    #[test]
    fn test_permission_json_is_valid() {
        let parsed: serde_json::Value =
            serde_json::from_str(OPENCODE_PERMISSION_JSON).expect("valid JSON");
        assert!(parsed["bash"]["hcom *"].is_string());
    }
}
