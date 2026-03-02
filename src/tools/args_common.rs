//! Shared argument parsing infrastructure for CLI tools.
//!
//! functions, and token manipulation utilities used by claude_args, gemini_args,
//! and codex_args parsers.
//!
//! Each tool has specific flags and semantics, but shares:
//! - Token parsing patterns (flags, values, positionals)
//! - ArgsSpec field structure (source, clean_tokens, positional_tokens, etc.)
//! - Helper functions for token manipulation

use std::collections::HashSet;

// ==================== Source Type ====================

/// Where the args came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceType {
    Cli,
    Env,
    None,
}

// ==================== Flag Value ====================

/// A flag value that may be a single string or a list (for repeatable flags).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlagValue {
    Single(String),
    List(Vec<String>),
}

impl FlagValue {
    /// Get as single string (returns first element of list).
    pub fn as_str(&self) -> &str {
        match self {
            FlagValue::Single(s) => s,
            FlagValue::List(v) => v.first().map(|s| s.as_str()).unwrap_or(""),
        }
    }

    /// Get as list of strings.
    pub fn as_list(&self) -> Vec<&str> {
        match self {
            FlagValue::Single(s) => vec![s.as_str()],
            FlagValue::List(v) => v.iter().map(|s| s.as_str()).collect(),
        }
    }
}

// ==================== Token Helpers ====================

/// Extract flag name from token, handling --flag=value syntax.
/// Returns lowercase flag name or None if not a flag.
pub fn extract_flag_name_from_token(token: &str) -> Option<String> {
    let lower = token.to_lowercase();
    if !lower.starts_with('-') {
        return None;
    }
    if lower.contains('=') {
        Some(lower.split('=').next()?.to_string())
    } else {
        Some(lower)
    }
}

/// Extract normalized (lowercase) flag names from token list.
pub fn extract_flag_names_from_tokens(tokens: &[String]) -> HashSet<String> {
    tokens
        .iter()
        .filter_map(|t| extract_flag_name_from_token(t))
        .collect()
}

/// Check if a token looks like a known flag (not a positional value).
///
/// Takes tool-specific flag configuration. No catch-all `-` check — only
/// matches known flags to avoid rejecting values like "- check" or "-1".
pub fn looks_like_flag(
    token_lower: &str,
    exact_flags: &HashSet<String>,
    prefix_flags: &[String],
) -> bool {
    if exact_flags.contains(token_lower) {
        return true;
    }
    if token_lower == "--" {
        return true;
    }
    prefix_flags.iter().any(|p| token_lower.starts_with(p.as_str()))
}

/// Remove duplicate boolean flags, keeping first occurrence.
/// Only deduplicates flags in the provided set.
pub fn deduplicate_boolean_flags(tokens: &[String], boolean_flags: &HashSet<String>) -> Vec<String> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut result = Vec::new();

    for token in tokens {
        let lower = token.to_lowercase();
        if boolean_flags.contains(&lower) {
            if seen.contains(&lower) {
                continue;
            }
            seen.insert(lower);
        }
        result.push(token.clone());
    }

    result
}

/// Add or remove a boolean flag from token list.
pub fn toggle_flag(tokens: &[String], flag: &str, desired: bool) -> Vec<String> {
    let flag_lower = flag.to_lowercase();
    let filtered: Vec<String> = tokens
        .iter()
        .filter(|t| t.to_lowercase() != flag_lower)
        .cloned()
        .collect();

    if desired {
        let mut result = vec![flag.to_string()];
        result.extend(filtered);
        result
    } else {
        filtered
    }
}

/// Set a value flag, replacing any existing occurrence.
/// Handles both --flag value and --flag=value forms.
pub fn set_value_flag(tokens: &[String], flag: &str, value: &str) -> Vec<String> {
    let flag_lower = flag.to_lowercase();
    let mut result = Vec::new();
    let mut skip_next = false;

    for token in tokens {
        if skip_next {
            skip_next = false;
            continue;
        }
        let token_lower = token.to_lowercase();
        if token_lower == flag_lower {
            skip_next = true;
            continue;
        }
        if token_lower.starts_with(&format!("{}=", flag_lower)) {
            continue;
        }
        result.push(token.clone());
    }

    result.push(flag.to_string());
    result.push(value.to_string());
    result
}

/// Remove all occurrences of a flag and its value.
pub fn remove_flag_with_value(tokens: &[String], flag: &str) -> Vec<String> {
    let flag_lower = flag.to_lowercase();
    let mut result = Vec::new();
    let mut skip_next = false;

    for token in tokens {
        if skip_next {
            skip_next = false;
            continue;
        }
        let token_lower = token.to_lowercase();
        if token_lower == flag_lower {
            skip_next = true;
            continue;
        }
        if token_lower.starts_with(&format!("{}=", flag_lower)) {
            continue;
        }
        result.push(token.clone());
    }

    result
}

/// Set or replace the first positional argument.
pub fn set_positional(tokens: &[String], value: &str, positional_indexes: &[usize]) -> Vec<String> {
    let mut result = tokens.to_vec();
    if !positional_indexes.is_empty() {
        result[positional_indexes[0]] = value.to_string();
    } else {
        result.push(value.to_string());
    }
    result
}

/// Remove the first positional argument.
pub fn remove_positional(tokens: &[String], positional_indexes: &[usize]) -> Vec<String> {
    if positional_indexes.is_empty() {
        return tokens.to_vec();
    }
    let idx = positional_indexes[0];
    let mut result = tokens[..idx].to_vec();
    result.extend_from_slice(&tokens[idx + 1..]);
    result
}

// ==================== Common ArgsSpec Methods ====================

/// Check for user-provided flags in tokens (only scans before `--` separator).
pub fn has_flag_in_tokens(clean_tokens: &[String], names: &[&str], prefixes: &[&str]) -> bool {
    let name_set: HashSet<String> = names.iter().map(|n| n.to_lowercase()).collect();
    let prefix_list: Vec<String> = prefixes.iter().map(|p| p.to_lowercase()).collect();

    let dash_idx = clean_tokens
        .iter()
        .position(|t| t == "--")
        .unwrap_or(clean_tokens.len());

    for token in &clean_tokens[..dash_idx] {
        let lower = token.to_lowercase();
        if name_set.contains(&lower) {
            return true;
        }
        if prefix_list.iter().any(|p| lower.starts_with(p.as_str())) {
            return true;
        }
    }
    false
}

/// Rebuild token list from parsed spec fields.
/// If `include_subcommand`, prepends subcommand. If `include_positionals`, keeps all
/// tokens; otherwise filters out positional indexes.
pub fn rebuild_tokens_from(
    clean_tokens: &[String],
    positional_indexes: &[usize],
    subcommand: Option<&str>,
    include_positionals: bool,
    include_subcommand: bool,
) -> Vec<String> {
    let mut tokens = Vec::new();
    if include_subcommand {
        if let Some(sub) = subcommand {
            tokens.push(sub.to_string());
        }
    }
    if include_positionals {
        tokens.extend(clean_tokens.iter().cloned());
    } else {
        let pos_set: HashSet<usize> = positional_indexes.iter().copied().collect();
        tokens.extend(
            clean_tokens
                .iter()
                .enumerate()
                .filter(|(i, _)| !pos_set.contains(i))
                .map(|(_, t)| t.clone()),
        );
    }
    tokens
}

// ==================== Shell Utilities ====================

/// Simple shell-safe quoting for env string serialization.
pub fn shell_quote(s: &str) -> String {
    if s.is_empty() {
        return "''".to_string();
    }
    if s.chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.' || c == '/' || c == ':')
    {
        s.to_string()
    } else {
        format!("'{}'", s.replace('\'', "'\\''"))
    }
}

/// Simple shell string splitting (handles single and double quotes).
pub fn shell_split(s: &str) -> Result<Vec<String>, String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut chars = s.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;

    while let Some(ch) = chars.next() {
        if in_single {
            if ch == '\'' {
                in_single = false;
            } else {
                current.push(ch);
            }
        } else if in_double {
            if ch == '"' {
                in_double = false;
            } else if ch == '\\' {
                if let Some(&next) = chars.peek() {
                    if next == '"' || next == '\\' || next == '$' || next == '`' {
                        current.push(chars.next().unwrap());
                    } else {
                        current.push(ch);
                    }
                }
            } else {
                current.push(ch);
            }
        } else if ch == '\'' {
            in_single = true;
        } else if ch == '"' {
            in_double = true;
        } else if ch == '\\' {
            if let Some(next) = chars.next() {
                current.push(next);
            }
        } else if ch.is_whitespace() {
            if !current.is_empty() {
                tokens.push(current.clone());
                current.clear();
            }
        } else {
            current.push(ch);
        }
    }

    if in_single || in_double {
        return Err("unterminated quote".to_string());
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    Ok(tokens)
}

// ==================== String Distance ====================

/// Simple Levenshtein distance for flag suggestion.
pub fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let m = a.len();
    let n = b.len();

    let mut dp = vec![vec![0usize; n + 1]; m + 1];
    for i in 0..=m {
        dp[i][0] = i;
    }
    for j in 0..=n {
        dp[0][j] = j;
    }
    for i in 1..=m {
        for j in 1..=n {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            dp[i][j] = (dp[i - 1][j] + 1)
                .min(dp[i][j - 1] + 1)
                .min(dp[i - 1][j - 1] + cost);
        }
    }
    dp[m][n]
}

/// Find closest match for a flag name (Levenshtein-based).
pub fn find_close_match(input: &str, candidates: &[String]) -> Option<String> {
    let input_lower = input.to_lowercase();
    let mut best: Option<(usize, &String)> = None;

    for candidate in candidates {
        let dist = levenshtein(&input_lower, &candidate.to_lowercase());
        let threshold = (candidate.len() as f64 * 0.4).ceil() as usize;
        if dist <= threshold && best.is_none_or(|(d, _)| dist < d) {
            best = Some((dist, candidate));
        }
    }

    best.map(|(_, s)| s.clone())
}

// ==================== Tests ====================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_flag_name() {
        assert_eq!(
            extract_flag_name_from_token("--model"),
            Some("--model".to_string())
        );
        assert_eq!(
            extract_flag_name_from_token("--model=opus"),
            Some("--model".to_string())
        );
        assert_eq!(
            extract_flag_name_from_token("-p"),
            Some("-p".to_string())
        );
        assert_eq!(extract_flag_name_from_token("value"), None);
    }

    #[test]
    fn test_extract_flag_names_from_tokens() {
        let tokens: Vec<String> = vec![
            "--model".into(),
            "opus".into(),
            "--verbose".into(),
            "text".into(),
        ];
        let names = extract_flag_names_from_tokens(&tokens);
        assert!(names.contains("--model"));
        assert!(names.contains("--verbose"));
        assert!(!names.contains("opus"));
    }

    #[test]
    fn test_deduplicate_boolean_flags() {
        let bool_flags: HashSet<String> = ["--verbose", "--help"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let tokens: Vec<String> = vec![
            "--verbose".into(),
            "--model".into(),
            "opus".into(),
            "--verbose".into(),
        ];
        let result = deduplicate_boolean_flags(&tokens, &bool_flags);
        assert_eq!(result, vec!["--verbose", "--model", "opus"]);
    }

    #[test]
    fn test_toggle_flag_add() {
        let tokens: Vec<String> = vec!["--model".into(), "opus".into()];
        let result = toggle_flag(&tokens, "--verbose", true);
        assert_eq!(result[0], "--verbose");
    }

    #[test]
    fn test_toggle_flag_remove() {
        let tokens: Vec<String> = vec!["--verbose".into(), "--model".into(), "opus".into()];
        let result = toggle_flag(&tokens, "--verbose", false);
        assert!(!result.contains(&"--verbose".to_string()));
    }

    #[test]
    fn test_set_value_flag() {
        let tokens: Vec<String> = vec!["--model".into(), "sonnet".into(), "--verbose".into()];
        let result = set_value_flag(&tokens, "--model", "opus");
        assert!(result.contains(&"opus".to_string()));
        assert!(!result.contains(&"sonnet".to_string()));
    }

    #[test]
    fn test_remove_flag_with_value() {
        let tokens: Vec<String> = vec!["--model".into(), "opus".into(), "--verbose".into()];
        let result = remove_flag_with_value(&tokens, "--model");
        assert!(!result.contains(&"--model".to_string()));
        assert!(!result.contains(&"opus".to_string()));
        assert!(result.contains(&"--verbose".to_string()));
    }

    #[test]
    fn test_set_positional() {
        let tokens: Vec<String> = vec!["--verbose".into(), "old".into()];
        let result = set_positional(&tokens, "new", &[1]);
        assert_eq!(result[1], "new");
    }

    #[test]
    fn test_set_positional_append() {
        let tokens: Vec<String> = vec!["--verbose".into()];
        let result = set_positional(&tokens, "prompt", &[]);
        assert_eq!(result.last().unwrap(), "prompt");
    }

    #[test]
    fn test_remove_positional() {
        let tokens: Vec<String> = vec!["--verbose".into(), "prompt".into(), "--model".into()];
        let result = remove_positional(&tokens, &[1]);
        assert_eq!(result, vec!["--verbose", "--model"]);
    }

    #[test]
    fn test_shell_quote() {
        assert_eq!(shell_quote("simple"), "simple");
        assert_eq!(shell_quote("has space"), "'has space'");
        assert_eq!(shell_quote(""), "''");
        assert_eq!(shell_quote("it's"), "'it'\\''s'");
    }

    #[test]
    fn test_shell_split() {
        assert_eq!(
            shell_split("--model opus --verbose").unwrap(),
            vec!["--model", "opus", "--verbose"]
        );
        assert_eq!(
            shell_split("'hello world' --flag").unwrap(),
            vec!["hello world", "--flag"]
        );
        assert!(shell_split("'unterminated").is_err());
    }

    #[test]
    fn test_levenshtein() {
        assert_eq!(levenshtein("kitten", "sitting"), 3);
        assert_eq!(levenshtein("", "abc"), 3);
        assert_eq!(levenshtein("same", "same"), 0);
    }

    #[test]
    fn test_find_close_match() {
        let candidates: Vec<String> = vec![
            "--verbose".into(),
            "--model".into(),
            "--version".into(),
        ];
        assert_eq!(
            find_close_match("--verbos", &candidates),
            Some("--verbose".to_string())
        );
        assert_eq!(find_close_match("--xyz-totally-different", &candidates), None);
    }

    #[test]
    fn test_looks_like_flag() {
        let exact: HashSet<String> = ["--verbose", "--model", "-p"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let prefixes: Vec<String> = vec!["--model=".to_string()];

        assert!(looks_like_flag("--verbose", &exact, &prefixes));
        assert!(looks_like_flag("--model=opus", &exact, &prefixes));
        assert!(looks_like_flag("--", &exact, &prefixes));
        assert!(!looks_like_flag("value", &exact, &prefixes));
    }
}
