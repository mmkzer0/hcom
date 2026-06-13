//! Shell token helpers used by launch configuration and runner scripts.

/// Simple shell-safe quoting for runner script serialization.
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

/// Split a configured argument string while preserving quoted values.
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
                    if matches!(next, '"' | '\\' | '$' | '`') {
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
                tokens.push(std::mem::take(&mut current));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_quote_handles_spaces_and_quotes() {
        assert_eq!(shell_quote("simple"), "simple");
        assert_eq!(shell_quote("has space"), "'has space'");
        assert_eq!(shell_quote(""), "''");
        assert_eq!(shell_quote("it's"), "'it'\\''s'");
    }

    #[test]
    fn shell_split_preserves_quoted_values() {
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
}
