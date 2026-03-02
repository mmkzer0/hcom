//! Transcript detail level definitions and validation.
//!
//! content is displayed: normal (truncated), full (complete), detailed (with tool I/O).

use std::fmt;

/// Detail level for transcript display.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetailLevel {
    /// Truncated output (default).
    Normal,
    /// Complete text.
    Full,
    /// Complete text with tool I/O.
    Detailed,
}

impl DetailLevel {
    /// All valid detail levels.
    pub const ALL: &[DetailLevel] = &[
        DetailLevel::Normal,
        DetailLevel::Full,
        DetailLevel::Detailed,
    ];

    /// Parse from string, returning error message on failure.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "normal" => Ok(DetailLevel::Normal),
            "full" => Ok(DetailLevel::Full),
            "detailed" => Ok(DetailLevel::Detailed),
            _ => Err(format!(
                "Invalid detail level '{}'. Must be one of: detailed, full, normal \
                 (maps to hcom transcript flags: detailed=--detailed | full=--full | normal=default)",
                s
            )),
        }
    }

    /// String representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            DetailLevel::Normal => "normal",
            DetailLevel::Full => "full",
            DetailLevel::Detailed => "detailed",
        }
    }

    /// Corresponding transcript command flag.
    pub fn to_flag(&self) -> &'static str {
        match self {
            DetailLevel::Normal => "",
            DetailLevel::Full => "--full",
            DetailLevel::Detailed => "--detailed",
        }
    }

    /// Human description.
    pub fn description(&self) -> &'static str {
        match self {
            DetailLevel::Normal => "truncated (default)",
            DetailLevel::Full => "complete text",
            DetailLevel::Detailed => "complete text with tool I/O and edits",
        }
    }

    /// Whether this level requires full (non-truncated) output.
    pub fn is_full_output(&self) -> bool {
        matches!(self, DetailLevel::Full | DetailLevel::Detailed)
    }
}

impl fmt::Display for DetailLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Validate a detail level string. Returns Ok(()) or descriptive error.
pub fn validate_detail_level(detail: &str) -> Result<(), String> {
    DetailLevel::parse(detail).map(|_| ())
}

/// Check if detail level requires full output (not truncated).
pub fn is_full_output_detail(detail: &str) -> bool {
    DetailLevel::parse(detail)
        .map(|d| d.is_full_output())
        .unwrap_or(false)
}

/// Help text describing detail levels.
pub fn get_detail_help_text() -> &'static str {
    "Format: range:detail (e.g., 3-14:normal,6:full,22-30:detailed)"
}

/// Mapping description for help text.
pub fn get_detail_mapping_text() -> &'static str {
    "normal = truncated | full = --full flag | detailed = --detailed flag"
}

/// JSON example for help text.
pub fn get_detail_json_example() -> &'static str {
    r#"["10-15:normal", "20:full", "30-35:detailed"]"#
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_valid() {
        assert_eq!(DetailLevel::parse("normal").unwrap(), DetailLevel::Normal);
        assert_eq!(DetailLevel::parse("full").unwrap(), DetailLevel::Full);
        assert_eq!(
            DetailLevel::parse("detailed").unwrap(),
            DetailLevel::Detailed
        );
    }

    #[test]
    fn test_parse_invalid() {
        let err = DetailLevel::parse("verbose").unwrap_err();
        assert!(err.contains("Invalid detail level 'verbose'"));
        assert!(err.contains("detailed, full, normal"));
    }

    #[test]
    fn test_as_str_roundtrip() {
        for level in DetailLevel::ALL {
            assert_eq!(DetailLevel::parse(level.as_str()).unwrap(), *level);
        }
    }

    #[test]
    fn test_to_flag() {
        assert_eq!(DetailLevel::Normal.to_flag(), "");
        assert_eq!(DetailLevel::Full.to_flag(), "--full");
        assert_eq!(DetailLevel::Detailed.to_flag(), "--detailed");
    }

    #[test]
    fn test_is_full_output() {
        assert!(!DetailLevel::Normal.is_full_output());
        assert!(DetailLevel::Full.is_full_output());
        assert!(DetailLevel::Detailed.is_full_output());
    }

    #[test]
    fn test_is_full_output_detail_str() {
        assert!(!is_full_output_detail("normal"));
        assert!(is_full_output_detail("full"));
        assert!(is_full_output_detail("detailed"));
        assert!(!is_full_output_detail("invalid"));
    }

    #[test]
    fn test_validate_detail_level() {
        assert!(validate_detail_level("normal").is_ok());
        assert!(validate_detail_level("bad").is_err());
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", DetailLevel::Normal), "normal");
        assert_eq!(format!("{}", DetailLevel::Full), "full");
        assert_eq!(format!("{}", DetailLevel::Detailed), "detailed");
    }
}
