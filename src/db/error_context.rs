//! Shared utility for extracting rich error context from PostgreSQL errors.
//! Used across all SQL file execution: schema init, migrations, baselines.

use sqlx::postgres::{PgDatabaseError, PgErrorPosition};

/// Rich error context extracted from PostgreSQL errors
#[derive(Debug, Clone)]
pub struct SqlErrorContext {
    /// The primary error message
    pub message: String,
    /// Line number in the SQL file (converted from character position)
    pub line_number: Option<usize>,
    /// Character position in the SQL content (1-indexed)
    #[allow(dead_code)]
    pub position: Option<usize>,
    /// Additional detail from PostgreSQL
    pub detail: Option<String>,
    /// Hint for fixing the error
    pub hint: Option<String>,
    /// Context (e.g., PL/pgSQL function context)
    pub context: Option<String>,
    /// PostgreSQL error code (e.g., "42P01" for undefined_table)
    #[allow(dead_code)]
    pub code: Option<String>,
}

impl SqlErrorContext {
    /// Extract rich error context from a sqlx error
    ///
    /// Uses structured data from PgDatabaseError - no string parsing needed.
    pub fn from_sqlx_error(error: &sqlx::Error, sql_content: &str) -> Self {
        if let Some(db_error) = error.as_database_error()
            && let Some(pg_error) = db_error.try_downcast_ref::<PgDatabaseError>()
        {
            let position = pg_error.position().map(|pos| match pos {
                PgErrorPosition::Original(p) => p,
                PgErrorPosition::Internal { position, .. } => position,
            });
            let line_number = position.map(|p| position_to_line(sql_content, p));

            return Self {
                message: pg_error.message().to_string(),
                line_number,
                position,
                detail: pg_error.detail().map(|s| s.to_string()),
                hint: pg_error.hint().map(|s| s.to_string()),
                context: pg_error.r#where().map(|s| s.to_string()),
                code: Some(pg_error.code().to_string()),
            };
        }

        // Fallback for non-PostgreSQL errors
        Self {
            message: error.to_string(),
            line_number: None,
            position: None,
            detail: None,
            hint: None,
            context: None,
            code: None,
        }
    }

    /// Format the error for display with file context
    pub fn format(&self, file_path: &str, sql_content: &str) -> String {
        let mut msg = format!("SQL error in '{}'", file_path);

        if let Some(line) = self.line_number {
            msg.push_str(&format!(" at line {}", line));
        }
        msg.push_str(":\n\n");

        // Primary error message
        msg.push_str(&format!("  {}\n", self.message));

        // Additional PostgreSQL context
        if let Some(detail) = &self.detail {
            msg.push_str(&format!("\n  Detail: {}", detail));
        }
        if let Some(hint) = &self.hint {
            msg.push_str(&format!("\n  Hint: {}", hint));
        }
        if let Some(ctx) = &self.context {
            msg.push_str(&format!("\n  Context: {}", ctx));
        }

        // Show relevant lines from the SQL content
        if let Some(line) = self.line_number {
            msg.push_str(&format!("\n\n{}", format_line_context(sql_content, line)));
        }

        msg
    }
}

/// Convert 1-indexed character position to line number
pub fn position_to_line(content: &str, position: usize) -> usize {
    let end = (position.saturating_sub(1)).min(content.len());
    content[..end].chars().filter(|c| *c == '\n').count() + 1
}

/// Format SQL content showing context around the error line
pub fn format_line_context(content: &str, error_line: usize) -> String {
    let lines: Vec<&str> = content.lines().collect();
    let total_lines = lines.len();
    const CONTEXT_LINES: usize = 3;

    let error_idx = error_line.saturating_sub(1);
    let start_idx = error_idx.saturating_sub(CONTEXT_LINES);
    let end_idx = (error_idx + CONTEXT_LINES + 1).min(total_lines);

    let mut result = String::new();

    if start_idx > 0 {
        result.push_str(&format!("  ... [{} lines above]\n", start_idx));
    }

    for (idx, line) in lines[start_idx..end_idx].iter().enumerate() {
        let line_num = start_idx + idx + 1;
        let marker = if line_num == error_line { ">" } else { " " };
        result.push_str(&format!("  {} {:4} | {}\n", marker, line_num, line));
    }

    if end_idx < total_lines {
        result.push_str(&format!("  ... [{} lines below]", total_lines - end_idx));
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_position_to_line_first_line() {
        let content = "SELECT 1;\nSELECT 2;\nSELECT 3;";
        assert_eq!(position_to_line(content, 1), 1);
        assert_eq!(position_to_line(content, 5), 1);
    }

    #[test]
    fn test_position_to_line_second_line() {
        let content = "SELECT 1;\nSELECT 2;\nSELECT 3;";
        // Position 11 is first char of second line (after "SELECT 1;\n")
        assert_eq!(position_to_line(content, 11), 2);
    }

    #[test]
    fn test_position_to_line_third_line() {
        let content = "SELECT 1;\nSELECT 2;\nSELECT 3;";
        // Position 21 is first char of third line
        assert_eq!(position_to_line(content, 21), 3);
    }

    #[test]
    fn test_position_to_line_empty_content() {
        assert_eq!(position_to_line("", 1), 1);
        assert_eq!(position_to_line("", 100), 1);
    }

    #[test]
    fn test_position_to_line_beyond_content() {
        let content = "SELECT 1;";
        assert_eq!(position_to_line(content, 1000), 1);
    }

    #[test]
    fn test_format_line_context_middle() {
        let content = "line 1\nline 2\nline 3\nline 4\nline 5\nline 6\nline 7";
        let result = format_line_context(content, 4);
        assert!(result.contains(">    4 | line 4"));
        assert!(result.contains("     3 | line 3"));
        assert!(result.contains("     5 | line 5"));
    }

    #[test]
    fn test_format_line_context_first_line() {
        let content = "line 1\nline 2\nline 3\nline 4\nline 5";
        let result = format_line_context(content, 1);
        assert!(result.contains(">    1 | line 1"));
        assert!(!result.contains("lines above"));
    }

    #[test]
    fn test_format_line_context_last_line() {
        let content = "line 1\nline 2\nline 3\nline 4\nline 5";
        let result = format_line_context(content, 5);
        assert!(result.contains(">    5 | line 5"));
        assert!(!result.contains("lines below"));
    }

    #[test]
    fn test_format_line_context_shows_surrounding_lines() {
        let content =
            "line 1\nline 2\nline 3\nline 4\nline 5\nline 6\nline 7\nline 8\nline 9\nline 10";
        let result = format_line_context(content, 5);

        // Should show lines 2-8 (3 before and 3 after line 5)
        assert!(result.contains("     2 | line 2"));
        assert!(result.contains("     3 | line 3"));
        assert!(result.contains("     4 | line 4"));
        assert!(result.contains(">    5 | line 5"));
        assert!(result.contains("     6 | line 6"));
        assert!(result.contains("     7 | line 7"));
        assert!(result.contains("     8 | line 8"));

        // Should show "lines above" indicator since we're not at the start
        assert!(result.contains("lines above"));
        // Should show "lines below" indicator since we're not at the end
        assert!(result.contains("lines below"));
    }

    #[test]
    fn test_sql_error_context_format_with_line_number() {
        let ctx = SqlErrorContext {
            message: "relation \"users\" does not exist".to_string(),
            line_number: Some(3),
            position: Some(21),
            detail: None,
            hint: Some("Check if the table exists".to_string()),
            context: None,
            code: Some("42P01".to_string()),
        };

        let content = "SELECT 1;\nSELECT 2;\nSELECT * FROM users;";
        let result = ctx.format("schema.sql", content);

        assert!(result.contains("SQL error in 'schema.sql' at line 3"));
        assert!(result.contains("relation \"users\" does not exist"));
        assert!(result.contains("Hint: Check if the table exists"));
        assert!(result.contains(">    3 | SELECT * FROM users;"));
    }

    #[test]
    fn test_sql_error_context_format_without_line_number() {
        let ctx = SqlErrorContext {
            message: "connection refused".to_string(),
            line_number: None,
            position: None,
            detail: None,
            hint: None,
            context: None,
            code: None,
        };

        let content = "SELECT 1;";
        let result = ctx.format("schema.sql", content);

        assert!(result.contains("SQL error in 'schema.sql':"));
        assert!(result.contains("connection refused"));
        // Should not contain line context when no line number
        assert!(!result.contains("|"));
    }
}
