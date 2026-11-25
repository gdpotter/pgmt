use anyhow::{Result, anyhow};
use sqlx::PgPool;

use crate::db::error_context::SqlErrorContext;
use crate::schema_loader::SchemaFile;

/// Unified SQL content executor that consolidates SQL execution with enhanced error reporting
pub struct SqlContentExecutor {
    pool: PgPool,
    config: SqlExecutorConfig,
}

/// Configuration for SQL content execution behavior
#[derive(Debug, Clone)]
pub struct SqlExecutorConfig {
    /// Characters to show before truncating content in error messages
    pub content_truncation: usize,
    /// Type of source context for error messages
    pub source_context: SourceContextStyle,
    /// Whether to continue on certain types of errors (reserved for future implementation)
    #[allow(dead_code)]
    pub continue_on_error: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SourceContextStyle {
    File, // Display as file context
    Step, // Display as step context
    #[allow(dead_code)]
    Custom(String), // Custom context label
}

impl Default for SqlExecutorConfig {
    fn default() -> Self {
        Self {
            content_truncation: 300,
            source_context: SourceContextStyle::File,
            continue_on_error: false,
        }
    }
}

/// Enhanced SQL execution error with configurable formatting
#[derive(Debug)]
pub struct SqlExecutionError {
    pub source_context: String,
    pub sql_content: String,
    pub line_number: Option<usize>,
    pub postgres_error: String,
    pub pg_detail: Option<String>,
    pub pg_hint: Option<String>,
    pub pg_context: Option<String>,
    pub suggestion: Option<String>,
    pub dependencies_info: Option<String>,
}

impl SqlExecutionError {
    /// Describe `error`, raised running `sql` from `source`.
    pub fn from_sqlx_error(error: &sqlx::Error, sql: &str, source: &str) -> Self {
        let ctx = SqlErrorContext::from_sqlx_error(error, sql);
        Self {
            source_context: source.to_string(),
            sql_content: sql.to_string(),
            line_number: ctx.line_number,
            postgres_error: ctx.message,
            pg_detail: ctx.detail,
            pg_hint: ctx.hint,
            pg_context: ctx.context,
            suggestion: None,
            dependencies_info: None,
        }
    }

    pub fn format_error(&self, context_style: &SourceContextStyle, truncate_at: usize) -> String {
        let mut error_msg = String::new();

        // Format header based on context style
        let context_label = match context_style {
            SourceContextStyle::File => "schema file",
            SourceContextStyle::Step => "migration step",
            SourceContextStyle::Custom(label) => label,
        };

        error_msg.push_str(&format!(
            "❌ Failed to apply {} '{}'",
            context_label, self.source_context
        ));

        // Add dependencies info if available
        if let Some(deps_info) = &self.dependencies_info {
            error_msg.push_str(&format!(" {}", deps_info));
        }

        error_msg.push_str("\n\n🐘 Database Error:\n");
        error_msg.push_str(&self.postgres_error);

        if let Some(line_num) = self.line_number {
            error_msg.push_str(&format!(" (Line {})", line_num));
        }

        // Add PostgreSQL detail/hint/context if available
        if let Some(detail) = &self.pg_detail {
            error_msg.push_str(&format!("\n\n  Detail: {}", detail));
        }
        if let Some(hint) = &self.pg_hint {
            error_msg.push_str(&format!("\n  Hint: {}", hint));
        }
        if let Some(ctx) = &self.pg_context {
            error_msg.push_str(&format!("\n  Context: {}", ctx));
        }

        // Show content preview - prioritize context around error line if available
        let content_preview = if let Some(line_num) = self.line_number {
            Self::format_content_with_line_context(&self.sql_content, line_num, truncate_at)
        } else {
            // Fallback to original behavior: show from beginning
            let trimmed = self.sql_content.trim();
            if trimmed.len() > truncate_at {
                format!(
                    "{}...\n\n[Content truncated - {} total characters]",
                    &trimmed[..truncate_at],
                    trimmed.len()
                )
            } else {
                trimmed.to_string()
            }
        };

        error_msg.push_str(&format!("\n\n📄 Content:\n{}", content_preview.trim_end()));

        if let Some(suggestion) = &self.suggestion {
            error_msg.push_str(&format!("\n\n💡 Suggestion: {}", suggestion));
        }

        error_msg
    }

    /// Format content showing context around the error line
    /// Shows a window of lines centered on the error location
    fn format_content_with_line_context(
        content: &str,
        error_line: usize,
        max_chars: usize,
    ) -> String {
        let lines: Vec<&str> = content.lines().collect();
        let total_lines = lines.len();

        // Show 3 lines before and 3 lines after the error line
        const CONTEXT_LINES: usize = 3;

        // Calculate line window (1-indexed to 0-indexed conversion)
        let error_idx = error_line.saturating_sub(1);
        let start_idx = error_idx.saturating_sub(CONTEXT_LINES);
        let end_idx = (error_idx + CONTEXT_LINES + 1).min(total_lines);

        let mut result = String::new();

        // Add prefix if we're not showing from the beginning
        if start_idx > 0 {
            result.push_str(&format!(
                "... [Showing lines {}-{} of {}]\n\n",
                start_idx + 1,
                end_idx,
                total_lines
            ));
        }

        // Show the context window with line numbers
        for (idx, line) in lines[start_idx..end_idx].iter().enumerate() {
            let line_num = start_idx + idx + 1;
            let is_error_line = line_num == error_line;

            if is_error_line {
                result.push_str(&format!("❌ {:4} | {}\n", line_num, line));
            } else {
                result.push_str(&format!("   {:4} | {}\n", line_num, line));
            }
        }

        // Add suffix if we're not showing to the end
        if end_idx < total_lines {
            result.push_str(&format!("\n... [{} more lines]", total_lines - end_idx));
        }

        // If result is still too long, truncate it
        if result.len() > max_chars {
            format!(
                "{}...\n\n[Content truncated - {} total characters]",
                &result[..max_chars],
                result.len()
            )
        } else {
            result
        }
    }
}

impl std::fmt::Display for SqlExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Default to enhanced formatting with reasonable truncation
        write!(f, "{}", self.format_error(&SourceContextStyle::File, 500))
    }
}

impl std::error::Error for SqlExecutionError {}

impl SqlContentExecutor {
    pub fn new(pool: PgPool, config: SqlExecutorConfig) -> Self {
        Self { pool, config }
    }

    /// Execute SQL content with optional dependency context
    pub async fn execute_content_with_deps(
        &self,
        content: &str,
        source: &str,
        deps_info: Option<String>,
    ) -> Result<()> {
        // Skip empty content
        let trimmed_content = content.trim();
        if trimmed_content.is_empty() {
            return Ok(());
        }

        // Execute the SQL content using raw_sql (properly supports multiple statements)
        match sqlx::raw_sql(sqlx::AssertSqlSafe(content.to_string()))
            .execute(&self.pool)
            .await
        {
            Ok(_) => Ok(()),
            Err(sqlx_error) => {
                let error_info = SqlExecutionError {
                    suggestion: Self::schema_file_suggestion(&sqlx_error),
                    dependencies_info: deps_info,
                    ..SqlExecutionError::from_sqlx_error(&sqlx_error, content, source)
                };

                let formatted_error = error_info
                    .format_error(&self.config.source_context, self.config.content_truncation);
                Err(anyhow!(formatted_error))
            }
        }
    }

    /// Execute a single schema file with enhanced error reporting
    pub async fn execute_schema_file(&self, file: &SchemaFile) -> Result<()> {
        let deps_info = if !file.dependencies.is_empty() {
            Some(format!("(depends on: {})", file.dependencies.join(", ")))
        } else {
            None
        };

        self.execute_content_with_deps(&file.content, &file.relative_path, deps_info)
            .await
    }
}

impl SqlContentExecutor {
    /// A pgmt-specific next step for a schema file error, where one exists.
    /// Anything Postgres already explains (syntax, permissions, type
    /// mismatches) gets nothing, so the database error stays the focus.
    fn schema_file_suggestion(error: &sqlx::Error) -> Option<String> {
        let code = error.as_database_error()?.code()?;
        match code.as_ref() {
            // undefined_table, undefined_object, undefined_function, invalid_schema_name
            "42P01" | "42704" | "42883" | "3F000" => Some(
                "If this is defined in another schema file, add a `-- require:` header \
                 for that file so it's applied first."
                    .to_string(),
            ),
            // duplicate_table, duplicate_object, duplicate_function, duplicate_schema
            "42P07" | "42710" | "42723" | "42P06" => Some(
                "The shadow database is rebuilt from your schema files on every run, so \
                 something earlier in this run already created it -- usually a duplicate \
                 definition in another schema file."
                    .to_string(),
            ),
            _ => None,
        }
    }
}

/// Specialized executor for schema files with dependency-aware error reporting
pub struct SchemaFileExecutor {
    inner: SqlContentExecutor,
}

impl SchemaFileExecutor {
    pub fn new(pool: PgPool) -> Self {
        let config = SqlExecutorConfig {
            content_truncation: 400,
            source_context: SourceContextStyle::File,
            continue_on_error: false,
        };

        Self {
            inner: SqlContentExecutor::new(pool, config),
        }
    }

    /// Execute a single schema file
    pub async fn execute_schema_file(&self, file: &SchemaFile) -> Result<()> {
        self.inner.execute_schema_file(file).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_execution_error_display() {
        let error = SqlExecutionError {
            source_context: "tables/users.sql".to_string(),
            sql_content: "CREATE TABLE users (id SERIAL, email TEXTT)".to_string(),
            line_number: Some(1),
            postgres_error: "type \"textt\" does not exist".to_string(),
            pg_detail: None,
            pg_hint: Some("Check spelling of type name".to_string()),
            pg_context: None,
            suggestion: Some("Check for typos in data type names".to_string()),
            dependencies_info: None,
        };

        let display = format!("{}", error);
        assert!(display.contains("tables/users.sql"));
        assert!(display.contains("Line 1"));
        assert!(display.contains("Check for typos"));
        assert!(display.contains("Hint: Check spelling"));
    }

    #[test]
    fn test_sql_execution_error_with_pg_context() {
        let error = SqlExecutionError {
            source_context: "functions/process.sql".to_string(),
            sql_content: "CREATE FUNCTION test() RETURNS void AS $$ BEGIN SELECT * FROM missing_table; END; $$ LANGUAGE plpgsql;".to_string(),
            line_number: Some(1),
            postgres_error: "relation \"missing_table\" does not exist".to_string(),
            pg_detail: Some("Table was dropped in migration V123".to_string()),
            pg_hint: None,
            pg_context: Some("PL/pgSQL function test() line 1 at SQL statement".to_string()),
            suggestion: None,
            dependencies_info: None,
        };

        let display = format!("{}", error);
        assert!(display.contains("functions/process.sql"));
        assert!(display.contains("Detail: Table was dropped"));
        assert!(display.contains("Context: PL/pgSQL function"));
    }
}
