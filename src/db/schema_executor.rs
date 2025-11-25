use anyhow::{Result, anyhow};
use sqlx::{Executor, PgPool};
use std::path::Path;

use crate::render::Safety;
use crate::schema_loader::SchemaFile;

/// Unified SQL content executor that consolidates SQL execution with enhanced error reporting
pub struct SqlContentExecutor {
    pool: PgPool,
    config: SqlExecutorConfig,
}

/// Configuration for SQL content execution behavior
#[derive(Debug, Clone)]
pub struct SqlExecutorConfig {
    /// Level of error formatting detail
    pub error_level: ErrorLevel,
    /// Style of progress reporting
    pub progress_style: ProgressStyle,
    /// Characters to show before truncating content in error messages
    pub content_truncation: usize,
    /// Type of source context for error messages
    pub source_context: SourceContextStyle,
    /// Whether to show safety indicators for apply operations
    pub safety_indicators: bool,
    /// Whether to continue on certain types of errors (reserved for future implementation)
    #[allow(dead_code)]
    pub continue_on_error: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ErrorLevel {
    Enhanced, // Include line numbers, better formatting
    WithTips, // Include troubleshooting tips and suggestions
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProgressStyle {
    None,      // No progress indicators
    FileCount, // Show file x/y progress
    StepCount, // Show step x/y progress
    Detailed,  // Verbose progress with content preview
}

#[derive(Debug, Clone, PartialEq)]
pub enum SourceContextStyle {
    File,     // Display as file context
    Step,     // Display as step context
    Baseline, // Display as baseline context
    #[allow(dead_code)]
    Custom(String), // Custom context label
}

impl Default for SqlExecutorConfig {
    fn default() -> Self {
        Self {
            error_level: ErrorLevel::Enhanced,
            progress_style: ProgressStyle::FileCount,
            content_truncation: 300,
            source_context: SourceContextStyle::File,
            safety_indicators: false,
            continue_on_error: false,
        }
    }
}

/// Legacy schema executor - maintained for backward compatibility
pub struct SchemaExecutor {}

/// Enhanced SQL execution error with configurable formatting
#[derive(Debug)]
pub struct SqlExecutionError {
    pub source_context: String,
    pub sql_content: String,
    pub line_number: Option<usize>,
    pub postgres_error: String,
    pub suggestion: Option<String>,
    pub troubleshooting_tips: Vec<String>,
    pub dependencies_info: Option<String>,
}

impl SqlExecutionError {
    pub fn format_error(
        &self,
        error_level: &ErrorLevel,
        context_style: &SourceContextStyle,
        truncate_at: usize,
    ) -> String {
        let mut error_msg = String::new();

        // Format header based on context style
        let context_label = match context_style {
            SourceContextStyle::File => "schema file",
            SourceContextStyle::Step => "migration step",
            SourceContextStyle::Baseline => "baseline",
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

        // Add line number for enhanced error level
        if matches!(error_level, ErrorLevel::Enhanced | ErrorLevel::WithTips)
            && let Some(line_num) = self.line_number
        {
            error_msg.push_str(&format!(" (Line {})", line_num));
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

        error_msg.push_str(&format!("\n\n📄 Content:\n{}", content_preview));

        // Add troubleshooting tips for WithTips error level
        if matches!(error_level, ErrorLevel::WithTips) && !self.troubleshooting_tips.is_empty() {
            error_msg.push_str("\n\nTroubleshooting tips:\n");
            for tip in &self.troubleshooting_tips {
                error_msg.push_str(&format!("• {}\n", tip));
            }
        }

        // Add basic suggestion for enhanced levels
        if matches!(error_level, ErrorLevel::Enhanced | ErrorLevel::WithTips)
            && let Some(suggestion) = &self.suggestion
        {
            error_msg.push_str(&format!("\n💡 Suggestion: {}", suggestion));
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
        write!(
            f,
            "{}",
            self.format_error(&ErrorLevel::Enhanced, &SourceContextStyle::File, 500)
        )
    }
}

impl std::error::Error for SqlExecutionError {}

impl SqlContentExecutor {
    pub fn new(pool: PgPool, config: SqlExecutorConfig) -> Self {
        Self { pool, config }
    }

    /// Core unified method - executes SQL content with enhanced error handling
    pub async fn execute_content(&self, content: &str, source: &str) -> Result<()> {
        self.execute_content_with_deps(content, source, None).await
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

        // Execute the SQL content using sqlx Executor (supports multiple statements)
        match self.pool.execute(content).await {
            Ok(_) => Ok(()),
            Err(sqlx_error) => {
                let error_info = SqlExecutionError {
                    source_context: source.to_string(),
                    sql_content: content.to_string(),
                    line_number: Self::extract_line_number_from_error(&sqlx_error),
                    postgres_error: sqlx_error.to_string(),
                    suggestion: Self::generate_suggestion(&sqlx_error),
                    troubleshooting_tips: Self::generate_troubleshooting_tips(&sqlx_error),
                    dependencies_info: deps_info,
                };

                let formatted_error = error_info.format_error(
                    &self.config.error_level,
                    &self.config.source_context,
                    self.config.content_truncation,
                );
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

    /// Execute baseline SQL with appropriate context
    pub async fn execute_baseline(&self, baseline_sql: &str, source: &str) -> Result<()> {
        self.execute_content(baseline_sql, source).await
    }

    /// Execute a migration step with safety indicators and step context
    pub async fn execute_step(
        &self,
        step_sql: &str,
        safety: Safety,
        step_num: usize,
    ) -> Result<()> {
        // Show progress indicator if configured
        if matches!(
            self.config.progress_style,
            ProgressStyle::StepCount | ProgressStyle::Detailed
        ) {
            let step_prefix = match safety {
                Safety::Safe => "✅",
                Safety::Destructive => "⚠️",
            };

            if self.config.safety_indicators {
                println!(
                    "{} Executing step {}: {:?} operation",
                    step_prefix, step_num, safety
                );
            } else {
                println!("Executing step {}", step_num);
            }

            if matches!(self.config.progress_style, ProgressStyle::Detailed) {
                println!("{}", step_sql);
            }
        }

        self.execute_content(step_sql, &format!("step {}", step_num))
            .await
    }
}

// Legacy SchemaExecutor implementation for backward compatibility
impl SchemaExecutor {
    /// Execute SQL on any executor (pool or transaction) with enhanced error reporting
    /// This method is kept for migration compatibility
    pub async fn execute_sql_with_enhanced_errors<'e, E>(
        executor: E,
        file_path: &Path,
        sql_content: &str,
    ) -> Result<sqlx::postgres::PgQueryResult>
    where
        E: Executor<'e, Database = sqlx::Postgres>,
    {
        let relative_path = file_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("unknown_file.sql");

        // Execute the SQL content
        match executor.execute(sql_content).await {
            Ok(result) => Ok(result),
            Err(sqlx_error) => {
                let error_info = SqlExecutionError {
                    source_context: relative_path.to_string(),
                    sql_content: sql_content.to_string(),
                    line_number: SqlContentExecutor::extract_line_number_from_error(&sqlx_error),
                    postgres_error: sqlx_error.to_string(),
                    suggestion: SqlContentExecutor::generate_suggestion(&sqlx_error),
                    troubleshooting_tips: SqlContentExecutor::generate_troubleshooting_tips(
                        &sqlx_error,
                    ),
                    dependencies_info: None,
                };

                let formatted_error =
                    error_info.format_error(&ErrorLevel::Enhanced, &SourceContextStyle::File, 200); // Use smaller truncation for legacy compatibility
                Err(anyhow!(formatted_error))
            }
        }
    }
}

// Static helper methods for error analysis (used by both SqlContentExecutor and SchemaExecutor)
impl SqlContentExecutor {
    /// Extract line number from PostgreSQL error message if available
    fn extract_line_number_from_error(error: &sqlx::Error) -> Option<usize> {
        // PostgreSQL sometimes includes line numbers in error messages
        // Look for patterns like "at line 5" or "LINE 5:"
        let error_str = error.to_string();

        if let Ok(re) = regex::Regex::new(r"(?i)(?:at line|line)\s+(\d+)")
            && let Some(captures) = re.captures(&error_str)
            && let Some(line_match) = captures.get(1)
        {
            return line_match.as_str().parse().ok();
        }

        None
    }

    /// Generate helpful suggestions based on common error patterns
    fn generate_suggestion(error: &sqlx::Error) -> Option<String> {
        let error_str = error.to_string().to_lowercase();

        if error_str.contains("type") && error_str.contains("does not exist") {
            Some("Check for typos in data type names. Common types: TEXT, INTEGER, BOOLEAN, TIMESTAMP".to_string())
        } else if error_str.contains("syntax error") && error_str.contains("check") {
            Some(
                "Syntax error near CHECK. Verify CHECK constraint syntax and parentheses."
                    .to_string(),
            )
        } else if error_str.contains("column") && error_str.contains("does not exist") {
            Some(
                "Verify column names and ensure tables are created before referencing them."
                    .to_string(),
            )
        } else if error_str.contains("relation") && error_str.contains("does not exist") {
            Some("Table/view does not exist. Check dependency order and table names.".to_string())
        } else if error_str.contains("syntax error at or near") {
            if let Some(near_word) = Self::extract_near_word(&error_str) {
                Some(format!(
                    "Syntax error near '{}'. Check SQL syntax and keywords.",
                    near_word
                ))
            } else {
                Some("SQL syntax error. Verify SQL syntax and keywords.".to_string())
            }
        } else {
            None
        }
    }

    /// Generate contextual troubleshooting tips based on error patterns
    fn generate_troubleshooting_tips(error: &sqlx::Error) -> Vec<String> {
        let error_str = error.to_string();
        let mut tips = Vec::new();

        if error_str.contains("cannot insert multiple commands into a prepared statement") {
            tips.push(
                "Multiple SQL commands detected. This should be handled automatically.".to_string(),
            );
            tips.push(
                "Ensure each SQL statement ends with a semicolon and is separated by newlines."
                    .to_string(),
            );
        }

        if error_str.contains("already exists") {
            tips.push(
                "This object already exists. Check if this file is being applied multiple times."
                    .to_string(),
            );
            tips.push(
                "Consider if there are duplicate definitions or manual changes to the database."
                    .to_string(),
            );
        }

        if error_str.contains("does not exist") {
            tips.push(
                "A referenced object doesn't exist. Check if dependencies are properly specified."
                    .to_string(),
            );
            tips.push("Verify that dependent files are listed in the correct order.".to_string());
            tips.push("Check if `-- require:` headers are present and correct.".to_string());
        }

        if error_str.contains("syntax error") || error_str.contains("parse error") {
            tips.push(
                "There's a SQL syntax error. Check the SQL syntax in this content.".to_string(),
            );
            tips.push("Look for missing semicolons, unmatched parentheses, or typos.".to_string());
        }

        if error_str.contains("permission denied") {
            tips.push(
                "Permission denied. Check database user permissions for this operation."
                    .to_string(),
            );
        }

        tips
    }

    /// Extract the word mentioned in "syntax error at or near" messages
    fn extract_near_word(error_str: &str) -> Option<String> {
        if let Some(start) = error_str.find("at or near \"") {
            let start = start + 12; // Length of "at or near \""
            if let Some(end) = error_str[start..].find('"') {
                return Some(error_str[start..start + end].to_string());
            }
        }
        None
    }
}

/// Specialized executor for schema files with dependency-aware error reporting
pub struct SchemaFileExecutor {
    inner: SqlContentExecutor,
}

impl SchemaFileExecutor {
    pub fn new(pool: PgPool, verbose: bool) -> Self {
        let config = SqlExecutorConfig {
            error_level: ErrorLevel::WithTips,
            progress_style: if verbose {
                ProgressStyle::FileCount
            } else {
                ProgressStyle::None
            },
            content_truncation: 400,
            source_context: SourceContextStyle::File,
            safety_indicators: false,
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

/// Specialized executor for baseline files with enhanced progress reporting
pub struct BaselineExecutor {
    inner: SqlContentExecutor,
}

impl BaselineExecutor {
    pub fn new(pool: PgPool, verbose: bool, force: bool) -> Self {
        let config = SqlExecutorConfig {
            error_level: if force {
                ErrorLevel::Enhanced
            } else {
                ErrorLevel::WithTips
            },
            progress_style: if verbose {
                ProgressStyle::Detailed
            } else {
                ProgressStyle::None
            },
            content_truncation: 300,
            source_context: SourceContextStyle::Baseline,
            safety_indicators: false,
            continue_on_error: force,
        };

        Self {
            inner: SqlContentExecutor::new(pool, config),
        }
    }

    /// Execute baseline SQL with enhanced reporting
    pub async fn execute_baseline(&self, baseline_sql: &str, source: &str) -> Result<()> {
        self.inner.execute_baseline(baseline_sql, source).await
    }
}

/// Specialized executor for migration steps with safety indicators
pub struct ApplyStepExecutor {
    inner: SqlContentExecutor,
}

impl ApplyStepExecutor {
    pub fn new(pool: PgPool, verbose: bool, show_safety: bool, dry_run: bool) -> Self {
        let config = SqlExecutorConfig {
            error_level: ErrorLevel::Enhanced,
            progress_style: if verbose {
                if dry_run {
                    ProgressStyle::Detailed
                } else {
                    ProgressStyle::StepCount
                }
            } else {
                ProgressStyle::None
            },
            content_truncation: 250,
            source_context: SourceContextStyle::Step,
            safety_indicators: show_safety,
            continue_on_error: false,
        };

        Self {
            inner: SqlContentExecutor::new(pool, config),
        }
    }

    /// Execute a migration step with safety indicators
    pub async fn execute_step(
        &self,
        step_sql: &str,
        safety: Safety,
        step_num: usize,
    ) -> Result<()> {
        self.inner.execute_step(step_sql, safety, step_num).await
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
            suggestion: Some("Check for typos in data type names".to_string()),
            troubleshooting_tips: vec![],
            dependencies_info: None,
        };

        let display = format!("{}", error);
        assert!(display.contains("tables/users.sql"));
        assert!(display.contains("Line 1"));
        assert!(display.contains("Check for typos"));
    }
}
