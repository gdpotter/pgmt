use crate::config::types::TrackingTable;
use crate::db::error_context::SqlErrorContext;
use crate::migration::section_parser::{
    BackoffStrategy, LockTimeoutAction, MigrationSection, RetryConfig, TransactionMode,
};
use crate::migration_tracking::section_tracking::*;
use crate::progress::SectionReporter;
use anyhow::Result;
use sqlx::PgPool;
use std::time::{Duration, Instant};

/// Format a SQL execution error with rich PostgreSQL context (DETAIL, HINT, etc.)
fn format_section_error(error: sqlx::Error, sql: &str, section_name: &str) -> anyhow::Error {
    let ctx = SqlErrorContext::from_sqlx_error(&error, sql);
    anyhow::anyhow!("{}", ctx.format(section_name, sql))
}

/// Execution mode controls retry and timeout behavior
#[derive(Debug, Clone, Copy)]
pub enum ExecutionMode {
    /// Production mode: full retry logic, timeouts, and progress tracking
    Production,
    /// Validation mode: no retries or timeouts, just execute SQL
    Validation,
}

pub struct SectionExecutor {
    pool: PgPool,
    tracking_table: TrackingTable,
    reporter: SectionReporter,
    mode: ExecutionMode,
}

impl SectionExecutor {
    pub fn new(
        pool: PgPool,
        tracking_table: TrackingTable,
        reporter: SectionReporter,
        mode: ExecutionMode,
    ) -> Self {
        Self {
            pool,
            tracking_table,
            reporter,
            mode,
        }
    }

    /// Execute a single section
    pub async fn execute_section(
        &mut self,
        migration_version: u64,
        section: &MigrationSection,
    ) -> Result<()> {
        // In Validation mode, skip all tracking and just execute SQL
        if matches!(self.mode, ExecutionMode::Validation) {
            return self.execute_validation(section).await;
        }

        // Check if section already completed (for resume)
        if let Some(SectionStatus::Completed) = get_section_status(
            &self.pool,
            &self.tracking_table,
            migration_version,
            &section.name,
        )
        .await?
        {
            self.reporter.skip_section(&section.name);
            return Ok(());
        }

        // Execute based on mode
        let result = match section.mode {
            TransactionMode::Transactional => {
                self.execute_transactional(migration_version, section).await
            }
            TransactionMode::NonTransactional => {
                self.execute_non_transactional(migration_version, section)
                    .await
            }
            TransactionMode::Autocommit => {
                self.execute_autocommit(migration_version, section).await
            }
        };

        match &result {
            Ok(_) => {}
            Err(e) => {
                self.reporter.fail_section(&section.name, e);
            }
        }

        result
    }

    /// Execute section in a transaction
    async fn execute_transactional(
        &mut self,
        migration_version: u64,
        section: &MigrationSection,
    ) -> Result<()> {
        self.reporter
            .start_section(&section.name, section.description.as_deref());

        let start = Instant::now();
        record_section_start(
            &self.pool,
            &self.tracking_table,
            migration_version,
            &section.name,
        )
        .await?;

        // Set statement timeout
        let timeout_ms = section.timeout.as_millis();

        // Begin transaction
        let mut tx = self.pool.begin().await?;

        // Set timeout for this transaction
        sqlx::query(&format!("SET LOCAL statement_timeout = '{}'", timeout_ms))
            .execute(&mut *tx)
            .await?;

        // Execute SQL - use raw execute to support multiple statements
        use sqlx::Executor;
        let result = match tx.execute(section.sql.as_str()).await {
            Ok(result) => result,
            Err(e) => {
                tx.rollback().await?;
                record_section_failed(
                    &self.pool,
                    &self.tracking_table,
                    migration_version,
                    &section.name,
                    &e.to_string(),
                )
                .await?;
                return Err(format_section_error(e, &section.sql, &section.name));
            }
        };

        // Commit transaction
        tx.commit().await?;

        let duration = start.elapsed();
        let rows = result.rows_affected() as i64;

        record_section_complete(
            &self.pool,
            &self.tracking_table,
            migration_version,
            &section.name,
            Some(rows),
            duration.as_millis() as i64,
        )
        .await?;

        self.reporter
            .complete_section(&section.name, duration, Some(rows as usize));

        Ok(())
    }

    /// Execute section without transaction (for CONCURRENTLY, etc.)
    async fn execute_non_transactional(
        &mut self,
        migration_version: u64,
        section: &MigrationSection,
    ) -> Result<()> {
        self.reporter
            .start_section(&section.name, section.description.as_deref());

        let default_retry_config = RetryConfig::default();
        let retry_config = section
            .retry_config
            .as_ref()
            .unwrap_or(&default_retry_config);
        let start = Instant::now();

        record_section_start(
            &self.pool,
            &self.tracking_table,
            migration_version,
            &section.name,
        )
        .await?;

        // Retry loop
        for attempt in 1..=retry_config.attempts {
            self.reporter.attempt(attempt, retry_config.attempts);

            // Set statement timeout for this attempt
            let timeout_ms = section.timeout.as_millis();
            sqlx::query(&format!("SET statement_timeout = '{}'", timeout_ms))
                .execute(&self.pool)
                .await?;

            // Execute SQL - use raw execute to support multiple statements
            use sqlx::Executor;
            match self.pool.execute(section.sql.as_str()).await {
                Ok(result) => {
                    let duration = start.elapsed();
                    let rows = result.rows_affected() as i64;

                    record_section_complete(
                        &self.pool,
                        &self.tracking_table,
                        migration_version,
                        &section.name,
                        Some(rows),
                        duration.as_millis() as i64,
                    )
                    .await?;

                    self.reporter.complete_section_with_retry(
                        &section.name,
                        duration,
                        Some(rows as usize),
                        attempt,
                        retry_config.attempts,
                    );

                    return Ok(());
                }
                Err(e) => {
                    let is_lock_timeout = is_lock_timeout_error(&e);
                    let should_retry = attempt < retry_config.attempts
                        && (retry_config.on_lock_timeout == LockTimeoutAction::Retry
                            || !is_lock_timeout);

                    if should_retry {
                        let delay = calculate_retry_delay(retry_config, attempt);
                        self.reporter
                            .retry(&section.name, attempt, &e.into(), delay);

                        tokio::time::sleep(delay).await;
                        continue;
                    } else {
                        // Final attempt failed
                        record_section_failed(
                            &self.pool,
                            &self.tracking_table,
                            migration_version,
                            &section.name,
                            &e.to_string(),
                        )
                        .await?;

                        return Err(format_section_error(e, &section.sql, &section.name));
                    }
                }
            }
        }

        unreachable!()
    }

    /// Execute section with autocommit (for batch processing)
    async fn execute_autocommit(
        &mut self,
        migration_version: u64,
        section: &MigrationSection,
    ) -> Result<()> {
        self.reporter
            .start_section(&section.name, section.description.as_deref());

        let start = Instant::now();
        record_section_start(
            &self.pool,
            &self.tracking_table,
            migration_version,
            &section.name,
        )
        .await?;

        // For now, execute normally without batching
        let result = sqlx::query(&section.sql)
            .execute(&self.pool)
            .await
            .map_err(|e| format_section_error(e, &section.sql, &section.name))?;

        let duration = start.elapsed();
        let rows = result.rows_affected() as i64;

        record_section_complete(
            &self.pool,
            &self.tracking_table,
            migration_version,
            &section.name,
            Some(rows),
            duration.as_millis() as i64,
        )
        .await?;

        self.reporter
            .complete_section(&section.name, duration, Some(rows as usize));

        Ok(())
    }

    /// Execute section in validation mode (no retries, no timeouts, no tracking)
    async fn execute_validation(&mut self, section: &MigrationSection) -> Result<()> {
        use sqlx::Executor;
        match section.mode {
            TransactionMode::Transactional => {
                let mut tx = self.pool.begin().await?;
                tx.execute(section.sql.as_str())
                    .await
                    .map_err(|e| format_section_error(e, &section.sql, &section.name))?;
                tx.commit().await?;
            }
            TransactionMode::NonTransactional | TransactionMode::Autocommit => {
                self.pool
                    .execute(section.sql.as_str())
                    .await
                    .map_err(|e| format_section_error(e, &section.sql, &section.name))?;
            }
        };

        Ok(())
    }
}

/// Check if error is a lock timeout
fn is_lock_timeout_error(error: &sqlx::Error) -> bool {
    let err_str = error.to_string().to_lowercase();
    err_str.contains("timeout") || err_str.contains("lock")
}

/// Calculate retry delay with optional exponential backoff
fn calculate_retry_delay(config: &RetryConfig, attempt: u32) -> Duration {
    match config.backoff {
        BackoffStrategy::None => config.delay,
        BackoffStrategy::Exponential => {
            let multiplier = 2_u64.pow(attempt.saturating_sub(1));
            config.delay.saturating_mul(multiplier.min(32) as u32) // Cap at 32x
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_lock_timeout_error() {
        // Test the string matching logic
        // Real database lock errors will contain "lock" or "timeout" in their messages
        // For unit testing, we verify the logic works correctly

        // Configuration error with "timeout" keyword should be detected
        let timeout_error = sqlx::Error::Configuration("statement timeout".into());
        assert!(is_lock_timeout_error(&timeout_error));

        // Configuration error with "lock" keyword should be detected
        let lock_error = sqlx::Error::Configuration("lock not available".into());
        assert!(is_lock_timeout_error(&lock_error));

        // Error without keywords should not be detected
        let other_error = sqlx::Error::Configuration("invalid syntax".into());
        assert!(!is_lock_timeout_error(&other_error));
    }

    #[test]
    fn test_calculate_retry_delay_none() {
        let config = RetryConfig {
            attempts: 5,
            delay: Duration::from_secs(2),
            backoff: BackoffStrategy::None,
            on_lock_timeout: LockTimeoutAction::Retry,
        };

        assert_eq!(calculate_retry_delay(&config, 1), Duration::from_secs(2));
        assert_eq!(calculate_retry_delay(&config, 2), Duration::from_secs(2));
        assert_eq!(calculate_retry_delay(&config, 5), Duration::from_secs(2));
    }

    #[test]
    fn test_calculate_retry_delay_exponential() {
        let config = RetryConfig {
            attempts: 5,
            delay: Duration::from_secs(1),
            backoff: BackoffStrategy::Exponential,
            on_lock_timeout: LockTimeoutAction::Retry,
        };

        assert_eq!(calculate_retry_delay(&config, 1), Duration::from_secs(1)); // 2^0 = 1
        assert_eq!(calculate_retry_delay(&config, 2), Duration::from_secs(2)); // 2^1 = 2
        assert_eq!(calculate_retry_delay(&config, 3), Duration::from_secs(4)); // 2^2 = 4
        assert_eq!(calculate_retry_delay(&config, 4), Duration::from_secs(8)); // 2^3 = 8
        assert_eq!(calculate_retry_delay(&config, 5), Duration::from_secs(16)); // 2^4 = 16
    }

    #[test]
    fn test_calculate_retry_delay_exponential_capped() {
        let config = RetryConfig {
            attempts: 10,
            delay: Duration::from_secs(1),
            backoff: BackoffStrategy::Exponential,
            on_lock_timeout: LockTimeoutAction::Retry,
        };

        // Should cap at 32x
        assert_eq!(calculate_retry_delay(&config, 6), Duration::from_secs(32)); // 2^5 = 32
        assert_eq!(calculate_retry_delay(&config, 7), Duration::from_secs(32)); // Capped
        assert_eq!(calculate_retry_delay(&config, 10), Duration::from_secs(32)); // Capped
    }
}
