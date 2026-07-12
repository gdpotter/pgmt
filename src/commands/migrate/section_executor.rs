use crate::config::types::TrackingTable;
use crate::db::error_context::SqlErrorContext;
use crate::migration::section_parser::{
    BackoffStrategy, LockTimeoutAction, MigrationSection, RetryConfig, TransactionMode,
};
use crate::migration_tracking::section_tracking::*;
use crate::progress::SectionReporter;
use anyhow::Result;
use sqlx::PgPool;
use sqlx::postgres::PgDatabaseError;
use std::time::{Duration, Instant};

/// Format a SQL execution error with rich PostgreSQL context (DETAIL, HINT, etc.)
fn format_section_error(error: sqlx::Error, sql: &str, section_name: &str) -> anyhow::Error {
    let ctx = SqlErrorContext::from_sqlx_error(&error, sql);
    anyhow::anyhow!("{}", ctx.format(section_name, sql))
}

/// Read-only probe for INVALID indexes, run only after a non-transactional
/// section has exhausted its retries.
///
/// A failed or interrupted `CREATE INDEX CONCURRENTLY` leaves an INVALID index
/// behind; the naive retry then dies with `already exists` and the user gets no
/// hint about the leftover or the fix. This query lists any invalid indexes and,
/// if there are some, returns a guidance note to append to the failure.
///
/// Design constraints (do not relax): pgmt NEVER auto-drops the index and NEVER
/// parses the section SQL — this is detect-and-report only. The probe must never
/// mask the original error: if the query itself fails (permissions, etc.), we
/// log at debug and return `None`, leaving the original error untouched.
async fn detect_invalid_index_guidance(pool: &PgPool) -> Option<String> {
    let rows: Vec<(String, String)> = match sqlx::query_as(
        r#"SELECT n.nspname, c.relname
FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE NOT i.indisvalid
  AND n.nspname NOT IN ('pg_catalog', 'pg_toast', 'information_schema')
ORDER BY n.nspname, c.relname"#,
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::debug!("invalid-index detection query failed, skipping guidance: {e}");
            return None;
        }
    };

    if rows.is_empty() {
        return None;
    }

    let qualified: Vec<String> = rows
        .iter()
        .map(|(schema, name)| format!("\"{schema}\".\"{name}\""))
        .collect();
    // Use the first invalid index's bare name in the DROP guidance example.
    let drop_name = &rows[0].1;

    Some(format!(
        "Invalid index(es) found: {}. A failed or interrupted CREATE INDEX \
CONCURRENTLY leaves an INVALID index behind, and re-running the CREATE will \
fail with 'already exists'. To make this section safely re-runnable, add \
`DROP INDEX CONCURRENTLY IF EXISTS {};` before the CREATE in this section \
(the edit runs through your normal review process).",
        qualified.join(", "),
        drop_name
    ))
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

        // Begin transaction
        let mut tx = self.pool.begin().await?;

        // Set timeouts for this transaction
        let timeout_ms = section.timeout.as_millis();
        sqlx::query(&format!("SET LOCAL statement_timeout = '{}'", timeout_ms))
            .execute(&mut *tx)
            .await?;

        if let Some(lock_timeout) = section.lock_timeout {
            let lock_timeout_ms = lock_timeout.as_millis();
            sqlx::query(&format!("SET LOCAL lock_timeout = '{}'", lock_timeout_ms))
                .execute(&mut *tx)
                .await?;
        }

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

        let duration = start.elapsed();
        let rows = result.rows_affected() as i64;

        // Record completion INSIDE the section's transaction, before commit, so
        // the `completed` tracking row and the section's DDL commit atomically:
        // a crash can never leave the DDL applied with the row stuck at
        // `running`. The recorded duration excludes the commit itself, which is
        // an acceptable trade for the closed crash window.
        record_section_complete(
            &mut *tx,
            &self.tracking_table,
            migration_version,
            &section.name,
            Some(rows),
            duration.as_millis() as i64,
        )
        .await?;

        // Commit transaction (atomically persists both the DDL and the record)
        tx.commit().await?;

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

            // Set timeouts for this attempt
            let timeout_ms = section.timeout.as_millis();
            sqlx::query(&format!("SET statement_timeout = '{}'", timeout_ms))
                .execute(&self.pool)
                .await?;

            if let Some(lock_timeout) = section.lock_timeout {
                let lock_timeout_ms = lock_timeout.as_millis();
                sqlx::query(&format!("SET lock_timeout = '{}'", lock_timeout_ms))
                    .execute(&self.pool)
                    .await?;
            }

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
                    let is_lock_timeout = classify_timeout_error(&e) == Some(TimeoutKind::Lock);
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
                        // Final attempt failed. Before recording the failure,
                        // run a READ-ONLY probe for INVALID indexes left behind
                        // by a failed/interrupted CREATE INDEX CONCURRENTLY, so
                        // the enriched guidance lands in `last_error` too. This
                        // NEVER auto-drops anything and NEVER parses the SQL.
                        let guidance = detect_invalid_index_guidance(&self.pool).await;

                        let recorded_error = match &guidance {
                            Some(note) => format!("{}\n\n{}", e, note),
                            None => e.to_string(),
                        };
                        record_section_failed(
                            &self.pool,
                            &self.tracking_table,
                            migration_version,
                            &section.name,
                            &recorded_error,
                        )
                        .await?;

                        let formatted = format_section_error(e, &section.sql, &section.name);
                        return Err(match guidance {
                            Some(note) => anyhow::anyhow!("{}\n\n{}", formatted, note),
                            None => formatted,
                        });
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

        // Set timeouts
        let timeout_ms = section.timeout.as_millis();
        sqlx::query(&format!("SET statement_timeout = '{}'", timeout_ms))
            .execute(&self.pool)
            .await?;

        if let Some(lock_timeout) = section.lock_timeout {
            let lock_timeout_ms = lock_timeout.as_millis();
            sqlx::query(&format!("SET lock_timeout = '{}'", lock_timeout_ms))
                .execute(&self.pool)
                .await?;
        }

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

/// PostgreSQL error classification for timeout errors
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimeoutKind {
    /// Lock not available (error code 55P03)
    Lock,
    /// Query/statement cancelled (error code 57014)
    Statement,
}

/// Classify a timeout error using PostgreSQL error codes
fn classify_timeout_error(error: &sqlx::Error) -> Option<TimeoutKind> {
    if let Some(db_error) = error.as_database_error()
        && let Some(pg_error) = db_error.try_downcast_ref::<PgDatabaseError>()
    {
        return match pg_error.code() {
            "55P03" => Some(TimeoutKind::Lock),
            "57014" => Some(TimeoutKind::Statement),
            _ => None,
        };
    }
    None
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
    fn test_classify_timeout_error_non_database_errors() {
        // Non-database errors should return None
        let config_error = sqlx::Error::Configuration("statement timeout".into());
        assert_eq!(classify_timeout_error(&config_error), None);

        let other_error = sqlx::Error::Configuration("lock not available".into());
        assert_eq!(classify_timeout_error(&other_error), None);

        let syntax_error = sqlx::Error::Configuration("invalid syntax".into());
        assert_eq!(classify_timeout_error(&syntax_error), None);
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
