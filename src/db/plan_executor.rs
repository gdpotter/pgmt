//! Running a generated plan against a database.
//!
//! A plan arrives as the sections [`sectionize_unmoduled`] cuts, so every
//! caller runs it with the same transaction boundaries a migration of the
//! same steps would have. A transactional section runs as one transaction,
//! sent a batch of statements per round trip; a non-transactional section
//! holds a single statement and runs on its own.
//!
//! [`sectionize_unmoduled`]: crate::modules::sectionize_unmoduled

use anyhow::{Result, anyhow};
use sqlx::{PgConnection, PgPool};

use crate::db::schema_executor::{SourceContextStyle, SqlExecutionError};
use crate::diff::operations::SqlRenderer;
use crate::migration::section_parser::TransactionMode;
use crate::modules::StepSection;
use crate::render::RenderedSql;

/// Statements sent per round trip within a transactional section.
const BATCH_SIZE: usize = 500;

/// One section of a plan, rendered.
#[derive(Debug, Clone)]
pub struct RenderedSection {
    pub mode: TransactionMode,
    pub statements: Vec<RenderedSql>,
}

/// Render each section's steps, keeping the sections' boundaries.
pub fn render_sections(sections: &[StepSection]) -> Vec<RenderedSection> {
    sections
        .iter()
        .map(|section| RenderedSection {
            mode: section.mode,
            statements: section
                .steps
                .iter()
                .flat_map(|step| step.to_sql())
                .collect(),
        })
        .collect()
}

/// Keep the statements `keep` accepts, visiting them in execution order, and
/// drop the sections left empty. Every other boundary stays where it was.
pub fn retain_statements(
    sections: &[RenderedSection],
    mut keep: impl FnMut(&RenderedSql) -> bool,
) -> Vec<RenderedSection> {
    sections
        .iter()
        .filter_map(|section| {
            let statements: Vec<RenderedSql> = section
                .statements
                .iter()
                .filter(|statement| keep(statement))
                .cloned()
                .collect();
            (!statements.is_empty()).then(|| RenderedSection {
                statements,
                ..section.clone()
            })
        })
        .collect()
}

/// Run `sections` in order, calling `progress` with the number of statements
/// run so far after each round trip.
///
/// A failed transactional section rolls back whole. It is then replayed, the
/// failed batch one statement at a time, in a transaction that is rolled back
/// too, so the error names the statement that broke and the database is left
/// as the section found it.
pub async fn execute_sections(
    pool: &PgPool,
    sections: &[RenderedSection],
    mut progress: impl FnMut(usize),
) -> Result<()> {
    let mut done = 0;
    for section in sections {
        let statements: Vec<&str> = section.statements.iter().map(|s| s.sql.as_str()).collect();
        match section.mode {
            TransactionMode::Transactional => {
                execute_transactional(pool, &statements, done, &mut progress).await?
            }
            TransactionMode::NonTransactional | TransactionMode::Autocommit => {
                for (i, statement) in statements.iter().enumerate() {
                    sqlx::raw_sql(sqlx::AssertSqlSafe(statement.to_string()))
                        .execute(pool)
                        .await
                        .map_err(|e| statement_error(&e, statement, done + i + 1))?;
                    progress(done + i + 1);
                }
            }
        }
        done += statements.len();
    }
    Ok(())
}

/// Run one transactional section, `done` statements into the plan.
async fn execute_transactional(
    pool: &PgPool,
    statements: &[&str],
    done: usize,
    progress: &mut impl FnMut(usize),
) -> Result<()> {
    let mut tx = pool.begin().await?;
    for (i, batch) in statements.chunks(BATCH_SIZE).enumerate() {
        if let Err(batch_error) = execute_batch(&mut tx, batch).await {
            tx.rollback().await?;
            let first = done + i * BATCH_SIZE + 1;
            return Err(locate_failure(pool, statements, i, done)
                .await?
                .unwrap_or_else(|| statement_error(&batch_error, &batch.join("\n"), first)));
        }
        progress(done + (i * BATCH_SIZE + batch.len()));
    }
    tx.commit().await?;
    Ok(())
}

async fn execute_batch(conn: &mut PgConnection, batch: &[&str]) -> sqlx::Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    sqlx::raw_sql(sqlx::AssertSqlSafe(batch.join("\n")))
        .execute(conn)
        .await
        .map(|_| ())
}

/// Replay a section that broke in batch number `failed`: the batches before it
/// as they ran, then that batch one statement at a time, all in a transaction
/// that is rolled back. Returns the error naming the statement that broke, or
/// `None` if the replay ran clean.
async fn locate_failure(
    pool: &PgPool,
    statements: &[&str],
    failed: usize,
    done: usize,
) -> Result<Option<anyhow::Error>> {
    let mut tx = pool.begin().await?;
    let start = failed * BATCH_SIZE;
    let mut found = None;
    if execute_batch(&mut tx, &statements[..start]).await.is_ok() {
        let end = (start + BATCH_SIZE).min(statements.len());
        for (i, statement) in statements[start..end].iter().enumerate() {
            if let Err(e) = execute_batch(&mut tx, &[statement]).await {
                found = Some(statement_error(&e, statement, done + start + i + 1));
                break;
            }
        }
    }
    tx.rollback().await?;
    Ok(found)
}

fn statement_error(error: &sqlx::Error, sql: &str, number: usize) -> anyhow::Error {
    let error = SqlExecutionError::from_sqlx_error(error, sql, &format!("step {number}"));
    anyhow!(error.format_error(&SourceContextStyle::Step, 250))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render::Safety;

    fn statement(sql: &str, safety: Safety) -> RenderedSql {
        RenderedSql {
            safety,
            ..RenderedSql::new(sql.to_string())
        }
    }

    /// Filtering leaves every boundary where the cut put it, and a section it
    /// empties is dropped rather than run as an empty transaction.
    #[test]
    fn test_retain_statements_keeps_boundaries_and_drops_empty_sections() {
        let sections = [
            RenderedSection {
                mode: TransactionMode::Transactional,
                statements: vec![
                    statement("a", Safety::Safe),
                    statement("drop b", Safety::Destructive),
                ],
            },
            RenderedSection {
                mode: TransactionMode::Transactional,
                statements: vec![statement("drop c", Safety::Destructive)],
            },
            RenderedSection {
                mode: TransactionMode::NonTransactional,
                statements: vec![statement("d", Safety::Safe)],
            },
        ];

        let safe = retain_statements(&sections, |s| s.safety == Safety::Safe);

        let kept: Vec<(TransactionMode, Vec<&str>)> = safe
            .iter()
            .map(|s| {
                (
                    s.mode,
                    s.statements.iter().map(|r| r.sql.as_str()).collect(),
                )
            })
            .collect();
        assert_eq!(
            kept,
            [
                (TransactionMode::Transactional, vec!["a"]),
                (TransactionMode::NonTransactional, vec!["d"]),
            ]
        );
    }
}
