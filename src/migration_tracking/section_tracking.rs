use crate::config::types::TrackingTable;
use crate::migration::section_parser::MigrationSection;
use anyhow::{Context, Result};
use sqlx::PgPool;
use std::str::FromStr;

/// Format the sections tracking table name with proper schema qualification
pub(crate) fn format_sections_table_name(tracking_table: &TrackingTable) -> String {
    format!(
        r#""{}"."{}_sections""#,
        tracking_table.schema, tracking_table.name
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SectionStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

impl SectionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

impl FromStr for SectionStatus {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            _ => Err(anyhow::anyhow!("Unknown section status: {}", s)),
        }
    }
}

/// Ensure the section tracking table exists (and migrate older tables to the
/// current shape).
///
/// Sections are keyed by `(migration_version, is_baseline, section_name)`:
/// baselines execute through the same section machinery as migrations, and a
/// version can host both (a baseline generated alongside a migration), so
/// their section rows must not collide.
pub async fn ensure_section_tracking_table(
    pool: &PgPool,
    tracking_table: &TrackingTable,
) -> Result<()> {
    let sections_table = format_sections_table_name(tracking_table);

    sqlx::query(&format!(
        r#"
        CREATE TABLE IF NOT EXISTS {} (
            migration_version BIGINT NOT NULL,
            is_baseline BOOLEAN NOT NULL,
            section_name TEXT NOT NULL,
            section_order INT NOT NULL,
            status TEXT NOT NULL,
            started_at TIMESTAMP WITH TIME ZONE,
            completed_at TIMESTAMP WITH TIME ZONE,
            attempts INT DEFAULT 0,
            last_error TEXT,
            rows_affected BIGINT,
            duration_ms BIGINT,
            PRIMARY KEY (migration_version, is_baseline, section_name)
        )
        "#,
        sections_table
    ))
    .execute(pool)
    .await
    .context("Failed to create section tracking table")?;

    migrate_section_table_schema(pool, &sections_table).await?;

    // Create index for querying by status
    sqlx::query(&format!(
        "CREATE INDEX IF NOT EXISTS idx_{}_status ON {}(migration_version, status)",
        sections_table.replace("\"", "").replace(".", "_"),
        sections_table
    ))
    .execute(pool)
    .await
    .context("Failed to create section tracking table index")?;

    Ok(())
}

/// "Migrate the migrator" for the sections table: older tables lack
/// `is_baseline` (every existing row is a migration section — backfill FALSE,
/// no column DEFAULT) and key sections by `(migration_version, section_name)`.
async fn migrate_section_table_schema(pool: &PgPool, sections_table: &str) -> Result<()> {
    let has_is_baseline: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM pg_attribute
             WHERE attrelid = $1::regclass AND attname = 'is_baseline' AND NOT attisdropped
         )",
    )
    .bind(sections_table)
    .fetch_one(pool)
    .await
    .with_context(|| format!("Failed to inspect columns of {}", sections_table))?;

    if !has_is_baseline {
        let mut tx = pool.begin().await?;
        sqlx::query(&format!(
            "ALTER TABLE {} ADD COLUMN is_baseline BOOLEAN",
            sections_table
        ))
        .execute(&mut *tx)
        .await?;
        sqlx::query(&format!(
            "UPDATE {} SET is_baseline = FALSE",
            sections_table
        ))
        .execute(&mut *tx)
        .await?;
        sqlx::query(&format!(
            "ALTER TABLE {} ALTER COLUMN is_baseline SET NOT NULL",
            sections_table
        ))
        .execute(&mut *tx)
        .await?;
        tx.commit()
            .await
            .with_context(|| format!("Failed to add is_baseline to {}", sections_table))?;
    }

    crate::migration_tracking::ensure_primary_key(
        pool,
        sections_table,
        &["migration_version", "is_baseline", "section_name"],
    )
    .await?;

    Ok(())
}

/// Initialize sections for a migration or baseline (idempotent: rows already
/// present — e.g. on a resumed run — are left untouched).
///
/// Each entry pairs a section with its `section_order`: the section's index in
/// the FULL parsed migration/baseline file, supplied by the caller. The order
/// MUST be stable per migration version rather than derived from a local
/// `enumerate()` — under module subset deploy the sections of one version are
/// registered across SEPARATE calls (base first, a module later), so a
/// per-call index would restart at 0 and collide distinct sections on the same
/// `section_order`.
pub async fn initialize_sections(
    pool: &PgPool,
    tracking_table: &TrackingTable,
    migration_version: u64,
    is_baseline: bool,
    sections: &[(i32, MigrationSection)],
) -> Result<()> {
    let sections_table = format_sections_table_name(tracking_table);

    for (order, section) in sections {
        sqlx::query(&format!(
            "INSERT INTO {} (migration_version, is_baseline, section_name, section_order, status, attempts)
             VALUES ($1, $2, $3, $4, $5, 0)
             ON CONFLICT (migration_version, is_baseline, section_name) DO NOTHING",
            sections_table
        ))
        .bind(migration_version as i64)
        .bind(is_baseline)
        .bind(&section.name)
        .bind(*order)
        .bind(SectionStatus::Pending.as_str())
        .execute(pool)
        .await?;
    }

    Ok(())
}

/// Status of every recorded section for a version. Empty for legacy rows
/// (recorded on completion by older pgmt, before per-section registration).
pub async fn section_statuses(
    pool: &PgPool,
    tracking_table: &TrackingTable,
    migration_version: u64,
    is_baseline: bool,
) -> Result<std::collections::BTreeMap<String, SectionStatus>> {
    let sections_table = format_sections_table_name(tracking_table);

    let rows: Vec<(String, String)> = sqlx::query_as(&format!(
        "SELECT section_name, status FROM {} WHERE migration_version = $1 AND is_baseline = $2",
        sections_table
    ))
    .bind(migration_version as i64)
    .bind(is_baseline)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|(name, status)| Ok((name, SectionStatus::from_str(&status)?)))
        .collect()
}

/// Get status of a specific section
pub async fn get_section_status(
    pool: &PgPool,
    tracking_table: &TrackingTable,
    migration_version: u64,
    is_baseline: bool,
    section_name: &str,
) -> Result<Option<SectionStatus>> {
    let sections_table = format_sections_table_name(tracking_table);

    let row: Option<(String,)> = sqlx::query_as(&format!(
        "SELECT status FROM {} WHERE migration_version = $1 AND is_baseline = $2 AND section_name = $3",
        sections_table
    ))
    .bind(migration_version as i64)
    .bind(is_baseline)
    .bind(section_name)
    .fetch_optional(pool)
    .await?;

    row.map(|(status,)| SectionStatus::from_str(&status))
        .transpose()
}

/// Record section start
pub async fn record_section_start(
    pool: &PgPool,
    tracking_table: &TrackingTable,
    migration_version: u64,
    is_baseline: bool,
    section_name: &str,
) -> Result<()> {
    let sections_table = format_sections_table_name(tracking_table);

    sqlx::query(&format!(
        "UPDATE {}
         SET status = $1, started_at = NOW(), attempts = attempts + 1
         WHERE migration_version = $2 AND is_baseline = $3 AND section_name = $4",
        sections_table
    ))
    .bind(SectionStatus::Running.as_str())
    .bind(migration_version as i64)
    .bind(is_baseline)
    .bind(section_name)
    .execute(pool)
    .await?;

    Ok(())
}

/// Record section completion.
///
/// Accepts any executor so the caller can pass either a pool (autocommit /
/// non-transactional paths) or the section's own transaction (`&mut *tx`).
/// For transactional sections, recording completion inside the section's
/// transaction — before its commit — makes the `completed` tracking row and
/// the section's DDL atomic: the row exists if and only if the DDL committed.
pub async fn record_section_complete<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    tracking_table: &TrackingTable,
    migration_version: u64,
    is_baseline: bool,
    section_name: &str,
    rows_affected: Option<i64>,
    duration_ms: i64,
) -> Result<()> {
    let sections_table = format_sections_table_name(tracking_table);

    sqlx::query(&format!(
        "UPDATE {}
         SET status = $1, completed_at = NOW(), rows_affected = $2, duration_ms = $3
         WHERE migration_version = $4 AND is_baseline = $5 AND section_name = $6",
        sections_table
    ))
    .bind(SectionStatus::Completed.as_str())
    .bind(rows_affected)
    .bind(duration_ms)
    .bind(migration_version as i64)
    .bind(is_baseline)
    .bind(section_name)
    .execute(executor)
    .await?;

    Ok(())
}

/// Record section failure
pub async fn record_section_failed(
    pool: &PgPool,
    tracking_table: &TrackingTable,
    migration_version: u64,
    is_baseline: bool,
    section_name: &str,
    error: &str,
) -> Result<()> {
    let sections_table = format_sections_table_name(tracking_table);

    sqlx::query(&format!(
        "UPDATE {}
         SET status = $1, last_error = $2
         WHERE migration_version = $3 AND is_baseline = $4 AND section_name = $5",
        sections_table
    ))
    .bind(SectionStatus::Failed.as_str())
    .bind(error)
    .bind(migration_version as i64)
    .bind(is_baseline)
    .bind(section_name)
    .execute(pool)
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_section_status_as_str() {
        assert_eq!(SectionStatus::Pending.as_str(), "pending");
        assert_eq!(SectionStatus::Running.as_str(), "running");
        assert_eq!(SectionStatus::Completed.as_str(), "completed");
        assert_eq!(SectionStatus::Failed.as_str(), "failed");
    }

    #[test]
    fn test_section_status_from_str() {
        assert_eq!(
            "pending".parse::<SectionStatus>().unwrap(),
            SectionStatus::Pending
        );
        assert_eq!(
            "running".parse::<SectionStatus>().unwrap(),
            SectionStatus::Running
        );
        assert_eq!(
            "completed".parse::<SectionStatus>().unwrap(),
            SectionStatus::Completed
        );
        assert_eq!(
            "failed".parse::<SectionStatus>().unwrap(),
            SectionStatus::Failed
        );
        assert!("invalid".parse::<SectionStatus>().is_err());
    }
}
