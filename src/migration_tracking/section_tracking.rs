use crate::config::types::TrackingTable;
use crate::migration::section_parser::MigrationSection;
use anyhow::{Context, Result};
use sqlx::PgPool;
use std::str::FromStr;

/// Format the sections tracking table name with proper schema qualification
fn format_sections_table_name(tracking_table: &TrackingTable) -> String {
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

/// Ensure the section tracking table exists
pub async fn ensure_section_tracking_table(
    pool: &PgPool,
    tracking_table: &TrackingTable,
) -> Result<()> {
    let sections_table = format_sections_table_name(tracking_table);

    sqlx::query(&format!(
        r#"
        CREATE TABLE IF NOT EXISTS {} (
            migration_version BIGINT NOT NULL,
            section_name TEXT NOT NULL,
            section_order INT NOT NULL,
            status TEXT NOT NULL,
            started_at TIMESTAMP WITH TIME ZONE,
            completed_at TIMESTAMP WITH TIME ZONE,
            attempts INT DEFAULT 0,
            last_error TEXT,
            rows_affected BIGINT,
            duration_ms BIGINT,
            PRIMARY KEY (migration_version, section_name)
        )
        "#,
        sections_table
    ))
    .execute(pool)
    .await
    .context("Failed to create section tracking table")?;

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

/// Initialize sections for a migration
pub async fn initialize_sections(
    pool: &PgPool,
    tracking_table: &TrackingTable,
    migration_version: u64,
    sections: &[MigrationSection],
) -> Result<()> {
    let sections_table = format_sections_table_name(tracking_table);

    for (order, section) in sections.iter().enumerate() {
        sqlx::query(&format!(
            "INSERT INTO {} (migration_version, section_name, section_order, status, attempts)
             VALUES ($1, $2, $3, $4, 0)
             ON CONFLICT (migration_version, section_name) DO NOTHING",
            sections_table
        ))
        .bind(migration_version as i64)
        .bind(&section.name)
        .bind(order as i32)
        .bind(SectionStatus::Pending.as_str())
        .execute(pool)
        .await?;
    }

    Ok(())
}

/// Get status of a specific section
pub async fn get_section_status(
    pool: &PgPool,
    tracking_table: &TrackingTable,
    migration_version: u64,
    section_name: &str,
) -> Result<Option<SectionStatus>> {
    let sections_table = format_sections_table_name(tracking_table);

    let row: Option<(String,)> = sqlx::query_as(&format!(
        "SELECT status FROM {} WHERE migration_version = $1 AND section_name = $2",
        sections_table
    ))
    .bind(migration_version as i64)
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
    section_name: &str,
) -> Result<()> {
    let sections_table = format_sections_table_name(tracking_table);

    sqlx::query(&format!(
        "UPDATE {}
         SET status = $1, started_at = NOW(), attempts = attempts + 1
         WHERE migration_version = $2 AND section_name = $3",
        sections_table
    ))
    .bind(SectionStatus::Running.as_str())
    .bind(migration_version as i64)
    .bind(section_name)
    .execute(pool)
    .await?;

    Ok(())
}

/// Record section completion
pub async fn record_section_complete(
    pool: &PgPool,
    tracking_table: &TrackingTable,
    migration_version: u64,
    section_name: &str,
    rows_affected: Option<i64>,
    duration_ms: i64,
) -> Result<()> {
    let sections_table = format_sections_table_name(tracking_table);

    sqlx::query(&format!(
        "UPDATE {}
         SET status = $1, completed_at = NOW(), rows_affected = $2, duration_ms = $3
         WHERE migration_version = $4 AND section_name = $5",
        sections_table
    ))
    .bind(SectionStatus::Completed.as_str())
    .bind(rows_affected)
    .bind(duration_ms)
    .bind(migration_version as i64)
    .bind(section_name)
    .execute(pool)
    .await?;

    Ok(())
}

/// Record section failure
pub async fn record_section_failed(
    pool: &PgPool,
    tracking_table: &TrackingTable,
    migration_version: u64,
    section_name: &str,
    error: &str,
) -> Result<()> {
    let sections_table = format_sections_table_name(tracking_table);

    sqlx::query(&format!(
        "UPDATE {}
         SET status = $1, last_error = $2
         WHERE migration_version = $3 AND section_name = $4",
        sections_table
    ))
    .bind(SectionStatus::Failed.as_str())
    .bind(error)
    .bind(migration_version as i64)
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
