pub mod section_tracking;

use crate::config::types::TrackingTable;
use anyhow::{Context, Result};
use sqlx::{PgPool, Row};

pub use section_tracking::{ensure_section_tracking_table, initialize_sections};

/// Safely convert migration version from u64 to i64 for database storage
/// Migration versions are Unix timestamps, which will exceed i64::MAX around year 2262
pub fn version_to_db(version: u64) -> Result<i64> {
    i64::try_from(version).with_context(|| {
        format!(
            "Migration version {} is too large for database storage (exceeds i64::MAX). \
             This typically indicates a timestamp far in the future or corrupted version data.",
            version
        )
    })
}

/// Safely convert migration version from i64 database storage to u64
/// Handles negative values (which shouldn't exist) gracefully
pub fn version_from_db(version: i64) -> u64 {
    if version < 0 {
        tracing::warn!(
            "Found negative migration version in database: {}. This indicates corrupted data.",
            version
        );
        0
    } else {
        version as u64 // Safe cast since we checked for negative
    }
}

/// Safely format a schema-qualified table name for SQL queries
/// This prevents SQL injection by properly escaping SQL identifiers
pub fn format_tracking_table_name(tracking_table: &TrackingTable) -> Result<String> {
    // Validate that schema and table names contain only valid SQL identifier characters
    // Allow alphanumeric, underscore, and dollar sign (PostgreSQL identifier rules)
    fn is_valid_sql_identifier(name: &str) -> bool {
        if name.is_empty() {
            return false;
        }

        let first_char = name.chars().next().unwrap();
        if !first_char.is_alphabetic() && first_char != '_' {
            return false;
        }

        name.chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '$')
    }

    if !is_valid_sql_identifier(&tracking_table.schema) {
        return Err(anyhow::anyhow!(
            "Invalid schema name '{}': must contain only letters, numbers, underscores, and dollar signs, starting with letter or underscore",
            tracking_table.schema
        ));
    }

    if !is_valid_sql_identifier(&tracking_table.name) {
        return Err(anyhow::anyhow!(
            "Invalid table name '{}': must contain only letters, numbers, underscores, and dollar signs, starting with letter or underscore",
            tracking_table.name
        ));
    }

    // Use double quotes to properly escape SQL identifiers
    Ok(format!(
        r#""{}"."{}""#,
        tracking_table.schema, tracking_table.name
    ))
}

/// Record representing a migration in the tracking table
/// Currently unused but reserved for future functionality
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct MigrationRecord {
    pub version: u64,
    #[allow(dead_code)]
    pub description: String,
    #[allow(dead_code)]
    pub checksum: String,
    #[allow(dead_code)]
    pub applied_at: Option<String>,
}

/// Initialize the migration tracking table in the database
pub async fn ensure_tracking_table_exists(
    pool: &PgPool,
    tracking_table: &TrackingTable,
) -> Result<()> {
    let tracking_table_name = format_tracking_table_name(tracking_table)?;

    sqlx::query(&format!(
        r#"
        CREATE TABLE IF NOT EXISTS {} (
            version BIGINT PRIMARY KEY,
            description TEXT NOT NULL,
            applied_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            checksum TEXT NOT NULL,
            applied_by TEXT DEFAULT CURRENT_USER
        )
        "#,
        tracking_table_name
    ))
    .execute(pool)
    .await
    .with_context(|| format!("Failed to create tracking table {}", tracking_table_name))?;

    Ok(())
}

/// Insert a baseline record into the migration tracking table
pub async fn record_baseline_as_applied(
    pool: &PgPool,
    tracking_table: &TrackingTable,
    version: u64,
    description: &str,
    checksum: &str,
) -> Result<()> {
    let tracking_table_name = format_tracking_table_name(tracking_table)?;

    // First ensure the tracking table exists
    ensure_tracking_table_exists(pool, tracking_table).await?;

    // Insert the baseline record
    sqlx::query(&format!(
        "INSERT INTO {} (version, description, checksum) VALUES ($1, $2, $3)",
        tracking_table_name
    ))
    .bind(version_to_db(version)?)
    .bind(description)
    .bind(checksum)
    .execute(pool)
    .await
    .with_context(|| format!("Failed to record baseline {} in tracking table", version))?;

    Ok(())
}

/// Check if a migration version is already applied
#[allow(dead_code)]
pub async fn is_migration_applied(
    pool: &PgPool,
    tracking_table: &TrackingTable,
    version: u64,
) -> Result<bool> {
    let tracking_table_name = format_tracking_table_name(tracking_table)?;

    // First ensure the tracking table exists
    ensure_tracking_table_exists(pool, tracking_table).await?;

    let result = sqlx::query(&format!(
        "SELECT COUNT(*) as count FROM {} WHERE version = $1",
        tracking_table_name
    ))
    .bind(version_to_db(version)?)
    .fetch_one(pool)
    .await?;

    let count: i64 = result.get("count");
    Ok(count > 0)
}

/// Get all applied migrations ordered by version
/// Currently unused but reserved for future functionality
#[allow(dead_code)]
pub async fn get_applied_migrations(
    pool: &PgPool,
    tracking_table: &TrackingTable,
) -> Result<Vec<MigrationRecord>> {
    let tracking_table_name = format_tracking_table_name(tracking_table)?;

    // First ensure the tracking table exists
    ensure_tracking_table_exists(pool, tracking_table).await?;

    let rows = sqlx::query(&format!(
        "SELECT version, description, checksum, applied_at::TEXT FROM {} ORDER BY version",
        tracking_table_name
    ))
    .fetch_all(pool)
    .await?;

    let mut migrations = Vec::new();
    for row in rows {
        migrations.push(MigrationRecord {
            version: version_from_db(row.get::<i64, _>("version")),
            description: row.get("description"),
            checksum: row.get("checksum"),
            applied_at: row.get("applied_at"),
        });
    }

    Ok(migrations)
}

/// Calculate checksum for migration content
pub fn calculate_checksum(content: &str) -> String {
    format!("{:x}", md5::compute(content))
}
