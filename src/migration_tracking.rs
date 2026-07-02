pub mod advisory_lock;
pub mod section_tracking;

use crate::config::types::TrackingTable;
use anyhow::{Context, Result};
use sqlx::PgPool;

pub use advisory_lock::MigrationLock;
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

/// Initialize the migration tracking table in the database.
///
/// This is the single source of truth for the tracking table's shape. All
/// callers (apply, status, provision, init, …) must route through here rather
/// than defining their own `CREATE TABLE`, so the schema can't drift between
/// commands.
///
/// The primary key is `(version, is_baseline)`: one version can host both a
/// migration row and a baseline row (a baseline generated alongside a
/// migration covers it), and they are tracked independently.
pub async fn ensure_tracking_table_exists(
    pool: &PgPool,
    tracking_table: &TrackingTable,
) -> Result<()> {
    let tracking_table_name = format_tracking_table_name(tracking_table)?;

    sqlx::query(&format!(
        r#"
        CREATE TABLE IF NOT EXISTS {} (
            version BIGINT NOT NULL,
            description TEXT NOT NULL,
            applied_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            checksum TEXT NOT NULL,
            applied_by TEXT DEFAULT CURRENT_USER,
            is_baseline BOOLEAN NOT NULL,
            PRIMARY KEY (version, is_baseline)
        )
        "#,
        tracking_table_name
    ))
    .execute(pool)
    .await
    .with_context(|| format!("Failed to create tracking table {}", tracking_table_name))?;

    migrate_tracking_table_schema(pool, tracking_table, &tracking_table_name).await?;

    Ok(())
}

/// Columns of a table's primary key, in index order. Empty if the table has
/// no primary key. `qualified_name` must be a safely-quoted qualified name
/// (from `format_tracking_table_name` or equivalent).
pub(crate) async fn primary_key_columns(
    pool: &PgPool,
    qualified_name: &str,
) -> Result<Vec<String>> {
    let columns: Vec<String> = sqlx::query_scalar(
        "SELECT a.attname::text
         FROM pg_index i
         JOIN pg_attribute a ON a.attrelid = i.indrelid
              AND a.attnum = ANY(i.indkey)
         WHERE i.indrelid = $1::regclass AND i.indisprimary
         ORDER BY array_position(i.indkey, a.attnum)",
    )
    .bind(qualified_name)
    .fetch_all(pool)
    .await
    .with_context(|| format!("Failed to inspect primary key of {}", qualified_name))?;
    Ok(columns)
}

/// Replace a table's primary key with `PRIMARY KEY (columns…)` if its current
/// PK differs. Used by the "migrate the migrator" steps: pgmt's own tracking
/// tables gained `is_baseline` in their keys, and older deployments must be
/// brought up to shape in one transaction.
pub(crate) async fn ensure_primary_key(
    pool: &PgPool,
    qualified_name: &str,
    expected_columns: &[&str],
) -> Result<()> {
    let current = primary_key_columns(pool, qualified_name).await?;
    if current == expected_columns {
        return Ok(());
    }

    let pk_name: Option<String> = sqlx::query_scalar(
        "SELECT conname::text FROM pg_constraint
         WHERE conrelid = $1::regclass AND contype = 'p'",
    )
    .bind(qualified_name)
    .fetch_optional(pool)
    .await?;

    let mut tx = pool.begin().await?;
    if let Some(pk_name) = pk_name {
        sqlx::query(&format!(
            r#"ALTER TABLE {} DROP CONSTRAINT "{}""#,
            qualified_name,
            pk_name.replace('"', "")
        ))
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query(&format!(
        "ALTER TABLE {} ADD PRIMARY KEY ({})",
        qualified_name,
        expected_columns.join(", ")
    ))
    .execute(&mut *tx)
    .await?;
    tx.commit().await.with_context(|| {
        format!(
            "Failed to migrate primary key of {} to ({})",
            qualified_name,
            expected_columns.join(", ")
        )
    })?;

    Ok(())
}

/// Bring a pre-existing tracking table up to the current column shape.
///
/// pgmt's own tracking schema can change between versions ("migrate the
/// migrator"). This reconciles older tables idempotently. It reads the existing
/// columns once (a cheap catalog query, no lock) and only issues `ALTER`
/// statements for columns that are actually missing, so routine commands don't
/// take an `ACCESS EXCLUSIVE` lock on every run.
async fn migrate_tracking_table_schema(
    pool: &PgPool,
    tracking_table: &TrackingTable,
    tracking_table_name: &str,
) -> Result<()> {
    let existing_columns: Vec<String> = sqlx::query_scalar(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = $2",
    )
    .bind(&tracking_table.schema)
    .bind(&tracking_table.name)
    .fetch_all(pool)
    .await
    .with_context(|| format!("Failed to inspect columns of {}", tracking_table_name))?;

    let has = |name: &str| existing_columns.iter().any(|c| c == name);

    // `applied_by` was missing from the earlier inline `CREATE TABLE` definitions
    // in apply.rs/status.rs, so some tables in the wild lack it.
    if !has("applied_by") {
        sqlx::query(&format!(
            "ALTER TABLE {} ADD COLUMN applied_by TEXT DEFAULT CURRENT_USER",
            tracking_table_name
        ))
        .execute(pool)
        .await
        .with_context(|| format!("Failed to add applied_by to {}", tracking_table_name))?;
    }

    // `is_baseline`: add nullable, backfill existing rows to FALSE (every existing
    // row is a migration), then enforce NOT NULL. No column DEFAULT — every insert
    // sets the value explicitly. Done in one transaction so the column is never
    // observed in a half-migrated, NULL-permitting state.
    if !has("is_baseline") {
        let mut tx = pool.begin().await?;
        sqlx::query(&format!(
            "ALTER TABLE {} ADD COLUMN is_baseline BOOLEAN",
            tracking_table_name
        ))
        .execute(&mut *tx)
        .await?;
        sqlx::query(&format!(
            "UPDATE {} SET is_baseline = FALSE",
            tracking_table_name
        ))
        .execute(&mut *tx)
        .await?;
        sqlx::query(&format!(
            "ALTER TABLE {} ALTER COLUMN is_baseline SET NOT NULL",
            tracking_table_name
        ))
        .execute(&mut *tx)
        .await?;
        tx.commit()
            .await
            .with_context(|| format!("Failed to add is_baseline to {}", tracking_table_name))?;
    }

    // The PK grew from (version) to (version, is_baseline) so a migration and
    // a baseline can coexist at one version. Runs after the is_baseline column
    // is guaranteed present and NOT NULL.
    ensure_primary_key(pool, tracking_table_name, &["version", "is_baseline"]).await?;

    Ok(())
}

/// Insert a baseline record into the migration tracking table (is_baseline = TRUE).
///
/// Written by `migrate provision` after it applies a baseline to a fresh target.
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

    // Insert the baseline record (is_baseline = TRUE)
    sqlx::query(&format!(
        "INSERT INTO {} (version, description, checksum, is_baseline) VALUES ($1, $2, $3, TRUE)",
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

/// Register a migration as started: insert its tracking row and its Pending
/// section rows in ONE transaction, before any section executes.
///
/// Writing the row at start (rather than on completion) is what lets a failed
/// migration resume: the version row exists, and "fully applied" is *derived*
/// — every section in the (checksummed) file has a Completed row. Legacy rows
/// with NO section rows at all were recorded on completion by older pgmt and
/// are fully applied by construction. That distinction is only sound because
/// this insert is atomic: a crash can never leave a version row with zero
/// section rows.
///
/// `ON CONFLICT DO NOTHING` on the section rows preserves any rows left by a
/// pre-refactor crashed apply (sections used to be initialized before the
/// version row existed), so their Completed statuses keep driving resume.
pub async fn register_migration_start(
    pool: &PgPool,
    tracking_table: &TrackingTable,
    version: u64,
    description: &str,
    checksum: &str,
    sections: &[crate::migration::section_parser::MigrationSection],
) -> Result<()> {
    let tracking_table_name = format_tracking_table_name(tracking_table)?;
    let sections_table = section_tracking::format_sections_table_name(tracking_table);

    let mut tx = pool.begin().await?;
    sqlx::query(&format!(
        "INSERT INTO {} (version, description, checksum, is_baseline) VALUES ($1, $2, $3, FALSE)",
        tracking_table_name
    ))
    .bind(version_to_db(version)?)
    .bind(description)
    .bind(checksum)
    .execute(&mut *tx)
    .await
    .with_context(|| format!("Failed to record migration {} in tracking table", version))?;

    for (order, section) in sections.iter().enumerate() {
        sqlx::query(&format!(
            "INSERT INTO {} (migration_version, is_baseline, section_name, section_order, status, attempts)
             VALUES ($1, FALSE, $2, $3, $4, 0)
             ON CONFLICT (migration_version, is_baseline, section_name) DO NOTHING",
            sections_table
        ))
        .bind(version_to_db(version)?)
        .bind(&section.name)
        .bind(order as i32)
        .bind(section_tracking::SectionStatus::Pending.as_str())
        .execute(&mut *tx)
        .await?;
    }

    tx.commit()
        .await
        .with_context(|| format!("Failed to register migration {} start", version))?;

    Ok(())
}

/// Calculate checksum for migration content
pub fn calculate_checksum(content: &str) -> String {
    format!("{:x}", md5::compute(content))
}
