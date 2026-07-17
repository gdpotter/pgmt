use crate::config::types::TrackingTable;
use crate::migration::section_parser::MigrationSection;
use crate::migration_tracking::{calculate_checksum, version_to_db};
use anyhow::{Context, Result};
use sqlx::PgPool;
use std::str::FromStr;

/// pgmt version stamped onto section rows at registration.
const PGMT_VERSION: &str = env!("CARGO_PKG_VERSION");

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
    /// A remap section covered by an established source at provision/adoption
    /// time: the objects are already present under the
    /// source's name, so nothing ran here — but the section is accounted for.
    /// `completed` keeps its invariant ("this DDL committed here") and absence
    /// keeps its ("crashed or never requested"); the incomplete-baseline guard
    /// treats completed|satisfied alike as covered.
    Satisfied,
    Failed,
}

impl SectionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Satisfied => "satisfied",
            Self::Failed => "failed",
        }
    }

    /// A terminal, covered state: the section's objects are present (whether it
    /// executed here or was covered by an established source). The
    /// incomplete-baseline guard and every "is this section done" check treat
    /// completed and satisfied identically.
    pub fn is_covered(&self) -> bool {
        matches!(self, Self::Completed | Self::Satisfied)
    }
}

impl FromStr for SectionStatus {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "satisfied" => Ok(Self::Satisfied),
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
            checksum TEXT,
            mode TEXT,
            module TEXT,
            applied_by TEXT,
            pgmt_version TEXT,
            PRIMARY KEY (migration_version, is_baseline, section_name)
        )
        "#,
        sections_table
    ))
    .execute(pool)
    .await
    .context("Failed to create section tracking table")?;

    migrate_section_table_schema(pool, tracking_table, &sections_table).await?;

    // The stored module subscription is part of the same evolve step:
    // the tables arrive here so every apply/provision path that ensures the
    // section table also has the subscription tables available. (The read-only
    // `migrate status` path never calls this — it probes instead.)
    crate::migration_tracking::TrackingStore::new(pool, tracking_table)?
        .ensure_subscription_tables()
        .await?;

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
/// Later revisions added per-section `checksum`/`mode`/`module`/`applied_by`/
/// `pgmt_version` (all nullable; NULL = legacy/pre-upgrade row).
async fn migrate_section_table_schema(
    pool: &PgPool,
    tracking_table: &TrackingTable,
    sections_table: &str,
) -> Result<()> {
    let existing_columns: Vec<String> = sqlx::query_scalar(
        "SELECT attname::text FROM pg_attribute
         WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped",
    )
    .bind(sections_table)
    .fetch_all(pool)
    .await
    .with_context(|| format!("Failed to inspect columns of {}", sections_table))?;
    let has = |name: &str| existing_columns.iter().any(|c| c == name);

    // `is_baseline`: add nullable, backfill existing rows to FALSE (every
    // existing row is a migration), then enforce NOT NULL — in one transaction
    // so the column is never observed half-migrated.
    if !has("is_baseline") {
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

    // Per-section immutability + self-describing attribution columns. All
    // nullable: a NULL checksum marks a legacy/pre-upgrade row (passes
    // validation), and `module` NULL means the base (unmoduled) section.
    for column in ["checksum", "mode", "module", "applied_by", "pgmt_version"] {
        if !has(column) {
            sqlx::query(&format!(
                "ALTER TABLE {} ADD COLUMN {} TEXT",
                sections_table, column
            ))
            .execute(pool)
            .await
            .with_context(|| format!("Failed to add {} to {}", column, sections_table))?;
        }
    }

    crate::migration_tracking::ensure_primary_key(
        pool,
        sections_table,
        &["migration_version", "is_baseline", "section_name"],
    )
    .await?;

    backfill_synthetic_legacy_sections(pool, tracking_table, sections_table).await?;

    Ok(())
}

/// Give every legacy main-table row that has ZERO section rows one synthetic
/// `default` completed section row.
///
/// Before per-section tracking, "a main row with no section rows" *meant*
/// "fully applied by an older pgmt" — an ABSENCE carrying meaning. That is
/// fragile: module de-provisioning deletes section rows, which would make a
/// module-only migration read as legacy-fully-applied. Materializing the
/// implicit `default` section removes the heuristic: applied-ness is always a
/// derived function of present section rows.
///
/// A crossing-consumed baseline row (`crossed_at IS NOT NULL`) is exempt: it
/// legitimately carries zero section rows when the crossing relabeled nothing
/// the target holds (zero-trace). Backfilling a `default` completed row there
/// would forge content this target never applied. Only genuine legacy rows
/// (`crossed_at IS NULL`) with no sections are materialized.
///
/// Idempotent via the `NOT EXISTS` guard; skipped when the main table is
/// absent (the section table can be created before it).
async fn backfill_synthetic_legacy_sections(
    pool: &PgPool,
    tracking_table: &TrackingTable,
    sections_table: &str,
) -> Result<()> {
    let main_table = crate::migration_tracking::format_tracking_table_name(tracking_table)?;

    let main_exists: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
        .bind(&main_table)
        .fetch_one(pool)
        .await?;
    if main_exists.is_none() {
        return Ok(());
    }

    sqlx::query(&format!(
        "INSERT INTO {sections}
             (migration_version, is_baseline, section_name, section_order,
              status, completed_at, attempts)
         SELECT m.version, m.is_baseline, 'default', 0, 'completed', m.applied_at, 0
         FROM {main} m
         WHERE m.crossed_at IS NULL
           AND NOT EXISTS (
             SELECT 1 FROM {sections} s
             WHERE s.migration_version = m.version AND s.is_baseline = m.is_baseline)",
        sections = sections_table,
        main = main_table,
    ))
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "Failed to backfill synthetic legacy sections into {}",
            sections_table
        )
    })?;

    Ok(())
}

/// Insert a single Pending section row carrying its checksum/mode/module and
/// the recording pgmt version. `ON CONFLICT DO NOTHING` keeps registration
/// idempotent across resume / module-adoption re-registration. The column
/// list lives here so every registration path (migration, baseline,
/// initialize_sections) stores identical metadata.
pub(crate) async fn insert_pending_section<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    sections_table: &str,
    version: u64,
    is_baseline: bool,
    order: i32,
    section: &MigrationSection,
) -> Result<()> {
    sqlx::query(&format!(
        "INSERT INTO {} (migration_version, is_baseline, section_name, section_order,
                         status, attempts, checksum, mode, module, pgmt_version)
         VALUES ($1, $2, $3, $4, $5, 0, $6, $7, $8, $9)
         ON CONFLICT (migration_version, is_baseline, section_name) DO NOTHING",
        sections_table
    ))
    .bind(version_to_db(version)?)
    .bind(is_baseline)
    .bind(&section.name)
    .bind(order)
    .bind(SectionStatus::Pending.as_str())
    .bind(calculate_checksum(&section.checksum_content()))
    .bind(section.mode.as_str())
    .bind(section.module.as_deref())
    .bind(PGMT_VERSION)
    .execute(executor)
    .await?;
    Ok(())
}

/// A completed section is immutable at section granularity; an unapplied
/// (pending/failed/running) one may be fixed in the repo and re-run. Validate
/// the CURRENT file's sections against the rows registered for this version and
/// bring unapplied rows into sync.
///
/// Rules (matching "unit of resume = unit of immutability"):
/// - Rows with a NULL stored checksum are legacy/pre-upgrade — they always
///   pass and are ignored here (the caller keeps a file-level fallback guard).
/// - A COMPLETED checksummed row must match the file exactly: same checksum,
///   same transaction mode, same section_order, same module tag, and its name
///   must still exist in the file. Any divergence is a hard error naming the
///   section.
/// - A non-completed checksummed row whose checksum drifted is UPDATEd to the
///   current content (the fix-in-repo recovery path) with a printed notice.
///
/// Returns whether any checksummed section row was found — when none were, the
/// caller falls back to the legacy file-level checksum bail.
pub async fn validate_and_sync_section_checksums(
    pool: &PgPool,
    tracking_table: &TrackingTable,
    version: u64,
    is_baseline: bool,
    file_sections: &[MigrationSection],
) -> Result<bool> {
    use std::collections::BTreeMap;

    let sections_table = format_sections_table_name(tracking_table);
    let kind = if is_baseline { "baseline" } else { "migration" };

    // Current file: name -> (order, checksum, mode, module).
    let mut file_map: BTreeMap<&str, (i32, String, &'static str, Option<&str>)> = BTreeMap::new();
    for (idx, section) in file_sections.iter().enumerate() {
        file_map.insert(
            section.name.as_str(),
            (
                idx as i32,
                calculate_checksum(&section.checksum_content()),
                section.mode.as_str(),
                section.module.as_deref(),
            ),
        );
    }

    // (section_name, status, checksum, mode, section_order, module)
    type StoredSectionRow = (
        String,
        String,
        Option<String>,
        Option<String>,
        i32,
        Option<String>,
    );
    let rows: Vec<StoredSectionRow> = sqlx::query_as(&format!(
        "SELECT section_name, status, checksum, mode, section_order, module
             FROM {} WHERE migration_version = $1 AND is_baseline = $2",
        sections_table
    ))
    .bind(version_to_db(version)?)
    .bind(is_baseline)
    .fetch_all(pool)
    .await?;

    let mut has_checksummed = false;
    for (name, status, stored_checksum, stored_mode, stored_order, stored_module) in rows {
        // Legacy row: no stored checksum → always passes.
        let Some(stored_checksum) = stored_checksum else {
            continue;
        };
        has_checksummed = true;
        // Satisfied rows are terminal and covered, like completed: their
        // section is immutable (source-covered adoption record).
        let completed = matches!(
            status.parse::<SectionStatus>(),
            Ok(s) if s.is_covered()
        );

        let Some((order, checksum, mode, module)) = file_map.get(name.as_str()) else {
            // The section is gone from the file.
            if completed {
                anyhow::bail!(
                    "section '{name}' of {kind} {version} was applied but no longer exists in \
                     the file. Applied sections are immutable. Create a new migration for \
                     further changes."
                );
            }
            continue;
        };

        if completed {
            if &stored_checksum != checksum {
                anyhow::bail!(
                    "section '{name}' of {kind} {version} was modified after it was applied.\n\
                     Expected checksum: {stored_checksum}\n\
                     Actual checksum:   {checksum}\n\n\
                     Applied sections are immutable. Create a new migration for further changes. \
                     If the edit was conscious and its effects already match the database, \
                     re-stamp the stored checksum: pgmt migrate resolve --restamp {version}/{name}."
                );
            }
            if stored_order != *order {
                anyhow::bail!(
                    "section '{name}' of {kind} {version} was reordered after it was applied \
                     (was position {stored_order}, now {order}). Applied sections are immutable. \
                     Create a new migration for further changes."
                );
            }
            if let Some(sm) = &stored_mode
                && sm != mode
            {
                anyhow::bail!(
                    "section '{name}' of {kind} {version} changed transaction mode after it was \
                     applied (was '{sm}', now '{mode}'). Applied sections are immutable. Create a \
                     new migration for further changes."
                );
            }
            if stored_module.as_deref() != *module {
                anyhow::bail!(
                    "section '{name}' of {kind} {version} was re-tagged to a different module \
                     after it was applied. Applied sections are immutable. Create a new migration \
                     for further changes."
                );
            }
        } else if &stored_checksum != checksum {
            // Fix-in-repo recovery: an unapplied section may be edited and
            // re-run. Bring the row up to date before it executes.
            println!("section '{name}' changed since registration; updating");
            sqlx::query(&format!(
                "UPDATE {} SET checksum = $1, mode = $2, module = $3
                 WHERE migration_version = $4 AND is_baseline = $5 AND section_name = $6",
                sections_table
            ))
            .bind(checksum)
            .bind(*mode)
            .bind(*module)
            .bind(version_to_db(version)?)
            .bind(is_baseline)
            .bind(&name)
            .execute(pool)
            .await?;
        }
    }

    Ok(has_checksummed)
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
        insert_pending_section(
            pool,
            &sections_table,
            migration_version,
            is_baseline,
            *order,
            section,
        )
        .await?;
    }

    Ok(())
}

/// Record the given sections as `satisfied`: a remap
/// section whose source the target already holds, so nothing runs — the
/// objects are present under the source's name and a later crossing relabels
/// them. Insert-then-mark inside ONE transaction so the determination and the
/// row are atomic (the caller holds the advisory lock). Idempotent: an existing
/// row keeps its status (a resumed adoption never demotes a completed section).
pub async fn record_sections_satisfied(
    pool: &PgPool,
    tracking_table: &TrackingTable,
    version: u64,
    is_baseline: bool,
    sections: &[(i32, MigrationSection)],
) -> Result<()> {
    if sections.is_empty() {
        return Ok(());
    }
    let sections_table = format_sections_table_name(tracking_table);
    let mut tx = pool.begin().await?;
    for (order, section) in sections {
        insert_pending_section(
            &mut *tx,
            &sections_table,
            version,
            is_baseline,
            *order,
            section,
        )
        .await?;
        // Only a freshly-registered (pending) row becomes satisfied; a row that
        // already completed/satisfied here stays as it is.
        sqlx::query(&format!(
            "UPDATE {} SET status = $1, completed_at = NOW(), applied_by = CURRENT_USER
             WHERE migration_version = $2 AND is_baseline = $3 AND section_name = $4
               AND status = 'pending'",
            sections_table
        ))
        .bind(SectionStatus::Satisfied.as_str())
        .bind(version_to_db(version)?)
        .bind(is_baseline)
        .bind(&section.name)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit()
        .await
        .context("Failed to record satisfied sections")?;
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
         SET status = $1, started_at = NOW(), attempts = attempts + 1,
             applied_by = CURRENT_USER
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
        assert_eq!(SectionStatus::Satisfied.as_str(), "satisfied");
        assert_eq!(SectionStatus::Failed.as_str(), "failed");
    }

    #[test]
    fn test_section_status_covered() {
        assert!(SectionStatus::Completed.is_covered());
        assert!(SectionStatus::Satisfied.is_covered());
        assert!(!SectionStatus::Pending.is_covered());
        assert!(!SectionStatus::Running.is_covered());
        assert!(!SectionStatus::Failed.is_covered());
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
            "satisfied".parse::<SectionStatus>().unwrap(),
            SectionStatus::Satisfied
        );
        assert_eq!(
            "failed".parse::<SectionStatus>().unwrap(),
            SectionStatus::Failed
        );
        assert!("invalid".parse::<SectionStatus>().is_err());
    }
}
