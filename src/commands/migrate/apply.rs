use crate::commands::migrate::section_executor::{ExecutionMode, SectionExecutor};
use crate::config::Config;
use crate::migration::{
    ParsedMigration, discover_migrations, parse_migration_sections, validate_sections,
};
use crate::migration_tracking::{
    calculate_checksum, ensure_section_tracking_table, ensure_tracking_table_exists,
    format_tracking_table_name, initialize_sections, record_migration_as_applied, version_from_db,
};
use crate::progress::SectionReporter;
use anyhow::{Context, Result};
use sqlx::PgPool;
use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;
use tracing::debug;

pub async fn cmd_migrate_apply(
    config: &Config,
    root_dir: &Path,
    target: &crate::config::TargetUrl,
) -> Result<()> {
    println!("Applying migrations to target database");

    let migrations_dir = root_dir.join(&config.directories.migrations);
    if !migrations_dir.exists() {
        println!("No migrations directory found - nothing to apply");
        return Ok(());
    }

    let pool =
        crate::db::connection::connect_to_database(target.as_str(), "target database").await?;

    let migrations = discover_migrations(&migrations_dir)?;

    apply_pending_migrations(&pool, config, &migrations).await
}

/// Apply migration files to a database, skipping any already recorded in the
/// tracking table (and validating their checksums haven't drifted).
///
/// This is THE production apply path: section execution, checksum validation,
/// and tracking-table recording all live here, so `migrate apply` and
/// `migrate provision` can't diverge. The caller selects which migrations to
/// consider (e.g. provision passes only those after the baseline it just
/// applied) and is responsible for connecting to the target.
pub(crate) async fn apply_pending_migrations(
    pool: &PgPool,
    config: &Config,
    migrations: &[ParsedMigration],
) -> Result<()> {
    let tracking_table_name = format_tracking_table_name(&config.migration.tracking_table)?;

    // Ensure the tracking tables exist (and are migrated to the current shape)
    ensure_tracking_table_exists(pool, &config.migration.tracking_table).await?;
    ensure_section_tracking_table(pool, &config.migration.tracking_table).await?;

    // Get applied rows: version -> (checksum, is_baseline).
    let applied_migrations: HashMap<u64, (String, bool)> =
        sqlx::query_as::<_, (i64, String, bool)>(&format!(
            "SELECT version, checksum, is_baseline FROM {}",
            tracking_table_name
        ))
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|(v, checksum, is_baseline)| (version_from_db(v), (checksum, is_baseline)))
        .collect();

    // A recorded baseline covers every migration up to its version. Those
    // migration files (if still present alongside the baseline) must be skipped
    // rather than re-applied or checksum-compared against the baseline.
    let baseline_version = applied_migrations
        .iter()
        .filter(|(_, (_, is_baseline))| *is_baseline)
        .map(|(version, _)| *version)
        .max();

    // Apply unapplied migrations
    for migration in migrations {
        // Skip migrations covered by a recorded baseline.
        if baseline_version.is_some_and(|bv| migration.version <= bv) {
            debug!(
                "Migration {} is covered by the baseline, skipping",
                migration.version
            );
            continue;
        }

        // Read migration SQL first so we can validate checksum
        let migration_sql = std::fs::read_to_string(&migration.path).with_context(|| {
            format!(
                "Failed to read migration file: {}",
                migration.path.display()
            )
        })?;

        // Calculate checksum
        let checksum = calculate_checksum(&migration_sql);

        // Check if migration was already applied
        if let Some((stored_checksum, _)) = applied_migrations.get(&migration.version) {
            // Validate checksum hasn't changed
            if stored_checksum != &checksum {
                anyhow::bail!(
                    "Migration {} has been modified after being applied!\n\
                     Expected checksum: {}\n\
                     Actual checksum:   {}\n\n\
                     Migrations must be immutable once applied. If you need to make changes:\n\
                     • Create a new migration with the changes\n\
                     • Or roll back and recreate this migration (dangerous in production)",
                    migration.version,
                    stored_checksum,
                    checksum
                );
            }
            debug!("Migration {} already applied, skipping", migration.version);
            continue;
        }

        println!(
            "\nApplying migration {} - {}",
            migration.version, migration.description
        );

        let start = Instant::now();

        // Parse migration into sections
        let sections = parse_migration_sections(&migration.path, &migration_sql)
            .with_context(|| format!("Failed to parse migration {}", migration.version))?;

        // Validate sections
        validate_sections(&sections).with_context(|| {
            format!(
                "Invalid section configuration in migration {}",
                migration.version
            )
        })?;

        // Initialize section tracking
        initialize_sections(
            pool,
            &config.migration.tracking_table,
            migration.version,
            &sections,
        )
        .await?;

        // Create section executor
        let reporter = SectionReporter::new(sections.len(), false); // TODO: Add verbose flag to config
        let mut executor = SectionExecutor::new(
            pool.clone(),
            config.migration.tracking_table.clone(),
            reporter,
            ExecutionMode::Production,
        );

        // Execute each section
        for section in &sections {
            executor
                .execute_section(migration.version, section)
                .await
                .with_context(|| {
                    format!(
                        "Migration {} failed at section '{}'",
                        migration.version, section.name
                    )
                })?;
        }

        let duration = start.elapsed();

        // Record migration as applied
        record_migration_as_applied(
            pool,
            &config.migration.tracking_table,
            migration.version,
            &migration.description,
            &checksum,
        )
        .await?;

        // Report completion
        let reporter = SectionReporter::new(sections.len(), false);
        reporter.migration_summary(duration, sections.len());
    }

    Ok(())
}
