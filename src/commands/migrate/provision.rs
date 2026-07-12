use crate::commands::migrate::apply::apply_pending_migrations;
use crate::config::Config;
use crate::migration::baseline::apply_baseline_to_target;
use crate::migration::{discover_migrations, find_latest_baseline};
use crate::migration_tracking::{
    MigrationLock, calculate_checksum, ensure_tracking_table_exists, format_tracking_table_name,
    record_baseline_as_applied,
};
use anyhow::{Context, Result};
use sqlx::PgPool;
use std::path::Path;

/// Provision a database from a baseline + post-baseline migrations.
///
/// Unlike `migrate apply` (which only maintains an already-established
/// database), provision is willing to lay down the baseline on a fresh target.
/// It reads the tracking table to decide whether the database is already
/// established, and never diffs or generates SQL against the target: everything
/// it runs is committed (the baseline file and the migration files). If the
/// baseline collides with objects already present, the atomic baseline apply
/// fails cleanly.
pub async fn cmd_migrate_provision(
    config: &Config,
    root_dir: &Path,
    target: &crate::config::TargetUrl,
    dry_run: bool,
) -> Result<()> {
    let pool =
        crate::db::connection::connect_to_database(target.as_str(), "target database").await?;

    // Serialize concurrent apply/provision runs against the same tracking table
    // BEFORE reading the tracking table or applying anything. Shares its key with
    // `migrate apply` (both derive it from the tracking table name), so the two
    // commands exclude each other. Held on a dedicated connection for the whole
    // run; released explicitly on every exit path (and on drop otherwise).
    let lock = MigrationLock::acquire(target.as_str(), &config.migration.tracking_table).await?;

    let result = provision_inner(config, root_dir, &pool, dry_run).await;
    lock.release().await?;
    result
}

async fn provision_inner(
    config: &Config,
    root_dir: &Path,
    pool: &PgPool,
    dry_run: bool,
) -> Result<()> {
    let migrations_dir = root_dir.join(&config.directories.migrations);
    let baselines_dir = root_dir.join(&config.directories.baselines);

    // Ensure the tracking table exists so we can read the target's state.
    ensure_tracking_table_exists(pool, &config.migration.tracking_table).await?;

    let established = tracking_has_rows(pool, config).await?;
    let migrations = discover_migrations(&migrations_dir)?;
    let latest_baseline = find_latest_baseline(&baselines_dir)?;

    // Already managed by pgmt: provision behaves like apply (catch up pending).
    if established {
        if dry_run {
            println!(
                "Database is already provisioned. Pending migrations would be applied (use `migrate apply`)."
            );
            return Ok(());
        }
        println!("Database is already provisioned; applying any pending migrations.");
        return apply_pending_migrations(pool, config, &migrations).await;
    }

    match latest_baseline {
        Some(baseline) => {
            let baseline_sql = std::fs::read_to_string(&baseline.path).with_context(|| {
                format!("Failed to read baseline file: {}", baseline.path.display())
            })?;
            let source = baseline
                .path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("baseline.sql");
            let post_baseline: Vec<_> = migrations
                .iter()
                .filter(|m| m.version > baseline.version)
                .cloned()
                .collect();

            if dry_run {
                println!(
                    "Would provision: apply baseline {} then {} migration(s).",
                    baseline.version,
                    post_baseline.len()
                );
                return Ok(());
            }

            println!("Applying baseline {}...", baseline.version);
            apply_baseline_to_target(pool, &baseline_sql, source).await?;
            record_baseline_as_applied(
                pool,
                &config.migration.tracking_table,
                baseline.version,
                "baseline",
                &calculate_checksum(&baseline_sql),
            )
            .await?;

            apply_pending_migrations(pool, config, &post_baseline).await?;
            println!("✅ Provisioned from baseline {}.", baseline.version);
        }
        None => {
            // No baseline: replay all migrations from scratch (valid on a fresh DB).
            if dry_run {
                println!(
                    "Would provision: apply {} migration(s) (no baseline).",
                    migrations.len()
                );
                return Ok(());
            }
            if migrations.is_empty() {
                println!("Nothing to provision: no baseline and no migrations found.");
                return Ok(());
            }
            println!("No baseline found; applying all migrations.");
            apply_pending_migrations(pool, config, &migrations).await?;
            println!("✅ Provisioned from migrations.");
        }
    }

    Ok(())
}

/// Whether the tracking table has any rows (i.e. pgmt already manages this DB).
async fn tracking_has_rows(pool: &PgPool, config: &Config) -> Result<bool> {
    let tracking_table_name = format_tracking_table_name(&config.migration.tracking_table)?;
    let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {}", tracking_table_name))
        .fetch_one(pool)
        .await?;
    Ok(count > 0)
}
