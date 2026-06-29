use crate::catalog::Catalog;
use crate::commands::migrate::apply::apply_pending_migrations;
use crate::config::Config;
use crate::config::filter::ObjectFilter;
use crate::migration::baseline::apply_baseline_to_target;
use crate::migration::{discover_migrations, find_latest_baseline};
use crate::migration_tracking::{
    calculate_checksum, ensure_tracking_table_exists, format_tracking_table_name,
    record_baseline_as_applied,
};
use anyhow::{Context, Result};
use sqlx::PgPool;
use std::path::Path;

/// Provision a database from a baseline + post-baseline migrations.
///
/// Unlike `migrate apply` (which only maintains an already-established
/// database), provision is willing to lay down the baseline on a fresh target.
/// It classifies the target by reading state only — the tracking table and a
/// managed-catalog presence check — and never diffs or generates SQL against
/// the target: everything it runs is committed (the baseline file and the
/// migration files).
pub async fn cmd_migrate_provision(
    config: &Config,
    root_dir: &Path,
    target: &crate::config::TargetUrl,
    dry_run: bool,
) -> Result<()> {
    let migrations_dir = root_dir.join(&config.directories.migrations);
    let baselines_dir = root_dir.join(&config.directories.baselines);

    let pool =
        crate::db::connection::connect_to_database(target.as_str(), "target database").await?;

    // Ensure the tracking table exists so we can read the target's state.
    ensure_tracking_table_exists(&pool, &config.migration.tracking_table).await?;

    let established = tracking_has_rows(&pool, config).await?;
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
        return apply_pending_migrations(&pool, config, &migrations).await;
    }

    match latest_baseline {
        Some(baseline) => {
            // A baseline lays down the full schema with CREATE statements, so the
            // target must be empty of managed objects. If it already has objects
            // (but no pgmt history), it's an existing database to adopt, not a
            // fresh one to provision.
            let filter = ObjectFilter::from_config(config);
            let catalog = Catalog::load_managed(&pool, &filter).await?;
            if has_managed_objects(&catalog) {
                anyhow::bail!(
                    "Target database already contains managed objects but has no pgmt \
                     migration history.\n\n\
                     If this is an existing database, adopt it with `pgmt init` (which records \
                     its current state as a baseline). `migrate provision` is for provisioning a \
                     fresh database."
                );
            }

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
            apply_baseline_to_target(&pool, &baseline_sql, source).await?;
            record_baseline_as_applied(
                &pool,
                &config.migration.tracking_table,
                baseline.version,
                "baseline",
                &calculate_checksum(&baseline_sql),
            )
            .await?;

            apply_pending_migrations(&pool, config, &post_baseline).await?;
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
            apply_pending_migrations(&pool, config, &migrations).await?;
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

/// Whether the managed catalog contains any user objects pgmt would manage.
///
/// Ignores schemas and extensions, which can exist on an otherwise fresh
/// database (e.g. `public`, `plpgsql`).
fn has_managed_objects(catalog: &Catalog) -> bool {
    !catalog.tables.is_empty()
        || !catalog.views.is_empty()
        || !catalog.types.is_empty()
        || !catalog.domains.is_empty()
        || !catalog.functions.is_empty()
        || !catalog.aggregates.is_empty()
        || !catalog.operators.is_empty()
        || !catalog.casts.is_empty()
        || !catalog.sequences.is_empty()
        || !catalog.indexes.is_empty()
        || !catalog.constraints.is_empty()
        || !catalog.triggers.is_empty()
        || !catalog.policies.is_empty()
}
