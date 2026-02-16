pub mod execution;
pub mod execution_helpers;
pub mod lock;
pub mod shutdown;
pub mod user_interaction;
pub mod verification;
pub mod watch;

pub use crate::db::connection::connect_with_retry;
pub use lock::ApplyLock;
pub use shutdown::ShutdownSignal;

/// Execution mode for apply operations
#[derive(Clone)]
pub enum ExecutionMode {
    /// Preview changes without applying them
    DryRun,
    /// Apply all changes without user confirmation
    Force,
    /// Apply only safe operations, skip destructive ones
    SafeOnly,
    /// Fail if any destructive operations exist (default in non-TTY)
    RequireApproval,
    /// Auto-apply safe, prompt for destructive (default in TTY)
    Interactive,
}

/// Outcome of an apply operation, used for exit code determination
#[derive(Debug, Clone, PartialEq)]
pub enum ApplyOutcome {
    /// No changes were needed
    NoChanges,
    /// All changes were applied successfully
    Applied,
    /// Safe changes applied, destructive skipped (safe-only mode)
    Skipped,
    /// Destructive operations exist, not applied (require-approval mode)
    DestructiveRequired,
    /// User cancelled the operation
    Cancelled,
}

use crate::catalog::Catalog;
use crate::config::{Config, ObjectFilter};
use crate::db::schema_processor::{SchemaProcessor, SchemaProcessorConfig};
use crate::diff::operations::SqlRenderer;
use crate::diff::{cascade, diff_all, diff_order};
use anyhow::{Context, Result};
use std::path::Path;
use tracing::info;

/// Main apply command entry point
pub async fn cmd_apply(
    config: &Config,
    root_dir: &Path,
    execution_mode: ExecutionMode,
) -> Result<ApplyOutcome> {
    let shutdown_signal = ShutdownSignal::new();
    shutdown_signal.wait_for_signal().await;

    info!("Checking for concurrent operations...");
    let _lock = ApplyLock::new(root_dir);
    _lock.acquire()?;

    info!("Connecting to development database...");
    let dev_pool =
        crate::db::connection::connect_to_database(&config.databases.dev, "development database")
            .await?;

    info!("Setting up shadow database...");
    let shadow_url = config.databases.shadow.get_connection_string().await?;
    let shadow_pool = connect_with_retry(&shadow_url).await?;

    info!("Processing schema to shadow database...");
    let schema_dir = root_dir.join(&config.directories.schema);
    let roles_file = root_dir.join(&config.directories.roles);

    // Clean shadow database first, then apply roles, then apply schema
    crate::db::cleaner::clean_shadow_db(&shadow_pool, &config.objects).await?;

    // Apply roles file before schema files (if it exists)
    crate::schema_ops::apply_roles_file(&shadow_pool, &roles_file).await?;

    let processor_config = SchemaProcessorConfig {
        verbose: true,
        clean_before_apply: false, // Already cleaned above
        objects: config.objects.clone(),
    };
    let processor = SchemaProcessor::new(shadow_pool.clone(), processor_config.clone());
    let processed_schema = processor.process_schema_directory(&schema_dir).await?;

    info!("Analyzing database catalogs...");
    let old_catalog = Catalog::load(&dev_pool)
        .await
        .context("Failed to load catalog from development database")?;
    let new_catalog = processed_schema.with_file_dependencies_applied();

    let filter = ObjectFilter::new(&config.objects, &config.migration.tracking_table);
    let old = filter.filter_catalog(old_catalog);
    let new = filter.filter_catalog(new_catalog);

    info!("Computing schema differences...");
    let raw_steps = diff_all(&old, &new);
    let full_steps = cascade::expand(raw_steps, &old, &new);
    let ordered = diff_order(full_steps, &old, &new)?;

    if ordered.is_empty() {
        println!("✅ No schema changes detected - database is up to date");
        return Ok(ApplyOutcome::NoChanges);
    }

    info!(
        "Found {} migration step{}",
        ordered.len(),
        if ordered.len() == 1 { "" } else { "s" }
    );

    let mut final_outcome = ApplyOutcome::Applied;

    loop {
        if shutdown_signal.is_shutdown() {
            println!("🛑 Shutdown signal received, stopping gracefully...");
            return Ok(ApplyOutcome::Cancelled);
        }

        match execution::execute_plan(
            &ordered,
            &dev_pool,
            execution_mode.clone(),
            &new,
            config,
        )
        .await
        {
            Ok(outcome) => {
                final_outcome = outcome;
                break;
            }
            Err(e) if e.to_string() == "REFRESH_REQUESTED" => {
                println!("🔄 Refreshing schema analysis...");

                info!("Re-processing schema to shadow database...");

                // Clean and re-apply roles before reprocessing schema
                crate::db::cleaner::clean_shadow_db(&shadow_pool, &config.objects).await?;
                crate::schema_ops::apply_roles_file(&shadow_pool, &roles_file).await?;

                let reprocessor =
                    SchemaProcessor::new(shadow_pool.clone(), processor_config.clone());
                let reprocessed_schema = reprocessor.process_schema_directory(&schema_dir).await?;

                info!("Re-analyzing database catalogs...");
                let new_old_catalog = Catalog::load(&dev_pool).await?;
                let new_new_catalog = reprocessed_schema.with_file_dependencies_applied();

                let old_filtered = filter.filter_catalog(new_old_catalog);
                let new_filtered = filter.filter_catalog(new_new_catalog);

                info!("Re-computing schema differences...");
                let new_raw_steps = diff_all(&old_filtered, &new_filtered);
                let new_full_steps = cascade::expand(new_raw_steps, &old_filtered, &new_filtered);
                let new_ordered = diff_order(new_full_steps, &old_filtered, &new_filtered)?;

                if new_ordered.is_empty() {
                    println!(
                        "✅ No schema changes detected after refresh - database is up to date"
                    );
                    break;
                }

                info!(
                    "Found {} migration step{} after refresh",
                    new_ordered.len(),
                    if new_ordered.len() == 1 { "" } else { "s" }
                );

                if matches!(execution_mode, ExecutionMode::Interactive) {
                    use crate::render::RenderedSql;
                    let rendered: Vec<RenderedSql> =
                        new_ordered.iter().flat_map(|step| step.to_sql()).collect();

                    execution::print_plan_header(&new_ordered);
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        execution::print_migration_summary(&rendered);
                    } else {
                        execution::print_concise_plan(&new_ordered);
                    }

                    match user_interaction::execute_with_user_control(
                        &rendered,
                        &new_ordered,
                        &dev_pool,
                        &new_filtered,
                        config,
                    )
                    .await
                    {
                        Ok(outcome) => {
                            final_outcome = outcome;
                            break;
                        }
                        Err(refresh_err) if refresh_err.to_string() == "REFRESH_REQUESTED" => {
                            continue;
                        }
                        Err(other_err) => return Err(other_err),
                    }
                } else {
                    continue;
                }
            }
            Err(other_err) => return Err(other_err),
        }
    }

    Ok(final_outcome)
}

/// Apply command with file watching support
pub async fn cmd_apply_watch(
    config: &Config,
    root_dir: &Path,
    execution_mode: ExecutionMode,
) -> Result<ApplyOutcome> {
    watch::cmd_apply_watch_impl(config, root_dir, execution_mode).await
}