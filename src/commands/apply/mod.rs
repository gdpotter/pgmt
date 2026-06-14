pub mod execution;
pub mod execution_helpers;
pub mod lock;
pub mod shutdown;
pub mod user_interaction;
pub mod verification;
pub mod watch;

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
use crate::config::{Config, DevUrl, ObjectFilter, ShadowDatabase};
use crate::diff::operations::SqlRenderer;
use crate::diff::plan;
use crate::schema_ops::apply_current_schema_to_shadow;
use anyhow::{Context, Result};
use std::path::Path;
use tracing::info;

/// Main apply command entry point
pub async fn cmd_apply(
    config: &Config,
    root_dir: &Path,
    execution_mode: ExecutionMode,
    dev: &DevUrl,
    shadow: &ShadowDatabase,
) -> Result<ApplyOutcome> {
    let shutdown_signal = ShutdownSignal::new();
    shutdown_signal.wait_for_signal().await;

    info!("Checking for concurrent operations...");
    let _lock = ApplyLock::new(root_dir);
    _lock.acquire()?;

    info!("Connecting to development database...");
    let dev_pool =
        crate::db::connection::connect_to_database(dev.as_str(), "development database").await?;

    info!("Processing schema to shadow database...");
    let new = apply_current_schema_to_shadow(config, root_dir, shadow).await?;

    info!("Analyzing database catalogs...");
    let filter = ObjectFilter::from_config(config);
    let old = Catalog::load_managed(&dev_pool, &filter)
        .await
        .context("Failed to load catalog from development database")?;

    info!("Computing schema differences...");
    let ordered = plan(&old, &new)?;

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

        match execution::execute_plan(&ordered, &dev_pool, execution_mode.clone(), &new, config)
            .await
        {
            Ok(outcome) => {
                final_outcome = outcome;
                break;
            }
            Err(e) if e.to_string() == "REFRESH_REQUESTED" => {
                println!("🔄 Refreshing schema analysis...");

                info!("Re-processing schema to shadow database...");
                let new_filtered = apply_current_schema_to_shadow(config, root_dir, shadow).await?;

                info!("Re-analyzing database catalogs...");
                let old_filtered = Catalog::load_managed(&dev_pool, &filter).await?;

                info!("Re-computing schema differences...");
                let new_ordered = plan(&old_filtered, &new_filtered)?;

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
    dev: &DevUrl,
    shadow: &ShadowDatabase,
) -> Result<ApplyOutcome> {
    watch::cmd_apply_watch_impl(config, root_dir, execution_mode, dev, shadow).await
}
