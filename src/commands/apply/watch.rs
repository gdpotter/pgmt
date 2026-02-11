use anyhow::{Context, Result};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use sqlx::PgPool;
use std::path::Path;
use std::sync::mpsc;
use std::time::Instant;
use tracing::error;

use crate::catalog::Catalog;
use crate::config::{Config, ObjectFilter};
use crate::db::schema_processor::{SchemaProcessor, SchemaProcessorConfig};
use crate::diff::operations::SqlRenderer;
use crate::diff::{cascade, diff_all, diff_order};
use crate::render::{RenderedSql, Safety};

use super::ApplyOutcome;
use super::ExecutionMode;
use super::connect_with_retry;
use super::execution;
use super::execution_helpers;
use super::lock::ApplyLock;
use super::shutdown::ShutdownSignal;
use super::user_interaction;
use crate::constants::{WATCH_DEBOUNCE_DURATION, WATCH_POLL_TIMEOUT};

/// Apply command with file watching support
pub async fn cmd_apply_watch_impl(
    config: &Config,
    root_dir: &Path,
    execution_mode: ExecutionMode,
    verbose: bool,
) -> Result<ApplyOutcome> {
    println!("👁️  Starting pgmt in watch mode...");
    println!("💡 Press Ctrl+C to stop watching");

    // Set up graceful shutdown handling
    let shutdown_signal = ShutdownSignal::new();
    shutdown_signal.wait_for_signal().await;

    // Acquire lock to prevent concurrent apply operations
    if verbose {
        println!("🔒 Checking for concurrent operations...");
    }
    let _lock = ApplyLock::new(root_dir);
    _lock.acquire()?;

    // Set up persistent database connections
    if verbose {
        println!("📊 Connecting to development database...");
    }
    let dev_pool = PgPool::connect(&config.databases.dev)
        .await
        .context("Failed to connect to development database")?;

    if verbose {
        println!("🛡️  Setting up shadow database...");
    }
    let shadow_url = config.databases.shadow.get_connection_string().await?;
    let shadow_pool = connect_with_retry(&shadow_url).await?;

    // Perform initial apply
    println!("\n🚀 Performing initial schema apply...");
    perform_single_apply(
        config,
        root_dir,
        &dev_pool,
        &shadow_pool,
        execution_mode.clone(),
        verbose,
    )
    .await?;

    // Set up file watching
    let schema_dir = root_dir.join(&config.directories.schema);
    println!("\n👁️  Watching for changes in: {}", schema_dir.display());

    let (tx, rx) = mpsc::channel();
    let mut watcher: RecommendedWatcher = Watcher::new(
        move |result: notify::Result<Event>| match result {
            Ok(event) => {
                if let Err(e) = tx.send(event) {
                    error!("Failed to send file event: {}", e);
                }
            }
            Err(e) => error!("File watcher error: {}", e),
        },
        notify::Config::default(),
    )?;

    watcher.watch(&schema_dir, RecursiveMode::Recursive)?;

    let mut last_apply = Instant::now();

    loop {
        // Check for shutdown signal
        if shutdown_signal.is_shutdown() {
            println!("🛑 Shutdown signal received, stopping watch mode...");
            break;
        }

        match rx.recv_timeout(WATCH_POLL_TIMEOUT) {
            Ok(event) => {
                // Check shutdown again before processing events
                if shutdown_signal.is_shutdown() {
                    println!("🛑 Shutdown signal received, stopping watch mode...");
                    break;
                }

                // Debounce rapid changes
                if last_apply.elapsed() < WATCH_DEBOUNCE_DURATION {
                    continue;
                }

                // Check if this is a relevant file change
                if let Some(changed_file) = get_changed_sql_file(&event) {
                    // Show which file triggered the change
                    let file_display = changed_file
                        .strip_prefix(root_dir)
                        .unwrap_or(changed_file)
                        .display();
                    println!("\n🔄 Change detected: {}", file_display);

                    match perform_single_apply(
                        config,
                        root_dir,
                        &dev_pool,
                        &shadow_pool,
                        execution_mode.clone(),
                        verbose,
                    )
                    .await
                    {
                        Ok(outcome) => {
                            match outcome {
                                ApplyOutcome::NoChanges => {
                                    println!("✅ No schema changes needed");
                                }
                                ApplyOutcome::Applied => {
                                    // Concise output already printed by execute_plan
                                }
                                ApplyOutcome::Skipped => {
                                    println!("✅ Safe operations applied (destructive skipped)");
                                }
                                ApplyOutcome::DestructiveRequired => {
                                    println!("⚠️  Destructive operations require --force");
                                }
                                ApplyOutcome::Cancelled => {
                                    println!("❌ Operation cancelled");
                                }
                            }
                            last_apply = Instant::now();
                        }
                        Err(e) => {
                            // Check if error is due to shutdown
                            if e.to_string().contains("interrupted")
                                || e.to_string().contains("cancelled")
                            {
                                println!("🛑 Operation interrupted, shutting down...");
                                break;
                            }
                            error!("❌ Failed to apply schema: {}", e);
                            println!("⚠️  Will retry on next file change");
                        }
                    }

                    println!("\n👁️  Continuing to watch for changes...");
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                // Normal timeout, continue watching
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                break;
            }
        }
    }

    println!("👋 Watch mode stopped");
    Ok(ApplyOutcome::Cancelled)
}

/// Get the first changed SQL file from an event, if any
fn get_changed_sql_file(event: &Event) -> Option<&Path> {
    // Only care about write events and creation/deletion
    match event.kind {
        EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_) => {
            // Find the first .sql file in the paths
            event
                .paths
                .iter()
                .find(|path| path.extension().is_some_and(|ext| ext == "sql"))
                .map(|p| p.as_path())
        }
        _ => None,
    }
}

/// Perform a single apply operation (shared by regular and watch mode)
async fn perform_single_apply(
    config: &Config,
    root_dir: &Path,
    dev_pool: &PgPool,
    shadow_pool: &PgPool,
    execution_mode: ExecutionMode,
    verbose: bool,
) -> Result<ApplyOutcome> {
    let schema_dir = root_dir.join(&config.directories.schema);
    let roles_file = root_dir.join(&config.directories.roles);

    // Clean shadow database first, then apply roles, then apply schema
    crate::db::cleaner::clean_shadow_db(shadow_pool, &config.objects).await?;

    // Apply roles file before schema files (if it exists)
    crate::schema_ops::apply_roles_file(shadow_pool, &roles_file).await?;

    // Process schema to shadow database
    let processor_config = SchemaProcessorConfig {
        verbose: true,
        clean_before_apply: false, // Already cleaned above
        objects: config.objects.clone(),
    };
    let processor = SchemaProcessor::new(shadow_pool.clone(), processor_config);
    let processed_schema = processor.process_schema_directory(&schema_dir).await?;

    // Analyze differences
    let old_catalog = Catalog::load(dev_pool).await?;
    let new_catalog = processed_schema.with_file_dependencies_applied();

    // Apply object filtering
    let filter = ObjectFilter::new(&config.objects, &config.migration.tracking_table);
    let old = filter.filter_catalog(old_catalog);
    let new = filter.filter_catalog(new_catalog);

    let raw_steps = diff_all(&old, &new);
    let full_steps = cascade::expand(raw_steps, &old, &new);
    let ordered = diff_order(full_steps, &old, &new)?;

    if ordered.is_empty() {
        return Ok(ApplyOutcome::NoChanges);
    }

    if verbose {
        println!(
            "📋 Found {} migration step{}",
            ordered.len(),
            if ordered.len() == 1 { "" } else { "s" }
        );
    }

    // Execute the migration plan
    execute_plan_watch_aware(&ordered, dev_pool, execution_mode, &new, config, verbose).await
}

/// Execute plan with watch mode optimizations
async fn execute_plan_watch_aware(
    steps: &[crate::diff::operations::MigrationStep],
    dev_pool: &PgPool,
    mode: ExecutionMode,
    expected_catalog: &Catalog,
    config: &Config,
    verbose: bool,
) -> Result<ApplyOutcome> {
    let rendered: Vec<RenderedSql> = steps.iter().flat_map(|step| step.to_sql()).collect();

    // Show plan summary
    execution::print_plan_header(steps);
    if verbose {
        execution::print_migration_summary(&rendered);
    } else {
        execution::print_concise_plan(steps);
    }

    match mode {
        ExecutionMode::DryRun => {
            println!("✅ Dry run - no changes applied");
            Ok(ApplyOutcome::Applied)
        }

        ExecutionMode::Force => {
            // Auto-apply everything in watch mode
            let outcome = execution_helpers::apply_all_rendered_steps(
                &rendered,
                dev_pool,
                expected_catalog,
                config,
                false,
            )
            .await?;
            if !verbose {
                println!("\n✅ Applied {} changes", steps.len());
            }
            Ok(outcome)
        }

        ExecutionMode::SafeOnly => {
            // Auto-apply safe operations, show destructive ones
            let outcome = execution_helpers::apply_safe_rendered_steps(
                &rendered,
                dev_pool,
                expected_catalog,
                config,
                true,
                false,
            )
            .await?;
            if !verbose {
                let applied = rendered.iter().filter(|s| s.safety == Safety::Safe).count();
                if applied > 0 {
                    println!("\n✅ Applied {} changes", applied);
                }
            }
            Ok(outcome)
        }

        ExecutionMode::RequireApproval => {
            let has_destructive = rendered.iter().any(|s| s.safety == Safety::Destructive);

            if has_destructive {
                println!("\n⚠️  Destructive operations detected:");
                for step in rendered.iter().filter(|s| s.safety == Safety::Destructive) {
                    let preview = step.sql.lines().next().unwrap_or("");
                    println!("   • {}", preview);
                }
                println!("\nRun with --force to apply, or resolve the schema changes.");
                Ok(ApplyOutcome::DestructiveRequired)
            } else {
                let outcome = execution_helpers::apply_all_rendered_steps(
                    &rendered,
                    dev_pool,
                    expected_catalog,
                    config,
                    false,
                )
                .await?;
                if !verbose {
                    println!("\n✅ Applied {} changes", steps.len());
                }
                Ok(outcome)
            }
        }

        ExecutionMode::Interactive => {
            // Check if all operations are safe
            let all_safe = rendered.iter().all(|s| s.safety == Safety::Safe);

            if all_safe {
                // Auto-apply when all operations are safe
                let outcome = execution_helpers::apply_all_rendered_steps(
                    &rendered,
                    dev_pool,
                    expected_catalog,
                    config,
                    false,
                )
                .await?;
                if !verbose {
                    println!("\n✅ Applied {} changes", steps.len());
                }
                Ok(outcome)
            } else {
                // Prompt when any destructive operations are present
                user_interaction::execute_with_user_control(
                    &rendered,
                    steps,
                    dev_pool,
                    expected_catalog,
                    config,
                    verbose,
                )
                .await
            }
        }
    }
}
