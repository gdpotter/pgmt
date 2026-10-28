use anyhow::Result;
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use sqlx::PgPool;
use std::path::Path;
use std::sync::mpsc;
use std::time::Instant;
use tracing::{error, info};

use crate::catalog::Catalog;
use crate::config::{Config, ObjectFilter};
use crate::modules::sectionize_unmoduled;

use super::ApplyOutcome;
use super::ExecutionMode;
use super::execution;
use super::lock::ApplyLock;
use super::shutdown::ShutdownSignal;
use crate::constants::{WATCH_DEBOUNCE_DURATION, WATCH_POLL_TIMEOUT};

/// Apply command with file watching support
pub async fn cmd_apply_watch_impl(
    config: &Config,
    root_dir: &Path,
    execution_mode: ExecutionMode,
    dev: &crate::config::DevUrl,
    shadow: &crate::config::ShadowDatabase,
) -> Result<ApplyOutcome> {
    println!("👁️  Starting pgmt in watch mode...");
    println!("💡 Press Ctrl+C to stop watching");

    // Set up graceful shutdown handling
    let shutdown_signal = ShutdownSignal::new();
    shutdown_signal.wait_for_signal().await;

    // Acquire lock to prevent concurrent apply operations
    info!("Checking for concurrent operations...");
    let _lock = ApplyLock::new(root_dir);
    _lock.acquire()?;

    // Set up persistent database connections
    info!("Connecting to development database...");
    let dev_pool =
        crate::db::connection::connect_to_database(dev.as_str(), "development database").await?;

    // Perform initial apply
    println!("\n🚀 Performing initial schema apply...");
    let initial_apply =
        perform_single_apply(config, root_dir, &dev_pool, shadow, execution_mode.clone()).await;
    if let Err(e) = initial_apply {
        println!("❌ Failed to apply schema: {:#}", e);
    };

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
                        shadow,
                        execution_mode.clone(),
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
                            println!("❌ Failed to apply schema: {:#}", e);
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
    shadow: &crate::config::ShadowDatabase,
    execution_mode: ExecutionMode,
) -> Result<ApplyOutcome> {
    // Process schema to shadow database. Each apply provisions and reclaims its
    // own fresh branch — reusing one across watch iterations would collide
    // (branch cleans are no-ops) and leak a branch per file-save.
    // The dev catalog loads while the schema files are applied.
    let filter = ObjectFilter::from_config(config);
    let (new, old) = tokio::try_join!(
        crate::schema_ops::apply_current_schema_to_shadow(config, root_dir, shadow),
        Catalog::load_managed(dev_pool, &filter),
    )?;

    let sections = sectionize_unmoduled(&old, &new)?;
    let step_count: usize = sections.iter().map(|s| s.steps.len()).sum();

    if step_count == 0 {
        return Ok(ApplyOutcome::NoChanges);
    }

    info!(
        "Found {} migration step{}",
        step_count,
        if step_count == 1 { "" } else { "s" }
    );

    execution::execute_plan(&sections, dev_pool, execution_mode, &new, config).await
}
