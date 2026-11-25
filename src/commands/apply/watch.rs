use anyhow::Result;
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use sqlx::PgPool;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, Instant};
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

    loop {
        if shutdown_signal.is_shutdown() {
            println!("🛑 Shutdown signal received, stopping watch mode...");
            break;
        }

        let first = match rx.recv_timeout(WATCH_POLL_TIMEOUT) {
            Ok(event) => match get_changed_sql_file(&event) {
                Some(path) => path.to_path_buf(),
                None => continue,
            },
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };
        let changed = collect_changes(first, &rx, WATCH_DEBOUNCE_DURATION);

        if shutdown_signal.is_shutdown() {
            println!("🛑 Shutdown signal received, stopping watch mode...");
            break;
        }

        println!(
            "\n🔄 Change detected: {}",
            describe_changes(&changed, root_dir)
        );

        match perform_single_apply(config, root_dir, &dev_pool, shadow, execution_mode.clone())
            .await
        {
            Ok(outcome) => match outcome {
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
            },
            Err(e) => {
                if shutdown_signal.is_shutdown() || is_prompt_interrupt(&e) {
                    println!("🛑 Operation interrupted, shutting down...");
                    break;
                }
                println!("❌ Failed to apply schema: {:#}", e);
                println!("⚠️  Will retry on next file change");
            }
        }

        println!("\n👁️  Continuing to watch for changes...");
    }

    println!("👋 Watch mode stopped");
    Ok(ApplyOutcome::Cancelled)
}

/// Wait out a burst of changes, starting from `first`: keep receiving until
/// no SQL file has changed for `quiet`, and return every file that did.
/// Editors emit several events per save, so one save becomes one apply.
/// Events that arrive after the burst stay queued, so a save made while an
/// apply runs triggers the next apply rather than being lost.
fn collect_changes(
    first: PathBuf,
    rx: &mpsc::Receiver<Event>,
    quiet: Duration,
) -> BTreeSet<PathBuf> {
    let mut changed = BTreeSet::from([first]);
    let mut deadline = Instant::now() + quiet;
    // A timeout ends the burst; a closed channel ends it too, and the watch
    // loop sees the disconnect on its next receive.
    while let Ok(event) = rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
        if let Some(path) = get_changed_sql_file(&event) {
            changed.insert(path.to_path_buf());
            deadline = Instant::now() + quiet;
        }
    }
    changed
}

/// "schema/a.sql", or "schema/a.sql and 2 more" for a burst across files
fn describe_changes(changed: &BTreeSet<PathBuf>, root_dir: &Path) -> String {
    let first = changed
        .first()
        .map(|path| {
            path.strip_prefix(root_dir)
                .unwrap_or(path)
                .display()
                .to_string()
        })
        .unwrap_or_default();
    match changed.len() {
        0 | 1 => first,
        n => format!("{first} and {} more", n - 1),
    }
}

/// Whether `error` is the user pressing Ctrl+C at a prompt. The prompt fails
/// with an interrupted read and raises SIGINT, but the shutdown flag that
/// SIGINT sets may not be set yet when the error arrives.
fn is_prompt_interrupt(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<dialoguer::Error>(),
            Some(dialoguer::Error::IO(io)) if io.kind() == std::io::ErrorKind::Interrupted
        )
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use notify::event::ModifyKind;
    use std::io;
    use std::thread;

    fn prompt_error(kind: io::ErrorKind) -> anyhow::Error {
        anyhow::Error::from(dialoguer::Error::IO(io::Error::new(kind, "read")))
    }

    #[test]
    fn ctrl_c_at_a_prompt_is_an_interrupt() {
        assert!(is_prompt_interrupt(&prompt_error(
            io::ErrorKind::Interrupted
        )));
        assert!(is_prompt_interrupt(
            &prompt_error(io::ErrorKind::Interrupted).context("Failed to review steps")
        ));
    }

    #[test]
    fn other_errors_are_not_interrupts() {
        assert!(!is_prompt_interrupt(&prompt_error(
            io::ErrorKind::UnexpectedEof
        )));
        assert!(!is_prompt_interrupt(&anyhow!(
            "new row violates check constraint: status <> 'cancelled' (interrupted)"
        )));
    }

    fn save(path: &str) -> Event {
        Event::new(EventKind::Modify(ModifyKind::Any)).add_path(PathBuf::from(path))
    }

    #[test]
    fn a_burst_of_events_is_one_change_set() {
        let (tx, rx) = mpsc::channel();
        // An editor's save: several events for the same file, plus a non-SQL file
        tx.send(save("/p/schema/a.sql")).unwrap();
        tx.send(save("/p/schema/a.sql")).unwrap();
        tx.send(save("/p/schema/a.sql.swp")).unwrap();
        tx.send(save("/p/schema/b.sql")).unwrap();

        let changed = collect_changes(
            PathBuf::from("/p/schema/a.sql"),
            &rx,
            Duration::from_millis(50),
        );

        assert_eq!(
            changed,
            BTreeSet::from([
                PathBuf::from("/p/schema/a.sql"),
                PathBuf::from("/p/schema/b.sql")
            ])
        );
    }

    #[test]
    fn a_change_inside_the_quiet_window_extends_it() {
        let (tx, rx) = mpsc::channel();
        let sender = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            tx.send(save("/p/schema/b.sql")).unwrap();
        });

        let changed = collect_changes(
            PathBuf::from("/p/schema/a.sql"),
            &rx,
            Duration::from_millis(300),
        );
        sender.join().unwrap();

        assert!(changed.contains(Path::new("/p/schema/b.sql")));
    }

    #[test]
    fn a_change_after_the_quiet_window_stays_queued() {
        let (tx, rx) = mpsc::channel();
        let sender = thread::spawn(move || {
            thread::sleep(Duration::from_millis(300));
            tx.send(save("/p/schema/b.sql")).unwrap();
        });

        let changed = collect_changes(
            PathBuf::from("/p/schema/a.sql"),
            &rx,
            Duration::from_millis(20),
        );
        sender.join().unwrap();

        assert_eq!(changed, BTreeSet::from([PathBuf::from("/p/schema/a.sql")]));
        // Not dropped: the watch loop receives it and applies again
        let queued = rx.try_recv().unwrap();
        assert_eq!(
            get_changed_sql_file(&queued),
            Some(Path::new("/p/schema/b.sql"))
        );
    }

    #[test]
    fn describes_a_burst_by_its_first_file() {
        let root = Path::new("/p");
        let one = BTreeSet::from([PathBuf::from("/p/schema/a.sql")]);
        let three = BTreeSet::from([
            PathBuf::from("/p/schema/c.sql"),
            PathBuf::from("/p/schema/a.sql"),
            PathBuf::from("/p/schema/b.sql"),
        ]);

        assert_eq!(describe_changes(&one, root), "schema/a.sql");
        assert_eq!(describe_changes(&three, root), "schema/a.sql and 2 more");
    }
}
