//! Concurrency tests for the migration advisory lock that serializes
//! `migrate apply` / `migrate provision` against the same database.
//!
//! These spawn the real `pgmt` binary (via `CARGO_BIN_EXE_pgmt`) as separate OS
//! processes so we exercise genuine cross-process contention on the Postgres
//! session advisory lock, not just in-process behavior.

use crate::helpers::cli::with_cli_helper;
use anyhow::Result;
use std::process::Output;
use std::time::{Duration, Instant};

/// A migration whose single section creates a table and then sleeps a few
/// seconds, so a concurrent run has a wide window to contend on the lock.
const SLOW_MIGRATION: &str = r#"-- pgmt:section name="slow"
CREATE TABLE lock_marker (id INT);
SELECT pg_sleep(3);
"#;

const SLOW_VERSION: &str = "1000000001";
const SLOW_FILENAME: &str = "1000000001_slow.sql";
const SLOW_SECTION: &str = "slow";

fn combined_output(o: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    )
}

/// Run `pgmt <args>` as a separate process against `project_root`.
fn spawn_pgmt(project_root: &std::path::Path, args: &[&str]) -> tokio::process::Command {
    let mut cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_pgmt"));
    cmd.args(args).current_dir(project_root);
    cmd
}

/// Two concurrent `migrate apply` processes against the same DB must serialize:
/// both succeed, the migration is applied exactly once, and one of them reports
/// waiting on the lock.
#[tokio::test]
async fn test_concurrent_applies_serialize() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        helper.write_migration_file(SLOW_FILENAME, SLOW_MIGRATION)?;

        let url = helper.dev_database_url.clone();
        let root = helper.project_root.clone();

        let out1 = spawn_pgmt(&root, &["migrate", "apply", "--target-url", &url]).output();
        let out2 = spawn_pgmt(&root, &["migrate", "apply", "--target-url", &url]).output();

        let start = Instant::now();
        let (out1, out2) = tokio::join!(out1, out2);
        let elapsed = start.elapsed();
        let out1 = out1?;
        let out2 = out2?;

        let combined = format!("{}\n{}", combined_output(&out1), combined_output(&out2));

        // Both processes must succeed. Without the lock, the second would race
        // into the same CREATE TABLE and fail with "already exists".
        assert!(
            out1.status.success() && out2.status.success(),
            "both applies should succeed under the advisory lock; combined output:\n{}",
            combined
        );

        // Serialization evidence: the loser observed the lock held and waited.
        assert!(
            combined.contains("waiting..."),
            "expected one process to report waiting on the advisory lock; combined output:\n{}",
            combined
        );

        // The table exists exactly once and the migration was recorded once.
        assert!(helper.table_exists_in_dev("public", "lock_marker").await?);

        let pool = helper.connect_to_dev_db().await?;
        let version_rows: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM public.pgmt_migrations WHERE version = $1")
                .bind(SLOW_VERSION.parse::<i64>().unwrap())
                .fetch_one(&pool)
                .await?;
        assert_eq!(version_rows, 1, "migration must be recorded exactly once");

        // The section ran exactly once (attempts = 1): the second process skipped
        // the already-applied migration instead of re-executing the section.
        let attempts: i32 = sqlx::query_scalar(
            "SELECT attempts FROM public.pgmt_migrations_sections \
             WHERE migration_version = $1 AND section_name = $2",
        )
        .bind(SLOW_VERSION.parse::<i64>().unwrap())
        .bind(SLOW_SECTION)
        .fetch_one(&pool)
        .await?;
        assert_eq!(attempts, 1, "the section must have executed exactly once");
        pool.close().await;

        // Sanity on serialization timing: the two ~3s sleeps did not overlap into
        // a single ~3s wall clock; the loser waited for the winner.
        assert!(
            elapsed >= Duration::from_secs(3),
            "serialized runs should take at least one sleep window; took {:?}",
            elapsed
        );

        Ok(())
    })
    .await
}

/// After a failed apply, the lock must be released so a follow-up apply does not
/// hang: it should fail promptly on the same bad SQL, not block on the lock.
#[tokio::test]
async fn test_lock_released_after_failed_apply() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        helper.write_migration_file(
            "1000000002_bad.sql",
            "-- pgmt:section name=\"boom\"\nSELECT this_is_not_valid_sql;\n",
        )?;

        let url = helper.dev_database_url.clone();
        let root = helper.project_root.clone();

        // First apply fails on the bad SQL.
        let first = spawn_pgmt(&root, &["migrate", "apply", "--target-url", &url])
            .output()
            .await?;
        assert!(
            !first.status.success(),
            "first apply should fail on the bad SQL; output:\n{}",
            combined_output(&first)
        );

        // Second apply must NOT hang on the lock. Bound it with a timeout: if the
        // lock leaked, pg_advisory_lock would block forever and this would time out.
        let second_fut = spawn_pgmt(&root, &["migrate", "apply", "--target-url", &url]).output();
        let second = tokio::time::timeout(Duration::from_secs(20), second_fut)
            .await
            .expect("second apply hung — the advisory lock was not released after failure")?;

        let combined = combined_output(&second);
        assert!(
            !second.status.success(),
            "second apply should also fail on the same bad SQL; output:\n{}",
            combined
        );
        // It failed on the SQL, not by waiting on the lock.
        assert!(
            !combined.contains("waiting..."),
            "second apply should not have waited on the lock; output:\n{}",
            combined
        );

        Ok(())
    })
    .await
}

/// `migrate provision` and `migrate apply` derive the same lock key from the
/// shared tracking table, so concurrent runs serialize just like two applies.
/// (Key sharing is also pinned at the unit level in the advisory_lock module.)
#[tokio::test]
async fn test_provision_and_apply_share_lock() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        // No baseline: provision replays migrations, so it runs the slow section
        // through the same apply loop and contends on the same lock as apply.
        helper.write_migration_file(SLOW_FILENAME, SLOW_MIGRATION)?;

        let url = helper.dev_database_url.clone();
        let root = helper.project_root.clone();

        let provision = spawn_pgmt(&root, &["migrate", "provision", "--target-url", &url]).output();
        let apply = spawn_pgmt(&root, &["migrate", "apply", "--target-url", &url]).output();

        let (provision, apply) = tokio::join!(provision, apply);
        let provision = provision?;
        let apply = apply?;

        let combined = format!(
            "{}\n{}",
            combined_output(&provision),
            combined_output(&apply)
        );

        assert!(
            provision.status.success() && apply.status.success(),
            "provision and apply should both succeed under the shared lock; output:\n{}",
            combined
        );
        assert!(
            combined.contains("waiting..."),
            "one of provision/apply should have waited on the shared lock; output:\n{}",
            combined
        );

        // The migration was applied exactly once across both commands.
        let pool = helper.connect_to_dev_db().await?;
        let version_rows: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM public.pgmt_migrations WHERE version = $1")
                .bind(SLOW_VERSION.parse::<i64>().unwrap())
                .fetch_one(&pool)
                .await?;
        assert_eq!(version_rows, 1, "migration must be recorded exactly once");
        pool.close().await;

        Ok(())
    })
    .await
}
