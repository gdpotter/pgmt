//! Crash-recovery tests for `migrate apply` section tracking.
//!
//! Real crash windows (a process dying between a section's commit and its
//! tracking record) can't be produced black-box, so these tests SEED the
//! post-crash state directly via SQL — inserting/flipping tracking rows and
//! adding or removing DDL — then assert how a follow-up apply recovers.
//!
//! The invariant under test: for transactional sections, a `completed` tracking
//! row exists if and only if the section's DDL committed. Completion is recorded
//! inside the section's own transaction, so the two are atomic.

use crate::helpers::cli::with_cli_helper;
use anyhow::Result;
use predicates::prelude::*;

/// Two-section migration whose second section creates a table and then runs
/// invalid SQL, so a plain `apply` fails partway: section one commits, section
/// two rolls back.
const FAILING_MIGRATION: &str = r#"-- pgmt:section name="one"
CREATE TABLE crash_one (id INT);

-- pgmt:section name="two"
CREATE TABLE crash_two (id INT);
SELECT this_is_not_valid_sql;
"#;
const FAILING_FILENAME: &str = "1000000010_crash_atomic.sql";
const FAILING_VERSION: i64 = 1000000010;

/// A section's `completed` row and its DDL are atomic: when a later section
/// fails, the earlier section is `completed` with its table present, and the
/// failing section is `failed` with its DDL rolled back.
#[tokio::test]
async fn test_completed_record_is_atomic_with_ddl() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        helper.write_migration_file(FAILING_FILENAME, FAILING_MIGRATION)?;

        // Apply fails on section two's bad SQL.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .failure()
            .stderr(predicate::str::contains("failed at section 'two'"));

        // Section one committed: completed row AND its table exists.
        let (status_one, _) = helper
            .section_row_in_dev(FAILING_VERSION, "one")
            .await?
            .expect("section one row should exist");
        assert_eq!(status_one, "completed", "section one must be completed");
        assert!(
            helper.table_exists_in_dev("public", "crash_one").await?,
            "section one's DDL must be present"
        );

        // Section two rolled back: failed row with last_error AND no table.
        let (status_two, last_error_two) = helper
            .section_row_in_dev(FAILING_VERSION, "two")
            .await?
            .expect("section two row should exist");
        assert_eq!(status_two, "failed", "section two must be failed");
        assert!(
            last_error_two.is_some(),
            "section two must record last_error"
        );
        assert!(
            !helper.table_exists_in_dev("public", "crash_two").await?,
            "section two's DDL must have rolled back with its transaction"
        );

        Ok(())
    })
    .await
}

/// A section stuck at `running` with its DDL absent (a crash before commit)
/// re-executes cleanly on the next apply and ends `completed` with its DDL
/// present — nothing committed, so nothing collides.
#[tokio::test]
async fn test_resume_from_seeded_running_without_ddl() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        helper.write_migration_file(FAILING_FILENAME, FAILING_MIGRATION)?;

        // First apply fails at section two, seeding the section rows (section one
        // completed, section two failed). The migration is NOT recorded in
        // pgmt_migrations, so a re-run re-enters the section loop.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .failure();

        // Simulate a crash of section one *before* commit: its DDL never landed
        // and its row is stuck at `running`.
        let pool = helper.connect_to_dev_db().await?;
        sqlx::query("DROP TABLE crash_one")
            .execute(&pool)
            .await?;
        pool.close().await;
        helper
            .seed_section_status(FAILING_VERSION, "one", "running")
            .await?;
        assert!(
            !helper.table_exists_in_dev("public", "crash_one").await?,
            "precondition: section one's DDL is absent"
        );

        // Re-run apply. Section one is `running` (not `completed`), so it
        // re-executes and commits cleanly; the overall apply still fails at the
        // (still-invalid) section two, which is fine — we assert on section one.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .failure();

        let (status_one, _) = helper
            .section_row_in_dev(FAILING_VERSION, "one")
            .await?
            .expect("section one row should exist");
        assert_eq!(
            status_one, "completed",
            "resumed section one must end completed"
        );
        assert!(
            helper.table_exists_in_dev("public", "crash_one").await?,
            "resumed section one must have re-created its DDL"
        );

        Ok(())
    })
    .await
}

/// A non-idempotent section stuck at `running` whose DDL DID commit (the
/// pre-fix crash window, or the residual window for non-transactional sections)
/// cannot be auto-recovered: re-running re-executes the CREATE and hits
/// `already exists`. This documents the residual gap — the apply must fail loud
/// and record the failure, not silently corrupt tracking.
#[tokio::test]
async fn test_seeded_running_with_ddl_reruns_and_fails_loud() -> Result<()> {
    const SOLO_MIGRATION: &str = r#"-- pgmt:section name="solo"
CREATE TABLE crash_solo (id INT);
"#;
    const SOLO_FILENAME: &str = "1000000011_crash_solo.sql";
    const SOLO_VERSION: i64 = 1000000011;

    with_cli_helper(async |helper| {
        helper.init_project()?;
        helper.write_migration_file(SOLO_FILENAME, SOLO_MIGRATION)?;

        // Apply the migration cleanly: the table and tracking rows now exist.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();
        assert!(helper.table_exists_in_dev("public", "crash_solo").await?);

        // Seed the pre-fix crash window: the migration was never recorded (so a
        // re-run re-enters), the section is stuck at `running`, but its DDL is
        // still present.
        let pool = helper.connect_to_dev_db().await?;
        sqlx::query("DELETE FROM public.pgmt_migrations WHERE version = $1")
            .bind(SOLO_VERSION)
            .execute(&pool)
            .await?;
        pool.close().await;
        helper
            .seed_section_status(SOLO_VERSION, "solo", "running")
            .await?;

        // Re-run apply: the CREATE TABLE collides with the surviving DDL. This
        // must fail loud (mentioning the section) and record the failure.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .failure()
            .stderr(predicate::str::contains("already exists"))
            .stderr(predicate::str::contains("solo"));

        let (status, last_error) = helper
            .section_row_in_dev(SOLO_VERSION, "solo")
            .await?
            .expect("solo section row should exist");
        assert_eq!(status, "failed", "the collided section must be failed");
        assert!(
            last_error.is_some_and(|e| e.contains("already exists")),
            "the failure must be recorded with the collision error"
        );

        Ok(())
    })
    .await
}
