//! Tests for non-transactional section failure reporting, specifically the
//! detect-and-report of INVALID indexes left behind by a failed or interrupted
//! CREATE INDEX CONCURRENTLY.
//!
//! When a unique CIC hits a duplicate (or a run crashes mid-CIC), Postgres
//! leaves an INVALID index behind. Re-running the CREATE then dies with
//! `already exists`. pgmt must NEVER auto-drop the index and NEVER parse the
//! section SQL — after the final retry fails it runs a READ-ONLY catalog probe
//! and, if any invalid index is present, appends guidance to the error (and to
//! the recorded `last_error`).

use crate::helpers::cli::with_cli_helper;
use anyhow::Result;
use predicates::prelude::*;

/// A transactional seed section plants duplicate values, then a
/// non-transactional section builds a UNIQUE index CONCURRENTLY over them — so
/// the CIC fails with a unique violation and leaves an INVALID index behind.
const DUP_CIC_MIGRATION: &str = r#"-- pgmt:section name="seed"
CREATE TABLE dup_cic (id INT, val INT);
INSERT INTO dup_cic (id, val) VALUES (1, 100), (2, 100);

-- pgmt:section name="build_index"
-- pgmt:  mode="non-transactional"
-- pgmt:  timeout="10s"
CREATE UNIQUE INDEX CONCURRENTLY idx_dup_cic_val ON dup_cic(val);
"#;
const DUP_CIC_FILENAME: &str = "1000000020_dup_cic.sql";
const DUP_CIC_VERSION: i64 = 1000000020;
const INVALID_INDEX_NAME: &str = "idx_dup_cic_val";

/// A non-transactional section whose CIC references a table that does not
/// exist: it fails BEFORE any index is created, so there is no invalid index
/// and the failure must stay plain (no invalid-index guidance).
const MISSING_TABLE_MIGRATION: &str = r#"-- pgmt:section name="build_index"
-- pgmt:  mode="non-transactional"
-- pgmt:  timeout="10s"
CREATE INDEX CONCURRENTLY idx_ghost ON ghost_table(col);
"#;
const MISSING_TABLE_FILENAME: &str = "1000000021_missing_table.sql";

/// Query whether a named index exists and is valid in the dev database.
/// Returns `(exists, is_valid)`.
async fn index_state(
    helper: &crate::helpers::cli::CliTestHelper,
    name: &str,
) -> Result<(bool, bool)> {
    let pool = helper.connect_to_dev_db().await?;
    let row: Option<(bool,)> = sqlx::query_as(
        "SELECT i.indisvalid
         FROM pg_index i
         JOIN pg_class c ON c.oid = i.indexrelid
         WHERE c.relname = $1",
    )
    .bind(name)
    .fetch_optional(&pool)
    .await?;
    pool.close().await;
    Ok(match row {
        Some((valid,)) => (true, valid),
        None => (false, false),
    })
}

/// A failed unique CIC over duplicate data must fail loud AND report the
/// leftover invalid index — in both stderr and the recorded `last_error` — with
/// the `DROP INDEX CONCURRENTLY IF EXISTS` guidance.
#[tokio::test]
async fn test_failed_cic_reports_invalid_index() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        helper.write_migration_file(DUP_CIC_FILENAME, DUP_CIC_MIGRATION)?;

        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .failure()
            .stderr(predicate::str::contains("Invalid index(es) found"))
            .stderr(predicate::str::contains(INVALID_INDEX_NAME))
            .stderr(predicate::str::contains(
                "DROP INDEX CONCURRENTLY IF EXISTS",
            ));

        // The CIC really did leave an INVALID index behind.
        let (exists, valid) = index_state(helper, INVALID_INDEX_NAME).await?;
        assert!(exists, "the failed CIC must leave the index behind");
        assert!(!valid, "the leftover index must be INVALID");

        // The enriched guidance also lands in the recorded last_error.
        let (status, last_error) = helper
            .section_row_in_dev(DUP_CIC_VERSION, "build_index")
            .await?
            .expect("build_index section row should exist");
        assert_eq!(status, "failed", "the CIC section must be failed");
        let last_error = last_error.expect("failed section must record last_error");
        assert!(
            last_error.contains("Invalid index(es) found"),
            "last_error must carry the invalid-index note: {last_error}"
        );
        assert!(
            last_error.contains(INVALID_INDEX_NAME),
            "last_error must name the invalid index: {last_error}"
        );
        assert!(
            last_error.contains("DROP INDEX CONCURRENTLY IF EXISTS"),
            "last_error must carry the DROP guidance: {last_error}"
        );

        Ok(())
    })
    .await
}

/// Following the documented recovery — remove the offending data and run the
/// recommended `DROP INDEX CONCURRENTLY IF EXISTS` manually (as the user would
/// after adding the DROP guard) — a re-run of apply completes the section and
/// leaves a VALID index.
#[tokio::test]
async fn test_invalid_index_guidance_enables_recovery() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        helper.write_migration_file(DUP_CIC_FILENAME, DUP_CIC_MIGRATION)?;

        // First apply fails at the CIC, leaving an invalid index.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .failure();
        let (_, valid) = index_state(helper, INVALID_INDEX_NAME).await?;
        assert!(!valid, "precondition: leftover index is INVALID");

        // Simulate the documented recovery: fix the data and drop the invalid
        // index (DROP INDEX CONCURRENTLY cannot run in a transaction, so it goes
        // through the pool's implicit autocommit).
        let pool = helper.connect_to_dev_db().await?;
        sqlx::query("DELETE FROM dup_cic WHERE id = 2")
            .execute(&pool)
            .await?;
        sqlx::query("DROP INDEX CONCURRENTLY IF EXISTS idx_dup_cic_val")
            .execute(&pool)
            .await?;
        pool.close().await;

        // Re-run apply: the seed section is already completed (skipped), and the
        // CIC now succeeds over the de-duplicated data.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();

        let (exists, valid) = index_state(helper, INVALID_INDEX_NAME).await?;
        assert!(
            exists && valid,
            "the recovered index must be present and VALID"
        );

        let (status, _) = helper
            .section_row_in_dev(DUP_CIC_VERSION, "build_index")
            .await?
            .expect("build_index section row should exist");
        assert_eq!(
            status, "completed",
            "the recovered CIC section must be completed"
        );

        Ok(())
    })
    .await
}

/// A non-transactional section that fails for an unrelated reason (its target
/// table does not exist) leaves no invalid index, so the error must stay plain
/// — no invalid-index guidance noise.
#[tokio::test]
async fn test_no_invalid_index_no_noise() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        helper.write_migration_file(MISSING_TABLE_FILENAME, MISSING_TABLE_MIGRATION)?;

        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .failure()
            .stderr(predicate::str::contains("Invalid index(es) found").not());

        Ok(())
    })
    .await
}
