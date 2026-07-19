//! `migrate resolve` — explicit break-glass repair of section tracking state.
//!
//! resolve is the escape hatch fix-in-repo can't reach: a manual hot-fix that
//! should be recorded (`--mark-completed`), or a consciously edited applied
//! section to re-stamp (`--restamp`). Each verb touches exactly one coordinate
//! and prints the before/after state. (A failed/stale section needs no verb —
//! the next apply re-runs it.)

use crate::helpers::cli::{section_checksum, with_cli_helper};
use anyhow::Result;
use predicates::prelude::*;

/// A three-section migration whose middle section runs invalid SQL, so a plain
/// apply fails partway: section one commits, section two rolls back, section
/// three stays pending.
const THREE_SECTION_FAILING: &str = r#"-- pgmt:section name="one"
CREATE TABLE mc_one (id INT);

-- pgmt:section name="two"
CREATE TABLE mc_two (id INT);
SELECT this_is_not_valid_sql;

-- pgmt:section name="three"
CREATE TABLE mc_three (id INT);
"#;
const THREE_SECTION_FILENAME: &str = "1000000100_resolve_mark.sql";
const THREE_SECTION_VERSION: i64 = 1000000100;

/// A DBA manually hot-fixed the DB and marks the failed section completed
/// without running it; the next apply proceeds past it (section three runs).
#[tokio::test]
async fn test_mark_completed_flow() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        helper.write_migration_file(THREE_SECTION_FILENAME, THREE_SECTION_FAILING)?;

        // First apply fails at section two.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .failure()
            .stderr(predicate::str::contains("failed at section 'two'"));

        let (status_two, _) = helper
            .section_row_in_dev(THREE_SECTION_VERSION, "two")
            .await?
            .expect("section two row exists");
        assert_eq!(status_two, "failed");
        let (status_three, _) = helper
            .section_row_in_dev(THREE_SECTION_VERSION, "three")
            .await?
            .expect("section three row exists");
        assert_eq!(status_three, "pending");

        // Simulate the DBA's manual fix: create section two's table by hand.
        let pool = helper.connect_to_dev_db().await?;
        sqlx::query("CREATE TABLE mc_two (id INT)")
            .execute(&pool)
            .await?;
        pool.close().await;

        // Mark section two completed — prints old->new and re-stamps metadata.
        helper
            .command()
            .args([
                "migrate",
                "resolve",
                "--mark-completed",
                &format!("{THREE_SECTION_VERSION}/two"),
                "--target-url",
                &helper.dev_database_url,
            ])
            .assert()
            .success()
            .stdout(predicate::str::contains("failed -> completed"))
            .stdout(predicate::str::contains("re-stamped"));

        let (status_two, _) = helper
            .section_row_in_dev(THREE_SECTION_VERSION, "two")
            .await?
            .expect("section two row exists");
        assert_eq!(status_two, "completed", "resolve marked section two done");

        // Next apply proceeds past section two and runs section three.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();

        assert!(
            helper.table_exists_in_dev("public", "mc_three").await?,
            "section three ran after section two was marked completed"
        );
        let (status_three, _) = helper
            .section_row_in_dev(THREE_SECTION_VERSION, "three")
            .await?
            .expect("section three row exists");
        assert_eq!(status_three, "completed");

        Ok(())
    })
    .await
}

const RESTAMP_ORIGINAL: &str = r#"-- pgmt:section name="one"
CREATE TABLE rsp_one (id INT);

-- pgmt:section name="two"
CREATE TABLE rsp_two (id INT);
"#;
const RESTAMP_FILENAME: &str = "1000000300_resolve_restamp.sql";
const RESTAMP_VERSION: i64 = 1000000300;

/// A completed migration is edited (conscious change to an applied section);
/// apply then bails on the section checksum. `--restamp` re-stamps the stored
/// checksums and apply proceeds cleanly.
#[tokio::test]
async fn test_restamp_flow() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        helper.write_migration_file(RESTAMP_FILENAME, RESTAMP_ORIGINAL)?;

        // Apply the migration cleanly: both sections completed.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();

        // Consciously edit an already-completed section's body.
        let edited = r#"-- pgmt:section name="one"
CREATE TABLE rsp_one (id INT); -- deliberately edited after apply

-- pgmt:section name="two"
CREATE TABLE rsp_two (id INT);
"#;
        helper.write_migration_file(RESTAMP_FILENAME, edited)?;

        // Apply now bails on the immutable completed section.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "was modified after it was applied",
            ));

        // Re-stamp all completed sections of the version.
        helper
            .command()
            .args([
                "migrate",
                "resolve",
                "--restamp",
                &RESTAMP_VERSION.to_string(),
                "--target-url",
                &helper.dev_database_url,
            ])
            .assert()
            .success()
            .stdout(predicate::str::contains("Re-stamping"))
            .stdout(predicate::str::contains("one:"));

        // The stored checksum reflects the edited content.
        let pool = helper.connect_to_dev_db().await?;
        let stored: Option<String> = sqlx::query_scalar(
            "SELECT checksum FROM public.pgmt_migrations_sections WHERE section_name = 'one'",
        )
        .fetch_one(&pool)
        .await?;
        pool.close().await;
        assert_eq!(
            stored,
            Some(section_checksum(edited, "one")),
            "restamp stored the edited section's checksum"
        );

        // Apply now proceeds cleanly (no-op — sections validate against the file).
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();

        Ok(())
    })
    .await
}

/// A consciously reordered COMPLETED section otherwise stays blocked forever:
/// apply bails on the reorder and (unlike a body edit) restamp must sync the
/// stored `section_order`, not just the checksum. Reorder two completed
/// sections, confirm apply bails, restamp (which reports the order change), then
/// apply cleanly.
#[tokio::test]
async fn test_reorder_restamp_flow() -> Result<()> {
    with_cli_helper(async |helper| {
        const FILENAME: &str = "1000000400_resolve_reorder.sql";
        const VERSION: i64 = 1000000400;
        let original = r#"-- pgmt:section name="one"
CREATE TABLE rso_one (id INT);

-- pgmt:section name="two"
CREATE TABLE rso_two (id INT);
"#;
        helper.init_project()?;
        helper.write_migration_file(FILENAME, original)?;

        // Apply cleanly: one at order 0, two at order 1.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();

        // Swap the two applied sections' order in the file (bodies unchanged).
        let reordered = r#"-- pgmt:section name="two"
CREATE TABLE rso_two (id INT);

-- pgmt:section name="one"
CREATE TABLE rso_one (id INT);
"#;
        helper.write_migration_file(FILENAME, reordered)?;

        // Apply bails on the reorder, pointing at the restamp recovery.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .failure()
            .stderr(predicate::str::contains("was reordered after it was applied"))
            .stderr(predicate::str::contains("resolve --restamp"));

        // Restamp syncs section_order and reports the change.
        helper
            .command()
            .args([
                "migrate",
                "resolve",
                "--restamp",
                &VERSION.to_string(),
                "--target-url",
                &helper.dev_database_url,
            ])
            .assert()
            .success()
            .stdout(predicate::str::contains("section_order:"));

        // The stored orders now match the reordered file (two=0, one=1).
        let pool = helper.connect_to_dev_db().await?;
        let order_one: i32 = sqlx::query_scalar(
            "SELECT section_order FROM public.pgmt_migrations_sections WHERE section_name = 'one'",
        )
        .fetch_one(&pool)
        .await?;
        let order_two: i32 = sqlx::query_scalar(
            "SELECT section_order FROM public.pgmt_migrations_sections WHERE section_name = 'two'",
        )
        .fetch_one(&pool)
        .await?;
        pool.close().await;
        assert_eq!(order_two, 0, "two moved to the front");
        assert_eq!(order_one, 1, "one moved to the back");

        // Apply now proceeds cleanly — the reorder is accepted.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();

        Ok(())
    })
    .await
}

/// Every refusal path: already-completed mark, nonexistent coordinate, missing
/// section part, and two verbs at once (clap-level).
#[tokio::test]
async fn test_resolve_refusals() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        // A clean, fully-applied two-section migration for the "completed" cases.
        helper.write_migration_file(
            "1000000400_done.sql",
            r#"-- pgmt:section name="alpha"
CREATE TABLE ref_alpha (id INT);

-- pgmt:section name="beta"
CREATE TABLE ref_beta (id INT);
"#,
        )?;
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();

        // mark-completed on an already-completed section is refused.
        helper
            .command()
            .args([
                "migrate",
                "resolve",
                "--mark-completed",
                "1000000400/alpha",
                "--target-url",
                &helper.dev_database_url,
            ])
            .assert()
            .failure()
            .stderr(predicate::str::contains("already completed"));

        // Nonexistent version/section is refused (no row → no creation).
        helper
            .command()
            .args([
                "migrate",
                "resolve",
                "--mark-completed",
                "9999999999/ghost",
                "--target-url",
                &helper.dev_database_url,
            ])
            .assert()
            .failure()
            .stderr(predicate::str::contains("no section row exists"));

        // Missing section part on mark-completed is refused.
        helper
            .command()
            .args([
                "migrate",
                "resolve",
                "--mark-completed",
                "1000000400",
                "--target-url",
                &helper.dev_database_url,
            ])
            .assert()
            .failure()
            .stderr(predicate::str::contains("section name is required"));

        // Two verbs at once is refused at the clap level.
        helper
            .command()
            .args([
                "migrate",
                "resolve",
                "--mark-completed",
                "1000000400/alpha",
                "--restamp",
                "1000000400/beta",
                "--target-url",
                &helper.dev_database_url,
            ])
            .assert()
            .failure()
            .stderr(predicate::str::contains("cannot be used with"));

        // No verb at all is refused at the clap level.
        helper
            .command()
            .args([
                "migrate",
                "resolve",
                "--target-url",
                &helper.dev_database_url,
            ])
            .assert()
            .failure()
            .stderr(
                predicate::str::contains("required")
                    .or(predicate::str::contains("the following required")),
            );

        // A `satisfied` section is covered exactly like `completed`:
        // mark-completed refuses to overwrite it, and restamp accepts it.
        let pool = helper.connect_to_dev_db().await?;
        sqlx::query(
            "UPDATE public.pgmt_migrations_sections SET status = 'satisfied' \
             WHERE migration_version = 1000000400 AND section_name = 'beta'",
        )
        .execute(&pool)
        .await?;
        pool.close().await;

        // mark-completed on a satisfied section is refused (already covered).
        helper
            .command()
            .args([
                "migrate",
                "resolve",
                "--mark-completed",
                "1000000400/beta",
                "--target-url",
                &helper.dev_database_url,
            ])
            .assert()
            .failure()
            .stderr(predicate::str::contains("already satisfied"));

        // restamp treats the satisfied section as covered and re-stamps it.
        helper
            .command()
            .args([
                "migrate",
                "resolve",
                "--restamp",
                "1000000400/beta",
                "--target-url",
                &helper.dev_database_url,
            ])
            .assert()
            .success();

        Ok(())
    })
    .await
}
