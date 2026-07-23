//! `migrate resolve` — explicit break-glass repair of section tracking state.
//!
//! resolve is the escape hatch fix-in-repo can't reach: a manual hot-fix that
//! should be recorded (`--mark-completed`), or a consciously edited applied
//! section to re-stamp (`--restamp`). Each verb touches exactly one coordinate
//! and prints the before/after state. (A failed/stale section needs no verb —
//! the next apply re-runs it.)

use crate::helpers::cli::{
    THREE_MODULES_YAML, enable_modules, section_checksum, with_cli_helper,
    write_three_module_schema,
};
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
            .stderr(predicate::str::contains(
                "was reordered after it was applied",
            ))
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

/// Append a harmless comment line into the named module's baseline section,
/// changing that section's checksum without changing what its DDL does.
fn edit_baseline_section(baseline: &str, module: &str) -> String {
    let needle = format!("module=\"{module}\"");
    let mut out = Vec::new();
    let mut edited = false;
    for line in baseline.lines() {
        out.push(line.to_string());
        if !edited && line.contains("pgmt:section") && line.contains(&needle) {
            out.push("-- restamp: conscious edit to an applied section".to_string());
            edited = true;
        }
    }
    assert!(
        edited,
        "section for module '{module}' not found in baseline"
    );
    out.join("\n")
}

/// The `--baseline` flavour of `--restamp` routes every query to the
/// is_baseline=TRUE row space and file discovery to the baselines dir. Provision
/// base + core from a baseline (core's section is applied), consciously edit
/// that applied `core` baseline section in the FILE so its stored checksum
/// drifts, then adopt billing — which re-enters the baseline apply and bails on
/// the drifted `core` section. `resolve --restamp <version> --baseline`
/// re-stamps the stored baseline checksums from the file, and the adoption then
/// runs clean.
#[tokio::test]
async fn test_restamp_baseline_flow() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        enable_modules(helper, THREE_MODULES_YAML)?;
        write_three_module_schema(helper)?;

        helper
            .command()
            .args(["migrate", "new", "initial", "--create-baseline"])
            .assert()
            .success();

        // Establish base + core from the baseline; billing not yet adopted.
        helper
            .command()
            .args([
                "migrate",
                "provision",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "core",
            ])
            .assert()
            .success();

        // Consciously edit the applied `core` baseline section in the file.
        let baseline_name = helper.list_baseline_files()?[0].clone();
        let baseline_path = helper.baselines_dir().join(&baseline_name);
        let original = std::fs::read_to_string(&baseline_path)?;
        let edited = edit_baseline_section(&original, "core");
        assert_ne!(edited, original, "the edit must change the file");
        std::fs::write(&baseline_path, &edited)?;

        let version = baseline_version_in_dev(helper).await?;

        // Adopting billing re-enters the baseline apply, which validates every
        // recorded baseline section against the file — the edited `core` section
        // now bails as an immutable applied section that changed.
        helper
            .command()
            .args([
                "migrate",
                "provision",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "billing",
            ])
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "was modified after it was applied",
            ))
            // The recovery hint must carry --baseline: the drifted row is a
            // baseline section, and the bare coordinate would find no row.
            .stderr(predicate::str::contains("--baseline"));

        // Re-stamp the baseline row space: core's stored checksum syncs to the
        // file. `--baseline` reports "core:" from the baselines dir, not a
        // migration.
        helper
            .command()
            .args([
                "migrate",
                "resolve",
                "--restamp",
                &version.to_string(),
                "--baseline",
                "--target-url",
                &helper.dev_database_url,
            ])
            .assert()
            .success()
            .stdout(predicate::str::contains("Re-stamping"))
            .stdout(predicate::str::contains("core:"));

        // The stored baseline checksum for `core` now reflects the edited file.
        let pool = helper.connect_to_dev_db().await?;
        let stored: Option<String> = sqlx::query_scalar(
            "SELECT checksum FROM public.pgmt_migrations_sections \
             WHERE section_name = 'core' AND is_baseline = TRUE",
        )
        .fetch_one(&pool)
        .await?;
        pool.close().await;
        assert_eq!(
            stored,
            Some(section_checksum(&edited, "core")),
            "restamp stored the edited baseline section's checksum"
        );

        // Adoption now proceeds cleanly and billing's objects land.
        helper
            .command()
            .args([
                "migrate",
                "provision",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "billing",
            ])
            .assert()
            .success();
        assert!(
            helper.table_exists_in_dev("public", "invoices").await?,
            "billing adopted after the baseline restamp"
        );

        Ok(())
    })
    .await
}

/// The `--baseline` flavour of `--mark-completed` records an is_baseline=TRUE
/// section as done. Provision base + core + billing (billing's DDL really
/// lands), seed billing's baseline section as `failed` (a crashed provision
/// whose objects nonetheless committed), confirm the incomplete-baseline guard
/// then refuses an `apply --modules billing`, mark the baseline section
/// completed, and confirm the guard no longer fires.
#[tokio::test]
async fn test_mark_completed_baseline_flow() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        enable_modules(helper, THREE_MODULES_YAML)?;
        write_three_module_schema(helper)?;

        helper
            .command()
            .args(["migrate", "new", "initial", "--create-baseline"])
            .assert()
            .success();

        // Establish base + core + billing; billing's objects really land here.
        helper
            .command()
            .args([
                "migrate",
                "provision",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "core,billing",
            ])
            .assert()
            .success();

        // Seed a crash: billing's baseline section is left `failed` even though
        // its objects are present. The incomplete-baseline guard now refuses an
        // `apply --modules billing`.
        seed_baseline_section_status(helper, "billing", "failed").await?;
        let version = baseline_version_in_dev(helper).await?;

        helper
            .command()
            .args([
                "migrate",
                "apply",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "billing",
            ])
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "have partially applied baseline content",
            ));

        // Record the baseline section completed (its DDL is already present).
        helper
            .command()
            .args([
                "migrate",
                "resolve",
                "--mark-completed",
                &format!("{version}/billing"),
                "--baseline",
                "--target-url",
                &helper.dev_database_url,
            ])
            .assert()
            .success()
            .stdout(predicate::str::contains("failed -> completed"));

        // The baseline billing row is now completed.
        let pool = helper.connect_to_dev_db().await?;
        let status: String = sqlx::query_scalar(
            "SELECT status FROM public.pgmt_migrations_sections \
             WHERE section_name = 'billing' AND is_baseline = TRUE",
        )
        .fetch_one(&pool)
        .await?;
        pool.close().await;
        assert_eq!(
            status, "completed",
            "resolve marked the baseline section done"
        );

        // The guard no longer fires: `apply --modules billing` runs clean.
        helper
            .command()
            .args([
                "migrate",
                "apply",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "billing",
            ])
            .assert()
            .success();

        Ok(())
    })
    .await
}

/// `--mark-completed --baseline` must flip the lookup to the is_baseline=TRUE
/// row space. Pointed at a coordinate that exists ONLY as a migration row
/// (is_baseline=FALSE), it finds no baseline row and refuses with the no-row
/// message naming the baseline kind — proving the flag routes the query rather
/// than matching the migration row.
#[tokio::test]
async fn test_mark_completed_baseline_refuses_migration_coordinate() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        helper.write_migration_file(
            "1000000800_solo.sql",
            "-- pgmt:section name=\"solo\"\nCREATE TABLE mcb_solo (id INT);\n",
        )?;

        // Apply records a MIGRATION row (is_baseline=FALSE) for 1000000800/solo.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();

        // With --baseline the same coordinate resolves to the is_baseline=TRUE
        // row space, where no row exists → refuse, naming the baseline kind.
        helper
            .command()
            .args([
                "migrate",
                "resolve",
                "--mark-completed",
                "1000000800/solo",
                "--baseline",
                "--target-url",
                &helper.dev_database_url,
            ])
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "no section row exists for baseline 1000000800/solo",
            ));

        Ok(())
    })
    .await
}

/// The baseline version recorded in the tracking table.
async fn baseline_version_in_dev(helper: &crate::helpers::cli::CliTestHelper) -> Result<i64> {
    let pool = helper.connect_to_dev_db().await?;
    let version: i64 =
        sqlx::query_scalar("SELECT version FROM public.pgmt_migrations WHERE is_baseline")
            .fetch_one(&pool)
            .await?;
    pool.close().await;
    Ok(version)
}

/// Flip a single baseline section row to a given status (seed a crashed
/// provision without producing a real crash window).
async fn seed_baseline_section_status(
    helper: &crate::helpers::cli::CliTestHelper,
    section_name: &str,
    status: &str,
) -> Result<()> {
    let pool = helper.connect_to_dev_db().await?;
    let affected = sqlx::query(
        "UPDATE public.pgmt_migrations_sections
         SET status = $1,
             completed_at = CASE WHEN $1 = 'completed' THEN completed_at ELSE NULL END
         WHERE is_baseline AND section_name = $2",
    )
    .bind(status)
    .bind(section_name)
    .execute(&pool)
    .await?
    .rows_affected();
    pool.close().await;
    assert_eq!(
        affected, 1,
        "expected exactly one baseline section '{section_name}' to seed"
    );
    Ok(())
}
