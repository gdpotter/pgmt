//! The incomplete-baseline guard in `migrate apply`, scoped by module.
//!
//! A crashed `provision` leaves non-completed baseline SECTION rows. The guard
//! refuses to apply onto that half-built state — but the refusal is scoped to
//! what actually failed to land:
//!
//! - An incomplete BASE section (module IS NULL) means the baseline's base
//!   content is half-built → the version is untrustworthy as a base watermark →
//!   refuse EVERY apply (original, version-scoped message).
//! - Incomplete MODULE sections mean only those modules are half-built → the
//!   base watermark is honest (base sections completed, or the base was caught
//!   up via migrations before adoption) → refuse only when the selection names
//!   an affected module; otherwise proceed with a warning.
//!
//! Where a real crashed provision is awkward, we SEED the post-crash state (a
//! failed baseline section) directly, per the pattern in `apply_crash_states`.

use crate::helpers::cli::with_cli_helper;
use anyhow::Result;
use predicates::prelude::*;

const THREE_MODULES_YAML: &str = r#"
modules:
  core:
    paths: ["schema/core/**"]
  billing:
    paths: ["schema/billing/**"]
    depends_on: [core]
  analytics:
    paths: ["schema/analytics/**"]
    depends_on: [core]
"#;

fn enable_modules(helper: &crate::helpers::cli::CliTestHelper, yaml: &str) -> Result<()> {
    let config_path = helper.project_root.join("pgmt.yaml");
    let mut config = std::fs::read_to_string(&config_path)?;
    config.push_str(yaml);
    std::fs::write(config_path, config)?;
    Ok(())
}

fn write_three_module_schema(helper: &crate::helpers::cli::CliTestHelper) -> Result<()> {
    helper.write_schema_file(
        "core/users.sql",
        "CREATE TABLE users (id SERIAL PRIMARY KEY);",
    )?;
    helper.write_schema_file(
        "billing/invoices.sql",
        "-- require: core/users.sql\n\
         CREATE TABLE invoices (id SERIAL PRIMARY KEY, user_id INT REFERENCES users(id));",
    )?;
    helper.write_schema_file(
        "analytics/events.sql",
        "-- require: core/users.sql\n\
         CREATE TABLE events (id SERIAL PRIMARY KEY, user_id INT REFERENCES users(id));",
    )?;
    Ok(())
}

/// The baseline version recorded in the tracking table.
async fn baseline_version(helper: &crate::helpers::cli::CliTestHelper) -> Result<i64> {
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

/// HEADLINE: a failed `provision --modules billing` (billing's baseline section
/// has bad SQL) leaves billing's baseline content half-built. That must NOT
/// brick the whole pipeline: a bare `migrate apply` still succeeds (with a
/// warning), while `apply --modules billing` refuses. After fixing the section
/// and completing the provision, `apply --modules billing` succeeds.
#[tokio::test]
async fn test_failed_module_adoption_does_not_block_base_apply() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        enable_modules(helper, THREE_MODULES_YAML)?;
        write_three_module_schema(helper)?;

        helper
            .command()
            .args(["migrate", "new", "initial", "--create-baseline"])
            .assert()
            .success();

        // Establish base + core from the baseline (billing not yet adopted).
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

        // Break billing's baseline section, then adopt it: core is already
        // established, so only billing's (now-broken) baseline section runs and
        // fails. The base and core sections are untouched.
        let baseline_name = helper.list_baseline_files()?[0].clone();
        let baseline_path = helper.baselines_dir().join(&baseline_name);
        let original_baseline = std::fs::read_to_string(&baseline_path)?;
        let broken = inject_bad_sql_into_billing_section(&original_baseline);
        assert_ne!(broken, original_baseline, "the injection must change the file");
        std::fs::write(&baseline_path, &broken)?;

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
            .failure();

        // The billing baseline section is now incomplete (module = 'billing');
        // the base and core sections completed, so the base watermark is honest.
        let version = baseline_version(helper).await?;

        // A bare `migrate apply` does not touch billing: it proceeds, warning
        // that billing's adoption is unfinished and naming the exact command.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success()
            .stderr(predicate::str::contains(format!(
                "module(s) billing have partially applied baseline content (baseline {version})"
            )))
            .stderr(predicate::str::contains(
                "pgmt migrate provision --modules billing",
            ));

        // But `apply --modules billing` refuses — its migrations would build on
        // the half-applied baseline.
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
                "pgmt migrate provision --modules billing",
            ));
        assert!(
            !helper.table_exists_in_dev("public", "invoices").await?,
            "billing's failed section rolled back — invoices must not exist"
        );

        // Fix the section (per-section checksums allow editing a failed
        // section) and complete the adoption.
        std::fs::write(&baseline_path, &original_baseline)?;
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
            "billing must be adopted after the fix + re-provision"
        );

        // With billing's baseline complete, `apply --modules billing` no longer
        // trips the guard.
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

/// An incomplete BASE (module IS NULL) baseline section makes the version an
/// untrustworthy watermark — refuse BOTH a bare apply and a `--modules` apply,
/// with the original version-scoped message.
#[tokio::test]
async fn test_incomplete_base_baseline_blocks_all_applies() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        enable_modules(helper, THREE_MODULES_YAML)?;
        write_three_module_schema(helper)?;
        // An unmoduled file → a base ("default") baseline section.
        helper.write_schema_file("meta.sql", "CREATE TABLE meta (k TEXT PRIMARY KEY);")?;

        helper
            .command()
            .args(["migrate", "new", "initial", "--create-baseline"])
            .assert()
            .success();

        // Provision the base only, then seed a crash: the base "default"
        // baseline section is stuck non-completed.
        helper
            .command()
            .args([
                "migrate",
                "provision",
                "--target-url",
                &helper.dev_database_url,
            ])
            .assert()
            .success();
        seed_baseline_section_status(helper, "default", "failed").await?;
        let version = baseline_version(helper).await?;

        let expected = format!(
            "baseline {version} is only partially applied — a `migrate provision` did not finish"
        );

        // Bare apply refuses.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .failure()
            .stderr(predicate::str::contains(expected.clone()));

        // `--modules` apply refuses too — the base watermark is untrustworthy
        // regardless of which module is named.
        helper
            .command()
            .args([
                "migrate",
                "apply",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "core",
            ])
            .assert()
            .failure()
            .stderr(predicate::str::contains(expected));

        Ok(())
    })
    .await
}

/// The module-scoped refusal is an API: assert its exact wording, including the
/// affected module name and the precise `pgmt migrate provision --modules ...`
/// recovery command.
#[tokio::test]
async fn test_incomplete_baseline_error_names_module_and_command() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        enable_modules(helper, THREE_MODULES_YAML)?;
        write_three_module_schema(helper)?;

        helper
            .command()
            .args(["migrate", "new", "initial", "--create-baseline"])
            .assert()
            .success();

        // Full adoption of billing, then seed a crash by flipping its baseline
        // section to 'failed'.
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
        seed_baseline_section_status(helper, "billing", "failed").await?;
        let version = baseline_version(helper).await?;

        // The exact refusal string, treated as an API.
        let expected = format!(
            "module(s) billing have partially applied baseline content (baseline {version}) — a \
             `migrate provision --modules billing` did not finish. Complete it with `pgmt migrate \
             provision --modules billing` before applying its migrations."
        );
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
            .stderr(predicate::str::contains(expected));

        Ok(())
    })
    .await
}

/// Insert a failing statement inside billing's baseline section by placing it
/// on its own line immediately after the section's `-- pgmt:section` header, so
/// the whole (transactional) section rolls back when it executes.
fn inject_bad_sql_into_billing_section(baseline: &str) -> String {
    let mut out = Vec::new();
    let mut injected = false;
    for line in baseline.lines() {
        out.push(line.to_string());
        if !injected
            && line.contains("pgmt:section")
            && line.contains("module=\"billing\"")
        {
            out.push("SELECT this_is_not_valid_sql;".to_string());
            injected = true;
        }
    }
    assert!(injected, "billing section header not found in baseline");
    out.join("\n")
}
