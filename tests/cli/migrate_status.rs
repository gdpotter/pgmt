//! `migrate status` against a target database and its per-module rollup.
//!
//! Status is the production triage tool: it can now report on a deployment
//! target (`--target-url` / `PGMT_TARGET_URL` / yaml target) and falls back to
//! dev when none is set. It is strictly read-only — it never creates or evolves
//! the tracking tables on the reported database.

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

/// `migrate status --target-url <db>` reports on that database; without the
/// flag it falls back to dev. Applying a migration to the target only (dev
/// stays empty) proves the routing: the two invocations report different state.
#[tokio::test]
async fn test_status_against_target_url() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        helper.write_migration_file("1000_initial.sql", "CREATE TABLE widgets (id SERIAL);")?;

        let target_url = helper.create_extra_database().await?;

        // Apply the migration to the TARGET database (not dev).
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &target_url])
            .assert()
            .success();

        // Status against the target reports the applied migration.
        helper
            .command()
            .args(["migrate", "status", "--target-url", &target_url])
            .assert()
            .success()
            .stdout(predicate::str::contains(
                "Migration status for target database",
            ))
            .stdout(predicate::str::contains("Applied migrations:"))
            .stdout(predicate::str::contains("1000"));

        // Status with no flag falls back to dev, which was never touched.
        // Clear any ambient PGMT_TARGET_URL so the fallback is exercised.
        helper
            .command()
            .env_remove("PGMT_TARGET_URL")
            .args(["migrate", "status"])
            .assert()
            .success()
            .stdout(predicate::str::contains(
                "Migration status for dev database",
            ))
            .stdout(predicate::str::contains("No migrations have been applied"));

        Ok(())
    })
    .await
}

/// The per-module rollup: an established module shows its applied count, an
/// unestablished declared module is marked expected-on-subset-targets, and a
/// module with a failed section points at the correct resume command.
#[tokio::test]
async fn test_status_module_rollup() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        enable_modules(helper, THREE_MODULES_YAML)?;
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

        helper
            .command()
            .args(["migrate", "new", "initial", "--create-baseline"])
            .assert()
            .success();

        let target_url = helper.create_extra_database().await?;

        // Provision base + billing (pulls in core). analytics is left out.
        helper
            .command()
            .args([
                "migrate",
                "provision",
                "--target-url",
                &target_url,
                "--modules",
                "billing",
            ])
            .assert()
            .success();

        // Seed a failed section: flip billing's baseline section to 'failed',
        // simulating a crashed provision of that module.
        let pool = sqlx::PgPool::connect(&target_url).await?;
        let flipped = sqlx::query(
            "UPDATE public.pgmt_migrations_sections SET status = 'failed' \
             WHERE section_name = 'billing' AND is_baseline",
        )
        .execute(&pool)
        .await?;
        assert_eq!(
            flipped.rows_affected(),
            1,
            "expected exactly one billing baseline section row to flip"
        );
        pool.close().await;

        // Rollup lines: core established with a count, billing pointing at the
        // baseline-content resume command, analytics not established (expected).
        helper
            .command()
            .args(["migrate", "status", "--target-url", &target_url])
            .assert()
            .success()
            .stdout(predicate::str::contains("Modules:"))
            .stdout(predicate::str::contains(
                "established — 1 section(s) applied",
            ))
            .stdout(predicate::str::contains(
                "resume with `pgmt migrate provision --modules billing`",
            ))
            .stdout(predicate::str::contains("analytics"))
            .stdout(predicate::str::contains(
                "not established (expected on subset targets)",
            ));

        Ok(())
    })
    .await
}

/// Status is read-only: run against a fresh empty database and it neither
/// errors nor creates any pgmt tracking tables (no advisory lock, no evolve).
#[tokio::test]
async fn test_status_does_not_mutate() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        let empty_url = helper.create_extra_database().await?;

        helper
            .command()
            .args(["migrate", "status", "--target-url", &empty_url])
            .assert()
            .success()
            .stdout(predicate::str::contains("No migrations have been applied"));

        // No pgmt tracking tables were created by the read-only status run.
        let pool = sqlx::PgPool::connect(&empty_url).await?;
        let main_exists: bool =
            sqlx::query_scalar("SELECT to_regclass('public.pgmt_migrations') IS NOT NULL")
                .fetch_one(&pool)
                .await?;
        let sections_exists: bool =
            sqlx::query_scalar("SELECT to_regclass('public.pgmt_migrations_sections') IS NOT NULL")
                .fetch_one(&pool)
                .await?;
        pool.close().await;
        assert!(!main_exists, "status must not create the tracking table");
        assert!(
            !sections_exists,
            "status must not create the section tracking table"
        );

        Ok(())
    })
    .await
}
