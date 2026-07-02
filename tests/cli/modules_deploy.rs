//! Subset deploy (Phase 3a): `--modules` on apply/provision, dependency
//! closure, adoption (replay vs baseline-content), zero-trace skipping, and
//! the conservative intra-migration coupling guard.

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

/// Subset provision deploys only the named modules (+ deps + base); the
/// unselected module's objects don't exist and its sections leave NO rows —
/// nothing in the target names a module it didn't ask for (§9).
/// Later, `provision --modules analytics` adopts it from the same baseline.
#[tokio::test]
async fn test_subset_provision_and_late_adoption() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        enable_modules(helper, THREE_MODULES_YAML)?;
        write_three_module_schema(helper)?;

        helper
            .command()
            .args(["migrate", "new", "initial", "--create-baseline"])
            .assert()
            .success();

        // Provision only billing: core is pulled in as its dependency.
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
            .success()
            .stdout(predicate::str::contains(
                "Including module 'core' (required by 'billing')",
            ));

        assert!(helper.table_exists_in_dev("public", "users").await?);
        assert!(helper.table_exists_in_dev("public", "invoices").await?);
        assert!(
            !helper.table_exists_in_dev("public", "events").await?,
            "unselected analytics must not deploy"
        );

        // Zero trace: no section row names analytics.
        let pool = helper.connect_to_dev_db().await?;
        let analytics_rows: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM public.pgmt_migrations_sections WHERE section_name LIKE 'analytics%'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(analytics_rows, 0, "no trace of unrequested modules");
        pool.close().await;

        // Adopt analytics later from the committed baseline.
        helper
            .command()
            .args([
                "migrate",
                "provision",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "analytics",
            ])
            .assert()
            .success()
            .stdout(predicate::str::contains("Adopting module(s) analytics"));

        assert!(helper.table_exists_in_dev("public", "events").await?);

        // Idempotent: adopting again is a no-op apply.
        helper
            .command()
            .args([
                "migrate",
                "provision",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "analytics",
            ])
            .assert()
            .success();

        Ok(())
    })
    .await
}

/// `apply --modules X` adopts a module whose whole history is in the
/// migrations (younger than the baseline) — pure replay. A module whose
/// pre-baseline state lives in the baseline refuses with provision guidance.
#[tokio::test]
async fn test_apply_adoption_replay_vs_baseline_content() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        enable_modules(helper, THREE_MODULES_YAML)?;
        write_three_module_schema(helper)?;

        helper
            .command()
            .args(["migrate", "new", "initial", "--create-baseline"])
            .assert()
            .success();

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

        // analytics is in the baseline: apply cannot adopt it.
        helper
            .command()
            .args([
                "migrate",
                "apply",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "analytics",
            ])
            .assert()
            .failure()
            .stderr(predicate::str::contains("requires baseline content"))
            .stderr(predicate::str::contains("provision --modules analytics"));

        // A brand-new module after the baseline: its whole history is in the
        // migrations, so plain apply adopts it.
        let config_path = helper.project_root.join("pgmt.yaml");
        let mut config = std::fs::read_to_string(&config_path)?;
        config.push_str("  audit:\n    paths: [\"schema/audit/**\"]\n");
        std::fs::write(config_path, config)?;
        helper.write_schema_file(
            "audit/log.sql",
            "CREATE TABLE audit_log (id SERIAL PRIMARY KEY);",
        )?;
        helper
            .command()
            .args(["migrate", "new", "add_audit"])
            .assert()
            .success();

        helper
            .command()
            .args([
                "migrate",
                "apply",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "audit",
            ])
            .assert()
            .success();
        assert!(helper.table_exists_in_dev("public", "audit_log").await?);

        Ok(())
    })
    .await
}

/// Bare `apply` on a module project runs only the base: module sections are
/// skipped (info for never-established modules) and leave no rows.
#[tokio::test]
async fn test_bare_apply_runs_base_only() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        enable_modules(helper, THREE_MODULES_YAML)?;
        write_three_module_schema(helper)?;
        // An unmoduled file → the base, which always deploys.
        helper.write_schema_file("meta.sql", "CREATE TABLE meta (k TEXT PRIMARY KEY);")?;

        helper
            .command()
            .args(["migrate", "new", "initial"])
            .assert()
            .success();

        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success()
            .stdout(predicate::str::contains("Skipping module"));

        assert!(helper.table_exists_in_dev("public", "meta").await?);
        assert!(
            !helper.table_exists_in_dev("public", "users").await?,
            "modules never deploy implicitly"
        );

        Ok(())
    })
    .await
}

/// The conservative coupling guard: a migration whose selected section is
/// preceded by a pending section of an unselected ESTABLISHED module refuses
/// to run partially (order encodes potential dependency).
#[tokio::test]
async fn test_coupled_migration_refuses_partial_deploy() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        enable_modules(helper, THREE_MODULES_YAML)?;

        helper.write_schema_file(
            "core/accounts.sql",
            "CREATE TABLE accounts (id SERIAL PRIMARY KEY);",
        )?;
        helper.write_schema_file(
            "billing/invoices.sql",
            "-- require: core/accounts.sql\n\
             CREATE TABLE invoices (id SERIAL PRIMARY KEY, account_id INT REFERENCES accounts(id));",
        )?;
        helper
            .command()
            .args(["migrate", "new", "initial", "--create-baseline"])
            .assert()
            .success();
        helper
            .command()
            .args([
                "migrate",
                "provision",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "all",
            ])
            .assert()
            .success();

        // Coupled change: billing's FK drop must precede core's table drop.
        std::fs::remove_file(helper.project_root.join("schema/core/accounts.sql"))?;
        helper.write_schema_file(
            "billing/invoices.sql",
            "CREATE TABLE invoices (id SERIAL PRIMARY KEY, account_id INT);",
        )?;
        helper
            .command()
            .args(["migrate", "new", "drop_accounts", "--create-baseline"])
            .assert()
            .success();

        // Deploying core alone would drop the table out from under billing's
        // pending FK-drop: refused.
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
            .stderr(predicate::str::contains("couples module 'billing'"))
            .stderr(predicate::str::contains("deploy them together"));

        // Together it lands.
        helper
            .command()
            .args([
                "migrate",
                "apply",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "core,billing",
            ])
            .assert()
            .success();
        assert!(!helper.table_exists_in_dev("public", "accounts").await?);

        Ok(())
    })
    .await
}

/// --modules validation: unknown names and non-module projects error clearly.
#[tokio::test]
async fn test_modules_flag_validation() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        // Non-module project: --modules is an error.
        helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL);")?;
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
            .stderr(predicate::str::contains("declares no `modules:`"));

        // Module project: unknown module named.
        enable_modules(helper, THREE_MODULES_YAML)?;
        std::fs::create_dir_all(helper.project_root.join("schema/core"))?;
        helper
            .command()
            .args([
                "migrate",
                "apply",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "nonexistent",
            ])
            .assert()
            .failure()
            .stderr(predicate::str::contains("unknown module 'nonexistent'"));

        Ok(())
    })
    .await
}
