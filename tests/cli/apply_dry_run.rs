//! `migrate apply --dry-run`: the preview reports the pending migrations, the
//! sections that would run and the module sections it would skip, while
//! leaving the target untouched — no DDL, no tracking rows.

use crate::helpers::cli::{
    THREE_MODULES_YAML, enable_modules, with_cli_helper, write_three_module_schema,
};
use anyhow::Result;
use predicates::prelude::*;

/// A pending migration is listed with its sections, and nothing about the
/// target changes: its DDL never runs and no tracking row is written. A real
/// apply afterwards still does the work.
#[tokio::test]
async fn test_apply_dry_run_previews_without_touching_target() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        helper.write_migration_file(
            "1000_initial.sql",
            r#"-- pgmt:section name="tables" mode="transactional"
CREATE TABLE users (id SERIAL PRIMARY KEY);

-- pgmt:section name="seed" mode="transactional"
INSERT INTO users DEFAULT VALUES;
"#,
        )?;

        helper
            .command()
            .args([
                "migrate",
                "apply",
                "--target-url",
                &helper.dev_database_url,
                "--dry-run",
            ])
            .assert()
            .success()
            .stdout(predicate::str::contains("dry run"))
            .stdout(predicate::str::contains("Would apply migration 1000"))
            .stdout(predicate::str::contains("Would run section 'tables'"))
            .stdout(predicate::str::contains("Would run section 'seed'"));

        // The target is untouched: no object, and no tracking row for 1000.
        assert!(
            !helper.table_exists_in_dev("public", "users").await?,
            "dry run must not execute migration DDL"
        );
        let pool = helper.connect_to_dev_db().await?;
        let recorded: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM public.pgmt_migrations WHERE version = 1000")
                .fetch_one(&pool)
                .await?;
        assert_eq!(recorded, 0, "dry run must record nothing");
        let sections: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM public.pgmt_migrations_sections")
                .fetch_one(&pool)
                .await?;
        assert_eq!(sections, 0, "dry run must record no section rows");
        pool.close().await;

        // The real apply still has the whole migration to do.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success()
            .stdout(predicate::str::contains("Applying migration 1000"));
        assert!(helper.table_exists_in_dev("public", "users").await?);

        // Up to date: the preview says so and still changes nothing.
        helper
            .command()
            .args([
                "migrate",
                "apply",
                "--target-url",
                &helper.dev_database_url,
                "--dry-run",
            ])
            .assert()
            .success()
            .stdout(predicate::str::contains("Nothing to apply"));

        Ok(())
    })
    .await
}

/// On a module project, a bare dry run previews the base and names the module
/// sections it would skip — the same verdict the real bare apply reaches.
#[tokio::test]
async fn test_apply_dry_run_reports_skipped_module_sections() -> Result<()> {
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
            .args([
                "migrate",
                "apply",
                "--target-url",
                &helper.dev_database_url,
                "--dry-run",
            ])
            .assert()
            .success()
            .stdout(predicate::str::contains("Would apply migration"))
            .stdout(predicate::str::contains(
                "Would skip module 'core' sections (not established here)",
            ))
            .stdout(predicate::str::contains(
                "Would skip module 'analytics' sections (not established here)",
            ));

        assert!(
            !helper.table_exists_in_dev("public", "meta").await?,
            "dry run must not deploy the base either"
        );
        assert!(!helper.table_exists_in_dev("public", "users").await?);

        Ok(())
    })
    .await
}
