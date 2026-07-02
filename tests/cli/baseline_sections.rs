//! Multi-section baselines: baselines use the same `-- pgmt:section` header
//! syntax as migrations and execute section-by-section with real execution
//! modes. A header-less baseline is a single transactional "default" section,
//! preserving the historical all-or-nothing apply.

use crate::helpers::cli::with_cli_helper;
use anyhow::Result;
use predicates::prelude::*;

/// A hand-written baseline with a transactional section and a
/// non-transactional `CREATE INDEX CONCURRENTLY` section provisions correctly:
/// previously the whole baseline ran inside one implicit transaction, so
/// CONCURRENTLY could not appear in a baseline at all.
#[tokio::test]
async fn test_provision_multi_section_baseline() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        helper.write_migration_file(
            "1000_initial.sql",
            "CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT NOT NULL);",
        )?;
        std::fs::write(
            helper.baselines_dir().join("baseline_1000.sql"),
            r#"-- pgmt:section name="tables" mode="transactional"
CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT NOT NULL);

-- pgmt:section name="indexes" mode="non-transactional"
CREATE UNIQUE INDEX CONCURRENTLY users_email_idx ON users (email);
"#,
        )?;

        helper
            .command()
            .args([
                "migrate",
                "provision",
                "--target-url",
                &helper.dev_database_url,
            ])
            .assert()
            .success()
            .stdout(predicate::str::contains("Provisioned from baseline"));

        // Both sections applied: table and the concurrently-built index exist.
        assert!(helper.table_exists_in_dev("public", "users").await?);
        let pool = helper.connect_to_dev_db().await?;
        let index_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_indexes
             WHERE schemaname = 'public' AND indexname = 'users_email_idx')",
        )
        .fetch_one(&pool)
        .await?;
        assert!(index_exists, "CONCURRENTLY index section should apply");

        // Both sections recorded as completed baseline sections.
        let sections: Vec<(String, String, bool)> = sqlx::query_as(
            "SELECT section_name, status, is_baseline
             FROM public.pgmt_migrations_sections
             ORDER BY section_order",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(
            sections,
            vec![
                ("tables".to_string(), "completed".to_string(), true),
                ("indexes".to_string(), "completed".to_string(), true),
            ],
            "baseline sections should be tracked under is_baseline = TRUE"
        );
        pool.close().await;

        Ok(())
    })
    .await
}

/// A header-less (generated) baseline records exactly one "default" baseline
/// section — the same shape legacy migrations get — and stays atomic.
#[tokio::test]
async fn test_provision_headerless_baseline_records_default_section() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        helper.write_schema_file("tables/users.sql", "CREATE TABLE users (id SERIAL);")?;

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
            ])
            .assert()
            .success();

        let pool = helper.connect_to_dev_db().await?;
        let sections: Vec<(String, String, bool)> = sqlx::query_as(
            "SELECT section_name, status, is_baseline FROM public.pgmt_migrations_sections",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(
            sections,
            vec![("default".to_string(), "completed".to_string(), true)],
            "header-less baseline should record one completed default section"
        );
        pool.close().await;

        Ok(())
    })
    .await
}

/// A failed multi-section baseline resumes: completed sections are skipped on
/// the next provision run, only the failed section re-executes.
#[tokio::test]
async fn test_provision_resumes_failed_multi_section_baseline() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        helper.write_migration_file("1000_initial.sql", "CREATE TABLE users (id SERIAL);")?;
        // Section "seed" fails on the first run: it reads from a table the
        // baseline does not create.
        std::fs::write(
            helper.baselines_dir().join("baseline_1000.sql"),
            r#"-- pgmt:section name="tables" mode="transactional"
CREATE TABLE users (id SERIAL);

-- pgmt:section name="seed" mode="transactional"
INSERT INTO users SELECT id FROM external_source;
"#,
        )?;

        helper
            .command()
            .args([
                "migrate",
                "provision",
                "--target-url",
                &helper.dev_database_url,
            ])
            .assert()
            .failure();

        // First section completed and its objects exist; second failed.
        let pool = helper.connect_to_dev_db().await?;
        let status: Vec<(String, String)> = sqlx::query_as(
            "SELECT section_name, status FROM public.pgmt_migrations_sections
             ORDER BY section_order",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(
            status,
            vec![
                ("tables".to_string(), "completed".to_string()),
                ("seed".to_string(), "failed".to_string()),
            ]
        );

        // Unblock the failing section, then re-provision: the completed
        // section must be skipped (re-running its CREATE TABLE would fail).
        sqlx::query("CREATE TABLE external_source (id INT)")
            .execute(&pool)
            .await?;
        pool.close().await;

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

        let pool = helper.connect_to_dev_db().await?;
        let statuses: Vec<String> = sqlx::query_scalar(
            "SELECT status FROM public.pgmt_migrations_sections ORDER BY section_order",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(statuses, vec!["completed", "completed"]);
        pool.close().await;

        Ok(())
    })
    .await
}

/// Version-pairing pin: a baseline covers the migration at its own version, so
/// regenerating from an unchanged schema right after `--create-baseline` must
/// detect no changes. (If replay used a `>=` boundary, the paired migration
/// would replay on top of the baseline and fail with "already exists".)
#[tokio::test]
async fn test_migrate_new_after_paired_baseline_detects_no_changes() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        helper.write_schema_file("tables/users.sql", "CREATE TABLE users (id SERIAL);")?;

        helper
            .command()
            .args(["migrate", "new", "initial", "--create-baseline"])
            .assert()
            .success();

        helper
            .command()
            .args(["migrate", "new", "noop"])
            .assert()
            .success()
            .stdout(predicate::str::contains("No changes detected"));

        assert_eq!(helper.list_migration_files()?.len(), 1);
        assert_eq!(helper.list_baseline_files()?.len(), 1);

        Ok(())
    })
    .await
}
