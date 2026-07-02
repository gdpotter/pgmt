//! First-touch registration semantics for `migrate apply`: the version row is
//! written when a migration STARTS, applied-ness is derived from per-section
//! completion, failed migrations resume, and legacy rows (recorded on
//! completion by older pgmt, with no section rows) are never re-run.

use crate::helpers::cli::with_cli_helper;
use anyhow::Result;
use predicates::prelude::*;

/// A migration that fails partway registers its version row up front, records
/// the failure per-section, and resumes on the next apply — completed sections
/// are skipped, only the failed one re-executes.
#[tokio::test]
async fn test_failed_migration_resumes_per_section() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        // Section "seed" fails until external_source exists. Section "tables"
        // must NOT re-execute on resume (its CREATE TABLE would collide).
        helper.write_migration_file(
            "1000_split.sql",
            r#"-- pgmt:section name="tables" mode="transactional"
CREATE TABLE users (id SERIAL PRIMARY KEY);

-- pgmt:section name="seed" mode="transactional"
INSERT INTO users SELECT id FROM external_source;
"#,
        )?;

        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .failure();

        // The version row exists already (registered at start), and the
        // section rows tell the story: one completed, one failed.
        let pool = helper.connect_to_dev_db().await?;
        let versions: Vec<i64> =
            sqlx::query_scalar("SELECT version FROM public.pgmt_migrations WHERE NOT is_baseline")
                .fetch_all(&pool)
                .await?;
        assert_eq!(versions, vec![1000], "version row is registered at start");
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

        // Status must not present the half-applied migration as done.
        helper
            .command()
            .args(["migrate", "status"])
            .assert()
            .success()
            .stdout(predicate::str::contains("INCOMPLETE"));

        // Unblock and re-apply: resumes, skipping the completed section.
        sqlx::query("CREATE TABLE external_source (id INT)")
            .execute(&pool)
            .await?;
        pool.close().await;

        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success()
            .stdout(predicate::str::contains("Resuming migration 1000"));

        let pool = helper.connect_to_dev_db().await?;
        let statuses: Vec<String> = sqlx::query_scalar(
            "SELECT status FROM public.pgmt_migrations_sections ORDER BY section_order",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(statuses, vec!["completed", "completed"]);
        let row_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM public.pgmt_migrations WHERE NOT is_baseline")
                .fetch_one(&pool)
                .await?;
        assert_eq!(row_count, 1, "resume must not insert a second version row");

        // A third apply is a clean no-op (fully applied, derived).
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();
        pool.close().await;

        Ok(())
    })
    .await
}

/// A legacy tracking row — version row present, NO section rows (recorded on
/// completion by an older pgmt, or before section tracking existed) — counts
/// as fully applied and is never re-executed.
#[tokio::test]
async fn test_legacy_row_without_sections_is_not_rerun() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        let migration_sql = "CREATE TABLE legacy_marker (id SERIAL PRIMARY KEY);";
        helper.write_migration_file("1000_legacy.sql", migration_sql)?;

        // Simulate the legacy shape by hand: version row with the correct
        // checksum, zero section rows. (Older pgmt wrote exactly this.)
        let pool = helper.connect_to_dev_db().await?;
        let checksum = pgmt::migration_tracking::calculate_checksum(migration_sql);
        sqlx::query(
            "CREATE TABLE public.pgmt_migrations (
                version BIGINT NOT NULL,
                description TEXT NOT NULL,
                applied_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                checksum TEXT NOT NULL,
                applied_by TEXT DEFAULT CURRENT_USER,
                is_baseline BOOLEAN NOT NULL,
                PRIMARY KEY (version, is_baseline)
            )",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO public.pgmt_migrations (version, description, checksum, is_baseline)
             VALUES (1000, 'legacy', $1, FALSE)",
        )
        .bind(&checksum)
        .execute(&pool)
        .await?;

        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();

        // If apply had re-run the migration, the table would now exist.
        assert!(
            !helper
                .table_exists_in_dev("public", "legacy_marker")
                .await?,
            "legacy fully-applied migration must not be re-executed"
        );
        pool.close().await;

        Ok(())
    })
    .await
}
