//! Per-section checksums: the unit of resume is the unit of immutability.
//!
//! An applied section is pinned (checksum/order/mode/module), but an unapplied
//! one can be fixed in the repo and re-run. Legacy rows (NULL checksum) fall
//! back to the whole-file guard. Section rows also carry self-describing
//! attribution (module literal, mode, pgmt_version, applied_by) recorded at
//! registration.

use crate::helpers::cli::{enable_modules, section_checksum, with_cli_helper};
use anyhow::Result;
use predicates::prelude::*;

/// A single-module `modules:` block for the attribution tests below.
const MODULES_YAML: &str = r#"
modules:
  core:
    paths: ["schema/core/**"]
"#;

/// THE headline test: a 3-section migration whose middle section has bad SQL
/// fails; fixing that section in the file and re-applying resumes (section 1
/// skipped, 2 + 3 run). File-level immutability would have wedged this.
#[tokio::test]
async fn test_edit_failed_section_and_resume() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        helper.write_migration_file(
            "1000_split.sql",
            r#"-- pgmt:section name="s1" mode="transactional"
CREATE TABLE t1 (id SERIAL PRIMARY KEY);

-- pgmt:section name="s2" mode="transactional"
CREATE TABLE t2 (id INT REFERENCES nonexistent(id));

-- pgmt:section name="s3" mode="transactional"
CREATE TABLE t3 (id SERIAL PRIMARY KEY);
"#,
        )?;

        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .failure();

        // s1 completed, s2 failed, s3 pending.
        let pool = helper.connect_to_dev_db().await?;
        let statuses: Vec<(String, String)> = sqlx::query_as(
            "SELECT section_name, status FROM public.pgmt_migrations_sections ORDER BY section_order",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(
            statuses,
            vec![
                ("s1".to_string(), "completed".to_string()),
                ("s2".to_string(), "failed".to_string()),
                ("s3".to_string(), "pending".to_string()),
            ]
        );
        pool.close().await;

        // Fix s2 in the file (the whole file's bytes change).
        let fixed = r#"-- pgmt:section name="s1" mode="transactional"
CREATE TABLE t1 (id SERIAL PRIMARY KEY);

-- pgmt:section name="s2" mode="transactional"
CREATE TABLE t2 (id SERIAL PRIMARY KEY);

-- pgmt:section name="s3" mode="transactional"
CREATE TABLE t3 (id SERIAL PRIMARY KEY);
"#;
        helper.write_migration_file("1000_split.sql", fixed)?;

        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success()
            .stdout(predicate::str::contains(
                "section 's2' changed since registration; updating",
            ));

        // All three tables now exist and every section completed.
        assert!(helper.table_exists_in_dev("public", "t1").await?);
        assert!(helper.table_exists_in_dev("public", "t2").await?);
        assert!(helper.table_exists_in_dev("public", "t3").await?);

        let pool = helper.connect_to_dev_db().await?;
        let s2_checksum: Option<String> = sqlx::query_scalar(
            "SELECT checksum FROM public.pgmt_migrations_sections WHERE section_name = 's2'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            s2_checksum,
            Some(section_checksum(fixed, "s2")),
            "s2's stored checksum reflects the FIXED content"
        );
        pool.close().await;

        // A further apply is a clean no-op (all completed sections validate).
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();

        Ok(())
    })
    .await
}

/// Editing an already-COMPLETED section is refused, and the error names the
/// section (not just the file).
#[tokio::test]
async fn test_edit_completed_section_bails_naming_section() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        // s1 completes, s2 fails → migration half-applied, s1 pinned.
        helper.write_migration_file(
            "1000_split.sql",
            r#"-- pgmt:section name="s1" mode="transactional"
CREATE TABLE t1 (id SERIAL PRIMARY KEY);

-- pgmt:section name="s2" mode="transactional"
CREATE TABLE t2 (id INT REFERENCES nonexistent(id));
"#,
        )?;
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .failure();

        // Edit the COMPLETED section s1.
        helper.write_migration_file(
            "1000_split.sql",
            r#"-- pgmt:section name="s1" mode="transactional"
CREATE TABLE t1 (id SERIAL PRIMARY KEY, extra TEXT);

-- pgmt:section name="s2" mode="transactional"
CREATE TABLE t2 (id INT REFERENCES nonexistent(id));
"#,
        )?;

        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .failure()
            .stderr(predicate::str::contains("section 's1'"))
            .stderr(predicate::str::contains(
                "was modified after it was applied",
            ))
            .stderr(predicate::str::contains("Applied sections are immutable"));

        Ok(())
    })
    .await
}

/// Reordering a completed section (structural guard: section_order is pinned)
/// is refused even though each section's own checksum is unchanged.
#[tokio::test]
async fn test_reorder_or_retag_completed_section_bails() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        // Both sections apply successfully.
        helper.write_migration_file(
            "1000_two.sql",
            r#"-- pgmt:section name="a" mode="transactional"
CREATE TABLE a (id SERIAL PRIMARY KEY);

-- pgmt:section name="b" mode="transactional"
CREATE TABLE b (id SERIAL PRIMARY KEY);
"#,
        )?;
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();

        // Swap the two sections (each body/header identical, positions change).
        helper.write_migration_file(
            "1000_two.sql",
            r#"-- pgmt:section name="b" mode="transactional"
CREATE TABLE b (id SERIAL PRIMARY KEY);

-- pgmt:section name="a" mode="transactional"
CREATE TABLE a (id SERIAL PRIMARY KEY);
"#,
        )?;

        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .failure()
            .stderr(predicate::str::contains("reordered after it was applied"));

        Ok(())
    })
    .await
}

/// Legacy section rows (NULL checksum, pre-upgrade) pass section-level
/// validation; with NO checksummed sections at all the whole-file immutability
/// bail still fires. Both halves are asserted.
#[tokio::test]
async fn test_legacy_null_checksum_passes() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        let original = "CREATE TABLE legacy (id SERIAL PRIMARY KEY);";
        helper.write_migration_file("1000_legacy.sql", original)?;
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();

        // Simulate a pre-upgrade row: null out the per-section checksum, keeping
        // the main-row (file) checksum intact.
        let pool = helper.connect_to_dev_db().await?;
        sqlx::query("UPDATE public.pgmt_migrations_sections SET checksum = NULL")
            .execute(&pool)
            .await?;
        pool.close().await;

        // Edit the file → the whole-file checksum drifts.
        helper.write_migration_file(
            "1000_legacy.sql",
            &format!("{original}\n-- edited after apply\n"),
        )?;

        // No checksummed sections remain, so the legacy file-level guard fires;
        // it is NOT a section-level bail.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "has been modified after being applied",
            ))
            .stderr(predicate::str::contains(
                "Migrations must be immutable once applied",
            ))
            .stderr(predicate::str::contains("Applied sections are immutable").not());

        Ok(())
    })
    .await
}

/// The schema evolve step adds the new columns to an old-shape sections table
/// and materializes a synthetic 'default' completed section for every legacy
/// main row with no section rows — eliminating the "zero section rows = fully
/// applied" heuristic. The legacy migration must not re-run, and the evolve is
/// idempotent.
#[tokio::test]
async fn test_evolve_adds_columns_and_synthetic_rows() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        let migration_sql = "CREATE TABLE legacy_marker (id SERIAL PRIMARY KEY);";
        helper.write_migration_file("1000_legacy.sql", migration_sql)?;

        // Build the pre-change tracking shape by hand: current main table, plus
        // an OLD-shape sections table WITHOUT the new columns, and a main row
        // with no section rows.
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
            "CREATE TABLE public.pgmt_migrations_sections (
                migration_version BIGINT NOT NULL,
                is_baseline BOOLEAN NOT NULL,
                section_name TEXT NOT NULL,
                section_order INT NOT NULL,
                status TEXT NOT NULL,
                started_at TIMESTAMP WITH TIME ZONE,
                completed_at TIMESTAMP WITH TIME ZONE,
                attempts INT DEFAULT 0,
                last_error TEXT,
                rows_affected BIGINT,
                duration_ms BIGINT,
                PRIMARY KEY (migration_version, is_baseline, section_name)
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
        pool.close().await;

        // Apply triggers the evolve + backfill.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();

        let pool = helper.connect_to_dev_db().await?;
        // New columns exist.
        for column in ["checksum", "mode", "module", "applied_by", "pgmt_version"] {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema='public' AND table_name='pgmt_migrations_sections'
                   AND column_name=$1)",
            )
            .bind(column)
            .fetch_one(&pool)
            .await?;
            assert!(exists, "column {column} added by evolve");
        }
        // Synthetic 'default' completed section with NULL checksum, order 0.
        let row: (String, i32, Option<String>) = sqlx::query_as(
            "SELECT status, section_order, checksum FROM public.pgmt_migrations_sections
             WHERE migration_version = 1000 AND section_name = 'default'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(row, ("completed".to_string(), 0, None));
        pool.close().await;

        // Legacy migration treated as applied — table never created.
        assert!(
            !helper
                .table_exists_in_dev("public", "legacy_marker")
                .await?,
            "legacy migration must not be re-executed"
        );

        // Idempotent: a second apply is a no-op and adds no rows.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();
        let pool = helper.connect_to_dev_db().await?;
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM public.pgmt_migrations_sections")
            .fetch_one(&pool)
            .await?;
        assert_eq!(count, 1, "backfill is idempotent");
        assert!(
            !helper
                .table_exists_in_dev("public", "legacy_marker")
                .await?
        );
        pool.close().await;

        Ok(())
    })
    .await
}

/// Section rows are self-describing at registration: the module literal (NULL
/// for the base), the transaction mode, the recording pgmt version, and the
/// user who ran them are all stored.
#[tokio::test]
async fn test_module_and_metadata_recorded_at_registration() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        enable_modules(helper, MODULES_YAML)?;
        // A base object and a module object → both a base 'default' section and
        // a 'core' section in the generated migration.
        helper.write_schema_file("base_config.sql", "CREATE TABLE base_config (id SERIAL);")?;
        helper.write_schema_file("core/users.sql", "CREATE TABLE users (id SERIAL);")?;

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
                "--modules",
                "all",
            ])
            .assert()
            .success();

        let pool = helper.connect_to_dev_db().await?;
        // (section_name, module, mode, pgmt_version, applied_by)
        type MetaRow = (
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        );
        let rows: Vec<MetaRow> = sqlx::query_as(
            "SELECT section_name, module, mode, pgmt_version, applied_by
                 FROM public.pgmt_migrations_sections ORDER BY section_order",
        )
        .fetch_all(&pool)
        .await?;
        pool.close().await;

        assert!(!rows.is_empty(), "sections recorded");
        // The base section carries a NULL module; the core section carries the
        // literal 'core'. Every row has mode/pgmt_version/applied_by set.
        let base = rows.iter().find(|r| r.1.is_none()).expect("base section");
        assert!(base.2.is_some(), "mode recorded on base");
        let core = rows
            .iter()
            .find(|r| r.1.as_deref() == Some("core"))
            .expect("core section carries module literal");
        assert!(core.2.is_some(), "mode recorded on core");
        for (name, _module, mode, pgmt_version, applied_by) in &rows {
            assert!(mode.is_some(), "mode set for {name}");
            assert!(pgmt_version.is_some(), "pgmt_version set for {name}");
            assert!(applied_by.is_some(), "applied_by set for {name}");
        }

        Ok(())
    })
    .await
}

/// Module-subset wedge: a target that applied only the base can still bare-apply
/// after an UNPROVISIONED module's section body is edited (its section has no
/// row here, so nothing is pinned). Adopting the module later records the edited
/// checksum.
#[tokio::test]
async fn test_edit_unprovisioned_module_section_on_subset_target() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        enable_modules(helper, MODULES_YAML)?;

        // Hand-written sectioned migration: a base section + a core-module
        // section. No baseline, so the module can be adopted by plain apply.
        let original = r#"-- pgmt:section name="default" mode="transactional"
CREATE TABLE base_config (id SERIAL PRIMARY KEY);

-- pgmt:section name="core" module="core" mode="transactional"
CREATE TABLE users (id SERIAL PRIMARY KEY);
"#;
        helper.write_migration_file("1000_init.sql", original)?;

        // Bare apply = base only. The core section leaves no row.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();
        assert!(helper.table_exists_in_dev("public", "base_config").await?);
        assert!(!helper.table_exists_in_dev("public", "users").await?);

        // Edit the unprovisioned core section body.
        let edited = r#"-- pgmt:section name="default" mode="transactional"
CREATE TABLE base_config (id SERIAL PRIMARY KEY);

-- pgmt:section name="core" module="core" mode="transactional"
CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT);
"#;
        helper.write_migration_file("1000_init.sql", edited)?;

        // Bare apply still succeeds — no section bail (core isn't pinned here).
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();

        // Adopt core: its section is registered + run with the EDITED checksum.
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
            .success();
        assert!(helper.table_exists_in_dev("public", "users").await?);

        let pool = helper.connect_to_dev_db().await?;
        let core_checksum: Option<String> = sqlx::query_scalar(
            "SELECT checksum FROM public.pgmt_migrations_sections WHERE section_name = 'core'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            core_checksum,
            Some(section_checksum(edited, "core")),
            "adopted module section records the edited checksum"
        );
        pool.close().await;

        Ok(())
    })
    .await
}
