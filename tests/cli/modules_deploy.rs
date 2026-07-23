//! Subset deploy: `--modules` on apply/provision, dependency
//! closure, adoption (replay vs baseline-content), zero-trace skipping, and
//! the conservative intra-migration coupling guard.

use crate::helpers::cli::{
    THREE_MODULES_YAML, enable_modules, next_version_tick, with_cli_helper,
    write_three_module_schema,
};
use anyhow::Result;
use predicates::prelude::*;

/// Subset provision deploys only the named modules (+ deps + base); the
/// unselected module's objects don't exist and its sections leave NO rows —
/// nothing in the target names a module it didn't ask for.
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
            ))
            // Parity with apply: the unselected module's baseline sections are
            // announced, not silently dropped.
            .stdout(predicate::str::contains(
                "Skipping module 'analytics' sections in baseline",
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

/// Bare `provision` on a module project deploys only the base — the same
/// explicit-modules rule as `apply` (nothing is inferred).
#[tokio::test]
async fn test_bare_provision_deploys_base_only() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        enable_modules(helper, THREE_MODULES_YAML)?;
        write_three_module_schema(helper)?;
        helper.write_schema_file("meta.sql", "CREATE TABLE meta (k TEXT PRIMARY KEY);")?;

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
            .success()
            .stdout(predicate::str::contains(
                "provisioning the unmoduled base only",
            ));

        assert!(helper.table_exists_in_dev("public", "meta").await?);
        assert!(
            !helper.table_exists_in_dev("public", "users").await?,
            "modules never deploy implicitly — provision included"
        );

        Ok(())
    })
    .await
}

/// PGMT_MODULES env is the flag's fallback: with it set, a bare `apply`
/// deploys the named modules.
#[tokio::test]
async fn test_pgmt_modules_env_fallback() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        enable_modules(helper, THREE_MODULES_YAML)?;
        write_three_module_schema(helper)?;

        helper
            .command()
            .args(["migrate", "new", "initial"])
            .assert()
            .success();

        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .env("PGMT_MODULES", "core")
            .assert()
            .success();

        assert!(helper.table_exists_in_dev("public", "users").await?);
        assert!(
            !helper.table_exists_in_dev("public", "invoices").await?,
            "env selects core only"
        );

        Ok(())
    })
    .await
}

/// `section_order` is a section's index in the FULL migration file,
/// stable per version even when a version's sections are registered across
/// SEPARATE apply calls. A bare `apply` registers only the base "default"
/// section; a later `apply --modules core` registers core's section for the
/// SAME version. The rows must carry DISTINCT orders reflecting file order —
/// not both collide at 0 because each registration call restarts `enumerate()`.
#[tokio::test]
async fn test_split_registration_keeps_distinct_section_order() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        enable_modules(helper, THREE_MODULES_YAML)?;
        write_three_module_schema(helper)?;
        // An unmoduled file → the base's "default" section.
        helper.write_schema_file("meta.sql", "CREATE TABLE meta (k TEXT PRIMARY KEY);")?;

        // A pure migration (no baseline): apply can adopt modules by replay.
        helper
            .command()
            .args(["migrate", "new", "initial"])
            .assert()
            .success();

        // First apply (bare): registers only the base "default" section for
        // this version; module sections leave no rows.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();

        // Second apply (--modules core): registers core's section for the SAME
        // version, in a separate registration call.
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
        assert!(helper.table_exists_in_dev("public", "meta").await?);
        assert!(helper.table_exists_in_dev("public", "users").await?);

        // Recover the version and the file's section layout.
        let migrations = helper.list_migration_files()?;
        assert_eq!(migrations.len(), 1);
        let version: i64 = migrations[0]
            .split('_')
            .next()
            .and_then(|v| v.parse().ok())
            .expect("migration filename starts with a numeric version");
        let sql = helper.read_migration_file(&migrations[0])?;

        let pool = helper.connect_to_dev_db().await?;
        let rows: Vec<(String, i32)> = sqlx::query_as(
            "SELECT section_name, section_order FROM public.pgmt_migrations_sections
             WHERE migration_version = $1 AND NOT is_baseline",
        )
        .bind(version)
        .fetch_all(&pool)
        .await?;
        pool.close().await;

        // Both registration calls left their section rows for this version.
        assert!(
            rows.iter().any(|(n, _)| n == "default"),
            "base 'default' section registered by the bare apply: {rows:?}"
        );
        assert!(
            rows.iter().any(|(n, _)| n == "core"),
            "'core' section registered by --modules core apply: {rows:?}"
        );

        // No two sections share an order (the collision-at-0 bug).
        let mut orders: Vec<i32> = rows.iter().map(|(_, o)| *o).collect();
        orders.sort_unstable();
        let mut distinct = orders.clone();
        distinct.dedup();
        assert_eq!(
            orders, distinct,
            "section_order values must be distinct across a version's sections: {rows:?}"
        );

        // Orders reflect FULL-file position: sections sorted by recorded order
        // must match sections sorted by their header position in the file.
        let mut by_db = rows.clone();
        by_db.sort_by_key(|(_, o)| *o);
        let mut by_file = rows.clone();
        by_file.sort_by_key(|(name, _)| {
            sql.find(&format!("name=\"{name}\""))
                .expect("each recorded section has a header in the file")
        });
        let db_order: Vec<&str> = by_db.iter().map(|(n, _)| n.as_str()).collect();
        let file_order: Vec<&str> = by_file.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(
            db_order, file_order,
            "section_order must reflect file order (base precedes module iff it \
             precedes in the file): {rows:?}"
        );

        Ok(())
    })
    .await
}

/// Adopting a module from a baseline requires the target's
/// ESTABLISHED modules to be caught up to that baseline version first — else
/// the baseline row would claim coverage the target doesn't have and later
/// applies would silently skip the established modules' pending migrations.
#[tokio::test]
async fn test_adoption_requires_established_modules_caught_up() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        enable_modules(
            helper,
            "\nmodules:\n  core:\n    paths: [\"schema/core/**\"]\n  billing:\n    paths: [\"schema/billing/**\"]\n    depends_on: [core]\n",
        )?;

        // Migration v1: core only. Provision the target with core (billing not
        // deployed) — the target is now established at v1 with core.
        helper.write_schema_file("core/users.sql", "CREATE TABLE users (id SERIAL PRIMARY KEY);")?;
        helper
            .command()
            .args(["migrate", "new", "initial"])
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

        // Migration v2 (+ paired baseline): a core change AND billing's first
        // appearance. billing now lives in a baseline, so adopting it needs
        // baseline content. The target is still at v1 — it never applied v2.
        next_version_tick();
        helper.write_schema_file(
            "core/users.sql",
            "CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT);",
        )?;
        helper.write_schema_file(
            "billing/invoices.sql",
            "-- require: core/users.sql\n\
             CREATE TABLE invoices (id SERIAL PRIMARY KEY, user_id INT REFERENCES users(id));",
        )?;
        helper
            .command()
            .args(["migrate", "new", "second", "--create-baseline"])
            .assert()
            .success();

        // Adopting billing must refuse: core is not caught up to the baseline
        // version, and rolling it forward is an explicit apply, not a side
        // effect of adopting billing.
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
            .stderr(predicate::str::contains("not caught up"))
            .stderr(predicate::str::contains("migrate apply --modules core"));
        assert!(
            !helper.table_exists_in_dev("public", "invoices").await?,
            "billing must not have been adopted while core is behind"
        );

        // Catch core up, then adoption succeeds.
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
        assert!(helper.table_exists_in_dev("public", "invoices").await?);
        // And core's v2 change landed (it was rolled forward, not skipped).
        let pool = helper.connect_to_dev_db().await?;
        let has_email: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_name = 'users' AND column_name = 'email')",
        )
        .fetch_one(&pool)
        .await?;
        assert!(has_email, "core's v2 column must be present, not skipped");
        pool.close().await;

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
