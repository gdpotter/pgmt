//! Module-tagged migration generation (Phase 2): `migrate new` on a module
//! project partitions diff steps into per-module sections, detects partition
//! divergence (re-tags, replayability breaks), and emits re-anchoring
//! baselines with `remaps`.

use crate::helpers::cli::with_cli_helper;
use anyhow::Result;
use predicates::prelude::*;

const MODULES_YAML: &str = r#"
modules:
  core:
    paths: ["schema/core/**"]
  billing:
    paths: ["schema/billing/**"]
    depends_on: [core]
"#;

/// Append a `modules:` block to the helper's default pgmt.yaml.
fn enable_modules(helper: &crate::helpers::cli::CliTestHelper, yaml: &str) -> Result<()> {
    let config_path = helper.project_root.join("pgmt.yaml");
    let mut config = std::fs::read_to_string(&config_path)?;
    config.push_str(yaml);
    std::fs::write(config_path, config)?;
    Ok(())
}

/// A module project generates module-tagged sections, the migration applies,
/// and a second `migrate new` sees no changes — proving the sectioned file
/// replays identically through the section-aware reconstruction.
#[tokio::test]
async fn test_module_project_generates_sectioned_migration() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        enable_modules(helper, MODULES_YAML)?;

        helper.write_schema_file(
            "core/users.sql",
            "CREATE TABLE users (id SERIAL PRIMARY KEY);",
        )?;
        helper.write_schema_file(
            "billing/invoices.sql",
            "-- require: core/users.sql\n\
             CREATE TABLE invoices (id SERIAL PRIMARY KEY, user_id INT REFERENCES users(id));",
        )?;
        // Unmoduled file: lands in the base's "default" section.
        helper.write_schema_file("audit.sql", "CREATE TABLE audit_log (id SERIAL);")?;

        helper
            .command()
            .args(["migrate", "new", "initial"])
            .assert()
            .success();

        let migrations = helper.list_migration_files()?;
        assert_eq!(migrations.len(), 1);
        let sql = helper.read_migration_file(&migrations[0])?;

        assert!(
            sql.contains(r#"-- pgmt:section name="core" module="core""#),
            "core section header expected:\n{sql}"
        );
        assert!(
            sql.contains(r#"-- pgmt:section name="billing" module="billing""#),
            "billing section header expected:\n{sql}"
        );
        assert!(
            sql.contains(r#"-- pgmt:section name="default""#),
            "unmoduled default section expected:\n{sql}"
        );
        assert!(
            !sql.contains(r#"name="default" module"#),
            "the base section must carry no module attribute:\n{sql}"
        );
        // Dependency order: core's table before billing's FK-bearing table.
        let core_pos = sql.find(r#"name="core""#).unwrap();
        let billing_pos = sql.find(r#"name="billing""#).unwrap();
        assert!(core_pos < billing_pos, "core section precedes billing");

        // The sectioned migration applies cleanly (naming the modules —
        // bare apply deploys only the base)...
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
        assert!(helper.table_exists_in_dev("public", "invoices").await?);

        // ...and replays identically: no spurious diff on regeneration.
        helper
            .command()
            .args(["migrate", "new", "noop"])
            .assert()
            .success()
            .stdout(predicate::str::contains("No changes detected"));

        Ok(())
    })
    .await
}

/// Modularizing an existing project is a pure re-tag: no DDL, but replaying
/// history would reproduce the old (unmoduled) ownership. `migrate new`
/// refuses without --create-baseline; with it, a baseline-only re-anchor is
/// emitted whose sections carry `remaps="(unmoduled)"`.
#[tokio::test]
async fn test_modularizing_existing_project_requires_baseline() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        // Pre-modules history.
        helper.write_schema_file(
            "core/users.sql",
            "CREATE TABLE users (id SERIAL PRIMARY KEY);",
        )?;
        helper
            .command()
            .args(["migrate", "new", "initial"])
            .assert()
            .success();

        // Declare modules over the existing files: a re-tag, no DDL change.
        enable_modules(
            helper,
            "\nmodules:\n  core:\n    paths: [\"schema/core/**\"]\n",
        )?;

        helper
            .command()
            .args(["migrate", "new", "modularize"])
            .assert()
            .failure()
            .stderr(predicate::str::contains("partition re-anchor required"))
            .stderr(predicate::str::contains("--create-baseline"));

        helper
            .command()
            .args(["migrate", "new", "modularize", "--create-baseline"])
            .assert()
            .success()
            .stdout(predicate::str::contains(
                "No schema changes - emitting re-anchoring baseline only.",
            ));

        // No second migration; one re-anchoring baseline with the remap.
        assert_eq!(helper.list_migration_files()?.len(), 1);
        let baselines = helper.list_baseline_files()?;
        assert_eq!(baselines.len(), 1);
        let baseline = helper.read_baseline_file(&baselines[0])?;
        assert!(
            baseline.contains(r#"-- pgmt:section name="core" module="core" remaps="(unmoduled)""#),
            "re-anchoring baseline must record prior ownership:\n{baseline}"
        );

        // Post-re-anchor, regeneration is quiet again: the baseline's tags
        // now agree with the files.
        helper
            .command()
            .args(["migrate", "new", "noop"])
            .assert()
            .success()
            .stdout(predicate::str::contains("No changes detected"));

        Ok(())
    })
    .await
}

/// Dropping an object another module's history references breaks that
/// module's replayability: refused without --create-baseline; with it, the
/// migration's sections are ordered drops-first (billing's FK drop before
/// core's table drop) and the re-anchoring baseline accompanies it.
#[tokio::test]
async fn test_cross_module_drop_requires_baseline() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        enable_modules(helper, MODULES_YAML)?;

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
            .args(["migrate", "new", "initial"])
            .assert()
            .success();

        // Drop accounts; billing keeps invoices but loses the FK.
        std::fs::remove_file(helper.project_root.join("schema/core/accounts.sql"))?;
        helper.write_schema_file(
            "billing/invoices.sql",
            "CREATE TABLE invoices (id SERIAL PRIMARY KEY, account_id INT);",
        )?;

        helper
            .command()
            .args(["migrate", "new", "drop_accounts"])
            .assert()
            .failure()
            .stderr(predicate::str::contains("breaks replay of module 'billing'"));

        helper
            .command()
            .args(["migrate", "new", "drop_accounts", "--create-baseline"])
            .assert()
            .success();

        let migrations = helper.list_migration_files()?;
        assert_eq!(migrations.len(), 2);
        let sql = helper.read_migration_file(&migrations[1])?;

        // Drops reverse the DAG: billing's constraint drop precedes core's
        // table drop.
        let billing_pos = sql
            .find(r#"module="billing""#)
            .expect("billing section expected");
        let core_pos = sql.find(r#"module="core""#).expect("core section expected");
        assert!(
            billing_pos < core_pos,
            "billing's FK drop must precede core's table drop:\n{sql}"
        );
        assert!(sql.contains("DROP TABLE"), "{sql}");

        Ok(())
    })
    .await
}

/// An undeclared cross-module reference warns (but does not fail) generation.
#[tokio::test]
async fn test_undeclared_cross_module_reference_warns() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        // billing deliberately does NOT declare depends_on: [core].
        enable_modules(
            helper,
            "\nmodules:\n  core:\n    paths: [\"schema/core/**\"]\n  billing:\n    paths: [\"schema/billing/**\"]\n",
        )?;

        helper.write_schema_file(
            "core/users.sql",
            "CREATE TABLE users (id SERIAL PRIMARY KEY);",
        )?;
        helper.write_schema_file(
            "billing/invoices.sql",
            "-- require: core/users.sql\n\
             CREATE TABLE invoices (id SERIAL PRIMARY KEY, user_id INT REFERENCES users(id));",
        )?;

        helper
            .command()
            .args(["migrate", "new", "initial"])
            .assert()
            .success()
            .stderr(predicate::str::contains("does not declare `depends_on:"));

        Ok(())
    })
    .await
}

/// Characterization: a project without `modules:` produces byte-for-byte the
/// same migrations as before — plain SQL, no section headers.
#[tokio::test]
async fn test_non_module_project_has_no_section_headers() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL);")?;

        helper
            .command()
            .args(["migrate", "new", "initial"])
            .assert()
            .success();

        let migrations = helper.list_migration_files()?;
        let sql = helper.read_migration_file(&migrations[0])?;
        assert!(
            !sql.contains("-- pgmt:section"),
            "non-module projects must stay header-less:\n{sql}"
        );

        Ok(())
    })
    .await
}

/// `migrate update` on a module project keeps the migration and its paired
/// baseline module-sectioned.
#[tokio::test]
async fn test_module_update_stays_sectioned() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        enable_modules(helper, MODULES_YAML)?;

        helper.write_schema_file(
            "core/users.sql",
            "CREATE TABLE users (id SERIAL PRIMARY KEY);",
        )?;
        helper
            .command()
            .args(["migrate", "new", "initial", "--create-baseline"])
            .assert()
            .success();

        // Grow core, regenerate the same migration.
        helper.write_schema_file(
            "core/users.sql",
            "CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT);",
        )?;
        helper
            .command()
            .args(["migrate", "update"])
            .assert()
            .success();

        let migrations = helper.list_migration_files()?;
        assert_eq!(migrations.len(), 1);
        let sql = helper.read_migration_file(&migrations[0])?;
        assert!(
            sql.contains(r#"-- pgmt:section name="core" module="core""#),
            "updated migration must stay sectioned:\n{sql}"
        );
        assert!(sql.contains("email"), "{sql}");

        let baselines = helper.list_baseline_files()?;
        assert_eq!(baselines.len(), 1);
        let baseline = helper.read_baseline_file(&baselines[0])?;
        assert!(
            baseline.contains(r#"module="core""#),
            "updated baseline must stay sectioned:\n{baseline}"
        );

        Ok(())
    })
    .await
}

/// `migrate baseline` checkpoints the log with module tags preserved: the
/// collapsed baseline's sections carry each object's historical module, and —
/// being a pure checkpoint that never changes ownership — no `remaps`.
#[tokio::test]
async fn test_migrate_baseline_checkpoint_keeps_module_sections() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        enable_modules(helper, MODULES_YAML)?;

        helper.write_schema_file(
            "core/users.sql",
            "CREATE TABLE users (id SERIAL PRIMARY KEY);",
        )?;
        helper.write_schema_file(
            "billing/invoices.sql",
            "-- require: core/users.sql\n\
             CREATE TABLE invoices (id SERIAL PRIMARY KEY, user_id INT REFERENCES users(id));",
        )?;
        helper
            .command()
            .args(["migrate", "new", "initial"])
            .assert()
            .success();

        helper
            .command()
            .args(["migrate", "baseline"])
            .assert()
            .success();

        let baselines = helper.list_baseline_files()?;
        assert_eq!(baselines.len(), 1);
        let baseline = helper.read_baseline_file(&baselines[0])?;
        assert!(
            baseline.contains(r#"module="core""#),
            "checkpoint must keep core's module tag:\n{baseline}"
        );
        assert!(
            baseline.contains(r#"module="billing""#),
            "checkpoint must keep billing's module tag:\n{baseline}"
        );
        assert!(
            !baseline.contains("remaps="),
            "a checkpoint never changes ownership — no remaps:\n{baseline}"
        );

        // The checkpoint replaces the migrations and replays cleanly: a fresh
        // `migrate new` reconstructs from it without spurious diffs.
        assert_eq!(helper.list_migration_files()?.len(), 0);
        helper
            .command()
            .args(["migrate", "new", "noop"])
            .assert()
            .success()
            .stdout(predicate::str::contains("No changes detected"));

        Ok(())
    })
    .await
}
