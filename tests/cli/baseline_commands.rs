use crate::helpers::cli::with_cli_helper;
use anyhow::Result;
use predicates::prelude::*;

/// Tests for baseline management commands (now under `migrate baseline`)
mod baseline_tests {
    use super::*;

    /// Test migrate baseline creates a baseline and deletes migrations by default
    #[tokio::test]
    async fn test_migrate_baseline_cleans_migrations() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Create schema and generate migrations
            helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL);")?;
            helper
                .command()
                .args(["migrate", "new", "add_users"])
                .assert()
                .success();

            tokio::time::sleep(tokio::time::Duration::from_millis(1100)).await;

            helper.write_schema_file(
                "orders.sql",
                "CREATE TABLE orders (id SERIAL, user_id INTEGER);",
            )?;
            helper
                .command()
                .args(["migrate", "new", "add_orders"])
                .assert()
                .success();

            // Verify starting state: 2 migrations, 0 baselines
            assert_eq!(helper.list_migration_files()?.len(), 2);
            assert_eq!(helper.list_baseline_files()?.len(), 0);

            // Create baseline (default: deletes all migrations)
            helper
                .command()
                .args(["migrate", "baseline"])
                .assert()
                .success()
                .stdout(predicate::str::contains("Baseline created successfully!"))
                .stdout(predicate::str::contains("Cleaned up 2 migration(s)"));

            // Verify: 0 migrations, 1 baseline
            assert_eq!(helper.list_migration_files()?.len(), 0);
            assert_eq!(helper.list_baseline_files()?.len(), 1);

            Ok(())
        })
        .await
    }

    /// Test migrate baseline --keep-migrations preserves migration files
    #[tokio::test]
    async fn test_migrate_baseline_keep_migrations() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            helper.write_schema_file(
                "users.sql",
                "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL);",
            )?;

            // Apply and create a migration
            helper
                .command()
                .args(["apply", "--force"])
                .assert()
                .success();

            helper
                .command()
                .args(["migrate", "new", "add_users"])
                .assert()
                .success();

            // Create baseline with --keep-migrations
            helper
                .command()
                .args(["migrate", "baseline", "--keep-migrations"])
                .assert()
                .success()
                .stdout(predicate::str::contains("Baseline created successfully!"));

            // Verify: migrations preserved, baseline created
            assert_eq!(helper.list_migration_files()?.len(), 1);
            assert_eq!(helper.list_baseline_files()?.len(), 1);

            Ok(())
        })
        .await
    }

    /// Test migrate baseline list command with empty directory
    #[tokio::test]
    async fn test_migrate_baseline_list_empty() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            helper
                .command()
                .args(["migrate", "baseline", "list"])
                .assert()
                .success()
                .stdout(predicate::str::contains("No baseline files found"));

            Ok(())
        })
        .await
    }

    /// Test migrate baseline list command with existing baselines
    #[tokio::test]
    async fn test_migrate_baseline_list_with_files() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL PRIMARY KEY);")?;

            helper
                .command()
                .args(["apply", "--force"])
                .assert()
                .success();

            // `migrate baseline` checkpoints the migration log, so create it first.
            helper
                .command()
                .args(["migrate", "new", "initial"])
                .assert()
                .success();
            helper
                .command()
                .args(["migrate", "baseline", "--keep-migrations"])
                .assert()
                .success();

            helper
                .command()
                .args(["migrate", "baseline", "list"])
                .assert()
                .success()
                .stdout(predicate::str::contains("Found 1 baseline(s):"))
                .stdout(predicate::str::contains("baseline_"));

            Ok(())
        })
        .await
    }

    /// Test migrate baseline --dry-run previews without making changes
    #[tokio::test]
    async fn test_migrate_baseline_dry_run() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL);")?;
            helper
                .command()
                .args(["migrate", "new", "add_users"])
                .assert()
                .success();

            tokio::time::sleep(tokio::time::Duration::from_millis(1100)).await;

            helper.write_schema_file(
                "orders.sql",
                "CREATE TABLE orders (id SERIAL, user_id INTEGER);",
            )?;
            helper
                .command()
                .args(["migrate", "new", "add_orders"])
                .assert()
                .success();

            assert_eq!(helper.list_migration_files()?.len(), 2);

            // Dry run should preview but not delete
            helper
                .command()
                .args(["migrate", "baseline", "--dry-run"])
                .assert()
                .success()
                .stdout(predicate::str::contains("DRY RUN"))
                .stdout(predicate::str::contains("Migrations to delete (2)"));

            // Verify nothing was changed
            assert_eq!(helper.list_migration_files()?.len(), 2);
            assert_eq!(helper.list_baseline_files()?.len(), 0);

            Ok(())
        })
        .await
    }

    /// Test migrate baseline deletes old baselines too
    #[tokio::test]
    async fn test_migrate_baseline_cleans_old_baselines() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL);")?;

            // Create first baseline (checkpointing the initial migration)
            helper
                .command()
                .args(["apply", "--force"])
                .assert()
                .success();
            helper
                .command()
                .args(["migrate", "new", "initial"])
                .assert()
                .success();
            helper
                .command()
                .args(["migrate", "baseline", "--keep-migrations"])
                .assert()
                .success();

            assert_eq!(helper.list_baseline_files()?.len(), 1);

            tokio::time::sleep(tokio::time::Duration::from_millis(1100)).await;

            // Create migration and then a new baseline (which should delete the old one)
            helper.write_schema_file(
                "orders.sql",
                "CREATE TABLE orders (id SERIAL, user_id INTEGER);",
            )?;
            helper
                .command()
                .args(["migrate", "new", "add_orders"])
                .assert()
                .success();

            helper
                .command()
                .args(["migrate", "baseline"])
                .assert()
                .success()
                .stdout(predicate::str::contains("Cleaned up"));

            // Should have exactly 1 baseline (new one) and 0 migrations
            assert_eq!(helper.list_baseline_files()?.len(), 1);
            assert_eq!(helper.list_migration_files()?.len(), 0);

            Ok(())
        })
        .await
    }

    /// Test migrate new with --create-baseline flag works correctly
    #[tokio::test]
    async fn test_migrate_new_with_create_baseline_flag() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL PRIMARY KEY);")?;

            helper
                .command()
                .args(["migrate", "new", "initial_schema", "--create-baseline"])
                .assert()
                .success()
                .stdout(predicate::str::contains("Created baseline:"))
                .stdout(predicate::str::contains("Baseline validation passed"));

            let migrations = helper.list_migration_files()?;
            let baselines = helper.list_baseline_files()?;
            assert_eq!(migrations.len(), 1);
            assert_eq!(baselines.len(), 1);

            Ok(())
        })
        .await
    }

    /// Regression: `--create-baseline` on a *non-initial* migration must capture
    /// the full schema, not just that migration's delta. The baseline used to be
    /// written from the migration SQL (a delta against the prior state), so it
    /// omitted everything earlier migrations created — and validation rejected it.
    #[tokio::test]
    async fn test_create_baseline_on_non_initial_migration_is_full_schema() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // First migration (no baseline): users.
            helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL PRIMARY KEY);")?;
            helper
                .command()
                .args(["migrate", "new", "add_users"])
                .assert()
                .success();

            // Second migration WITH --create-baseline: adds posts.
            helper.write_schema_file("posts.sql", "CREATE TABLE posts (id SERIAL PRIMARY KEY);")?;
            helper
                .command()
                .args(["migrate", "new", "add_posts", "--create-baseline"])
                .assert()
                .success()
                .stdout(predicate::str::contains("Baseline validation passed"));

            // The baseline is a from-scratch snapshot: it must include BOTH the
            // pre-existing users table and the newly added posts table.
            let baselines = helper.list_baseline_files()?;
            assert_eq!(baselines.len(), 1);
            let baseline = helper.read_baseline_file(&baselines[0])?;
            assert!(
                baseline.contains("users"),
                "baseline must include the pre-existing users table:\n{baseline}"
            );
            assert!(
                baseline.contains("posts"),
                "baseline must include the newly added posts table:\n{baseline}"
            );

            Ok(())
        })
        .await
    }

    /// Version-pairing pin: `migrate update` on a migration with a same-version
    /// baseline regenerates that baseline as a FULL schema snapshot, at the
    /// same (paired) version as the migration.
    #[tokio::test]
    async fn test_migrate_update_regenerates_full_baseline() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Migration + paired baseline: users only.
            helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL PRIMARY KEY);")?;
            helper
                .command()
                .args(["migrate", "new", "initial", "--create-baseline"])
                .assert()
                .success();

            // Grow the schema, then regenerate the same migration.
            helper.write_schema_file("posts.sql", "CREATE TABLE posts (id SERIAL PRIMARY KEY);")?;
            helper
                .command()
                .args(["migrate", "update"])
                .assert()
                .success();

            let migrations = helper.list_migration_files()?;
            let baselines = helper.list_baseline_files()?;
            assert_eq!(migrations.len(), 1);
            assert_eq!(baselines.len(), 1, "update must not leave stray baselines");

            // Still paired: baseline version == migration version.
            let migration_version: String = migrations[0]
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            let baseline_version: String = baselines[0]
                .trim_start_matches("baseline_")
                .trim_end_matches(".sql")
                .to_string();
            assert_eq!(
                baseline_version, migration_version,
                "regenerated baseline must keep the migration's version"
            );

            // And it is a full snapshot, not the update's delta.
            let baseline = helper.read_baseline_file(&baselines[0])?;
            assert!(
                baseline.contains("users"),
                "updated baseline must include the pre-existing users table:\n{baseline}"
            );
            assert!(
                baseline.contains("posts"),
                "updated baseline must include the newly added posts table:\n{baseline}"
            );

            Ok(())
        })
        .await
    }

    /// Checkpoint semantics: `migrate baseline` collapses the migration LOG,
    /// never the schema files. Un-migrated schema drift stays out of the
    /// baseline and surfaces in the next `migrate new` instead.
    #[tokio::test]
    async fn test_migrate_baseline_checkpoints_history_not_files() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // History: users only.
            helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL PRIMARY KEY);")?;
            helper
                .command()
                .args(["migrate", "new", "add_users"])
                .assert()
                .success();

            // Drift: posts exists in the files but in no migration.
            helper.write_schema_file("posts.sql", "CREATE TABLE posts (id SERIAL PRIMARY KEY);")?;

            helper
                .command()
                .args(["migrate", "baseline"])
                .assert()
                .success();

            let baselines = helper.list_baseline_files()?;
            assert_eq!(baselines.len(), 1);
            let baseline = helper.read_baseline_file(&baselines[0])?;
            assert!(
                baseline.contains("users"),
                "checkpoint must contain what history created:\n{baseline}"
            );
            assert!(
                !baseline.contains("posts"),
                "un-migrated drift must NOT be smuggled into the checkpoint:\n{baseline}"
            );

            // The drift lands where it belongs: the next migration.
            helper
                .command()
                .args(["migrate", "new", "add_posts"])
                .assert()
                .success();
            let migrations = helper.list_migration_files()?;
            assert_eq!(
                migrations.len(),
                1,
                "old migrations collapsed, drift is new"
            );
            let migration = helper.read_migration_file(&migrations[0])?;
            assert!(
                migration.contains("posts") && !migration.contains("users"),
                "the next migration captures exactly the drift:\n{migration}"
            );

            Ok(())
        })
        .await
    }

    /// Regression: when the latest migration has a PAIRED baseline (created by
    /// `migrate new --create-baseline`), the checkpoint shares its version and
    /// therefore its file path. The cleanup phase must not delete the freshly
    /// written checkpoint as an "old" baseline — that destroyed the entire
    /// history (migrations deleted, zero baselines left).
    #[tokio::test]
    async fn test_migrate_baseline_survives_paired_baseline_at_same_version() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;
            helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL PRIMARY KEY);")?;

            helper
                .command()
                .args(["migrate", "new", "initial", "--create-baseline"])
                .assert()
                .success();

            helper
                .command()
                .args(["migrate", "baseline"])
                .assert()
                .success();

            assert_eq!(helper.list_migration_files()?.len(), 0, "log collapsed");
            let baselines = helper.list_baseline_files()?;
            assert_eq!(
                baselines.len(),
                1,
                "the checkpoint must survive its own cleanup"
            );

            // And the project still reconstructs from it.
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

    /// With no migrations there is no log to checkpoint: refuse with guidance
    /// instead of snapshotting the schema files under a made-up version.
    #[tokio::test]
    async fn test_migrate_baseline_errors_without_migrations() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;
            helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL);")?;

            helper
                .command()
                .args(["migrate", "baseline"])
                .assert()
                .failure()
                .stderr(predicate::str::contains("no migrations to checkpoint"))
                .stderr(predicate::str::contains("--create-baseline"));

            Ok(())
        })
        .await
    }
}
