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

            // Create first baseline
            helper
                .command()
                .args(["apply", "--force"])
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
}
