use crate::helpers::cli::with_cli_helper;
use anyhow::Result;
use predicates::prelude::*;

/// Tests for baseline management commands
mod baseline_tests {
    use super::*;

    /// Test baseline create command
    #[tokio::test]
    async fn test_baseline_create() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Create a simple schema
            helper.write_schema_file(
                "users.sql",
                "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL);",
            )?;

            // Apply the schema first
            helper
                .command()
                .args(["apply", "--force"])
                .assert()
                .success();

            // Create a baseline
            helper
                .command()
                .args(["baseline", "create"])
                .assert()
                .success()
                .stdout(predicate::str::contains(
                    "Creating baseline schema snapshot",
                ))
                .stdout(predicate::str::contains("Baseline created successfully!"));

            // Verify baseline file was created
            let baselines = helper.list_baseline_files()?;
            assert_eq!(baselines.len(), 1);

            Ok(())
        })
        .await
    }

    /// Test baseline list command with empty directory
    #[tokio::test]
    async fn test_baseline_list_empty() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // List baselines when none exist
            helper
                .command()
                .args(["baseline", "list"])
                .assert()
                .success()
                .stdout(predicate::str::contains("No baseline files found"));

            Ok(())
        })
        .await
    }

    /// Test baseline list command with existing baselines
    #[tokio::test]
    async fn test_baseline_list_with_files() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Create a simple schema
            helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL PRIMARY KEY);")?;

            // Apply schema and create a baseline
            helper
                .command()
                .args(["apply", "--force"])
                .assert()
                .success();

            helper
                .command()
                .args(["baseline", "create"])
                .assert()
                .success();

            // List baselines
            helper
                .command()
                .args(["baseline", "list"])
                .assert()
                .success()
                .stdout(predicate::str::contains("Found 1 baseline(s):"))
                .stdout(predicate::str::contains("baseline_V"));

            Ok(())
        })
        .await
    }

    /// Test baseline clean command dry run
    #[tokio::test]
    async fn test_baseline_clean_dry_run() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Create first schema and baseline
            helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL);")?;
            helper
                .command()
                .args(["apply", "--force"])
                .assert()
                .success();
            helper
                .command()
                .args(["baseline", "create"])
                .assert()
                .success();

            // Add delay to ensure different timestamps
            tokio::time::sleep(tokio::time::Duration::from_millis(1100)).await;

            // Add additional schema (not replace) and create another baseline
            helper.write_schema_file(
                "orders.sql",
                "CREATE TABLE orders (id SERIAL, user_id INTEGER);",
            )?;
            helper
                .command()
                .args(["apply", "--force"])
                .assert()
                .success();
            helper
                .command()
                .args(["baseline", "create"])
                .assert()
                .success();

            // Verify we have 2 baselines
            let baselines = helper.list_baseline_files()?;
            assert_eq!(baselines.len(), 2);

            // Test dry run (should not delete anything)
            helper
                .command()
                .args(["baseline", "clean", "--keep", "1", "--dry-run"])
                .assert()
                .success()
                .stdout(predicate::str::contains("DRY RUN"))
                .stdout(predicate::str::contains("No files were actually deleted"));

            // Verify files still exist
            let baselines_after = helper.list_baseline_files()?;
            assert_eq!(baselines_after.len(), 2);

            Ok(())
        })
        .await
    }

    /// Test baseline clean command with keep parameter
    #[tokio::test]
    async fn test_baseline_clean_with_keep() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Create multiple schemas and baselines with actual schema changes
            for i in 1..=4 {
                helper.write_schema_file(
                    &format!("table{}.sql", i),
                    &format!("CREATE TABLE table{} (id SERIAL, name TEXT);", i),
                )?;
                helper
                    .command()
                    .args(["apply", "--force"])
                    .assert()
                    .success();
                helper
                    .command()
                    .args(["baseline", "create"])
                    .assert()
                    .success();
                // Larger delay to ensure different timestamps
                tokio::time::sleep(tokio::time::Duration::from_millis(1100)).await;
            }

            // Verify we have 4 baselines
            let baselines = helper.list_baseline_files()?;
            assert_eq!(baselines.len(), 4);

            // Clean keeping only the 2 most recent
            helper
                .command()
                .args(["baseline", "clean", "--keep", "2"])
                .assert()
                .success()
                .stdout(predicate::str::contains(
                    "Keeping 2 most recent baseline(s):",
                ))
                .stdout(predicate::str::contains("Cleanup complete!"));

            // Verify only 2 baselines remain
            let baselines_after = helper.list_baseline_files()?;
            assert_eq!(baselines_after.len(), 2);

            Ok(())
        })
        .await
    }

    /// Test migrate new with --create-baseline flag works correctly
    #[tokio::test]
    async fn test_migrate_new_with_create_baseline_flag() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Create a simple schema
            helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL PRIMARY KEY);")?;

            // Generate migration with baseline creation
            helper
                .command()
                .args(["migrate", "new", "initial_schema", "--create-baseline"])
                .assert()
                .success()
                .stdout(predicate::str::contains("Created baseline:"))
                .stdout(predicate::str::contains("Baseline validation passed"));

            // Verify both migration and baseline were created
            let migrations = helper.list_migration_files()?;
            let baselines = helper.list_baseline_files()?;
            assert_eq!(migrations.len(), 1);
            assert_eq!(baselines.len(), 1);

            Ok(())
        })
        .await
    }
}
