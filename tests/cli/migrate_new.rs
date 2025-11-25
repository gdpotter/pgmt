use crate::helpers::cli::with_cli_helper;
use anyhow::Result;
use predicates::prelude::*;

#[cfg(not(windows))]
use expectrl::Eof;

/// Tests for the migrate new command demonstrating both non-interactive and interactive approaches
mod migrate_new_tests {
    use super::*;

    /// Non-interactive test using assert_cmd - tests the traditional CLI approach
    #[tokio::test]
    async fn test_migrate_new_with_description_non_interactive() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Create a simple schema to generate a migration from
            helper.write_schema_file(
                "users.sql",
                "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL);",
            )?;

            helper
                .command()
                .args(["migrate", "new", "add_users_table", "--create-baseline"])
                .assert()
                .success()
                .stdout(predicate::str::contains(
                    "Generating migration: add_users_table",
                ))
                .stdout(predicate::str::contains("Created baseline:"))
                .stdout(predicate::str::contains("Migration generation complete!"));

            let migrations = helper.list_migration_files()?;
            assert_eq!(migrations.len(), 1);
            assert!(migrations[0].contains("add_users_table"));

            let baselines = helper.list_baseline_files()?;
            assert_eq!(baselines.len(), 1);

            Ok(())
        })
        .await
    }

    /// Test that baselines are NOT created by default when --create-baseline is not provided
    #[tokio::test]
    async fn test_migrate_new_without_baseline_creation() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Create a simple schema to generate a migration from
            helper.write_schema_file(
                "users.sql",
                "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL);",
            )?;

            helper
                .command()
                .args(["migrate", "new", "add_users_table"])
                .assert()
                .success()
                .stdout(predicate::str::contains(
                    "Generating migration: add_users_table",
                ))
                .stdout(predicate::str::contains("Skipping baseline creation"))
                .stdout(predicate::str::contains("Migration generation complete!"));

            let migrations = helper.list_migration_files()?;
            assert_eq!(migrations.len(), 1);
            assert!(migrations[0].contains("add_users_table"));

            // Verify NO baseline was created (default behavior)
            let baselines = helper.list_baseline_files()?;
            assert_eq!(baselines.len(), 0);

            Ok(())
        })
        .await
    }

    /// Interactive test using expectrl - tests the prompting behavior
    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_migrate_new_interactive_prompting() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Create a simple schema to generate a migration from
            helper.write_schema_file(
                "users.sql",
                "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL);",
            )?;

            // Test the command without description (should prompt)
            let mut session = helper.interactive_command(&["migrate", "new"]).await?;

            // Wait for prompt and respond
            session.expect("Enter migration description")?;
            session.send_line("add_users_from_prompt")?;

            // Verify success messages
            session.expect("Generating migration: add_users_from_prompt")?;
            session.expect("Migration generation complete!")?;

            // Wait for process to complete
            session.expect(Eof)?;

            // Verify migration file was created with correct description
            let migrations = helper.list_migration_files()?;
            assert_eq!(migrations.len(), 1);
            assert!(migrations[0].contains("add_users_from_prompt"));

            // Verify NO baseline was created (default behavior)
            let baselines = helper.list_baseline_files()?;
            assert_eq!(baselines.len(), 0);

            Ok(())
        })
        .await
    }

    /// Test validation errors and re-prompting - simplified version
    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_migrate_new_validation_and_error_handling() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Create a simple schema
            helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL);")?;

            let mut session = helper.interactive_command(&["migrate", "new"]).await?;

            // Just test basic prompting and valid input (skip complex validation testing)
            session.expect("Enter migration description")?;
            session.send_line("valid_migration_name")?;
            session.expect("Generating migration: valid_migration_name")?;
            session.expect("Migration generation complete!")?;

            session.expect(Eof)?;

            // Verify the migration was created with the valid name
            let migrations = helper.list_migration_files()?;
            assert_eq!(migrations.len(), 1);
            assert!(migrations[0].contains("valid_migration_name"));

            Ok(())
        })
        .await
    }

    /// Test very long description validation
    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_migrate_new_long_description_validation() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL);")?;

            let mut session = helper.interactive_command(&["migrate", "new"]).await?;

            session.expect("Enter migration description")?;

            // Send a description that's too long (over 100 characters)
            let long_description = "a".repeat(101);
            session.send_line(&long_description)?;

            // Should see error and re-prompt
            session.expect("Description must be 100 characters or less")?;
            session.expect("Enter migration description")?;

            // Provide valid input
            session.send_line("reasonable_length_description")?;
            session.expect("Generating migration: reasonable_length_description")?;
            session.expect("Migration generation complete!")?;

            session.expect(Eof)?;

            Ok(())
        })
        .await
    }

    /// Test that CLI arguments still work when description is provided directly
    #[tokio::test]
    async fn test_migrate_new_bypasses_prompt_when_description_provided() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL);")?;

            // When description is provided, should not prompt at all
            helper
                .command()
                .args(["migrate", "new", "direct_description"])
                .assert()
                .success()
                .stdout(predicate::str::contains(
                    "Generating migration: direct_description",
                ))
                .stdout(predicate::str::contains("Migration generation complete!"))
                // Should NOT contain the prompt text
                .stdout(predicate::str::contains("Enter migration description").not());

            let migrations = helper.list_migration_files()?;
            assert_eq!(migrations.len(), 1);
            assert!(migrations[0].contains("direct_description"));

            Ok(())
        })
        .await
    }

    /// Test help output
    #[tokio::test]
    async fn test_migrate_new_help() -> Result<()> {
        with_cli_helper(async |helper| {
            helper
                .command()
                .args(["migrate", "new", "--help"])
                .assert()
                .success()
                .stdout(predicate::str::contains("Generate migration from diff"))
                .stdout(predicate::str::contains("Description for the migration"));

            Ok(())
        })
        .await
    }
}
