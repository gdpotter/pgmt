/// Tests for pgmt diff command
/// Verifies CLI interface, output formats, and source comparison
use crate::helpers::cli::with_cli_helper;
use anyhow::Result;
use predicates::prelude::*;

mod diff_basic {
    use super::*;

    /// Test basic diff command with no arguments (dev → schema)
    #[tokio::test]
    async fn test_diff_default_no_changes() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Create schema
            helper.write_schema_file(
                "users.sql",
                "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100));",
            )?;

            // Apply schema to dev database
            helper
                .command()
                .args(["apply", "--force"])
                .assert()
                .success();

            // Diff should show no changes
            helper
                .command()
                .arg("diff")
                .assert()
                .success()
                .stdout(predicate::str::contains("No differences found"));

            Ok(())
        })
        .await
    }

    /// Test diff detects schema changes
    #[tokio::test]
    async fn test_diff_detects_schema_changes() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Create initial schema
            helper.write_schema_file(
                "users.sql",
                "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100));",
            )?;

            // Apply to dev database
            helper.command().args(["apply", "--force"]).assert().success();

            // Modify schema
            helper.write_schema_file(
                "users.sql",
                "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100), email VARCHAR(255));",
            )?;

            // Diff should detect the change
            helper
                .command()
                .arg("diff")
                .assert()
                .code(1) // Exit code 1 for differences found
                .stdout(predicate::str::contains("Found"))
                .stdout(predicate::str::contains("differences"));

            Ok(())
        })
        .await
    }
}

mod diff_formats {
    use super::*;

    /// Test --format summary output
    #[tokio::test]
    async fn test_diff_format_summary() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Create schema with table
            helper.write_schema_file(
                "users.sql",
                "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100));",
            )?;

            // Apply to dev
            helper.command().args(["apply", "--force"]).assert().success();

            // Add a new column - this will definitely be detected
            helper.write_schema_file(
                "users.sql",
                "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100), email VARCHAR(255));",
            )?;

            // Test summary format
            helper
                .command()
                .args(["diff", "--format", "summary"])
                .assert()
                .code(1)
                .stdout(predicate::str::contains("Schema Diff Summary"))
                .stdout(predicate::str::contains("changes"));

            Ok(())
        })
        .await
    }

    /// Test --format sql output
    #[tokio::test]
    async fn test_diff_format_sql() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL PRIMARY KEY);")?;

            helper
                .command()
                .args(["apply", "--force"])
                .assert()
                .success();

            helper.write_schema_file(
                "users.sql",
                "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100));",
            )?;

            // Test SQL format
            helper
                .command()
                .args(["diff", "--format", "sql"])
                .assert()
                .code(1)
                .stdout(predicate::str::contains("-- SQL to bring"))
                .stdout(predicate::str::contains("ALTER TABLE"));

            Ok(())
        })
        .await
    }

    /// Test --format json output
    #[tokio::test]
    async fn test_diff_format_json() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL PRIMARY KEY);")?;

            helper
                .command()
                .args(["apply", "--force"])
                .assert()
                .success();

            helper.write_schema_file(
                "users.sql",
                "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100));",
            )?;

            // Test JSON format - just verify it contains JSON-like output
            // The exact structure will be validated by looking for key JSON elements
            helper
                .command()
                .args(["diff", "--format", "json"])
                .assert()
                .code(1)
                .stdout(predicate::str::contains("has_differences"))
                .stdout(predicate::str::contains("summary"))
                .stdout(predicate::str::contains("changes"));

            Ok(())
        })
        .await
    }

    /// Test --format detailed shows actual diff for view changes
    #[tokio::test]
    async fn test_diff_format_detailed_shows_diff() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Create a view
            helper.write_schema_file(
                "tables/users.sql",
                "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);",
            )?;
            helper.write_schema_file(
                "views/user_list.sql",
                "CREATE VIEW user_list AS SELECT id FROM users;",
            )?;

            helper
                .command()
                .args(["apply", "--force"])
                .assert()
                .success();

            // Modify the view significantly (add a column)
            helper.write_schema_file(
                "views/user_list.sql",
                "CREATE VIEW user_list AS SELECT id, name FROM users;",
            )?;

            // Test detailed format shows the change
            // Note: Depending on dependencies, this might be DROP+CREATE or REPLACE
            // Both are valid, so we just check that differences are shown
            helper
                .command()
                .args(["diff", "--format", "detailed"])
                .assert()
                .code(1)
                .stdout(predicate::str::contains("Found"))
                .stdout(predicate::str::contains("differences"))
                .stdout(
                    predicate::str::contains("Diff:")
                        .or(predicate::str::contains("DROP VIEW"))
                        .or(predicate::str::contains("CREATE VIEW")),
                );

            Ok(())
        })
        .await
    }
}

// Note: The diff_sources module was removed because pgmt diff no longer supports
// --from and --to options. It now always compares schema files vs dev database.
// Use `pgmt migrate diff` to compare schema files vs target database.

mod diff_output_file {
    use super::*;
    use std::fs;

    /// Test --output-sql saves to file
    #[tokio::test]
    async fn test_diff_output_sql_to_file() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL PRIMARY KEY);")?;

            helper
                .command()
                .args(["apply", "--force"])
                .assert()
                .success();

            helper.write_schema_file(
                "users.sql",
                "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100));",
            )?;

            let output_file = helper.project_root.join("drift.sql");

            // Generate SQL to file
            helper
                .command()
                .args([
                    "diff",
                    "--format",
                    "sql",
                    "--output-sql",
                    output_file.to_str().unwrap(),
                ])
                .assert()
                .code(1)
                .stdout(predicate::str::contains("SQL saved to"));

            // Verify file was created and contains SQL
            assert!(output_file.exists());
            let content = fs::read_to_string(&output_file)?;
            assert!(content.contains("ALTER TABLE"));
            assert!(content.contains("-- SQL to bring"));

            Ok(())
        })
        .await
    }
}

mod diff_exit_codes {
    use super::*;

    /// Test exit code 0 when no differences
    #[tokio::test]
    async fn test_diff_exit_code_no_changes() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL PRIMARY KEY);")?;

            helper
                .command()
                .args(["apply", "--force"])
                .assert()
                .success();

            // Exit code 0 for no differences
            helper.command().arg("diff").assert().success();

            Ok(())
        })
        .await
    }

    /// Test exit code 1 when differences found
    #[tokio::test]
    async fn test_diff_exit_code_with_changes() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL PRIMARY KEY);")?;

            helper
                .command()
                .args(["apply", "--force"])
                .assert()
                .success();

            helper.write_schema_file(
                "users.sql",
                "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100));",
            )?;

            // Exit code 1 for differences found
            helper.command().arg("diff").assert().code(1);

            Ok(())
        })
        .await
    }
}
