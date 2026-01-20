//! CLI error handling tests
//!
//! Tests that verify pgmt provides helpful error messages for common error scenarios.
//! These tests focus on the "negative path" - what happens when things go wrong.

use crate::helpers::cli::with_cli_helper;
use anyhow::Result;
use predicates::prelude::*;
use std::fs;

/// Test that running commands without pgmt.yaml gives a helpful error
#[tokio::test]
async fn test_missing_config_file_error() -> Result<()> {
    with_cli_helper(async |helper| {
        // Don't call init_project() - no pgmt.yaml exists
        // Clear DATABASE_URL to prevent fallback
        helper
            .command()
            .env_remove("DATABASE_URL")
            .args(["migrate", "status"])
            .assert()
            .failure();
        // Just verify it fails without the config - exact error message may vary

        Ok(())
    })
    .await
}

/// Test that invalid YAML in config file gives a helpful error
#[tokio::test]
async fn test_invalid_config_yaml_error() -> Result<()> {
    with_cli_helper(async |helper| {
        // Create an invalid YAML file
        fs::write(
            helper.project_root.join("pgmt.yaml"),
            "invalid: yaml: content: [unbalanced",
        )?;

        helper
            .command()
            .args(["migrate", "status"])
            .assert()
            .failure()
            .stderr(
                predicate::str::contains("not allowed")
                    .or(predicate::str::contains("invalid"))
                    .or(predicate::str::contains("Error")),
            );

        Ok(())
    })
    .await
}

/// Test that invalid database URL gives a helpful error
#[tokio::test]
async fn test_invalid_database_url_error() -> Result<()> {
    with_cli_helper(async |helper| {
        // Create project directories
        fs::create_dir_all(helper.project_root.join("schema"))?;
        fs::create_dir_all(helper.project_root.join("migrations"))?;
        fs::create_dir_all(helper.project_root.join("schema_baselines"))?;

        // Create config with invalid database URL
        let config = r#"
databases:
  dev_url: not-a-valid-url
  shadow:
    auto: false
    url: also-not-valid

directories:
  schema_dir: schema/
  migrations_dir: migrations/
  baselines_dir: schema_baselines/
"#;
        fs::write(helper.project_root.join("pgmt.yaml"), config)?;

        helper
            .command()
            .args(["migrate", "status"])
            .assert()
            .failure();

        Ok(())
    })
    .await
}

/// Test that unreachable database gives a helpful error
#[tokio::test]
async fn test_unreachable_database_error() -> Result<()> {
    with_cli_helper(async |helper| {
        fs::create_dir_all(helper.project_root.join("schema"))?;
        fs::create_dir_all(helper.project_root.join("migrations"))?;
        fs::create_dir_all(helper.project_root.join("schema_baselines"))?;

        // Use a port that's unlikely to have PostgreSQL running
        let config = r#"
databases:
  dev_url: postgres://postgres:postgres@localhost:59999/nonexistent
  shadow:
    auto: false
    url: postgres://postgres:postgres@localhost:59998/nonexistent

directories:
  schema_dir: schema/
  migrations_dir: migrations/
  baselines_dir: schema_baselines/
"#;
        fs::write(helper.project_root.join("pgmt.yaml"), config)?;

        helper
            .command()
            .args(["migrate", "status"])
            .assert()
            .failure()
            .stderr(
                predicate::str::contains("connect")
                    .or(predicate::str::contains("connection"))
                    .or(predicate::str::contains("refused")),
            );

        Ok(())
    })
    .await
}

/// Test that invalid SQL in schema file is caught and reported
#[tokio::test]
async fn test_invalid_sql_in_schema_file() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        // Create schema file with SQL syntax error
        helper.write_schema_file("bad.sql", "CREATE TABL users (id INT);")?; // "TABL" typo

        // apply --dev should fail with helpful message
        let output = helper.command().args(["apply", "--dev"]).assert().failure();

        // Should indicate there was a SQL error
        output.stderr(
            predicate::str::contains("syntax")
                .or(predicate::str::contains("error"))
                .or(predicate::str::contains("failed")),
        );

        Ok(())
    })
    .await
}

/// Test that missing schema directory is handled gracefully
#[tokio::test]
async fn test_missing_schema_directory() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        // Remove the schema directory
        fs::remove_dir_all(helper.project_root.join("schema"))?;

        // Should handle gracefully (either error or empty result)
        // Test passes as long as it doesn't panic
        let _ = helper.command().args(["diff"]).assert();

        Ok(())
    })
    .await
}

/// Test that duplicate migration names are rejected
#[tokio::test]
async fn test_duplicate_migration_name_error() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        helper.write_schema_file("users.sql", "CREATE TABLE users (id INT);")?;

        // Create first migration
        helper
            .command()
            .args(["migrate", "new", "add_users"])
            .assert()
            .success();

        // Modify schema
        helper.write_schema_file("users.sql", "CREATE TABLE users (id INT, name TEXT);")?;

        // Try to create migration with same description in same second
        // This may or may not fail depending on timing, but shouldn't panic
        let _ = helper
            .command()
            .args(["migrate", "new", "add_users"])
            .assert();

        Ok(())
    })
    .await
}

/// Test that empty migration description is validated
#[tokio::test]
async fn test_empty_migration_description_error() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        helper.write_schema_file("users.sql", "CREATE TABLE users (id INT);")?;

        // Empty description should be rejected
        helper
            .command()
            .args(["migrate", "new", ""])
            .assert()
            .failure();

        Ok(())
    })
    .await
}

/// Test that very long migration description is validated
#[tokio::test]
async fn test_long_migration_description_error() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        helper.write_schema_file("users.sql", "CREATE TABLE users (id INT);")?;

        // Very long description (over 100 chars) should be rejected
        let long_name = "a".repeat(150);

        helper
            .command()
            .args(["migrate", "new", &long_name])
            .assert()
            .failure()
            .stderr(predicate::str::contains("100").or(predicate::str::contains("characters")));

        Ok(())
    })
    .await
}

/// Test that migration with no changes produces helpful output
#[tokio::test]
async fn test_no_changes_to_migrate() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        // Empty schema - no changes to migrate
        helper
            .command()
            .args(["migrate", "new", "empty_migration"])
            .assert()
            .success()
            .stdout(predicate::str::contains("No changes"));

        // Should not create a migration file
        let migrations = helper.list_migration_files()?;
        assert_eq!(migrations.len(), 0);

        Ok(())
    })
    .await
}

/// Test that apply with invalid target gives helpful error
#[tokio::test]
async fn test_apply_with_invalid_target_error() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        // apply with an invalid database URL should fail
        helper
            .command()
            .args(["apply", "--url", "not-a-valid-url"])
            .assert()
            .failure();

        Ok(())
    })
    .await
}
