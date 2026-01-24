use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::config::ColumnOrderMode;
use pgmt::validation::{apply_column_order_validation, validate_column_order};

/// Tests for column order validation during migration generation.
///
/// PostgreSQL's `ALTER TABLE ADD COLUMN` always appends columns to the end.
/// These tests verify that pgmt validates new columns are placed at the end
/// of table definitions to ensure schema files match physical column order.

#[tokio::test]
async fn test_column_order_new_column_at_end_valid() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &[],
            // Initial: table without email column
            &["CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)"],
            // Target: new column 'email' at the end - valid
            &["CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT)"],
            |steps, _final_catalog| {
                // Should have an alter table step to add the email column
                assert!(
                    !steps.is_empty(),
                    "Should generate migration steps for adding column"
                );
                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_column_order_new_column_in_middle_detected() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    // This test directly uses the validation function to test column order detection
    helper
        .run_catalogs_test(
            &[],
            // Initial: table without email column
            &["CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)"],
            // Target: new column 'email' in the middle - should be detected as violation
            &["CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT, name TEXT NOT NULL)"],
            |old_catalog, new_catalog| {
                let violations = validate_column_order(&old_catalog, &new_catalog);
                assert_eq!(violations.len(), 1);
                assert_eq!(violations[0].schema, "public");
                assert_eq!(violations[0].table, "users");
                assert_eq!(violations[0].new_column, "email");
                assert_eq!(violations[0].old_column_after, "name");
                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_column_order_strict_mode_rejects_middle_column() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_catalogs_test(
            &[],
            &["CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)"],
            &["CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT, name TEXT NOT NULL)"],
            |old_catalog, new_catalog| {
                let result = apply_column_order_validation(
                    &old_catalog,
                    &new_catalog,
                    ColumnOrderMode::Strict,
                );
                assert!(result.is_err());
                let err = result.unwrap_err().to_string();
                assert!(err.contains("Column order validation failed"));
                assert!(err.contains("email"));
                assert!(err.contains("name"));
                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_column_order_relaxed_mode_allows_middle_column() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_catalogs_test(
            &[],
            &["CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)"],
            &["CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT, name TEXT NOT NULL)"],
            |old_catalog, new_catalog| {
                let result = apply_column_order_validation(
                    &old_catalog,
                    &new_catalog,
                    ColumnOrderMode::Relaxed,
                );
                assert!(result.is_ok());
                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_column_order_new_table_no_validation_needed() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_catalogs_test(
            &[], // No common SQL
            &[], // No existing tables
            // New table with any column order is fine
            &["CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT, name TEXT NOT NULL)"],
            |old_catalog, new_catalog| {
                let violations = validate_column_order(&old_catalog, &new_catalog);
                assert!(
                    violations.is_empty(),
                    "New tables should not have column order violations"
                );
                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_column_order_multiple_new_columns_at_end() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_catalogs_test(
            &[],
            &["CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)"],
            // Multiple new columns at the end - valid
            &["CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT, phone TEXT, created_at TIMESTAMP)"],
            |old_catalog, new_catalog| {
                let violations = validate_column_order(&old_catalog, &new_catalog);
                assert!(
                    violations.is_empty(),
                    "Multiple new columns at the end should be valid"
                );
                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_column_order_dropped_column_does_not_affect_validation() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_catalogs_test(
            &[],
            &["CREATE TABLE users (id SERIAL PRIMARY KEY, legacy_field TEXT, name TEXT NOT NULL)"],
            // Drop legacy_field, add email at end - valid
            &["CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT)"],
            |old_catalog, new_catalog| {
                let violations = validate_column_order(&old_catalog, &new_catalog);
                assert!(
                    violations.is_empty(),
                    "Dropped columns should not affect validation"
                );
                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_column_order_multiple_tables_validated() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_catalogs_test(
            &[],
            &[
                "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)",
                "CREATE TABLE posts (id SERIAL PRIMARY KEY, title TEXT)",
            ],
            // One table valid, one with violation
            &[
                "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT, email TEXT)", // valid
                "CREATE TABLE posts (id SERIAL PRIMARY KEY, content TEXT, title TEXT)", // violation
            ],
            |old_catalog, new_catalog| {
                let violations = validate_column_order(&old_catalog, &new_catalog);
                assert_eq!(violations.len(), 1);
                assert_eq!(violations[0].table, "posts");
                assert_eq!(violations[0].new_column, "content");
                assert_eq!(violations[0].old_column_after, "title");
                Ok(())
            },
        )
        .await?;

    Ok(())
}
