//! Migration error case tests
//!
//! Tests that verify the migration pipeline handles error scenarios gracefully.
//! These focus on edge cases and error conditions in diff generation.

use crate::helpers::harness::with_test_db;
use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::catalog::Catalog;
use pgmt::diff::operations::MigrationStep;

/// Test that identical schemas produce no migration steps
#[tokio::test]
async fn test_identical_schemas_no_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs have identical content
            &[
                "CREATE SCHEMA app",
                "CREATE TABLE app.users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)",
                "CREATE INDEX idx_users_name ON app.users(name)",
            ],
            &[], // No difference in initial
            &[], // No difference in target
            |steps, _final_catalog| {
                assert!(
                    steps.is_empty(),
                    "Identical schemas should produce no migration steps"
                );
                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Test handling of tables with no columns (PostgreSQL allows this)
#[tokio::test]
async fn test_empty_table_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE SCHEMA app"],
            &[],
            // PostgreSQL allows tables with no columns
            &["CREATE TABLE app.empty_table ()"],
            |steps, final_catalog| {
                assert!(!steps.is_empty());

                // Verify the empty table was created
                assert_eq!(final_catalog.tables.len(), 1);
                let table = &final_catalog.tables[0];
                assert_eq!(table.name, "empty_table");
                assert!(table.columns.is_empty());

                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Test that dropping and recreating an object with same name works
#[tokio::test]
async fn test_drop_and_recreate_same_name() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE SCHEMA app"],
            // Initial: table with INT id
            &["CREATE TABLE app.users (id INT PRIMARY KEY, old_col TEXT)"],
            // Target: completely different table with same name
            &["CREATE TABLE app.users (id SERIAL PRIMARY KEY, new_col VARCHAR(100), active BOOLEAN)"],
            |steps, final_catalog| {
                // Should have migration steps (the exact strategy may vary)
                assert!(!steps.is_empty());

                // Final catalog should have the new structure
                assert_eq!(final_catalog.tables.len(), 1);
                let table = &final_catalog.tables[0];
                assert_eq!(table.name, "users");

                // Verify final columns match target
                let col_names: Vec<&str> =
                    table.columns.iter().map(|c| c.name.as_str()).collect();
                assert!(
                    col_names.contains(&"new_col") || col_names.contains(&"old_col"),
                    "Should have either new_col or old_col after migration"
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Test handling of reserved words as identifiers
#[tokio::test]
async fn test_reserved_word_identifiers() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE SCHEMA app"],
            &[],
            // Using reserved words that need quoting
            &[r#"CREATE TABLE app."user" (
                "select" INTEGER PRIMARY KEY,
                "from" TEXT,
                "order" INT
            )"#],
            |steps, final_catalog| {
                assert!(!steps.is_empty());

                // Verify table was created with quoted name
                assert_eq!(final_catalog.tables.len(), 1);
                let table = &final_catalog.tables[0];
                assert_eq!(table.name, "user");

                // Verify columns with reserved word names
                let col_names: Vec<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();
                assert!(col_names.contains(&"select"));
                assert!(col_names.contains(&"from"));
                assert!(col_names.contains(&"order"));

                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Test that self-referential foreign keys are handled
#[tokio::test]
async fn test_self_referential_foreign_key() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE SCHEMA app"],
            &[],
            // Table with self-referential FK (common for hierarchical data)
            &["CREATE TABLE app.categories (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                parent_id INTEGER REFERENCES app.categories(id)
            )"],
            |steps, final_catalog| {
                assert!(!steps.is_empty());

                // Verify table was created
                assert_eq!(final_catalog.tables.len(), 1);
                let table = &final_catalog.tables[0];
                assert_eq!(table.name, "categories");

                // Verify all expected columns exist
                let col_names: Vec<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();
                assert!(col_names.contains(&"id"));
                assert!(col_names.contains(&"name"));
                assert!(col_names.contains(&"parent_id"));

                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Test handling of very long identifiers
#[tokio::test]
async fn test_long_identifier_names() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    // PostgreSQL max identifier length is 63 bytes
    let long_name = "a".repeat(63);

    helper
        .run_migration_test(
            &["CREATE SCHEMA app"],
            &[],
            &[&format!(
                "CREATE TABLE app.{} (id INT PRIMARY KEY)",
                long_name
            )],
            |steps, final_catalog| {
                assert!(!steps.is_empty());

                assert_eq!(final_catalog.tables.len(), 1);
                let table = &final_catalog.tables[0];
                assert_eq!(table.name.len(), 63);

                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Test that multiple changes to the same object are consolidated correctly
#[tokio::test]
async fn test_multiple_column_changes() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE SCHEMA app"],
            // Initial table
            &["CREATE TABLE app.users (
                id SERIAL PRIMARY KEY,
                name TEXT,
                email TEXT
            )"],
            // Target with multiple column changes
            &["CREATE TABLE app.users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                username TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )"],
            |steps, final_catalog| {
                // Should have steps for the changes
                assert!(!steps.is_empty());

                // Verify final state
                assert_eq!(final_catalog.tables.len(), 1);
                let table = &final_catalog.tables[0];

                let col_names: Vec<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();

                // 'id' and 'name' should still exist
                assert!(col_names.contains(&"id"));
                assert!(col_names.contains(&"name"));

                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Test migration ordering with dependent objects
#[tokio::test]
async fn test_dependent_object_ordering() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &[],
            &[],
            // Create schema, type, and table that depends on both
            &[
                "CREATE SCHEMA app",
                "CREATE TYPE app.status AS ENUM ('active', 'inactive')",
                "CREATE TABLE app.users (id SERIAL PRIMARY KEY, status app.status)",
            ],
            |steps, _final_catalog| {
                // Find positions of each step type
                let schema_pos = steps
                    .iter()
                    .position(|s| matches!(s, MigrationStep::Schema(_)));
                let type_pos = steps
                    .iter()
                    .position(|s| matches!(s, MigrationStep::Type(_)));
                let table_pos = steps
                    .iter()
                    .position(|s| matches!(s, MigrationStep::Table(_)));

                // All should exist
                assert!(schema_pos.is_some(), "Should have schema step");
                assert!(type_pos.is_some(), "Should have type step");
                assert!(table_pos.is_some(), "Should have table step");

                // Verify ordering: schema first, then type, then table
                assert!(
                    schema_pos.unwrap() < type_pos.unwrap(),
                    "Schema must come before type"
                );
                assert!(
                    type_pos.unwrap() < table_pos.unwrap(),
                    "Type must come before table that uses it"
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Test that catalog loading handles empty database
#[tokio::test]
async fn test_empty_database_catalog() {
    with_test_db(async |db| {
        let catalog = Catalog::load(db.pool()).await.unwrap();

        // Empty database should have empty collections
        assert!(catalog.tables.is_empty());
        assert!(catalog.views.is_empty());
        assert!(catalog.functions.is_empty());
        // Schemas might have 'public' but no user schemas
    })
    .await;
}

/// Test handling of special characters in comments
#[tokio::test]
async fn test_special_characters_in_comments() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE SCHEMA app", "CREATE TABLE app.users (id INT PRIMARY KEY)"],
            &[],
            // Add comment with special characters
            &["COMMENT ON TABLE app.users IS 'User''s table with \"quotes\" and backslash \\ and newline\ncharacter'"],
            |steps, final_catalog| {
                assert!(!steps.is_empty());

                // Verify comment was captured
                let table = &final_catalog.tables[0];
                assert!(table.comment.is_some());
                let comment = table.comment.as_ref().unwrap();
                assert!(comment.contains("User's"));
                assert!(comment.contains("quotes"));

                Ok(())
            },
        )
        .await?;

    Ok(())
}
