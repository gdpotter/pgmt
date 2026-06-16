use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::diff::operations::{CommentOperation, MigrationStep, TableOperation, ViewOperation};

#[tokio::test]
async fn test_table_comment_ordering() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: just the schema
            &["CREATE SCHEMA test_schema"],
            // Initial DB only: nothing extra
            &[],
            // Target DB only: table with comment
            &[
                "CREATE TABLE test_schema.users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )",
                "COMMENT ON TABLE test_schema.users IS 'User account information'",
            ],
            // Verification closure
            |steps, _final_catalog| {
                assert!(!steps.is_empty());

                // Find table creation and comment steps
                let table_pos = steps.iter().position(|s| {
                matches!(s, MigrationStep::Table(TableOperation::Create { schema, name, .. })
                    if schema == "test_schema" && name == "users")
            }).expect("Should have table creation step");

                let comment_pos = steps
                    .iter()
                    .position(|s| matches!(s, MigrationStep::Comment(_)))
                    .expect("Should have table comment step");

                // Comment should come AFTER table creation
                assert!(
                    table_pos < comment_pos,
                    "Table creation (position {}) should come before comment (position {})",
                    table_pos,
                    comment_pos
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_view_comment_ordering() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema and base table
            &[
                "CREATE SCHEMA test_schema",
                "CREATE TABLE test_schema.users (id INTEGER, name TEXT)",
            ],
            // Initial DB only: nothing extra
            &[],
            // Target DB only: view with comment
            &[
                "CREATE VIEW test_schema.active_users AS
            SELECT * FROM test_schema.users WHERE name IS NOT NULL",
                "COMMENT ON VIEW test_schema.active_users IS 'Users with names'",
            ],
            // Verification closure
            |steps, _final_catalog| {
                assert!(!steps.is_empty());

                // Find view creation and comment steps
                let view_pos = steps
                    .iter()
                    .position(|s| {
                        matches!(s, MigrationStep::View(ViewOperation::Create { schema, name, .. })
                    if schema == "test_schema" && name == "active_users")
                    })
                    .expect("Should have view creation step");

                let comment_pos = steps
                    .iter()
                    .position(|s| matches!(s, MigrationStep::Comment(_)))
                    .expect("Should have view comment step");

                // Comment should come AFTER view creation
                assert!(
                    view_pos < comment_pos,
                    "View creation (position {}) should come before comment (position {})",
                    view_pos,
                    comment_pos
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_multiple_object_comment_ordering() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: just schema
            &["CREATE SCHEMA test_schema"],
            // Initial DB only: nothing extra
            &[],
            // Target DB only: table, view, and comments for both
            &[
                "CREATE TABLE test_schema.users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT
            )",
                "COMMENT ON TABLE test_schema.users IS 'User accounts'",
                "CREATE VIEW test_schema.user_emails AS
            SELECT id, email FROM test_schema.users WHERE email IS NOT NULL",
                "COMMENT ON VIEW test_schema.user_emails IS 'Users with email addresses'",
            ],
            // Verification closure
            |steps, _final_catalog| {
                assert!(!steps.is_empty());

                // Find all relevant step positions
                let table_pos = steps
                    .iter()
                    .position(|s| {
                        matches!(s, MigrationStep::Table(TableOperation::Create { name, .. })
                    if name == "users")
                    })
                    .expect("Should have table creation step");

                let view_pos = steps
                    .iter()
                    .position(|s| {
                        matches!(s, MigrationStep::View(ViewOperation::Create { name, .. })
                    if name == "user_emails")
                    })
                    .expect("Should have view creation step");

                // Comments are flat steps now, so distinguish them by their target.
                let table_comment_pos = steps
                    .iter()
                    .position(|s| {
                        matches!(s, MigrationStep::Comment(CommentOperation::Set { target, .. })
                        if target.name() == "users")
                    })
                    .expect("Should have table comment step");

                let view_comment_pos = steps
                    .iter()
                    .position(|s| {
                        matches!(s, MigrationStep::Comment(CommentOperation::Set { target, .. })
                        if target.name() == "user_emails")
                    })
                    .expect("Should have view comment step");

                // Verify ordering constraints:
                // 1. Table before table comment
                assert!(
                    table_pos < table_comment_pos,
                    "Table creation should come before table comment"
                );

                // 2. Table before view (dependency)
                assert!(
                    table_pos < view_pos,
                    "Table creation should come before view creation"
                );

                // 3. View before view comment
                assert!(
                    view_pos < view_comment_pos,
                    "View creation should come before view comment"
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_table_modification_with_comment_ordering() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema and initial table
            &[
                "CREATE SCHEMA test_schema",
                "CREATE TABLE test_schema.users (id INTEGER)",
            ],
            // Initial DB only: nothing extra
            &[],
            // Target DB only: add column and comment
            &[
                "DROP TABLE test_schema.users",
                "CREATE TABLE test_schema.users (
                id INTEGER,
                name TEXT NOT NULL
            )",
                "COMMENT ON TABLE test_schema.users IS 'Updated user table'",
            ],
            // Verification closure
            |steps, _final_catalog| {
                assert!(!steps.is_empty());

                // Find table modification and comment steps
                let alter_pos = steps
                    .iter()
                    .position(|s| {
                        matches!(s, MigrationStep::Table(TableOperation::Alter { name, .. })
                    if name == "users")
                    })
                    .expect("Should have table alteration step");

                let comment_pos = steps
                    .iter()
                    .position(|s| matches!(s, MigrationStep::Comment(_)))
                    .expect("Should have table comment step");

                // Comment should come AFTER table modification
                assert!(
                    alter_pos < comment_pos,
                    "Table modification (position {}) should come before comment (position {})",
                    alter_pos,
                    comment_pos
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_complex_dependency_chain_with_comments() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema only
            &["CREATE SCHEMA test_schema"],
            // Initial DB only: nothing extra
            &[],
            // Target DB only: complex dependency chain with comments
            &[
                "CREATE TYPE test_schema.user_status AS ENUM ('active', 'inactive')",
                "COMMENT ON TYPE test_schema.user_status IS 'User account status'",
                "CREATE TABLE test_schema.users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                status test_schema.user_status DEFAULT 'active'
            )",
                "COMMENT ON TABLE test_schema.users IS 'User accounts with status'",
                "CREATE VIEW test_schema.active_users AS
            SELECT * FROM test_schema.users WHERE status = 'active'",
                "COMMENT ON VIEW test_schema.active_users IS 'Currently active users'",
            ],
            // Verification closure
            |steps, _final_catalog| {
                assert!(!steps.is_empty());

                // Find all step positions
                let type_pos = steps
                    .iter()
                    .position(|s| matches!(s, MigrationStep::Type(_)))
                    .expect("Should have type creation step");

                let table_pos = steps
                    .iter()
                    .position(|s| matches!(s, MigrationStep::Table(TableOperation::Create { .. })))
                    .expect("Should have table creation step");

                let view_pos = steps
                    .iter()
                    .position(|s| matches!(s, MigrationStep::View(ViewOperation::Create { .. })))
                    .expect("Should have view creation step");

                // A comment's position, found by the object it annotates.
                let comment_pos = |obj_name: &str| {
                    steps
                        .iter()
                        .position(|s| {
                            matches!(s, MigrationStep::Comment(CommentOperation::Set { target, .. })
                            if target.name() == obj_name)
                        })
                        .unwrap_or_else(|| panic!("missing comment for {obj_name}"))
                };

                // Verify dependency ordering:
                // 1. Type before table (table depends on type)
                assert!(type_pos < table_pos, "Type should come before table");

                // 2. Table before view (view depends on table)
                assert!(table_pos < view_pos, "Table should come before view");

                // 3. Each comment comes after the object it annotates.
                assert!(type_pos < comment_pos("user_status"));
                assert!(table_pos < comment_pos("users"));
                assert!(view_pos < comment_pos("active_users"));

                Ok(())
            },
        )
        .await?;

    Ok(())
}
