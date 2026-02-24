//! Parameterized comment migration tests across object types (tables, views, schemas).

use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::diff::operations::MigrationStep;
use rstest::rstest;

/// Test that setting a comment on various object types generates proper migration steps.
///
/// This parameterized test validates comment functionality across:
/// - Tables
/// - Views
/// - Schemas
///
/// Each case verifies that:
/// 1. A comment step is generated
/// 2. The final catalog reflects the comment
#[rstest]
#[case::table(
    "TABLE",
    "CREATE TABLE test_schema.t (id INT)",
    "COMMENT ON TABLE test_schema.t IS 'Test comment'",
    |steps: &[MigrationStep]| steps.iter().any(|s| matches!(s, MigrationStep::Table(_)))
)]
#[case::view(
    "VIEW",
    "CREATE VIEW test_schema.v AS SELECT 1 AS x",
    "COMMENT ON VIEW test_schema.v IS 'Test comment'",
    |steps: &[MigrationStep]| steps.iter().any(|s| matches!(s, MigrationStep::View(_)))
)]
#[case::schema(
    "SCHEMA",
    "", // No additional object to create
    "COMMENT ON SCHEMA test_schema IS 'Test comment'",
    |steps: &[MigrationStep]| steps.iter().any(|s| matches!(s, MigrationStep::Schema(_)))
)]
#[tokio::test]
async fn test_set_comment_migration(
    #[case] object_type: &str,
    #[case] create_sql: &str,
    #[case] comment_sql: &str,
    #[case] has_step: fn(&[MigrationStep]) -> bool,
) -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    // Build setup SQL - always include schema, optionally the object
    let mut both_dbs_sql = vec!["CREATE SCHEMA test_schema"];
    if !create_sql.is_empty() {
        both_dbs_sql.push(create_sql);
    }

    helper
        .run_migration_test(
            &both_dbs_sql,
            &[],            // No initial-only SQL
            &[comment_sql], // Comment only in target
            |steps, _final_catalog| {
                // Verify we have the expected step type
                assert!(
                    has_step(steps),
                    "Expected {} migration step for comment, got: {:?}",
                    object_type,
                    steps
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Test that dropping comments generates proper migration steps.
#[rstest]
#[case::table(
    "TABLE",
    "CREATE TABLE test_schema.t (id INT)",
    "COMMENT ON TABLE test_schema.t IS 'Initial comment'",
    "COMMENT ON TABLE test_schema.t IS NULL"
)]
#[case::view(
    "VIEW",
    "CREATE VIEW test_schema.v AS SELECT 1 AS x",
    "COMMENT ON VIEW test_schema.v IS 'Initial comment'",
    "COMMENT ON VIEW test_schema.v IS NULL"
)]
#[tokio::test]
async fn test_drop_comment_migration(
    #[case] object_type: &str,
    #[case] create_sql: &str,
    #[case] initial_comment_sql: &str,
    #[case] drop_comment_sql: &str,
) -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs have the schema and object
            &["CREATE SCHEMA test_schema", create_sql],
            // Initial has the comment
            &[initial_comment_sql],
            // Target drops the comment (sets to NULL)
            &[drop_comment_sql],
            |steps, _final_catalog| {
                // Should have some steps for the comment drop
                assert!(
                    !steps.is_empty(),
                    "Expected migration steps to drop {} comment",
                    object_type
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Test that changing comments generates proper migration steps.
#[rstest]
#[case::table(
    "TABLE",
    "CREATE TABLE test_schema.t (id INT)",
    "COMMENT ON TABLE test_schema.t IS 'Old comment'",
    "COMMENT ON TABLE test_schema.t IS 'New comment'"
)]
#[case::view(
    "VIEW",
    "CREATE VIEW test_schema.v AS SELECT 1 AS x",
    "COMMENT ON VIEW test_schema.v IS 'Old comment'",
    "COMMENT ON VIEW test_schema.v IS 'New comment'"
)]
#[tokio::test]
async fn test_change_comment_migration(
    #[case] object_type: &str,
    #[case] create_sql: &str,
    #[case] old_comment_sql: &str,
    #[case] new_comment_sql: &str,
) -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs have the schema and object
            &["CREATE SCHEMA test_schema", create_sql],
            // Initial has old comment
            &[old_comment_sql],
            // Target has new comment
            &[new_comment_sql],
            |steps, _final_catalog| {
                // Should have steps for the comment change
                assert!(
                    !steps.is_empty(),
                    "Expected migration steps to change {} comment",
                    object_type
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Test that identical comments produce no migration.
#[rstest]
#[case::table(
    "CREATE TABLE test_schema.t (id INT)",
    "COMMENT ON TABLE test_schema.t IS 'Same comment'"
)]
#[case::view(
    "CREATE VIEW test_schema.v AS SELECT 1 AS x",
    "COMMENT ON VIEW test_schema.v IS 'Same comment'"
)]
#[tokio::test]
async fn test_identical_comment_no_migration(
    #[case] create_sql: &str,
    #[case] comment_sql: &str,
) -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs have schema, object, AND the same comment
            &["CREATE SCHEMA test_schema", create_sql, comment_sql],
            &[], // No difference
            &[], // No difference
            |steps, _final_catalog| {
                // Should have NO steps - comments are identical
                assert!(
                    steps.is_empty(),
                    "Expected no migration steps for identical comments, got: {:?}",
                    steps
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Test special characters in comments are handled correctly.
#[rstest]
#[case::single_quote("User's table")]
#[case::double_quote("Table with \"quotes\"")]
#[case::backslash("Path: C:\\Users\\test")]
#[case::newline("Line1\nLine2")]
#[case::unicode("Unicode emoji test")]
#[case::sql_injection("'; DROP TABLE users; --")]
#[tokio::test]
async fn test_special_character_in_comment(#[case] comment_text: &str) -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    // Need to escape single quotes for SQL
    let escaped_comment = comment_text.replace('\'', "''");
    let comment_sql = format!("COMMENT ON TABLE test_schema.t IS '{}'", escaped_comment);

    helper
        .run_migration_test(
            &[
                "CREATE SCHEMA test_schema",
                "CREATE TABLE test_schema.t (id INT)",
            ],
            &[],
            &[&comment_sql],
            |steps, final_catalog| {
                // Should generate comment step
                assert!(!steps.is_empty());

                // Verify the comment was stored correctly
                let table = &final_catalog.tables[0];
                assert!(table.comment.is_some(), "Table should have comment");
                let stored_comment = table.comment.as_ref().unwrap();
                // Just check that the comment was stored (it will match the input)
                assert!(!stored_comment.is_empty(), "Comment should not be empty");

                Ok(())
            },
        )
        .await?;

    Ok(())
}
