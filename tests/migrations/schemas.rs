use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::diff::operations::{CommentOperation, MigrationStep, SchemaOperation};

#[tokio::test]
async fn test_create_schema_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: empty (default public schema only)
            &[],
            // Initial DB only: nothing extra
            &[],
            // Target DB only: add the schema
            &["CREATE SCHEMA test_schema"],
            // Verification closure
            |steps, final_catalog| {
                // Should have CREATE SCHEMA step
                assert!(!steps.is_empty());
                let create_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Schema(SchemaOperation::Create { name })
                    if name == "test_schema")
                    })
                    .expect("Should have CreateSchema step");

                match create_step {
                    MigrationStep::Schema(SchemaOperation::Create { name }) => {
                        assert_eq!(name, "test_schema");
                    }
                    _ => panic!("Expected CreateSchema step"),
                }

                // Verify final state
                let test_schema = final_catalog
                    .schemas
                    .iter()
                    .find(|s| s.name == "test_schema")
                    .expect("Should have test_schema");
                assert_eq!(test_schema.name, "test_schema");

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_drop_schema_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: empty (default public schema only)
            &[],
            // Initial DB only: has the schema
            &["CREATE SCHEMA old_schema"],
            // Target DB only: nothing extra (schema is missing)
            &[],
            // Verification closure
            |steps, final_catalog| {
                // Should have DROP SCHEMA step
                assert!(!steps.is_empty());
                assert!(
                    steps.iter().any(|s| {
                        matches!(s, MigrationStep::Schema(SchemaOperation::Drop { name })
                    if name == "old_schema")
                    }),
                    "Should have DropSchema step"
                );

                // Verify final state - schema completely removed
                let has_old_schema = final_catalog.schemas.iter().any(|s| s.name == "old_schema");
                assert!(!has_old_schema);

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_schema_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema exists
            &["CREATE SCHEMA test_schema"],
            // Initial DB only: no comment
            &[],
            // Target DB only: add comment
            &["COMMENT ON SCHEMA test_schema IS 'Test schema for migrations'"],
            // Verification closure
            |steps, final_catalog| {
                // Should have SET COMMENT step
                assert!(!steps.is_empty());
                let _comment_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Schema(SchemaOperation::Comment(
                    CommentOperation::Set { target, .. }
                )) if target.name == "test_schema")
                    })
                    .expect("Should have SetComment step");

                // Verify final state
                let test_schema = final_catalog
                    .schemas
                    .iter()
                    .find(|s| s.name == "test_schema")
                    .expect("Should have test_schema");
                assert_eq!(test_schema.name, "test_schema");
                assert_eq!(
                    test_schema.comment,
                    Some("Test schema for migrations".to_string())
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_drop_schema_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema exists
            &["CREATE SCHEMA test_schema"],
            // Initial DB only: has comment
            &["COMMENT ON SCHEMA test_schema IS 'Test schema for migrations'"],
            // Target DB only: nothing extra (comment is missing)
            &[],
            // Verification closure
            |steps, final_catalog| {
                // Should have DROP COMMENT step
                assert!(!steps.is_empty());
                let _comment_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Schema(SchemaOperation::Comment(
                    CommentOperation::Drop { target }
                )) if target.name == "test_schema")
                    })
                    .expect("Should have DropComment step");

                // Verify final state
                let test_schema = final_catalog
                    .schemas
                    .iter()
                    .find(|s| s.name == "test_schema")
                    .expect("Should have test_schema");
                assert_eq!(test_schema.name, "test_schema");
                assert_eq!(test_schema.comment, None);

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_schema_with_objects_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: empty
            &[],
            // Initial DB only: nothing
            &[],
            // Target DB only: schema with table
            &[
                "CREATE SCHEMA test_schema",
                "CREATE TABLE test_schema.users (id INTEGER, name TEXT)",
            ],
            // Verification closure
            |steps, final_catalog| {
                // Should have CREATE SCHEMA and CREATE TABLE steps
                assert!(steps.len() >= 2);

                let _schema_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Schema(SchemaOperation::Create { name })
                    if name == "test_schema")
                    })
                    .expect("Should have CreateSchema step");

                // Verify final state
                let test_schema = final_catalog
                    .schemas
                    .iter()
                    .find(|s| s.name == "test_schema")
                    .expect("Should have test_schema");
                assert_eq!(test_schema.name, "test_schema");

                // Should also have the table
                assert_eq!(final_catalog.tables.len(), 1);
                let table = &final_catalog.tables[0];
                assert_eq!(table.schema, "test_schema");
                assert_eq!(table.name, "users");

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_public_schema_default_comment_ignored() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: empty (will have public schema by default)
            &[],
            // Initial DB only: add PostgreSQL's default comment to public
            &["COMMENT ON SCHEMA public IS 'standard public schema'"],
            // Target DB only: public has no comment (as if recreated by cleaner)
            &[],
            // Verification closure
            |steps, _final_catalog| {
                // Should have NO comment diff steps - default comment should be normalized
                let has_comment_step = steps.iter().any(|s| {
                    matches!(s, MigrationStep::Schema(SchemaOperation::Comment(_)))
                });
                assert!(
                    !has_comment_step,
                    "PostgreSQL's default 'standard public schema' comment should not generate a diff"
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_public_schema_custom_comment_detected() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: empty (will have public schema by default)
            &[],
            // Initial DB only: no custom comment on public
            &[],
            // Target DB only: add a custom comment to public
            &["COMMENT ON SCHEMA public IS 'My custom public schema'"],
            // Verification closure
            |steps, final_catalog| {
                // Should have SET COMMENT step for custom comment
                assert!(!steps.is_empty());
                let _comment_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Schema(SchemaOperation::Comment(
                    CommentOperation::Set { target, comment }
                )) if target.name == "public" && comment == "My custom public schema")
                    })
                    .expect("Should detect custom comment on public schema");

                // Verify final state
                let public_schema = final_catalog
                    .schemas
                    .iter()
                    .find(|s| s.name == "public")
                    .expect("Should have public schema");
                assert_eq!(
                    public_schema.comment,
                    Some("My custom public schema".to_string())
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_public_schema_removing_custom_comment() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: empty (will have public schema by default)
            &[],
            // Initial DB only: has a custom comment
            &["COMMENT ON SCHEMA public IS 'My custom comment'"],
            // Target DB only: comment removed (or back to default)
            &["COMMENT ON SCHEMA public IS 'standard public schema'"],
            // Verification closure
            |steps, final_catalog| {
                // Should have DROP COMMENT step (normalized to NULL)
                assert!(!steps.is_empty());
                let _comment_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Schema(SchemaOperation::Comment(
                    CommentOperation::Drop { target }
                )) if target.name == "public")
                    })
                    .expect("Should detect removal of custom comment (back to default)");

                // Verify final state - should be normalized to None
                let public_schema = final_catalog
                    .schemas
                    .iter()
                    .find(|s| s.name == "public")
                    .expect("Should have public schema");
                // The default comment should be normalized to None
                assert_eq!(public_schema.comment, None);

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_non_public_schema_comment_not_normalized() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: create a non-public schema
            &["CREATE SCHEMA other_schema"],
            // Initial DB only: no comment
            &[],
            // Target DB only: add the exact same "standard public schema" comment
            &["COMMENT ON SCHEMA other_schema IS 'standard public schema'"],
            // Verification closure
            |steps, final_catalog| {
                // Should have SET COMMENT step - normalization only applies to public schema
                assert!(!steps.is_empty());
                let _comment_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Schema(SchemaOperation::Comment(
                    CommentOperation::Set { target, comment }
                )) if target.name == "other_schema" && comment == "standard public schema")
                    })
                    .expect("Non-public schemas should not have comment normalization");

                // Verify final state
                let other_schema = final_catalog
                    .schemas
                    .iter()
                    .find(|s| s.name == "other_schema")
                    .expect("Should have other_schema");
                assert_eq!(
                    other_schema.comment,
                    Some("standard public schema".to_string())
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}
