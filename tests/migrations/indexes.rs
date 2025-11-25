use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::diff::operations::{CommentOperation, IndexOperation, MigrationStep};

#[tokio::test]
async fn test_index_create_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: table
            &["CREATE TABLE users (id SERIAL, email VARCHAR, name VARCHAR)"],
            // Initial DB only: nothing extra
            &[],
            // Target DB only: add index
            &["CREATE INDEX idx_users_email ON users (email)"],
            // Verification closure
            |steps, final_catalog| {
                // Should have CREATE INDEX step
                assert!(!steps.is_empty());
                let create_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Index(IndexOperation::Create(index))
                    if index.name == "idx_users_email")
                    })
                    .expect("Should have CreateIndex step");

                match create_step {
                    MigrationStep::Index(IndexOperation::Create(index)) => {
                        assert_eq!(index.name, "idx_users_email");
                        assert_eq!(index.table_name, "users");
                        assert_eq!(index.columns.len(), 1);
                        assert_eq!(index.columns[0].expression, "email");
                    }
                    _ => panic!("Expected CreateIndex step"),
                }

                // Verify final state
                let created_index = final_catalog
                    .indexes
                    .iter()
                    .find(|i| i.name == "idx_users_email")
                    .expect("Index should be created");

                assert_eq!(created_index.name, "idx_users_email");
                assert_eq!(created_index.table_name, "users");
                assert_eq!(created_index.columns.len(), 1);
                assert_eq!(created_index.columns[0].expression, "email");
                assert!(!created_index.is_unique);

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_index_drop_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: table
            &["CREATE TABLE users (id SERIAL, email VARCHAR)"],
            // Initial DB only: has index
            &["CREATE INDEX idx_users_email ON users (email)"],
            // Target DB only: nothing extra (index missing)
            &[],
            // Verification closure
            |steps, final_catalog| {
                // Should have DROP INDEX step
                assert!(!steps.is_empty());
                let _drop_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Index(IndexOperation::Drop { name, .. })
                    if name == "idx_users_email")
                    })
                    .expect("Should have DropIndex step");

                // Verify final state - index should be gone
                let has_index = final_catalog
                    .indexes
                    .iter()
                    .any(|i| i.name == "idx_users_email");
                assert!(!has_index);

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_unique_index_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: table
            &["CREATE TABLE users (id SERIAL, email VARCHAR UNIQUE)"],
            // Initial DB only: nothing extra
            &[],
            // Target DB only: add unique index explicitly
            &["CREATE UNIQUE INDEX idx_users_email_unique ON users (email)"],
            // Verification closure
            |steps, final_catalog| {
                // Should have CREATE INDEX step
                assert!(!steps.is_empty());
                let create_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Index(IndexOperation::Create(index))
                    if index.name == "idx_users_email_unique")
                    })
                    .expect("Should have CreateIndex step");

                match create_step {
                    MigrationStep::Index(IndexOperation::Create(index)) => {
                        assert_eq!(index.name, "idx_users_email_unique");
                        assert!(index.is_unique);
                    }
                    _ => panic!("Expected CreateIndex step"),
                }

                // Verify final state
                let created_index = final_catalog
                    .indexes
                    .iter()
                    .find(|i| i.name == "idx_users_email_unique")
                    .expect("Unique index should be created");
                assert!(created_index.is_unique);

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_expression_index_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: table
            &["CREATE TABLE users (id SERIAL, email VARCHAR, name VARCHAR)"],
            // Initial DB only: nothing extra
            &[],
            // Target DB only: add expression index
            &["CREATE INDEX idx_users_email_lower ON users (LOWER(email))"],
            // Verification closure
            |steps, final_catalog| {
                // Should have CREATE INDEX step
                assert!(!steps.is_empty());
                let create_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Index(IndexOperation::Create(index))
                    if index.name == "idx_users_email_lower")
                    })
                    .expect("Should have CreateIndex step");

                match create_step {
                    MigrationStep::Index(IndexOperation::Create(index)) => {
                        assert_eq!(index.name, "idx_users_email_lower");
                        assert_eq!(index.table_name, "users");
                        assert_eq!(index.columns.len(), 1);
                        assert!(index.columns[0].expression.contains("lower"));
                    }
                    _ => panic!("Expected CreateIndex step"),
                }

                // Verify final state
                let created_index = final_catalog
                    .indexes
                    .iter()
                    .find(|i| i.name == "idx_users_email_lower")
                    .expect("Expression index should be created");
                assert!(created_index.columns[0].expression.contains("lower"));

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_index_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: table and index
            &[
                "CREATE TABLE users (id SERIAL, email VARCHAR)",
                "CREATE INDEX idx_users_email ON users (email)",
            ],
            // Initial DB only: nothing extra
            &[],
            // Target DB only: add comment
            &["COMMENT ON INDEX idx_users_email IS 'Email lookup index'"],
            // Verification closure
            |steps, final_catalog| {
                // Should have SET COMMENT step
                assert!(!steps.is_empty());
                let _comment_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Index(IndexOperation::Comment(
                    CommentOperation::Set { target, comment }
                )) if target.name == "idx_users_email" && comment == "Email lookup index")
                    })
                    .expect("Should have SetComment step");

                // Verify final state
                let index = final_catalog
                    .indexes
                    .iter()
                    .find(|i| i.name == "idx_users_email")
                    .expect("Index should exist");
                assert_eq!(index.comment, Some("Email lookup index".to_string()));

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_multi_column_index_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: table
        &["CREATE TABLE users (id SERIAL, first_name VARCHAR, last_name VARCHAR, email VARCHAR)"],
        // Initial DB only: nothing extra
        &[],
        // Target DB only: add multi-column index
        &["CREATE INDEX idx_users_name ON users (last_name, first_name)"],
        // Verification closure
        |steps, final_catalog| {
            // Should have CREATE INDEX step
            assert!(!steps.is_empty());
            let create_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::Index(IndexOperation::Create(index))
                    if index.name == "idx_users_name")
            }).expect("Should have CreateIndex step");

            match create_step {
                MigrationStep::Index(IndexOperation::Create(index)) => {
                    assert_eq!(index.name, "idx_users_name");
                    assert_eq!(index.table_name, "users");
                    assert_eq!(index.columns.len(), 2);
                    assert_eq!(index.columns[0].expression, "last_name");
                    assert_eq!(index.columns[1].expression, "first_name");
                }
                _ => panic!("Expected CreateIndex step"),
            }

            // Verify final state
            let created_index = final_catalog.indexes.iter()
                .find(|i| i.name == "idx_users_name")
                .expect("Multi-column index should be created");
            assert_eq!(created_index.columns.len(), 2);

            Ok(())
        }
    ).await?;

    Ok(())
}
