use crate::helpers::harness::with_test_db;
use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::catalog::Catalog;
use pgmt::diff::operations::{CommentOperation, IndexOperation, MigrationStep, TypeOperation};
use pgmt::diff::{cascade, diff_all, diff_order};

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
                assert!(
                    steps.iter().any(|s| {
                        matches!(s, MigrationStep::Index(IndexOperation::Drop { name, .. })
                    if name == "idx_users_email")
                    }),
                    "Should have DropIndex step"
                );

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

#[tokio::test]
async fn test_index_cascade_on_type_change() -> Result<()> {
    with_test_db(async |initial_db| {
        with_test_db(async |target_db| {
            // Initial: composite type, table with that type, expression index
            initial_db
                .execute("CREATE TYPE dimensions AS (width INT, height INT)")
                .await;
            initial_db
                .execute("CREATE TABLE products (id SERIAL PRIMARY KEY, dims dimensions)")
                .await;
            initial_db
                .execute("CREATE INDEX idx_dims ON products (((dims).width))")
                .await;

            // Target: composite type gains a field
            target_db
                .execute("CREATE TYPE dimensions AS (width INT, height INT, depth INT)")
                .await;
            target_db
                .execute("CREATE TABLE products (id SERIAL PRIMARY KEY, dims dimensions)")
                .await;
            target_db
                .execute("CREATE INDEX idx_dims ON products (((dims).width))")
                .await;

            let initial_catalog = Catalog::load(initial_db.pool()).await?;
            let target_catalog = Catalog::load(target_db.pool()).await?;

            let mut steps = diff_all(&initial_catalog, &target_catalog);
            steps = cascade::expand(steps, &initial_catalog, &target_catalog);
            steps = diff_order(steps, &initial_catalog, &target_catalog)?;

            // The type change should cascade to the index
            let drop_index_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::Index(IndexOperation::Drop { name, .. })
                        if name == "idx_dims")
                })
                .expect("Should have DROP INDEX for idx_dims");

            let create_index_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::Index(IndexOperation::Create(index))
                        if index.name == "idx_dims")
                })
                .expect("Should have CREATE INDEX for idx_dims");

            let drop_type_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::Type(TypeOperation::Drop { name, .. })
                        if name == "dimensions")
                })
                .expect("Should have DROP TYPE for dimensions");

            let create_type_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::Type(TypeOperation::Create { name, .. })
                        if name == "dimensions")
                })
                .expect("Should have CREATE TYPE for dimensions");

            // Index must be dropped before type, and created after type
            assert!(
                drop_index_pos < drop_type_pos,
                "DROP INDEX should come before DROP TYPE"
            );
            assert!(
                create_type_pos < create_index_pos,
                "CREATE TYPE should come before CREATE INDEX"
            );

            Ok(())
        })
        .await
    })
    .await
}
