//! Tests for constraint migrations
use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::diff::operations::{ConstraintOperation, MigrationStep};

#[tokio::test]
async fn test_constraint_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: table and constraint
            &[
                "CREATE TABLE users (id SERIAL, email VARCHAR(100))",
                "ALTER TABLE users ADD CONSTRAINT users_email_unique UNIQUE (email)",
            ],
            // Initial DB only: nothing extra (no comment)
            &[],
            // Target DB only: add comment
            &["COMMENT ON CONSTRAINT users_email_unique ON users IS 'Ensure email uniqueness'"],
            // Verification closure
            |steps, final_catalog| {
                // Verify migration steps
                assert!(!steps.is_empty());
                let _comment_step = steps
                    .iter()
                    .find(|s| {
                        matches!(
                            s,
                            MigrationStep::Constraint(ConstraintOperation::Comment(_))
                        )
                    })
                    .expect("Should have constraint comment step");

                // Verify final state
                let created_constraint = final_catalog
                    .constraints
                    .iter()
                    .find(|c| c.name == "users_email_unique")
                    .expect("Constraint should exist");

                assert_eq!(
                    created_constraint.comment,
                    Some("Ensure email uniqueness".to_string())
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_drop_constraint_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: table and constraint
            &[
                "CREATE TABLE users (id SERIAL, email VARCHAR(100))",
                "ALTER TABLE users ADD CONSTRAINT users_email_unique UNIQUE (email)",
            ],
            // Initial DB only: has comment
            &["COMMENT ON CONSTRAINT users_email_unique ON users IS 'Ensure email uniqueness'"],
            // Target DB only: nothing extra (no comment)
            &[],
            // Verification closure
            |steps, final_catalog| {
                // Verify migration steps
                assert!(!steps.is_empty());
                let _comment_step = steps
                    .iter()
                    .find(|s| {
                        matches!(
                            s,
                            MigrationStep::Constraint(ConstraintOperation::Comment(_))
                        )
                    })
                    .expect("Should have constraint comment step");

                // Verify final state
                let created_constraint = final_catalog
                    .constraints
                    .iter()
                    .find(|c| c.name == "users_email_unique")
                    .expect("Constraint should exist");

                assert_eq!(created_constraint.comment, None);

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_drop_constraint_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: table
            &["CREATE TABLE users (id SERIAL, email VARCHAR(100))"],
            // Initial DB only: has constraint
            &["ALTER TABLE users ADD CONSTRAINT users_email_unique UNIQUE (email)"],
            // Target DB only: nothing extra (constraint missing)
            &[],
            // Verification closure
            |steps, final_catalog| {
                // Verify migration steps
                assert!(!steps.is_empty());
                let _drop_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Constraint(ConstraintOperation::Drop(identifier))
                    if identifier.name == "users_email_unique")
                    })
                    .expect("Should have drop constraint step");

                // Verify final state
                let found_constraint = final_catalog
                    .constraints
                    .iter()
                    .find(|c| c.name == "users_email_unique");

                assert!(found_constraint.is_none(), "Constraint should be dropped");

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_foreign_key_constraint_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: tables
        &[
            "CREATE TABLE users (id SERIAL PRIMARY KEY)",
            "CREATE TABLE orders (id SERIAL, user_id INTEGER)"
        ],
        // Initial DB only: nothing extra (no foreign key)
        &[],
        // Target DB only: add foreign key
        &["ALTER TABLE orders ADD CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id)"],
        // Verification closure
        |steps, final_catalog| {
            // Verify migration steps
            assert!(!steps.is_empty());
            let _create_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::Constraint(ConstraintOperation::Create(constraint))
                    if constraint.name == "orders_user_id_fkey")
            }).expect("Should have create foreign key step");

            // Verify final state
            let created_constraint = final_catalog.constraints.iter()
                .find(|c| c.name == "orders_user_id_fkey")
                .expect("Foreign key constraint should be created");

            assert_eq!(created_constraint.name, "orders_user_id_fkey");

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_check_constraint_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: table
            &["CREATE TABLE products (id SERIAL, price DECIMAL)"],
            // Initial DB only: nothing extra (no check constraint)
            &[],
            // Target DB only: add check constraint
            &["ALTER TABLE products ADD CONSTRAINT products_price_check CHECK (price > 0)"],
            // Verification closure
            |steps, final_catalog| {
                // Verify migration steps
                assert!(!steps.is_empty());
                let _create_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::Constraint(ConstraintOperation::Create(constraint))
                    if constraint.name == "products_price_check")
            }).expect("Should have create check constraint step");

                // Verify final state
                let created_constraint = final_catalog
                    .constraints
                    .iter()
                    .find(|c| c.name == "products_price_check")
                    .expect("Check constraint should be created");

                assert_eq!(created_constraint.name, "products_price_check");

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_constraint_modification_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: tables
        &[
            "CREATE TABLE users (id SERIAL PRIMARY KEY)",
            "CREATE TABLE orders (id SERIAL, user_id INTEGER)"
        ],
        // Initial DB only: foreign key without ON DELETE CASCADE
        &["ALTER TABLE orders ADD CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id)"],
        // Target DB only: foreign key with ON DELETE CASCADE
        &["ALTER TABLE orders ADD CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE"],
        // Verification closure
        |steps, final_catalog| {
            // Verify migration steps - should drop and recreate constraint
            assert!(!steps.is_empty());
            let _drop_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::Constraint(ConstraintOperation::Drop(identifier))
                    if identifier.name == "orders_user_id_fkey")
            }).expect("Should have drop constraint step");

            let _create_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::Constraint(ConstraintOperation::Create(constraint))
                    if constraint.name == "orders_user_id_fkey")
            }).expect("Should have create constraint step");

            // Verify final state
            let modified_constraint = final_catalog.constraints.iter()
                .find(|c| c.name == "orders_user_id_fkey")
                .expect("Modified constraint should exist");

            // Verify it has ON DELETE CASCADE
            match &modified_constraint.constraint_type {
                pgmt::catalog::constraint::ConstraintType::ForeignKey { on_delete, .. } => {
                    assert_eq!(on_delete, &Some("CASCADE".to_string()));
                },
                _ => panic!("Expected foreign key constraint"),
            }

            Ok(())
        }
    ).await?;

    Ok(())
}
