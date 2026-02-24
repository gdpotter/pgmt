//! Tests for constraint migrations
use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::diff::operations::{ColumnAction, ConstraintOperation, MigrationStep, TableOperation};

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
                assert!(
                    steps.iter().any(|s| {
                        matches!(s, MigrationStep::Constraint(ConstraintOperation::Drop(identifier))
                    if identifier.name == "users_email_unique")
                    }),
                    "Should have drop constraint step"
                );

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
            assert!(steps.iter().any(|s| {
                matches!(s, MigrationStep::Constraint(ConstraintOperation::Create(constraint))
                    if constraint.name == "orders_user_id_fkey")
            }), "Should have create foreign key step");

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
                assert!(steps.iter().any(|s| {
                matches!(s, MigrationStep::Constraint(ConstraintOperation::Create(constraint))
                    if constraint.name == "products_price_check")
            }), "Should have create check constraint step");

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
            assert!(steps.iter().any(|s| {
                matches!(s, MigrationStep::Constraint(ConstraintOperation::Drop(identifier))
                    if identifier.name == "orders_user_id_fkey")
            }), "Should have drop constraint step");

            assert!(steps.iter().any(|s| {
                matches!(s, MigrationStep::Constraint(ConstraintOperation::Create(constraint))
                    if constraint.name == "orders_user_id_fkey")
            }), "Should have create constraint step");

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

#[tokio::test]
async fn test_fk_constraint_cascade_on_column_type_change() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema
            &["CREATE SCHEMA app"],
            // Initial DB: tables with FK, referencing column will change type
            &[
                "CREATE TABLE app.users (id SERIAL PRIMARY KEY, tenant_id SMALLINT NOT NULL UNIQUE)",
                "CREATE TABLE app.orders (id SERIAL, tenant_id SMALLINT NOT NULL, user_id INTEGER)",
                "ALTER TABLE app.orders ADD CONSTRAINT orders_tenant_fkey FOREIGN KEY (tenant_id) REFERENCES app.users(tenant_id)",
            ],
            // Target DB: referencing column type changed
            &[
                "CREATE TABLE app.users (id SERIAL PRIMARY KEY, tenant_id BIGINT NOT NULL UNIQUE)",
                "CREATE TABLE app.orders (id SERIAL, tenant_id BIGINT NOT NULL, user_id INTEGER)",
                "ALTER TABLE app.orders ADD CONSTRAINT orders_tenant_fkey FOREIGN KEY (tenant_id) REFERENCES app.users(tenant_id)",
            ],
            |steps, final_catalog| {
                // Should have: Drop FK → Alter columns → Create FK
                let drop_fk = steps.iter().any(|s| {
                    matches!(s, MigrationStep::Constraint(ConstraintOperation::Drop(id))
                        if id.name == "orders_tenant_fkey")
                });
                let create_fk = steps.iter().any(|s| {
                    matches!(s, MigrationStep::Constraint(ConstraintOperation::Create(c))
                        if c.name == "orders_tenant_fkey")
                });
                let has_alter_type = steps.iter().any(|s| {
                    matches!(s, MigrationStep::Table(TableOperation::Alter { actions, .. })
                        if actions.iter().any(|a| matches!(a, ColumnAction::AlterType { .. })))
                });

                assert!(drop_fk, "Should have DROP FK constraint");
                assert!(create_fk, "Should have CREATE FK constraint");
                assert!(has_alter_type, "Should have ALTER COLUMN TYPE");

                // Verify ordering: DROP FK → ALTER → CREATE FK
                let drop_pos = steps
                    .iter()
                    .position(|s| {
                        matches!(s, MigrationStep::Constraint(ConstraintOperation::Drop(id))
                            if id.name == "orders_tenant_fkey")
                    })
                    .unwrap();
                let create_pos = steps
                    .iter()
                    .position(|s| {
                        matches!(s, MigrationStep::Constraint(ConstraintOperation::Create(c))
                            if c.name == "orders_tenant_fkey")
                    })
                    .unwrap();

                assert!(drop_pos < create_pos, "DROP FK should come before CREATE FK");

                // Verify final state
                let constraint = final_catalog
                    .constraints
                    .iter()
                    .find(|c| c.name == "orders_tenant_fkey")
                    .expect("FK constraint should exist");
                assert_eq!(constraint.name, "orders_tenant_fkey");

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_fk_constraint_cascade_on_referenced_column_type_change() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema
            &["CREATE SCHEMA app"],
            // Initial DB: tables with FK, REFERENCED column will change type
            &[
                "CREATE TABLE app.tenants (id SMALLINT PRIMARY KEY)",
                "CREATE TABLE app.users (id SERIAL, tenant_id SMALLINT NOT NULL)",
                "ALTER TABLE app.users ADD CONSTRAINT users_tenant_fkey FOREIGN KEY (tenant_id) REFERENCES app.tenants(id)",
            ],
            // Target DB: referenced column type changed (in tenants table)
            &[
                "CREATE TABLE app.tenants (id BIGINT PRIMARY KEY)",
                "CREATE TABLE app.users (id SERIAL, tenant_id BIGINT NOT NULL)",
                "ALTER TABLE app.users ADD CONSTRAINT users_tenant_fkey FOREIGN KEY (tenant_id) REFERENCES app.tenants(id)",
            ],
            |steps, final_catalog| {
                // Should cascade FK because referenced column type changed
                let drop_fk = steps.iter().any(|s| {
                    matches!(s, MigrationStep::Constraint(ConstraintOperation::Drop(id))
                        if id.name == "users_tenant_fkey")
                });
                let create_fk = steps.iter().any(|s| {
                    matches!(s, MigrationStep::Constraint(ConstraintOperation::Create(c))
                        if c.name == "users_tenant_fkey")
                });

                assert!(drop_fk, "Should have DROP FK (referenced column changed)");
                assert!(create_fk, "Should have CREATE FK");

                // Verify final state
                assert!(
                    final_catalog
                        .constraints
                        .iter()
                        .any(|c| c.name == "users_tenant_fkey"),
                    "FK constraint should exist"
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_composite_fk_cascade_on_column_type_change() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema
            &["CREATE SCHEMA app"],
            // Initial DB: tables with composite FK
            &[
                "CREATE TABLE app.tenants (id SMALLINT, region SMALLINT, PRIMARY KEY (id, region))",
                "CREATE TABLE app.users (id SERIAL, tenant_id SMALLINT NOT NULL, region SMALLINT NOT NULL)",
                "ALTER TABLE app.users ADD CONSTRAINT users_tenant_fkey FOREIGN KEY (tenant_id, region) REFERENCES app.tenants(id, region)",
            ],
            // Target DB: one column in composite FK changes type
            &[
                "CREATE TABLE app.tenants (id BIGINT, region SMALLINT, PRIMARY KEY (id, region))",
                "CREATE TABLE app.users (id SERIAL, tenant_id BIGINT NOT NULL, region SMALLINT NOT NULL)",
                "ALTER TABLE app.users ADD CONSTRAINT users_tenant_fkey FOREIGN KEY (tenant_id, region) REFERENCES app.tenants(id, region)",
            ],
            |steps, final_catalog| {
                // Should cascade FK because one column in composite changed
                let drop_fk = steps.iter().any(|s| {
                    matches!(s, MigrationStep::Constraint(ConstraintOperation::Drop(id))
                        if id.name == "users_tenant_fkey")
                });
                let create_fk = steps.iter().any(|s| {
                    matches!(s, MigrationStep::Constraint(ConstraintOperation::Create(c))
                        if c.name == "users_tenant_fkey")
                });

                assert!(drop_fk, "Should have DROP FK (composite key column changed)");
                assert!(create_fk, "Should have CREATE FK");

                // Verify final state
                assert!(
                    final_catalog
                        .constraints
                        .iter()
                        .any(|c| c.name == "users_tenant_fkey"),
                    "FK constraint should exist"
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_fk_constraint_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: tables
            &[
                "CREATE TABLE users (id SERIAL PRIMARY KEY)",
                "CREATE TABLE orders (id SERIAL, user_id INTEGER)",
            ],
            // Initial DB only: nothing extra
            &[],
            // Target DB only: FK constraint with comment
            &[
                "ALTER TABLE orders ADD CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id)",
                "COMMENT ON CONSTRAINT orders_user_id_fkey ON orders IS 'Links orders to users'",
            ],
            |steps, final_catalog| {
                // Should have both create and comment steps
                let create_pos = steps
                    .iter()
                    .position(|s| {
                        matches!(
                            s,
                            MigrationStep::Constraint(ConstraintOperation::Create(c))
                                if c.name == "orders_user_id_fkey"
                        )
                    })
                    .expect("Should have create FK step");

                let comment_pos = steps
                    .iter()
                    .position(|s| {
                        matches!(
                            s,
                            MigrationStep::Constraint(ConstraintOperation::Comment(_))
                        )
                    })
                    .expect("Should have comment step");

                assert!(
                    create_pos < comment_pos,
                    "FK create (pos {}) must come before comment (pos {})",
                    create_pos,
                    comment_pos
                );

                // Verify final state
                let constraint = final_catalog
                    .constraints
                    .iter()
                    .find(|c| c.name == "orders_user_id_fkey")
                    .expect("FK constraint should exist");
                assert_eq!(
                    constraint.comment,
                    Some("Links orders to users".to_string())
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_no_fk_cascade_without_column_type_change() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema
            &["CREATE SCHEMA app"],
            // Initial DB: tables with FK
            &[
                "CREATE TABLE app.users (id SERIAL PRIMARY KEY, name TEXT)",
                "CREATE TABLE app.orders (id SERIAL, user_id INTEGER NOT NULL)",
                "ALTER TABLE app.orders ADD CONSTRAINT orders_user_fkey FOREIGN KEY (user_id) REFERENCES app.users(id)",
            ],
            // Target DB: unrelated column changed, not the FK column
            &[
                "CREATE TABLE app.users (id SERIAL PRIMARY KEY, name VARCHAR(100))",
                "CREATE TABLE app.orders (id SERIAL, user_id INTEGER NOT NULL)",
                "ALTER TABLE app.orders ADD CONSTRAINT orders_user_fkey FOREIGN KEY (user_id) REFERENCES app.users(id)",
            ],
            |steps, final_catalog| {
                // Should NOT cascade FK - only unrelated column changed
                let drop_fk = steps.iter().any(|s| {
                    matches!(s, MigrationStep::Constraint(ConstraintOperation::Drop(id))
                        if id.name == "orders_user_fkey")
                });
                let create_fk = steps.iter().any(|s| {
                    matches!(s, MigrationStep::Constraint(ConstraintOperation::Create(c))
                        if c.name == "orders_user_fkey")
                });

                assert!(!drop_fk, "Should NOT drop FK when unrelated column changes");
                assert!(!create_fk, "Should NOT create FK when unrelated column changes");

                // Verify FK still exists
                assert!(
                    final_catalog
                        .constraints
                        .iter()
                        .any(|c| c.name == "orders_user_fkey"),
                    "FK constraint should still exist"
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}
