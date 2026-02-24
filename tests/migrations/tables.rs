use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::catalog::id::{DbObjectId, DependsOn};
use pgmt::diff::operations::{
    ColumnAction, CommentOperation, ConstraintOperation, MigrationStep, TableOperation,
    TypeOperation, ViewOperation,
};

#[tokio::test]
async fn test_create_table_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE SCHEMA test_schema"],
            &[],
            &["CREATE TABLE test_schema.users (
             id INTEGER PRIMARY KEY,
             name TEXT NOT NULL,
             email TEXT
         )"],
            |steps, final_catalog| {
                assert!(!steps.is_empty());
                let create_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::Table(TableOperation::Create { schema, name, .. })
                    if schema == "test_schema" && name == "users")
            }).expect("Should have CreateTable step");

                match create_step {
                    MigrationStep::Table(TableOperation::Create {
                        schema,
                        name,
                        columns,
                        primary_key,
                    }) => {
                        assert_eq!(schema, "test_schema");
                        assert_eq!(name, "users");
                        assert_eq!(columns.len(), 3);
                        assert_eq!(columns[0].name, "id");
                        assert_eq!(columns[1].name, "name");
                        assert_eq!(columns[2].name, "email");
                        assert!(primary_key.is_some());
                    }
                    _ => panic!("Expected CreateTable step"),
                }

                assert_eq!(final_catalog.tables.len(), 1);
                let created_table = &final_catalog.tables[0];
                assert_eq!(created_table.name, "users");
                assert_eq!(created_table.schema, "test_schema");
                assert_eq!(created_table.columns.len(), 3);

                assert_eq!(created_table.columns[0].name, "id");
                assert_eq!(created_table.columns[0].data_type, "integer");
                assert!(created_table.columns[0].not_null);

                assert_eq!(created_table.columns[1].name, "name");
                assert_eq!(created_table.columns[1].data_type, "text");
                assert!(created_table.columns[1].not_null);

                assert_eq!(created_table.columns[2].name, "email");
                assert_eq!(created_table.columns[2].data_type, "text");
                assert!(!created_table.columns[2].not_null);

                assert!(created_table.primary_key.is_some());
                let pk = created_table.primary_key.as_ref().unwrap();
                assert_eq!(pk.columns, vec!["id"]);

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_drop_table_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE SCHEMA test_schema"],
            &["CREATE TABLE test_schema.old_table (id INT)"],
            &[],
            |steps, final_catalog| {
                assert!(!steps.is_empty());
                assert!(
                    steps.iter().any(|s| {
                        matches!(s, MigrationStep::Table(TableOperation::Drop { schema, name })
                    if schema == "test_schema" && name == "old_table")
                    }),
                    "Should have DropTable step"
                );

                // Verify final state - table completely removed
                assert!(final_catalog.tables.is_empty());

                // Verify schema still exists but table is gone
                assert!(!final_catalog.schemas.is_empty());
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
async fn test_add_column_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE SCHEMA test_schema"],
            &["CREATE TABLE test_schema.users (id INTEGER)"],
            &["CREATE TABLE test_schema.users (id INTEGER, name TEXT)"],
            |steps, final_catalog| {
                assert!(!steps.is_empty());
                let alter_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Table(TableOperation::Alter { schema, name, .. })
                    if schema == "test_schema" && name == "users")
                    })
                    .expect("Should have AlterTable step");

                match alter_step {
                    MigrationStep::Table(TableOperation::Alter {
                        schema,
                        name,
                        actions,
                    }) => {
                        assert_eq!(schema, "test_schema");
                        assert_eq!(name, "users");
                        assert_eq!(actions.len(), 1);
                        match &actions[0] {
                            ColumnAction::Add { column } => {
                                assert_eq!(column.name, "name");
                                assert_eq!(column.data_type, "text");
                            }
                            _ => panic!("Expected AddColumn action"),
                        }
                    }
                    _ => panic!("Expected AlterTable step"),
                }

                assert_eq!(final_catalog.tables.len(), 1);
                let final_table = &final_catalog.tables[0];
                assert_eq!(final_table.schema, "test_schema");
                assert_eq!(final_table.name, "users");
                assert_eq!(final_table.columns.len(), 2);

                assert_eq!(final_table.columns[0].name, "id");
                assert_eq!(final_table.columns[0].data_type, "integer");
                assert!(!final_table.columns[0].not_null); // No PK constraint in this test

                assert_eq!(final_table.columns[1].name, "name");
                assert_eq!(final_table.columns[1].data_type, "text");
                assert!(!final_table.columns[1].not_null);

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_table_with_dependent_view_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        &["CREATE SCHEMA test_schema"],
        &[
            "CREATE TABLE test_schema.users (id INTEGER, count SMALLINT)",
            "CREATE VIEW test_schema.user_stats AS SELECT id, count FROM test_schema.users WHERE count > 0"
        ],
        &[
            "CREATE TABLE test_schema.users (id INTEGER, count BIGINT)",
            "CREATE VIEW test_schema.user_stats AS SELECT id, count FROM test_schema.users WHERE count > 0"
        ],
        |steps, final_catalog| {
            // Should have: Drop view → Alter table → Recreate view
            assert!(steps.len() >= 3);

            let drop_view_pos = steps.iter().position(|s| {
                matches!(s, MigrationStep::View(ViewOperation::Drop { schema, name })
                    if schema == "test_schema" && name == "user_stats")
            }).expect("Should have DropView step");

            let alter_table_pos = steps.iter().position(|s| {
                matches!(s, MigrationStep::Table(TableOperation::Alter { schema, name, .. })
                    if schema == "test_schema" && name == "users")
            }).expect("Should have AlterTable step");

            let create_view_pos = steps.iter().position(|s| {
                matches!(s, MigrationStep::View(ViewOperation::Create { schema, name, .. })
                    if schema == "test_schema" && name == "user_stats")
            }).expect("Should have CreateView step");

            assert!(drop_view_pos < alter_table_pos, "View should be dropped before table is altered");
            assert!(alter_table_pos < create_view_pos, "Table should be altered before view is recreated");

            assert_eq!(final_catalog.tables.len(), 1);
            assert_eq!(final_catalog.views.len(), 1);

            let final_table = &final_catalog.tables[0];
            assert_eq!(final_table.schema, "test_schema");
            assert_eq!(final_table.name, "users");
            assert_eq!(final_table.columns.len(), 2);

            assert_eq!(final_table.columns[0].name, "id");
            assert_eq!(final_table.columns[0].data_type, "integer");

            let count_column = final_table.columns.iter().find(|c| c.name == "count").unwrap();
            assert_eq!(count_column.data_type, "bigint"); // Type successfully changed

            let final_view = &final_catalog.views[0];
            assert_eq!(final_view.schema, "test_schema");
            assert_eq!(final_view.name, "user_stats");
            assert_eq!(final_view.columns.len(), 2);

            // Verify view dependencies are still tracked
            let depends_on_users = final_view.depends_on().iter().any(|dep| {
                matches!(dep, pgmt::catalog::id::DbObjectId::Table { schema, name }
                    if schema == "test_schema" && name == "users")
            });
            assert!(depends_on_users);

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_table_with_custom_type_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema
            &["CREATE SCHEMA test_schema"],
            // Initial DB only: custom type with 2 values + table
            &[
                "CREATE TYPE test_schema.status_type AS ENUM ('pending', 'active')",
                "CREATE TABLE test_schema.orders (id INTEGER, status test_schema.status_type)",
            ],
            // Target DB only: custom type with 3 values + table (enum value added)
            &[
                "CREATE TYPE test_schema.status_type AS ENUM ('pending', 'active', 'completed')",
                "CREATE TABLE test_schema.orders (id INTEGER, status test_schema.status_type)",
            ],
            // Verification closure
            |steps, final_catalog| {
                // Should have ALTER TYPE step to add enum value
                assert!(!steps.is_empty());
                let alter_type_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Type(TypeOperation::Alter { schema, name, .. })
                    if schema == "test_schema" && name == "status_type")
                    })
                    .expect("Should have AlterType step");

                // Verify the ALTER TYPE step details
                match alter_type_step {
                    MigrationStep::Type(TypeOperation::Alter {
                        schema,
                        name,
                        action,
                        definition,
                    }) => {
                        assert_eq!(schema, "test_schema");
                        assert_eq!(name, "status_type");
                        assert_eq!(action, "ADD VALUE");
                        assert!(definition.contains("'completed'"));
                    }
                    _ => panic!("Expected AlterType step"),
                }

                // Verify final state exactly
                assert_eq!(final_catalog.types.len(), 1);
                assert_eq!(final_catalog.tables.len(), 1);

                // Verify custom type was modified correctly
                let final_type = &final_catalog.types[0];
                assert_eq!(final_type.schema, "test_schema");
                assert_eq!(final_type.name, "status_type");
                if matches!(final_type.kind, pgmt::catalog::custom_type::TypeKind::Enum) {
                    assert_eq!(final_type.enum_values.len(), 3);
                    let enum_names: Vec<&str> = final_type
                        .enum_values
                        .iter()
                        .map(|e| e.name.as_str())
                        .collect();
                    assert!(enum_names.contains(&"pending"));
                    assert!(enum_names.contains(&"active"));
                    assert!(enum_names.contains(&"completed"));
                } else {
                    panic!("Expected enum type");
                }

                // Verify table still uses the custom type
                let final_table = &final_catalog.tables[0];
                assert_eq!(final_table.schema, "test_schema");
                assert_eq!(final_table.name, "orders");
                assert_eq!(final_table.columns.len(), 2);

                assert_eq!(final_table.columns[0].name, "id");
                assert_eq!(final_table.columns[0].data_type, "integer");

                assert_eq!(final_table.columns[1].name, "status");
                assert_eq!(
                    final_table.columns[1].data_type,
                    "\"test_schema\".\"status_type\""
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_cross_schema_table_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: multiple schemas
            &["CREATE SCHEMA inventory", "CREATE SCHEMA sales"],
            // Initial DB only: table in inventory schema
            &["CREATE TABLE inventory.products (id INTEGER, name TEXT)"],
            // Target DB only: same table + new table in sales schema
            &[
                "CREATE TABLE inventory.products (id INTEGER, name TEXT)",
                "CREATE TABLE sales.orders (id INTEGER, product_id INTEGER)",
            ],
            // Verification closure
            |steps, final_catalog| {
                // Should have CREATE TABLE step for orders
                assert!(!steps.is_empty());
                assert!(steps.iter().any(|s| {
                matches!(s, MigrationStep::Table(TableOperation::Create { schema, name, .. })
                    if schema == "sales" && name == "orders")
            }), "Should have CreateTable step for orders");

                // Verify final state exactly
                assert!(final_catalog.schemas.len() >= 2);
                assert_eq!(final_catalog.tables.len(), 2);

                // Verify both schemas exist
                let schema_names: Vec<&str> = final_catalog
                    .schemas
                    .iter()
                    .map(|s| s.name.as_str())
                    .collect();
                assert!(schema_names.contains(&"inventory"));
                assert!(schema_names.contains(&"sales"));

                // Verify original table still exists unchanged
                let products_table = final_catalog
                    .tables
                    .iter()
                    .find(|t| t.schema == "inventory" && t.name == "products")
                    .expect("Should have products table");
                assert_eq!(products_table.columns.len(), 2);
                assert_eq!(products_table.columns[0].name, "id");
                assert_eq!(products_table.columns[1].name, "name");

                // Verify new table was created correctly
                let orders_table = final_catalog
                    .tables
                    .iter()
                    .find(|t| t.schema == "sales" && t.name == "orders")
                    .expect("Should have orders table");
                assert_eq!(orders_table.columns.len(), 2);
                assert_eq!(orders_table.columns[0].name, "id");
                assert_eq!(orders_table.columns[0].data_type, "integer");
                assert_eq!(orders_table.columns[1].name, "product_id");
                assert_eq!(orders_table.columns[1].data_type, "integer");

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_table_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: schema and table
        &[
            "CREATE SCHEMA test_schema",
            "CREATE TABLE test_schema.users (id INTEGER, name TEXT)",
        ],
        // Initial DB only: nothing extra (no comment)
        &[],
        // Target DB only: add comment
        &["COMMENT ON TABLE test_schema.users IS 'User information table'"],
        // Verification closure
        |steps, final_catalog| {
            // Should have SET TABLE COMMENT step
            assert!(!steps.is_empty());
            let comment_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::Table(TableOperation::Comment(CommentOperation::Set { target, comment }))
                    if target.schema == "test_schema" && target.table == "users" && comment == "User information table")
            }).expect("Should have SetTableComment step");

            match comment_step {
                MigrationStep::Table(TableOperation::Comment(CommentOperation::Set { target, comment })) => {
                    assert_eq!(target.schema, "test_schema");
                    assert_eq!(target.table, "users");
                    assert_eq!(comment, "User information table");
                }
                _ => panic!("Expected SetTableComment step"),
            }

            // Verify final state has comment
            let table = &final_catalog.tables[0];
            assert_eq!(table.comment, Some("User information table".to_string()));

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_column_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema and table
            &[
                "CREATE SCHEMA test_schema",
                "CREATE TABLE test_schema.users (id INTEGER, name TEXT)",
            ],
            // Initial DB only: nothing extra (no comment)
            &[],
            // Target DB only: add column comment
            &["COMMENT ON COLUMN test_schema.users.name IS 'Full name of the user'"],
            // Verification closure
            |steps, final_catalog| {
                // Should have ALTER TABLE step with SET COLUMN COMMENT
                assert!(!steps.is_empty());
                let alter_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Table(TableOperation::Alter { schema, name, .. })
                    if schema == "test_schema" && name == "users")
                    })
                    .expect("Should have AlterTable step");

                match alter_step {
                    MigrationStep::Table(TableOperation::Alter {
                        schema,
                        name,
                        actions,
                    }) => {
                        assert_eq!(schema, "test_schema");
                        assert_eq!(name, "users");
                        assert_eq!(actions.len(), 1);
                        match &actions[0] {
                            ColumnAction::Comment(CommentOperation::Set { target, comment }) => {
                                assert_eq!(target.name, "name");
                                assert_eq!(comment, "Full name of the user");
                            }
                            _ => panic!("Expected SetColumnComment action"),
                        }
                    }
                    _ => panic!("Expected AlterTable step"),
                }

                // Verify final state has comment
                let table = &final_catalog.tables[0];
                let name_column = table.columns.iter().find(|c| c.name == "name").unwrap();
                assert_eq!(
                    name_column.comment,
                    Some("Full name of the user".to_string())
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_drop_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: schema and table
        &[
            "CREATE SCHEMA test_schema",
            "CREATE TABLE test_schema.users (id INTEGER, name TEXT)",
        ],
        // Initial DB only: has comments
        &[
            "COMMENT ON TABLE test_schema.users IS 'User information table'",
            "COMMENT ON COLUMN test_schema.users.name IS 'Full name of the user'",
        ],
        // Target DB only: nothing extra (no comments)
        &[],
        // Verification closure
        |steps, final_catalog| {
            // Should have both DROP TABLE COMMENT and DROP COLUMN COMMENT steps
            assert!(!steps.is_empty());

            let table_comment_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::Table(TableOperation::Comment(CommentOperation::Drop { target }))
                    if target.schema == "test_schema" && target.table == "users")
            }).expect("Should have DropTableComment step");

            // Verify the table comment step details
            match table_comment_step {
                MigrationStep::Table(TableOperation::Comment(CommentOperation::Drop { target })) => {
                    assert_eq!(target.schema, "test_schema");
                    assert_eq!(target.table, "users");
                }
                _ => panic!("Expected DropTableComment step"),
            }

            let alter_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::Table(TableOperation::Alter { schema, name, .. })
                    if schema == "test_schema" && name == "users")
            }).expect("Should have AlterTable step");

            match alter_step {
                MigrationStep::Table(TableOperation::Alter { actions, .. }) => {
                    assert_eq!(actions.len(), 1);
                    match &actions[0] {
                        ColumnAction::Comment(CommentOperation::Drop { target }) => {
                            assert_eq!(target.name, "name");
                        }
                        _ => panic!("Expected DropColumnComment action"),
                    }
                }
                _ => panic!("Expected AlterTable step"),
            }

            // Verify final state has no comments
            let table = &final_catalog.tables[0];
            assert_eq!(table.comment, None);
            let name_column = table.columns.iter().find(|c| c.name == "name").unwrap();
            assert_eq!(name_column.comment, None);

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_create_table_with_column_comments_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: just the schema
        &["CREATE SCHEMA test_schema"],
        // Initial DB only: nothing extra
        &[],
        // Target DB only: create table with column comments
        &[
            "CREATE TABLE test_schema.users (id INTEGER, name TEXT)",
            "COMMENT ON COLUMN test_schema.users.id IS 'Primary key'",
            "COMMENT ON COLUMN test_schema.users.name IS 'User full name'",
        ],
        // Verification closure
        |steps, final_catalog| {
            // Verify we have a CREATE TABLE step
            assert!(steps.iter().any(|s| {
                matches!(s, MigrationStep::Table(TableOperation::Create { schema, name, .. })
                    if schema == "test_schema" && name == "users")
            }), "Should have CreateTable step");

            // Check if we have column comment steps
            let column_comment_steps: Vec<_> = steps.iter().filter(|s| {
                match s {
                    MigrationStep::Table(TableOperation::Alter { schema, name, actions })
                        if schema == "test_schema" && name == "users" => {
                        actions.iter().any(|action| matches!(action, ColumnAction::Comment(_)))
                    }
                    _ => false
                }
            }).collect();

            // Should now have column comment steps with our fix
            assert!(!column_comment_steps.is_empty(),
                "Expected column comment migration steps for newly created table, but found none");

            // Verify we have 2 column comment operations
            let total_comment_actions: usize = column_comment_steps.iter().map(|s| {
                match s {
                    MigrationStep::Table(TableOperation::Alter { actions, .. }) => {
                        actions.iter().filter(|action| matches!(action, ColumnAction::Comment(_))).count()
                    }
                    _ => 0
                }
            }).sum();

            assert_eq!(total_comment_actions, 2,
                "Expected 2 column comment actions (one for 'id', one for 'name'), but found {}",
                total_comment_actions);

            // Verify final catalog has the comments
            let table = &final_catalog.tables[0];
            let id_column = table.columns.iter().find(|c| c.name == "id").unwrap();
            let name_column = table.columns.iter().find(|c| c.name == "name").unwrap();

            assert_eq!(id_column.comment, Some("Primary key".to_string()));
            assert_eq!(name_column.comment, Some("User full name".to_string()));

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_create_table_with_generated_columns_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: just the schema
        &["CREATE SCHEMA test_schema"],
        // Initial DB only: nothing extra
        &[],
        // Target DB only: create table with generated column
        &[
            "CREATE TABLE test_schema.users (id INTEGER, first_name TEXT, last_name TEXT, full_name TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED)",
        ],
        // Verification closure
        |steps, final_catalog| {
            // Verify we have a CREATE TABLE step
            let create_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::Table(TableOperation::Create { schema, name, .. })
                    if schema == "test_schema" && name == "users")
            }).expect("Should have CreateTable step");

            // The generated column should be included in the CREATE TABLE step, not as separate ALTER TABLE
            match create_step {
                MigrationStep::Table(TableOperation::Create { columns, .. }) => {
                    let full_name_col = columns.iter().find(|c| c.name == "full_name").unwrap();
                    assert!(full_name_col.generated.is_some(),
                        "Expected full_name column to have generated expression in CREATE TABLE step");
                    // PostgreSQL normalizes the expression format, so just check it contains the key parts
                    let generated_expr = full_name_col.generated.as_ref().unwrap();
                    assert!(generated_expr.contains("first_name"), "Generated expression should contain 'first_name'");
                    assert!(generated_expr.contains("last_name"), "Generated expression should contain 'last_name'");
                    assert!(generated_expr.contains("||"), "Generated expression should contain concatenation operator");
                }
                _ => panic!("Expected CreateTable step"),
            }

            // There should NOT be any ALTER TABLE steps for generated columns on a newly created table
            let generated_alter_steps: Vec<_> = steps.iter().filter(|s| {
                match s {
                    MigrationStep::Table(TableOperation::Alter { schema, name, actions })
                        if schema == "test_schema" && name == "users" => {
                        actions.iter().any(|action| {
                            // Check for any action that might be related to generated columns
                            // This could be an AddColumn with generated expression or similar
                            matches!(action, ColumnAction::Add { column } if column.generated.is_some())
                        })
                    }
                    _ => false
                }
            }).collect();

            assert!(generated_alter_steps.is_empty(),
                "Expected NO ALTER TABLE steps for generated columns on newly created table, but found {} steps",
                generated_alter_steps.len());

            // Verify final catalog has the generated column
            let table = &final_catalog.tables[0];
            let full_name_column = table.columns.iter().find(|c| c.name == "full_name").unwrap();
            assert!(full_name_column.generated.is_some());
            let final_generated_expr = full_name_column.generated.as_ref().unwrap();
            assert!(final_generated_expr.contains("first_name"), "Final catalog generated expression should contain 'first_name'");
            assert!(final_generated_expr.contains("last_name"), "Final catalog generated expression should contain 'last_name'");
            assert!(final_generated_expr.contains("||"), "Final catalog generated expression should contain concatenation operator");

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_primary_key_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: table with primary key
            &["CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)"],
            // Initial DB only: nothing extra (no comment)
            &[],
            // Target DB only: add primary key comment
            &["COMMENT ON CONSTRAINT users_pkey ON users IS 'Primary key for users table'"],
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
                    .expect("Should have primary key comment step");

                // Verify final state
                let created_table = final_catalog
                    .tables
                    .iter()
                    .find(|t| t.name == "users")
                    .expect("Table should exist");

                assert!(created_table.primary_key.is_some());
                let pk = created_table.primary_key.as_ref().unwrap();
                assert_eq!(pk.comment, Some("Primary key for users table".to_string()));

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_drop_primary_key_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: table with primary key
            &["CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)"],
            // Initial DB only: has primary key comment
            &["COMMENT ON CONSTRAINT users_pkey ON users IS 'Primary key for users table'"],
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
                    .expect("Should have primary key comment drop step");

                // Verify final state
                let created_table = final_catalog
                    .tables
                    .iter()
                    .find(|t| t.name == "users")
                    .expect("Table should exist");

                assert!(created_table.primary_key.is_some());
                let pk = created_table.primary_key.as_ref().unwrap();
                assert_eq!(pk.comment, None);

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_primary_key_comment_ordering() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: empty
            &[],
            // Initial DB only: nothing
            &[],
            // Target DB only: table with PK and comment
            &[
                "CREATE TABLE work_orders (id SERIAL PRIMARY KEY, description TEXT)",
                "COMMENT ON CONSTRAINT work_orders_pkey ON work_orders IS '@omit'",
            ],
            // Verification closure
            |steps, final_catalog| {
                // Find the CREATE TABLE step
                let table_step_index = steps
                    .iter()
                    .position(|s| {
                        matches!(s, MigrationStep::Table(TableOperation::Create { .. }))
                            && matches!(s.id(), DbObjectId::Table { name, .. } if name == "work_orders")
                    })
                    .expect("Should have CREATE TABLE step");

                // Find the COMMENT step
                let comment_step_index = steps
                    .iter()
                    .position(|s| {
                        matches!(
                            s,
                            MigrationStep::Constraint(ConstraintOperation::Comment(_))
                        )
                    })
                    .expect("Should have primary key comment step");

                // Verify CREATE TABLE comes before COMMENT
                assert!(
                    table_step_index < comment_step_index,
                    "CREATE TABLE (step {}) must come before COMMENT ON CONSTRAINT (step {})",
                    table_step_index,
                    comment_step_index
                );

                // Verify final state
                let created_table = final_catalog
                    .tables
                    .iter()
                    .find(|t| t.name == "work_orders")
                    .expect("Table should exist");

                assert!(created_table.primary_key.is_some());
                let pk = created_table.primary_key.as_ref().unwrap();
                assert_eq!(pk.comment, Some("@omit".to_string()));

                Ok(())
            },
        )
        .await?;

    Ok(())
}
