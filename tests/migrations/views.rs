use crate::helpers::harness::with_test_db;
use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::catalog::Catalog;
use pgmt::catalog::id::DependsOn;
use pgmt::diff::operations::{
    CommentOperation, MigrationStep, SchemaOperation, SqlRenderer, TableOperation, TypeOperation,
    ViewOperation,
};
use pgmt::diff::{cascade, diff_all, diff_order};

#[tokio::test]
async fn test_create_view_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        &[
            "CREATE SCHEMA test_schema",
            "CREATE TABLE test_schema.users (id INTEGER, name TEXT, email TEXT)",
        ],
        &[],
        &["CREATE VIEW test_schema.active_users AS SELECT id, name FROM test_schema.users WHERE name IS NOT NULL"],
        |steps, final_catalog| {
            assert!(!steps.is_empty());
            let create_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::View(ViewOperation::Create { schema, name, .. })
                    if schema == "test_schema" && name == "active_users")
            }).expect("Should have CreateView step");

            match create_step {
                MigrationStep::View(ViewOperation::Create { schema, name, definition, .. }) => {
                    assert_eq!(schema, "test_schema");
                    assert_eq!(name, "active_users");
                    assert!(definition.contains("SELECT"));
                    assert!(definition.contains("test_schema.users"));
                }
                _ => panic!("Expected CreateView step"),
            }

            assert_eq!(final_catalog.tables.len(), 1);
            assert_eq!(final_catalog.views.len(), 1);

            let created_view = &final_catalog.views[0];
            assert_eq!(created_view.name, "active_users");
            assert_eq!(created_view.schema, "test_schema");
            assert_eq!(created_view.columns.len(), 2);
            assert_eq!(created_view.columns[0].name, "id");
            assert_eq!(created_view.columns[1].name, "name");

            let depends_on_users = created_view.depends_on().iter().any(|dep| {
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
async fn test_drop_view_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &[
                "CREATE SCHEMA test_schema",
                "CREATE TABLE test_schema.users (id INTEGER, name TEXT)",
            ],
            // Initial DB only: has the view
            &["CREATE VIEW test_schema.old_view AS SELECT * FROM test_schema.users"],
            // Target DB only: nothing extra (view is missing)
            &[],
            // Verification closure
            |steps, final_catalog| {
                // Should have DROP VIEW step
                assert!(!steps.is_empty());
                let _drop_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::View(ViewOperation::Drop { schema, name })
                    if schema == "test_schema" && name == "old_view")
                    })
                    .expect("Should have DropView step");

                // Verify final state exactly
                assert_eq!(final_catalog.tables.len(), 1); // Table should remain
                assert_eq!(final_catalog.views.len(), 0); // View should be gone

                // Table should be unchanged
                let remaining_table = &final_catalog.tables[0];
                assert_eq!(remaining_table.schema, "test_schema");
                assert_eq!(remaining_table.name, "users");
                assert_eq!(remaining_table.columns.len(), 2);

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_view_column_change_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema and table
            &[
                "CREATE SCHEMA test_schema",
                "CREATE TABLE test_schema.users (id INTEGER, name TEXT, email TEXT)",
            ],
            // Initial DB only: view with 2 columns
            &["CREATE VIEW test_schema.user_info AS SELECT id, name FROM test_schema.users"],
            // Target DB only: view with 3 columns (column change triggers drop+create)
            &["CREATE VIEW test_schema.user_info AS SELECT id, name, email FROM test_schema.users"],
            // Verification closure
            |steps, final_catalog| {
                // Should have DROP VIEW + CREATE VIEW (column change requires drop+create)
                assert!(steps.len() >= 2);

                let drop_step_pos = steps
                    .iter()
                    .position(|s| {
                        matches!(s, MigrationStep::View(ViewOperation::Drop { schema, name })
                    if schema == "test_schema" && name == "user_info")
                    })
                    .expect("Should have DropView step");

                let create_step_pos = steps
                    .iter()
                    .position(|s| {
                        matches!(s, MigrationStep::View(ViewOperation::Create { schema, name, .. })
                    if schema == "test_schema" && name == "user_info")
                    })
                    .expect("Should have CreateView step");

                // Drop should come before create
                assert!(drop_step_pos < create_step_pos);

                // Verify final state exactly
                assert_eq!(final_catalog.tables.len(), 1);
                assert_eq!(final_catalog.views.len(), 1);

                let final_view = &final_catalog.views[0];
                assert_eq!(final_view.schema, "test_schema");
                assert_eq!(final_view.name, "user_info");
                assert_eq!(final_view.columns.len(), 3); // Now has 3 columns
                assert_eq!(final_view.columns[0].name, "id");
                assert_eq!(final_view.columns[1].name, "name");
                assert_eq!(final_view.columns[2].name, "email");

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_table_column_type_change_cascades_to_view() -> Result<()> {
    with_test_db(async |initial_db| {
        with_test_db(async |target_db| {
            // Initial state: table with SMALLINT, view depending on it
            initial_db.execute("CREATE SCHEMA test_schema").await;
            initial_db
                .execute("CREATE TABLE test_schema.users (id INTEGER, count SMALLINT)")
                .await;
            initial_db.execute("CREATE VIEW test_schema.user_stats AS SELECT id, count FROM test_schema.users WHERE count > 0").await;

            // Target state: table column type changed to BIGINT, view updated accordingly
            target_db.execute("CREATE SCHEMA test_schema").await;
            target_db
                .execute("CREATE TABLE test_schema.users (id INTEGER, count BIGINT)")
                .await;
            target_db.execute("CREATE VIEW test_schema.user_stats AS SELECT id, count FROM test_schema.users WHERE count > 0").await;

            // Load catalogs
            let initial_catalog = Catalog::load(initial_db.pool()).await?;
            let target_catalog = Catalog::load(target_db.pool()).await?;

            // Generate migration steps using full pipeline - cascade should handle view dependency
            let mut steps = diff_all(&initial_catalog, &target_catalog);
            steps = cascade::expand(steps, &initial_catalog, &target_catalog);
            steps = diff_order(steps, &initial_catalog, &target_catalog)?;

            // Should have multiple steps in correct order due to cascading
            assert!(steps.len() >= 3);

            // Find positions of key steps
            let drop_view_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::View(ViewOperation::Drop { schema, name })
                    if schema == "test_schema" && name == "user_stats")
                })
                .expect("Should have DropView step");

            let alter_table_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::Table(TableOperation::Alter { schema, name, .. })
                    if schema == "test_schema" && name == "users")
                })
                .expect("Should have AlterTable step");

            let create_view_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::View(ViewOperation::Create { schema, name, .. })
                    if schema == "test_schema" && name == "user_stats")
                })
                .expect("Should have CreateView step");

            // Verify correct dependency ordering
            assert!(
                drop_view_pos < alter_table_pos,
                "View should be dropped before table is altered"
            );
            assert!(
                alter_table_pos < create_view_pos,
                "Table should be altered before view is recreated"
            );

            // Apply migration
            for step in &steps {
                let sql_list = step.to_sql();
                for rendered in sql_list {
                    initial_db.execute(&rendered.sql).await;
                }
            }

            // Verify final state exactly
            let final_catalog = Catalog::load(initial_db.pool()).await?;
            assert_eq!(final_catalog.tables.len(), 1);
            assert_eq!(final_catalog.views.len(), 1);

            // Verify table column type changed
            let final_table = &final_catalog.tables[0];
            assert_eq!(final_table.schema, "test_schema");
            assert_eq!(final_table.name, "users");
            assert_eq!(final_table.columns.len(), 2);

            let count_column = final_table
                .columns
                .iter()
                .find(|c| c.name == "count")
                .unwrap();
            assert_eq!(count_column.data_type, "bigint");

            // Verify view was recreated correctly
            let final_view = &final_catalog.views[0];
            assert_eq!(final_view.schema, "test_schema");
            assert_eq!(final_view.name, "user_stats");
            assert_eq!(final_view.columns.len(), 2);

            // View should still depend on the table
            let depends_on_users = final_view.depends_on().iter().any(|dep| {
                matches!(dep, pgmt::catalog::id::DbObjectId::Table { schema, name }
                    if schema == "test_schema" && name == "users")
            });
            assert!(depends_on_users);

            Ok(())
        }).await
    }).await
}

#[tokio::test]
async fn test_view_dependency_chain() -> Result<()> {
    with_test_db(async |initial_db| {
        with_test_db(async |target_db| {
            // Initial state: table -> view1 -> view2 dependency chain
            initial_db.execute("CREATE SCHEMA test_schema").await;
            initial_db
                .execute("CREATE TABLE test_schema.orders (id INTEGER, total DECIMAL(10,2), status TEXT)")
                .await;
            initial_db.execute("CREATE VIEW test_schema.active_orders AS SELECT id, total FROM test_schema.orders WHERE status = 'active'").await;
            initial_db.execute("CREATE VIEW test_schema.high_value_orders AS SELECT id, total FROM test_schema.active_orders WHERE total > 100").await;

            // Target state: remove middle view (should cascade to dependent view)
            target_db.execute("CREATE SCHEMA test_schema").await;
            target_db
                .execute("CREATE TABLE test_schema.orders (id INTEGER, total DECIMAL(10,2), status TEXT)")
                .await;
            // Note: only high_value_orders is removed, active_orders remains

            // Load catalogs
            let initial_catalog = Catalog::load(initial_db.pool()).await?;
            let target_catalog = Catalog::load(target_db.pool()).await?;

            // Generate migration steps using full pipeline
            let mut steps = diff_all(&initial_catalog, &target_catalog);
            steps = cascade::expand(steps, &initial_catalog, &target_catalog);
            steps = diff_order(steps, &initial_catalog, &target_catalog)?;

            // Should have DROP VIEW steps for both views that were removed
            assert!(!steps.is_empty());

            let drop_high_value = steps.iter().any(|s| {
                matches!(s, MigrationStep::View(ViewOperation::Drop { schema, name })
                    if schema == "test_schema" && name == "high_value_orders")
            });
            assert!(drop_high_value);

            let drop_active = steps.iter().any(|s| {
                matches!(s, MigrationStep::View(ViewOperation::Drop { schema, name })
                    if schema == "test_schema" && name == "active_orders")
            });
            assert!(drop_active);

            // Apply migration
            for step in &steps {
                let sql_list = step.to_sql();
                for rendered in sql_list {
                    initial_db.execute(&rendered.sql).await;
                }
            }

            // Verify final state exactly
            let final_catalog = Catalog::load(initial_db.pool()).await?;
            assert_eq!(final_catalog.tables.len(), 1);
            assert_eq!(final_catalog.views.len(), 0); // Both views should be gone

            // Table should remain unchanged
            let remaining_table = &final_catalog.tables[0];
            assert_eq!(remaining_table.schema, "test_schema");
            assert_eq!(remaining_table.name, "orders");
            assert_eq!(remaining_table.columns.len(), 3);

            Ok(())
        }).await
    }).await
}

#[tokio::test]
async fn test_cross_schema_view_dependencies() -> Result<()> {
    with_test_db(async |initial_db| {
        with_test_db(async |target_db| {
            // Initial state: table in inventory schema only
            initial_db.execute("CREATE SCHEMA inventory").await;
            initial_db
                .execute("CREATE TABLE inventory.items (id INTEGER, name TEXT, quantity INTEGER)")
                .await;

            // Target state: add reporting schema with view that depends on inventory table
            target_db.execute("CREATE SCHEMA inventory").await;
            target_db.execute("CREATE SCHEMA reporting").await;
            target_db
                .execute("CREATE TABLE inventory.items (id INTEGER, name TEXT, quantity INTEGER)")
                .await;
            target_db.execute("CREATE VIEW reporting.low_stock AS SELECT id, name FROM inventory.items WHERE quantity < 10").await;

            // Load catalogs
            let initial_catalog = Catalog::load(initial_db.pool()).await?;
            let target_catalog = Catalog::load(target_db.pool()).await?;

            // Generate migration steps using full pipeline
            let mut steps = diff_all(&initial_catalog, &target_catalog);
            steps = cascade::expand(steps, &initial_catalog, &target_catalog);
            steps = diff_order(steps, &initial_catalog, &target_catalog)?;

            // Should have CREATE SCHEMA and CREATE VIEW steps
            assert!(!steps.is_empty());

            let create_schema = steps.iter().any(|s| {
                matches!(s, MigrationStep::Schema(SchemaOperation::Create { name })
                    if name == "reporting")
            });
            assert!(create_schema);

            let create_view = steps.iter().any(|s| {
                matches!(s, MigrationStep::View(ViewOperation::Create { schema, name, .. })
                    if schema == "reporting" && name == "low_stock")
            });
            assert!(create_view);

            // Verify ordering: schema creation should come before view creation
            let create_schema_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::Schema(SchemaOperation::Create { name })
                    if name == "reporting")
                })
                .expect("Should have CreateSchema step");

            let create_view_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::View(ViewOperation::Create { schema, name, .. })
                    if schema == "reporting" && name == "low_stock")
                })
                .expect("Should have CreateView step");

            assert!(
                create_schema_pos < create_view_pos,
                "Schema should be created before view"
            );

            // Apply migration
            for step in &steps {
                let sql_list = step.to_sql();
                for rendered in sql_list {
                    initial_db.execute(&rendered.sql).await;
                }
            }

            // Verify final state exactly
            let final_catalog = Catalog::load(initial_db.pool()).await?;
            assert!(final_catalog.schemas.len() >= 2);
            assert_eq!(final_catalog.tables.len(), 1);
            assert_eq!(final_catalog.views.len(), 1);

            // Verify schemas
            let schema_names: Vec<&str> = final_catalog
                .schemas
                .iter()
                .map(|s| s.name.as_str())
                .collect();
            assert!(schema_names.contains(&"inventory"));
            assert!(schema_names.contains(&"reporting"));

            // Verify view in reporting schema
            let low_stock_view = &final_catalog.views[0];
            assert_eq!(low_stock_view.schema, "reporting");
            assert_eq!(low_stock_view.name, "low_stock");
            assert_eq!(low_stock_view.columns.len(), 2);
            assert_eq!(low_stock_view.columns[0].name, "id");
            assert_eq!(low_stock_view.columns[1].name, "name");

            // Should depend on the table in the other schema
            let depends_on_items = low_stock_view.depends_on().iter().any(|dep| {
                matches!(dep, pgmt::catalog::id::DbObjectId::Table { schema, name }
                    if schema == "inventory" && name == "items")
            });
            assert!(depends_on_items);

            Ok(())
        }).await
    }).await
}

#[tokio::test]
async fn test_view_with_custom_type_dependency() -> Result<()> {
    with_test_db(async |initial_db| {
        with_test_db(async |target_db| {
            // Initial state: just schema
            initial_db.execute("CREATE SCHEMA test_schema").await;

            // Target state: custom type, table using it, and view using the table
            target_db.execute("CREATE SCHEMA test_schema").await;
            target_db
                .execute("CREATE TYPE test_schema.status_type AS ENUM ('pending', 'active', 'inactive')")
                .await;
            target_db.execute("CREATE TABLE test_schema.accounts (id INTEGER, name TEXT, status test_schema.status_type)").await;
            target_db.execute("CREATE VIEW test_schema.active_accounts AS SELECT id, name FROM test_schema.accounts WHERE status = 'active'").await;

            // Load catalogs
            let initial_catalog = Catalog::load(initial_db.pool()).await?;
            let target_catalog = Catalog::load(target_db.pool()).await?;

            // Generate migration steps using full pipeline
            let mut steps = diff_all(&initial_catalog, &target_catalog);
            steps = cascade::expand(steps, &initial_catalog, &target_catalog);
            steps = diff_order(steps, &initial_catalog, &target_catalog)?;

            // Should have CREATE TYPE, CREATE TABLE, CREATE VIEW in dependency order
            assert!(steps.len() >= 3);

            let create_type_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::Type(TypeOperation::Create { schema, name, .. })
                    if schema == "test_schema" && name == "status_type")
                })
                .expect("Should have CreateType step");

            let create_table_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::Table(TableOperation::Create { schema, name, .. })
                    if schema == "test_schema" && name == "accounts")
                })
                .expect("Should have CreateTable step");

            let create_view_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::View(ViewOperation::Create { schema, name, .. })
                    if schema == "test_schema" && name == "active_accounts")
                })
                .expect("Should have CreateView step");

            // Verify dependency ordering
            assert!(
                create_type_pos < create_table_pos,
                "Type should be created before table"
            );
            assert!(
                create_table_pos < create_view_pos,
                "Table should be created before view"
            );

            // Apply migration
            for step in &steps {
                let sql_list = step.to_sql();
                for rendered in sql_list {
                    initial_db.execute(&rendered.sql).await;
                }
            }

            // Verify final state exactly
            let final_catalog = Catalog::load(initial_db.pool()).await?;
            assert_eq!(final_catalog.types.len(), 1);
            assert_eq!(final_catalog.tables.len(), 1);
            assert_eq!(final_catalog.views.len(), 1);

            // Verify custom type
            let active_type = &final_catalog.types[0];
            assert_eq!(active_type.schema, "test_schema");
            assert_eq!(active_type.name, "status_type");

            // Verify table uses custom type
            let accounts_table = &final_catalog.tables[0];
            assert_eq!(accounts_table.schema, "test_schema");
            assert_eq!(accounts_table.name, "accounts");
            assert_eq!(accounts_table.columns.len(), 3);

            let status_column = accounts_table
                .columns
                .iter()
                .find(|c| c.name == "status")
                .unwrap();
            assert_eq!(status_column.data_type, "\"test_schema\".\"status_type\"");

            // Verify view depends on table
            let active_accounts = &final_catalog.views[0];
            assert_eq!(active_accounts.schema, "test_schema");
            assert_eq!(active_accounts.name, "active_accounts");
            assert_eq!(active_accounts.columns.len(), 2);
            assert_eq!(active_accounts.columns[0].name, "id");
            assert_eq!(active_accounts.columns[1].name, "name");

            let depends_on_accounts = active_accounts.depends_on().iter().any(|dep| {
                matches!(dep, pgmt::catalog::id::DbObjectId::Table { schema, name }
                    if schema == "test_schema" && name == "accounts")
            });
            assert!(depends_on_accounts);

            Ok(())
        }).await
    }).await
}

#[tokio::test]
async fn test_view_comment_migration() -> Result<()> {
    with_test_db(async |initial_db| {
        with_test_db(async |target_db| {
            // Initial state: view without comment
            initial_db.execute("CREATE SCHEMA test_schema").await;
            initial_db
                .execute("CREATE TABLE test_schema.users (id INTEGER, name TEXT)")
                .await;
            initial_db
                .execute("CREATE VIEW test_schema.user_summary AS SELECT id, name FROM test_schema.users")
                .await;

            // Target state: view with comment
            target_db.execute("CREATE SCHEMA test_schema").await;
            target_db
                .execute("CREATE TABLE test_schema.users (id INTEGER, name TEXT)")
                .await;
            target_db
                .execute("CREATE VIEW test_schema.user_summary AS SELECT id, name FROM test_schema.users")
                .await;
            target_db
                .execute("COMMENT ON VIEW test_schema.user_summary IS 'Summary view of user information'")
                .await;

            // Load catalogs
            let initial_catalog = Catalog::load(initial_db.pool()).await?;
            let target_catalog = Catalog::load(target_db.pool()).await?;

            // Generate migration steps using full pipeline
            let mut steps = diff_all(&initial_catalog, &target_catalog);
            steps = cascade::expand(steps, &initial_catalog, &target_catalog);
            steps = diff_order(steps, &initial_catalog, &target_catalog)?;

            // Should have SET VIEW COMMENT step
            assert!(!steps.is_empty());
            let comment_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::View(ViewOperation::Comment(CommentOperation::Set { target, comment }))
                    if target.schema == "test_schema" && target.name == "user_summary" && comment == "Summary view of user information")
            }).expect("Should have SetViewComment step");

            match comment_step {
                MigrationStep::View(ViewOperation::Comment(CommentOperation::Set { target, comment })) => {
                    assert_eq!(target.schema, "test_schema");
                    assert_eq!(target.name, "user_summary");
                    assert_eq!(comment, "Summary view of user information");
                }
                _ => panic!("Expected SetViewComment step"),
            }

            // Apply migration
            for step in &steps {
                let sql_list = step.to_sql();
                for rendered in sql_list {
                    initial_db.execute(&rendered.sql).await;
                }
            }

            // Verify final state
            let final_catalog = Catalog::load(initial_db.pool()).await?;
            assert_eq!(final_catalog.views.len(), 1);

            let commented_view = &final_catalog.views[0];
            assert_eq!(commented_view.schema, "test_schema");
            assert_eq!(commented_view.name, "user_summary");
            assert_eq!(
                commented_view.comment,
                Some("Summary view of user information".to_string())
            );

            Ok(())
        }).await
    }).await
}

#[tokio::test]
async fn test_drop_view_comment_migration() -> Result<()> {
    with_test_db(async |initial_db| {
        with_test_db(async |target_db| {
            // Initial state: view with comment
            initial_db.execute("CREATE SCHEMA test_schema").await;
            initial_db
                .execute("CREATE TABLE test_schema.users (id INTEGER, name TEXT)")
                .await;
            initial_db
                .execute("CREATE VIEW test_schema.user_summary AS SELECT id, name FROM test_schema.users")
                .await;
            initial_db
                .execute("COMMENT ON VIEW test_schema.user_summary IS 'Summary view of user information'")
                .await;

            // Target state: view without comment
            target_db.execute("CREATE SCHEMA test_schema").await;
            target_db
                .execute("CREATE TABLE test_schema.users (id INTEGER, name TEXT)")
                .await;
            target_db
                .execute("CREATE VIEW test_schema.user_summary AS SELECT id, name FROM test_schema.users")
                .await;

            // Load catalogs
            let initial_catalog = Catalog::load(initial_db.pool()).await?;
            let target_catalog = Catalog::load(target_db.pool()).await?;

            // Generate migration steps using full pipeline
            let mut steps = diff_all(&initial_catalog, &target_catalog);
            steps = cascade::expand(steps, &initial_catalog, &target_catalog);
            steps = diff_order(steps, &initial_catalog, &target_catalog)?;

            // Should have DROP VIEW COMMENT step
            assert!(!steps.is_empty());
            let comment_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::View(ViewOperation::Comment(CommentOperation::Drop { target }))
                    if target.schema == "test_schema" && target.name == "user_summary")
            }).expect("Should have DropViewComment step");

            match comment_step {
                MigrationStep::View(ViewOperation::Comment(CommentOperation::Drop { target })) => {
                    assert_eq!(target.schema, "test_schema");
                    assert_eq!(target.name, "user_summary");
                }
                _ => panic!("Expected DropViewComment step"),
            }

            // Apply migration
            for step in &steps {
                let sql_list = step.to_sql();
                for rendered in sql_list {
                    initial_db.execute(&rendered.sql).await;
                }
            }

            // Verify final state
            let final_catalog = Catalog::load(initial_db.pool()).await?;
            assert_eq!(final_catalog.views.len(), 1);

            let uncommented_view = &final_catalog.views[0];
            assert_eq!(uncommented_view.schema, "test_schema");
            assert_eq!(uncommented_view.name, "user_summary");
            assert_eq!(uncommented_view.comment, None);

            Ok(())
        }).await
    }).await
}

#[tokio::test]
async fn test_enable_security_invoker() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        &[
            "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)",
        ],
        &["CREATE VIEW user_view AS SELECT id, name FROM users"],
        &["CREATE VIEW user_view WITH (security_invoker = true) AS SELECT id, name FROM users"],
        |steps, _final_catalog| {
            assert!(!steps.is_empty());

            // Should have a SetOption step for security_invoker
            let set_option_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::View(ViewOperation::SetOption { option, enabled, .. })
                    if matches!(option, pgmt::diff::operations::ViewOption::SecurityInvoker) && *enabled)
            }).expect("Should have SetOption step for security_invoker");

            let sql = set_option_step.to_sql();
            assert!(sql[0].sql.contains("ALTER VIEW"));
            assert!(sql[0].sql.contains("SET (security_invoker = on)"));

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_disable_security_invoker() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        &[
            "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)",
        ],
        &["CREATE VIEW user_view WITH (security_invoker = true) AS SELECT id, name FROM users"],
        &["CREATE VIEW user_view AS SELECT id, name FROM users"],
        |steps, _final_catalog| {
            assert!(!steps.is_empty());

            // Should have a SetOption step to disable security_invoker
            let set_option_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::View(ViewOperation::SetOption { option, enabled, .. })
                    if matches!(option, pgmt::diff::operations::ViewOption::SecurityInvoker) && !*enabled)
            }).expect("Should have SetOption step to disable security_invoker");

            let sql = set_option_step.to_sql();
            assert!(sql[0].sql.contains("ALTER VIEW"));
            assert!(sql[0].sql.contains("RESET (security_invoker)"));

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_enable_security_barrier() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        &[
            "CREATE TABLE sensitive_data (id SERIAL PRIMARY KEY, user_id INT, data TEXT)",
        ],
        &["CREATE VIEW user_data AS SELECT id, data FROM sensitive_data WHERE user_id = current_setting('app.user_id')::INT"],
        &["CREATE VIEW user_data WITH (security_barrier = true) AS SELECT id, data FROM sensitive_data WHERE user_id = current_setting('app.user_id')::INT"],
        |steps, _final_catalog| {
            assert!(!steps.is_empty());

            // Should have a SetOption step for security_barrier
            let set_option_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::View(ViewOperation::SetOption { option, enabled, .. })
                    if matches!(option, pgmt::diff::operations::ViewOption::SecurityBarrier) && *enabled)
            }).expect("Should have SetOption step for security_barrier");

            let sql = set_option_step.to_sql();
            assert!(sql[0].sql.contains("ALTER VIEW"));
            assert!(sql[0].sql.contains("SET (security_barrier = on)"));

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_disable_security_barrier() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        &[
            "CREATE TABLE sensitive_data (id SERIAL PRIMARY KEY, user_id INT, data TEXT)",
        ],
        &["CREATE VIEW user_data WITH (security_barrier = true) AS SELECT id, data FROM sensitive_data WHERE user_id = current_setting('app.user_id')::INT"],
        &["CREATE VIEW user_data AS SELECT id, data FROM sensitive_data WHERE user_id = current_setting('app.user_id')::INT"],
        |steps, _final_catalog| {
            assert!(!steps.is_empty());

            // Should have a SetOption step to disable security_barrier
            let set_option_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::View(ViewOperation::SetOption { option, enabled, .. })
                    if matches!(option, pgmt::diff::operations::ViewOption::SecurityBarrier) && !*enabled)
            }).expect("Should have SetOption step to disable security_barrier");

            let sql = set_option_step.to_sql();
            assert!(sql[0].sql.contains("ALTER VIEW"));
            assert!(sql[0].sql.contains("RESET (security_barrier)"));

            Ok(())
        }
    ).await?;

    Ok(())
}
