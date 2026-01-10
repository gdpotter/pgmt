use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::diff::operations::{
    ColumnAction, CommentOperation, MigrationStep, PolicyOperation, TableOperation,
};
use pgmt::render::SqlRenderer;

#[tokio::test]
async fn test_create_policy_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: create table with RLS enabled
            &[
                "CREATE SCHEMA app",
                "CREATE TABLE app.users (id SERIAL PRIMARY KEY, email TEXT)",
                "ALTER TABLE app.users ENABLE ROW LEVEL SECURITY",
            ],
            // Initial DB: no policy
            &[],
            // Target DB: add policy
            &["CREATE POLICY user_select ON app.users FOR SELECT TO PUBLIC USING (true)"],
            |steps, final_catalog| -> Result<()> {
                // Should have a CreatePolicy step
                let create_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Policy(PolicyOperation::Create { policy })
                    if policy.name == "user_select")
                    })
                    .expect("Should have CreatePolicy step");

                match create_step {
                    MigrationStep::Policy(PolicyOperation::Create { policy }) => {
                        assert_eq!(policy.name, "user_select");
                        assert_eq!(policy.schema, "app");
                        assert_eq!(policy.table_name, "users");
                    }
                    _ => panic!("Expected CreatePolicy step"),
                }

                // Verify final state
                assert_eq!(final_catalog.policies.len(), 1);
                let created_policy = &final_catalog.policies[0];
                assert_eq!(created_policy.name, "user_select");
                assert_eq!(created_policy.schema, "app");

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_drop_policy_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: create table with RLS
            &[
                "CREATE TABLE users (id SERIAL PRIMARY KEY)",
                "ALTER TABLE users ENABLE ROW LEVEL SECURITY",
            ],
            // Initial DB: has policy
            &["CREATE POLICY old_policy ON users FOR ALL TO PUBLIC USING (true)"],
            // Target DB: policy removed
            &[],
            |steps, final_catalog| -> Result<()> {
                // Should have a DropPolicy step
                let drop_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Policy(PolicyOperation::Drop { identifier })
                    if identifier.name == "old_policy")
                    })
                    .expect("Should have DropPolicy step");

                match drop_step {
                    MigrationStep::Policy(PolicyOperation::Drop { identifier }) => {
                        assert_eq!(identifier.name, "old_policy");
                        assert_eq!(identifier.schema, "public");
                        assert_eq!(identifier.table, "users");
                    }
                    _ => panic!("Expected DropPolicy step"),
                }

                // Verify final state - no policies
                assert_eq!(final_catalog.policies.len(), 0);

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_alter_policy_roles() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: setup
            &[
                "CREATE TABLE posts (id SERIAL PRIMARY KEY, user_id INT, status TEXT)",
                "ALTER TABLE posts ENABLE ROW LEVEL SECURITY",
            ],
            // Initial DB: policy with simple USING clause
            &["CREATE POLICY post_policy ON posts FOR SELECT TO PUBLIC USING (true)"],
            // Target DB: policy with different USING clause (changing roles not feasible in tests)
            &["CREATE POLICY post_policy ON posts FOR SELECT TO PUBLIC USING (status = 'published')"],
            |steps, final_catalog| -> Result<()> {
                // Should have an AlterPolicy step (for USING change)
                let alter_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Policy(PolicyOperation::Alter { identifier, .. })
                    if identifier.name == "post_policy")
                    })
                    .expect("Should have AlterPolicy step");

                match alter_step {
                    MigrationStep::Policy(PolicyOperation::Alter {
                        identifier,
                        new_using,
                        ..
                    }) => {
                        assert_eq!(identifier.name, "post_policy");
                        assert!(new_using.is_some());
                        let using_expr = new_using.as_ref().unwrap().as_ref().unwrap();
                        assert!(using_expr.contains("published"));
                    }
                    _ => panic!("Expected AlterPolicy step"),
                }

                // Verify final state
                let policy = &final_catalog.policies[0];
                assert!(policy
                    .using_expr
                    .as_ref()
                    .unwrap()
                    .contains("published"));

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_alter_policy_using() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: setup
            &[
                "CREATE TABLE items (id SERIAL PRIMARY KEY, owner_id INT)",
                "ALTER TABLE items ENABLE ROW LEVEL SECURITY",
            ],
            // Initial DB: simple USING clause
            &["CREATE POLICY item_policy ON items FOR ALL TO PUBLIC USING (true)"],
            // Target DB: complex USING clause
            &[r#"CREATE POLICY item_policy ON items FOR ALL TO PUBLIC USING (owner_id = current_setting('app.user_id')::INT)"#],
            |steps, final_catalog| -> Result<()> {
                // Should have an AlterPolicy step
                let alter_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Policy(PolicyOperation::Alter { identifier, .. })
                    if identifier.name == "item_policy")
                    })
                    .expect("Should have AlterPolicy step");

                match alter_step {
                    MigrationStep::Policy(PolicyOperation::Alter { new_using, .. }) => {
                        assert!(new_using.is_some());
                        let using_expr = new_using.as_ref().unwrap().as_ref().unwrap();
                        assert!(using_expr.contains("current_setting"));
                    }
                    _ => panic!("Expected AlterPolicy step"),
                }

                // Verify final state
                let policy = &final_catalog.policies[0];
                assert!(policy.using_expr.as_ref().unwrap().contains("current_setting"));

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_replace_policy_command_change() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: setup
            &[
                "CREATE TABLE data (id SERIAL PRIMARY KEY)",
                "ALTER TABLE data ENABLE ROW LEVEL SECURITY",
            ],
            // Initial DB: FOR ALL
            &["CREATE POLICY data_policy ON data FOR ALL TO PUBLIC USING (true)"],
            // Target DB: FOR SELECT (requires REPLACE)
            &["CREATE POLICY data_policy ON data FOR SELECT TO PUBLIC USING (true)"],
            |steps, final_catalog| -> Result<()> {
                // Should have a ReplacePolicy step (not Alter, since command changed)
                let replace_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Policy(PolicyOperation::Replace { new_policy, .. })
                    if new_policy.name == "data_policy")
                    })
                    .expect("Should have ReplacePolicy step");

                match replace_step {
                    MigrationStep::Policy(PolicyOperation::Replace {
                        old_policy,
                        new_policy,
                    }) => {
                        assert_eq!(old_policy.name, "data_policy");
                        assert_eq!(new_policy.name, "data_policy");
                        // Commands should differ
                        assert_ne!(old_policy.command, new_policy.command);
                    }
                    _ => panic!("Expected ReplacePolicy step"),
                }

                // Verify final state
                assert_eq!(final_catalog.policies.len(), 1);

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_replace_policy_permissive_change() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: setup
            &[
                "CREATE TABLE secure (id SERIAL PRIMARY KEY)",
                "ALTER TABLE secure ENABLE ROW LEVEL SECURITY",
            ],
            // Initial DB: PERMISSIVE (default)
            &["CREATE POLICY secure_policy ON secure FOR ALL TO PUBLIC USING (true)"],
            // Target DB: RESTRICTIVE (requires REPLACE)
            &["CREATE POLICY secure_policy ON secure AS RESTRICTIVE FOR ALL TO PUBLIC USING (true)"],
            |steps, _final_catalog| -> Result<()> {
                // Should have a ReplacePolicy step
                let replace_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Policy(PolicyOperation::Replace { new_policy, .. })
                    if new_policy.name == "secure_policy")
                    })
                    .expect("Should have ReplacePolicy step");

                match replace_step {
                    MigrationStep::Policy(PolicyOperation::Replace {
                        old_policy,
                        new_policy,
                    }) => {
                        assert!(old_policy.permissive);
                        assert!(!new_policy.permissive);
                    }
                    _ => panic!("Expected ReplacePolicy step"),
                }

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_enable_rls_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: create table
            &["CREATE TABLE users (id SERIAL PRIMARY KEY)"],
            // Initial DB: RLS disabled
            &[],
            // Target DB: RLS enabled
            &["ALTER TABLE users ENABLE ROW LEVEL SECURITY"],
            |steps, final_catalog| -> Result<()> {
                // Should have an Alter step with EnableRls action
                let alter_step = steps.iter().find(|s| {
                    matches!(s, MigrationStep::Table(TableOperation::Alter { actions, .. })
                        if actions.iter().any(|a| matches!(a, ColumnAction::EnableRls)))
                });

                assert!(alter_step.is_some(), "Should have EnableRls action");

                // Verify SQL rendering
                let sql = alter_step.unwrap().to_sql();
                assert!(
                    sql.iter()
                        .any(|s| s.sql.contains("ENABLE ROW LEVEL SECURITY"))
                );

                // Verify final state
                let table = final_catalog
                    .tables
                    .iter()
                    .find(|t| t.name == "users")
                    .unwrap();
                assert!(table.rls_enabled);
                assert!(!table.rls_forced);

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_disable_rls_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: create table
            &["CREATE TABLE data (id SERIAL PRIMARY KEY)"],
            // Initial DB: RLS enabled
            &["ALTER TABLE data ENABLE ROW LEVEL SECURITY"],
            // Target DB: RLS disabled
            &[],
            |steps, final_catalog| -> Result<()> {
                // Should have an Alter step with DisableRls action
                let alter_step = steps.iter().find(|s| {
                    matches!(s, MigrationStep::Table(TableOperation::Alter { actions, .. })
                        if actions.iter().any(|a| matches!(a, ColumnAction::DisableRls)))
                });

                assert!(alter_step.is_some(), "Should have DisableRls action");

                // Verify SQL rendering
                let sql = alter_step.unwrap().to_sql();
                let disable_sql = sql
                    .iter()
                    .find(|s| s.sql.contains("DISABLE ROW LEVEL SECURITY"))
                    .expect("Should have DISABLE RLS SQL");

                // Should be marked as destructive
                assert_eq!(
                    disable_sql.safety,
                    pgmt::render::Safety::Destructive,
                    "DISABLE RLS should be destructive"
                );

                // Verify final state
                let table = final_catalog
                    .tables
                    .iter()
                    .find(|t| t.name == "data")
                    .unwrap();
                assert!(!table.rls_enabled);

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_force_rls_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: create table with RLS enabled
            &[
                "CREATE TABLE secure_data (id SERIAL PRIMARY KEY)",
                "ALTER TABLE secure_data ENABLE ROW LEVEL SECURITY",
            ],
            // Initial DB: RLS enabled but not forced
            &[],
            // Target DB: RLS forced
            &["ALTER TABLE secure_data FORCE ROW LEVEL SECURITY"],
            |steps, final_catalog| -> Result<()> {
                // Should have an Alter step with ForceRls action
                let alter_step = steps.iter().find(|s| {
                    matches!(s, MigrationStep::Table(TableOperation::Alter { actions, .. })
                        if actions.iter().any(|a| matches!(a, ColumnAction::ForceRls)))
                });

                assert!(alter_step.is_some(), "Should have ForceRls action");

                // Verify final state
                let table = final_catalog
                    .tables
                    .iter()
                    .find(|t| t.name == "secure_data")
                    .unwrap();
                assert!(table.rls_enabled);
                assert!(table.rls_forced);

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_policy_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: setup with policy
            &[
                "CREATE TABLE tasks (id SERIAL PRIMARY KEY)",
                "ALTER TABLE tasks ENABLE ROW LEVEL SECURITY",
                "CREATE POLICY task_policy ON tasks FOR ALL TO PUBLIC USING (true)",
            ],
            // Initial DB: no comment
            &[],
            // Target DB: add comment
            &["COMMENT ON POLICY task_policy ON tasks IS 'Allow all users to see tasks'"],
            |steps, final_catalog| -> Result<()> {
                // Should have a Comment step
                let comment_step = steps.iter().find(|s| {
                    matches!(s, MigrationStep::Policy(PolicyOperation::Comment(CommentOperation::Set { target, .. }))
                        if target.name == "task_policy")
                });

                assert!(comment_step.is_some(), "Should have policy comment step");

                // Verify final state
                let policy = &final_catalog.policies[0];
                assert_eq!(
                    policy.comment,
                    Some("Allow all users to see tasks".to_string())
                );

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_drop_policy_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: setup
            &[
                "CREATE TABLE items (id SERIAL PRIMARY KEY)",
                "ALTER TABLE items ENABLE ROW LEVEL SECURITY",
                "CREATE POLICY item_policy ON items FOR ALL TO PUBLIC USING (true)",
            ],
            // Initial DB: has comment
            &["COMMENT ON POLICY item_policy ON items IS 'Old comment'"],
            // Target DB: no comment
            &[],
            |steps, final_catalog| -> Result<()> {
                // Should have a Comment drop step
                let comment_step = steps.iter().find(|s| {
                    matches!(s, MigrationStep::Policy(PolicyOperation::Comment(CommentOperation::Drop { target }))
                        if target.name == "item_policy")
                });

                assert!(
                    comment_step.is_some(),
                    "Should have policy comment drop step"
                );

                // Verify final state
                let policy = &final_catalog.policies[0];
                assert_eq!(policy.comment, None);

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_policy_ordering_after_table() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: nothing
            &[],
            // Initial DB: nothing
            &[],
            // Target DB: create table and policy together
            &[
                "CREATE TABLE projects (id SERIAL PRIMARY KEY)",
                "ALTER TABLE projects ENABLE ROW LEVEL SECURITY",
                "CREATE POLICY project_policy ON projects FOR ALL TO PUBLIC USING (true)",
            ],
            |steps, final_catalog| -> Result<()> {
                // Find table creation and policy creation
                let table_step_idx = steps
                    .iter()
                    .position(|s| matches!(s, MigrationStep::Table(TableOperation::Create { name, .. }) if name == "projects"))
                    .expect("Should have CreateTable step");

                let policy_step_idx = steps
                    .iter()
                    .position(|s| matches!(s, MigrationStep::Policy(PolicyOperation::Create { policy }) if policy.name == "project_policy"))
                    .expect("Should have CreatePolicy step");

                // Policy must come after table
                assert!(
                    policy_step_idx > table_step_idx,
                    "Policy creation should come after table creation"
                );

                // Verify final state
                assert_eq!(final_catalog.tables.len(), 1);
                assert_eq!(final_catalog.policies.len(), 1);

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_create_table_with_rls_enabled() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: empty
            &[],
            // Initial DB: no table
            &[],
            // Target DB: table with RLS enabled
            &[
                "CREATE TABLE secure_users (id SERIAL PRIMARY KEY, name TEXT)",
                "ALTER TABLE secure_users ENABLE ROW LEVEL SECURITY",
            ],
            |steps, final_catalog| -> Result<()> {
                // Should have CreateTable step
                let create_step = steps.iter().find(|s| {
                    matches!(s, MigrationStep::Table(TableOperation::Create { name, .. })
                        if name == "secure_users")
                });
                assert!(create_step.is_some(), "Should have CreateTable step");

                // Should have EnableRls step
                let enable_rls_step = steps.iter().find(|s| {
                    matches!(s, MigrationStep::Table(TableOperation::Alter { name, actions, .. })
                        if name == "secure_users" && actions.iter().any(|a| matches!(a, ColumnAction::EnableRls)))
                });
                assert!(enable_rls_step.is_some(), "Should have EnableRls step");

                // Verify SQL rendering
                let sql = enable_rls_step.unwrap().to_sql();
                assert!(
                    sql.iter()
                        .any(|s| s.sql.contains("ENABLE ROW LEVEL SECURITY"))
                );

                // Verify final state
                let table = final_catalog
                    .tables
                    .iter()
                    .find(|t| t.name == "secure_users")
                    .unwrap();
                assert!(table.rls_enabled, "RLS should be enabled");
                assert!(!table.rls_forced, "RLS should not be forced");

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_create_table_with_rls_forced() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: empty
            &[],
            // Initial DB: no table
            &[],
            // Target DB: table with RLS enabled and forced
            &[
                "CREATE TABLE admin_data (id SERIAL PRIMARY KEY, value TEXT)",
                "ALTER TABLE admin_data ENABLE ROW LEVEL SECURITY",
                "ALTER TABLE admin_data FORCE ROW LEVEL SECURITY",
            ],
            |steps, final_catalog| -> Result<()> {
                // Should have CreateTable step
                let create_step = steps.iter().find(|s| {
                    matches!(s, MigrationStep::Table(TableOperation::Create { name, .. })
                        if name == "admin_data")
                });
                assert!(create_step.is_some(), "Should have CreateTable step");

                // Should have EnableRls step
                let enable_rls_step = steps.iter().find(|s| {
                    matches!(s, MigrationStep::Table(TableOperation::Alter { name, actions, .. })
                        if name == "admin_data" && actions.iter().any(|a| matches!(a, ColumnAction::EnableRls)))
                });
                assert!(enable_rls_step.is_some(), "Should have EnableRls step");

                // Should have ForceRls step
                let force_rls_step = steps.iter().find(|s| {
                    matches!(s, MigrationStep::Table(TableOperation::Alter { name, actions, .. })
                        if name == "admin_data" && actions.iter().any(|a| matches!(a, ColumnAction::ForceRls)))
                });
                assert!(force_rls_step.is_some(), "Should have ForceRls step");

                // Verify SQL rendering for EnableRls
                let enable_sql = enable_rls_step.unwrap().to_sql();
                assert!(
                    enable_sql
                        .iter()
                        .any(|s| s.sql.contains("ENABLE ROW LEVEL SECURITY"))
                );

                // Verify SQL rendering for ForceRls
                let force_sql = force_rls_step.unwrap().to_sql();
                assert!(
                    force_sql
                        .iter()
                        .any(|s| s.sql.contains("FORCE ROW LEVEL SECURITY"))
                );

                // Verify final state
                let table = final_catalog
                    .tables
                    .iter()
                    .find(|t| t.name == "admin_data")
                    .unwrap();
                assert!(table.rls_enabled, "RLS should be enabled");
                assert!(table.rls_forced, "RLS should be forced");

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_policy_cascade_on_column_type_change() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema
            &["CREATE SCHEMA app"],
            // Initial DB: table with SMALLINT, RLS enabled, policy referencing column
            &[
                "CREATE TABLE app.users (id INTEGER, status SMALLINT)",
                "ALTER TABLE app.users ENABLE ROW LEVEL SECURITY",
                "CREATE POLICY user_status ON app.users FOR SELECT USING (status > 0)",
            ],
            // Target DB: table with BIGINT (column type changed), same policy
            &[
                "CREATE TABLE app.users (id INTEGER, status BIGINT)",
                "ALTER TABLE app.users ENABLE ROW LEVEL SECURITY",
                "CREATE POLICY user_status ON app.users FOR SELECT USING (status > 0)",
            ],
            |steps, final_catalog| {
                // Should have: Drop policy → Alter table → Create policy
                assert!(steps.len() >= 3);

                let drop_policy_pos = steps
                    .iter()
                    .position(|s| {
                        matches!(s, MigrationStep::Policy(PolicyOperation::Drop { identifier })
                        if identifier.table == "users" && identifier.name == "user_status")
                    })
                    .expect("Should have DropPolicy step");

                let alter_table_pos = steps
                    .iter()
                    .position(|s| {
                        matches!(s, MigrationStep::Table(TableOperation::Alter { schema, name, actions })
                        if schema == "app" && name == "users"
                        && actions.iter().any(|a| matches!(a, ColumnAction::AlterType { .. })))
                    })
                    .expect("Should have AlterTable step");

                let create_policy_pos = steps
                    .iter()
                    .position(|s| {
                        matches!(s, MigrationStep::Policy(PolicyOperation::Create { policy })
                        if policy.table_name == "users" && policy.name == "user_status")
                    })
                    .expect("Should have CreatePolicy step");

                assert!(
                    drop_policy_pos < alter_table_pos,
                    "Policy should be dropped before table is altered"
                );
                assert!(
                    alter_table_pos < create_policy_pos,
                    "Table should be altered before policy is recreated"
                );

                // Verify final state
                assert_eq!(final_catalog.policies.len(), 1);
                let policy = &final_catalog.policies[0];
                assert_eq!(policy.name, "user_status");

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_multiple_policies_cascade_on_column_type_change() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema
            &["CREATE SCHEMA app"],
            // Initial DB: table with two policies
            &[
                "CREATE TABLE app.items (id INTEGER, priority SMALLINT, owner_id INTEGER)",
                "ALTER TABLE app.items ENABLE ROW LEVEL SECURITY",
                "CREATE POLICY priority_policy ON app.items FOR SELECT USING (priority > 0)",
                "CREATE POLICY owner_policy ON app.items FOR UPDATE USING (owner_id = 1)",
            ],
            // Target DB: column type changed
            &[
                "CREATE TABLE app.items (id INTEGER, priority BIGINT, owner_id INTEGER)",
                "ALTER TABLE app.items ENABLE ROW LEVEL SECURITY",
                "CREATE POLICY priority_policy ON app.items FOR SELECT USING (priority > 0)",
                "CREATE POLICY owner_policy ON app.items FOR UPDATE USING (owner_id = 1)",
            ],
            |steps, final_catalog| {
                // Both policies should be dropped and recreated
                let drop_priority = steps.iter().any(|s| {
                    matches!(s, MigrationStep::Policy(PolicyOperation::Drop { identifier })
                        if identifier.name == "priority_policy")
                });
                let drop_owner = steps.iter().any(|s| {
                    matches!(s, MigrationStep::Policy(PolicyOperation::Drop { identifier })
                        if identifier.name == "owner_policy")
                });
                let create_priority = steps.iter().any(|s| {
                    matches!(s, MigrationStep::Policy(PolicyOperation::Create { policy })
                        if policy.name == "priority_policy")
                });
                let create_owner = steps.iter().any(|s| {
                    matches!(s, MigrationStep::Policy(PolicyOperation::Create { policy })
                        if policy.name == "owner_policy")
                });

                assert!(drop_priority, "Should drop priority_policy");
                assert!(drop_owner, "Should drop owner_policy");
                assert!(create_priority, "Should create priority_policy");
                assert!(create_owner, "Should create owner_policy");

                // Verify final state
                assert_eq!(final_catalog.policies.len(), 2);

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_no_policy_cascade_without_column_type_change() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema
            &["CREATE SCHEMA app"],
            // Initial DB: table with policy
            &[
                "CREATE TABLE app.data (id INTEGER, value INTEGER)",
                "ALTER TABLE app.data ENABLE ROW LEVEL SECURITY",
                "CREATE POLICY data_policy ON app.data FOR SELECT USING (value > 0)",
            ],
            // Target DB: only default value changed (not type)
            &[
                "CREATE TABLE app.data (id INTEGER, value INTEGER DEFAULT 42)",
                "ALTER TABLE app.data ENABLE ROW LEVEL SECURITY",
                "CREATE POLICY data_policy ON app.data FOR SELECT USING (value > 0)",
            ],
            |steps, final_catalog| {
                // Should NOT have policy drop/create - only the default change
                let has_policy_drop = steps
                    .iter()
                    .any(|s| matches!(s, MigrationStep::Policy(PolicyOperation::Drop { .. })));
                let has_policy_create = steps
                    .iter()
                    .any(|s| matches!(s, MigrationStep::Policy(PolicyOperation::Create { .. })));

                assert!(
                    !has_policy_drop,
                    "Should NOT drop policy when only default changes"
                );
                assert!(
                    !has_policy_create,
                    "Should NOT create policy when only default changes"
                );

                // Verify final state - policy unchanged
                assert_eq!(final_catalog.policies.len(), 1);
                assert_eq!(final_catalog.policies[0].name, "data_policy");

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_policy_cascade_replaces_alter_on_column_type_change() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema
            &["CREATE SCHEMA app"],
            // Initial DB: table with policy
            &[
                "CREATE TABLE app.users (id INTEGER, tenant_id SMALLINT)",
                "ALTER TABLE app.users ENABLE ROW LEVEL SECURITY",
                "CREATE POLICY tenant_isolation ON app.users FOR ALL USING (tenant_id = 1)",
            ],
            // Target DB: column type changed AND policy expression changed
            // This would normally generate ALTER POLICY, but that also fails on type change
            &[
                "CREATE TABLE app.users (id INTEGER, tenant_id BIGINT)",
                "ALTER TABLE app.users ENABLE ROW LEVEL SECURITY",
                "CREATE POLICY tenant_isolation ON app.users FOR ALL USING (tenant_id = 2)",
            ],
            |steps, final_catalog| {
                // Should have DROP+CREATE policy, NOT ALTER POLICY
                let has_policy_alter = steps
                    .iter()
                    .any(|s| matches!(s, MigrationStep::Policy(PolicyOperation::Alter { .. })));
                let has_policy_drop = steps.iter().any(|s| {
                    matches!(s, MigrationStep::Policy(PolicyOperation::Drop { identifier })
                        if identifier.name == "tenant_isolation")
                });
                let has_policy_create = steps.iter().any(|s| {
                    matches!(s, MigrationStep::Policy(PolicyOperation::Create { policy })
                        if policy.name == "tenant_isolation")
                });

                assert!(
                    !has_policy_alter,
                    "Should NOT have ALTER POLICY when column type is changing"
                );
                assert!(has_policy_drop, "Should have DROP POLICY");
                assert!(has_policy_create, "Should have CREATE POLICY");

                // Verify ordering: DROP policy → ALTER table → CREATE policy
                let drop_pos = steps
                    .iter()
                    .position(|s| matches!(s, MigrationStep::Policy(PolicyOperation::Drop { .. })))
                    .unwrap();
                let alter_pos = steps
                    .iter()
                    .position(|s| matches!(s, MigrationStep::Table(TableOperation::Alter { .. })))
                    .unwrap();
                let create_pos = steps
                    .iter()
                    .position(|s| {
                        matches!(s, MigrationStep::Policy(PolicyOperation::Create { .. }))
                    })
                    .unwrap();

                assert!(drop_pos < alter_pos, "DROP POLICY before ALTER TABLE");
                assert!(alter_pos < create_pos, "ALTER TABLE before CREATE POLICY");

                // Verify final state has the new expression
                assert_eq!(final_catalog.policies.len(), 1);
                let policy = &final_catalog.policies[0];
                assert!(policy.using_expr.as_ref().unwrap().contains("2"));

                Ok(())
            },
        )
        .await?;
    Ok(())
}
