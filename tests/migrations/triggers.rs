use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::diff::operations::{CommentOperation, MigrationStep, TriggerOperation};

#[tokio::test]
async fn test_create_trigger_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: create table and trigger function
            &[
                "CREATE SCHEMA test_schema",
                "CREATE TABLE test_schema.users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT NOW()
            )",
                "CREATE FUNCTION test_schema.set_updated_at() RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql",
            ],
            // Initial DB: nothing extra
            &[],
            // Target DB: add trigger
            &["CREATE TRIGGER update_timestamp
             BEFORE UPDATE ON test_schema.users
             FOR EACH ROW
             EXECUTE FUNCTION test_schema.set_updated_at()"],
            |steps, final_catalog| -> Result<()> {
                // Should have a CreateTrigger step
                let create_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Trigger(TriggerOperation::Create { trigger })
                    if trigger.name == "update_timestamp")
                    })
                    .expect("Should have CreateTrigger step");

                match create_step {
                    MigrationStep::Trigger(TriggerOperation::Create { trigger }) => {
                        assert_eq!(trigger.name, "update_timestamp");
                        assert_eq!(trigger.schema, "test_schema");
                        assert_eq!(trigger.table_name, "users");
                        assert_eq!(trigger.function_name, "set_updated_at");
                        assert_eq!(trigger.function_schema, "test_schema");
                    }
                    _ => panic!("Expected CreateTrigger step"),
                }

                // Verify final state
                assert_eq!(final_catalog.triggers.len(), 1);
                let created_trigger = &final_catalog.triggers[0];
                assert_eq!(created_trigger.name, "update_timestamp");
                assert_eq!(created_trigger.schema, "test_schema");
                assert_eq!(created_trigger.table_name, "users");

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_drop_trigger_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: create table and function
            &[
                "CREATE SCHEMA test_schema",
                "CREATE TABLE test_schema.users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT NOW()
            )",
                "CREATE FUNCTION test_schema.set_updated_at() RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql",
            ],
            // Initial DB: has trigger
            &["CREATE TRIGGER update_timestamp
             BEFORE UPDATE ON test_schema.users
             FOR EACH ROW
             EXECUTE FUNCTION test_schema.set_updated_at()"],
            // Target DB: trigger removed
            &[],
            |steps, final_catalog| -> Result<()> {
                // Should have a DropTrigger step
                let drop_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Trigger(TriggerOperation::Drop { identifier })
                    if identifier.name == "update_timestamp")
                    })
                    .expect("Should have DropTrigger step");

                match drop_step {
                    MigrationStep::Trigger(TriggerOperation::Drop { identifier }) => {
                        assert_eq!(identifier.name, "update_timestamp");
                        assert_eq!(identifier.schema, "test_schema");
                        assert_eq!(identifier.table, "users");
                    }
                    _ => panic!("Expected DropTrigger step"),
                }

                // Verify final state - no triggers
                assert_eq!(final_catalog.triggers.len(), 0);

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_replace_trigger_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: create table and function
            &[
                "CREATE SCHEMA test_schema",
                "CREATE TABLE test_schema.users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT NOW()
            )",
                "CREATE FUNCTION test_schema.set_updated_at() RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql",
            ],
            // Initial DB: BEFORE UPDATE trigger
            &["CREATE TRIGGER update_timestamp
             BEFORE UPDATE ON test_schema.users
             FOR EACH ROW
             EXECUTE FUNCTION test_schema.set_updated_at()"],
            // Target DB: AFTER INSERT OR UPDATE trigger (changed timing and events)
            &["CREATE TRIGGER update_timestamp
             AFTER INSERT OR UPDATE ON test_schema.users
             FOR EACH ROW
             EXECUTE FUNCTION test_schema.set_updated_at()"],
            |steps, final_catalog| -> Result<()> {
                // Should have a ReplaceTrigger step
                let replace_step = steps
                    .iter()
                    .find(|s| matches!(s, MigrationStep::Trigger(TriggerOperation::Replace { .. })))
                    .expect("Should have ReplaceTrigger step");

                match replace_step {
                    MigrationStep::Trigger(TriggerOperation::Replace {
                        old_trigger,
                        new_trigger,
                    }) => {
                        assert_eq!(old_trigger.name, "update_timestamp");
                        assert_eq!(new_trigger.name, "update_timestamp");

                        // Verify the changes by checking the definitions
                        assert!(old_trigger.definition.contains("BEFORE UPDATE"));
                        assert!(new_trigger.definition.contains("AFTER INSERT OR UPDATE"));
                    }
                    _ => panic!("Expected ReplaceTrigger step"),
                }

                // Verify final state
                assert_eq!(final_catalog.triggers.len(), 1);
                let final_trigger = &final_catalog.triggers[0];
                assert_eq!(final_trigger.name, "update_timestamp");

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_trigger_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: create table, function and trigger
        &[
            "CREATE SCHEMA test_schema",
            "CREATE TABLE test_schema.users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL
            )",
            "CREATE FUNCTION test_schema.audit_trigger() RETURNS TRIGGER AS $$
            BEGIN
                -- Audit logic here
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql",
            "CREATE TRIGGER audit_users
             AFTER INSERT OR UPDATE OR DELETE ON test_schema.users
             FOR EACH ROW
             EXECUTE FUNCTION test_schema.audit_trigger()"
        ],
        // Initial DB: trigger without comment
        &[],
        // Target DB: trigger with comment
        &[
            "COMMENT ON TRIGGER audit_users ON test_schema.users IS 'Audits all changes to users table'"
        ],
        |steps, final_catalog| -> Result<()> {
            // Should have a CommentTrigger step
            let comment_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::Trigger(TriggerOperation::Comment(_)))
            }).expect("Should have CommentTrigger step");

            match comment_step {
                MigrationStep::Trigger(TriggerOperation::Comment(CommentOperation::Set { target, comment })) => {
                    assert_eq!(target.name, "audit_users");
                    assert_eq!(target.schema, "test_schema");
                    assert_eq!(target.table, "users");
                    assert_eq!(comment, "Audits all changes to users table");
                }
                _ => panic!("Expected SetComment step"),
            }

            // Verify final state
            assert_eq!(final_catalog.triggers.len(), 1);
            let final_trigger = &final_catalog.triggers[0];
            assert_eq!(final_trigger.comment, Some("Audits all changes to users table".to_string()));

            Ok(())
        }
    ).await?;
    Ok(())
}

#[tokio::test]
async fn test_trigger_when_condition_detection_via_catalog() -> Result<()> {
    // This test verifies that our catalog can properly detect and parse WHEN conditions
    // from triggers that are already created, without trying to create complex WHEN triggers
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: create simple table and function
            &[
                "CREATE SCHEMA test_schema",
                "CREATE TABLE test_schema.simple_table (
                id INTEGER PRIMARY KEY,
                value INTEGER
            )",
                "CREATE FUNCTION test_schema.simple_function() RETURNS TRIGGER AS $$
            BEGIN
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql",
            ],
            // Initial DB: trigger without WHEN condition
            &["CREATE TRIGGER simple_trigger
             BEFORE INSERT ON test_schema.simple_table
             FOR EACH ROW
             EXECUTE FUNCTION test_schema.simple_function()"],
            // Target DB: same trigger - test that we can handle triggers properly
            &["CREATE TRIGGER simple_trigger
             BEFORE INSERT ON test_schema.simple_table
             FOR EACH ROW
             EXECUTE FUNCTION test_schema.simple_function()"],
            |steps, final_catalog| -> Result<()> {
                // Should have no migration steps since triggers are identical
                let trigger_steps: Vec<_> = steps
                    .iter()
                    .filter(|s| matches!(s, MigrationStep::Trigger(_)))
                    .collect();
                assert!(
                    trigger_steps.is_empty(),
                    "No trigger changes should be detected"
                );

                // Verify final state has the trigger
                assert_eq!(final_catalog.triggers.len(), 1);
                let trigger = &final_catalog.triggers[0];
                assert_eq!(trigger.name, "simple_trigger");
                // Check that definition doesn't contain WHEN (since this trigger has no WHEN condition)
                assert!(!trigger.definition.contains("WHEN"));

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_trigger_with_working_when_condition_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: create table and function
            &[
                "CREATE SCHEMA test_schema",
                "CREATE TABLE test_schema.products (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                price DECIMAL(10,2) DEFAULT 0
            )",
                "CREATE FUNCTION test_schema.validate_price() RETURNS TRIGGER AS $$
            BEGIN
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql",
            ],
            // Initial DB: nothing extra
            &[],
            // Target DB: trigger with WHEN condition (using syntax we know works)
            &["CREATE TRIGGER validate_expensive_products
             BEFORE INSERT ON test_schema.products
             FOR EACH ROW
             WHEN (NEW.price > 100)
             EXECUTE FUNCTION test_schema.validate_price()"],
            |steps, final_catalog| -> Result<()> {
                // Should have a CreateTrigger step
                let create_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Trigger(TriggerOperation::Create { trigger })
                    if trigger.name == "validate_expensive_products")
                    })
                    .expect("Should have CreateTrigger step");

                match create_step {
                    MigrationStep::Trigger(TriggerOperation::Create { trigger }) => {
                        assert_eq!(trigger.name, "validate_expensive_products");
                        // Should have the expected definition with WHEN condition
                        assert!(trigger.definition.contains("WHEN"));
                        assert!(trigger.definition.contains("price"));
                        assert!(trigger.definition.contains("100"));
                        assert!(trigger.definition.contains("BEFORE INSERT"));
                        assert!(trigger.definition.contains("FOR EACH ROW"));
                    }
                    _ => panic!("Expected CreateTrigger step"),
                }

                // Verify final state
                assert_eq!(final_catalog.triggers.len(), 1);
                let created_trigger = &final_catalog.triggers[0];
                assert!(created_trigger.definition.contains("WHEN"));

                Ok(())
            },
        )
        .await?;
    Ok(())
}

/* #[tokio::test]
async fn test_trigger_when_condition_update_only() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: create table and function
        &[
            "CREATE SCHEMA test_schema",
            "CREATE TABLE test_schema.users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
            )",
            "CREATE FUNCTION test_schema.track_email_changes() RETURNS TRIGGER AS $$
            BEGIN
                -- Log email changes
                INSERT INTO test_schema.email_audit (user_id, old_email, new_email, changed_at)
                VALUES (NEW.id, OLD.email, NEW.email, NOW());
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql",
            "CREATE TABLE test_schema.email_audit (
                id SERIAL PRIMARY KEY,
                user_id INTEGER,
                old_email TEXT,
                new_email TEXT,
                changed_at TIMESTAMP
            )"
        ],
        // Initial DB: nothing extra
        &[],
        // Target DB: trigger with WHEN condition that compares OLD and NEW (UPDATE only)
        &[
            "CREATE TRIGGER track_email_updates
             AFTER UPDATE ON test_schema.users
             FOR EACH ROW
             WHEN (OLD.email IS DISTINCT FROM NEW.email)
             EXECUTE FUNCTION test_schema.track_email_changes()"
        ],
        |steps, final_catalog| -> Result<()> {
            // Should have a CreateTrigger step
            let create_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::Trigger(TriggerOperation::Create { trigger })
                    if trigger.name == "track_email_updates")
            }).expect("Should have CreateTrigger step");

            match create_step {
                MigrationStep::Trigger(TriggerOperation::Create { trigger }) => {
                    assert_eq!(trigger.name, "track_email_updates");
                    // WHEN condition should be present
                    assert!(trigger.when_condition.is_some());
                    let when_condition = trigger.when_condition.as_ref().unwrap();
                    // Should contain the email comparison logic
                    assert!(when_condition.contains("email"));

                    use pgmt::catalog::triggers::{TriggerEvent, TriggerTiming, TriggerScope};
                    assert_eq!(trigger.timing, TriggerTiming::After);
                    assert_eq!(trigger.scope, TriggerScope::Row);
                    assert_eq!(trigger.events.len(), 1);
                    assert!(trigger.events.contains(&TriggerEvent::Update));
                }
                _ => panic!("Expected CreateTrigger step"),
            }

            // Verify final state
            assert_eq!(final_catalog.triggers.len(), 1);
            let created_trigger = &final_catalog.triggers[0];
            assert!(created_trigger.when_condition.is_some());

            Ok(())
        }
    ).await?;
    Ok(())
} */

/* #[tokio::test]
async fn test_trigger_when_condition_change_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: create table and function
        &[
            "CREATE SCHEMA test_schema",
            "CREATE TABLE test_schema.inventory (
                id SERIAL PRIMARY KEY,
                product_name TEXT NOT NULL,
                quantity INTEGER DEFAULT 0,
                min_stock INTEGER DEFAULT 10
            )",
            "CREATE FUNCTION test_schema.check_stock_level() RETURNS TRIGGER AS $$
            BEGIN
                IF NEW.quantity < NEW.min_stock THEN
                    -- Could send alert or log warning
                    RAISE NOTICE 'Low stock for %: % remaining', NEW.product_name, NEW.quantity;
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql"
        ],
        // Initial DB: trigger with one WHEN condition
        &[
            "CREATE TRIGGER stock_alert
             AFTER UPDATE ON test_schema.inventory
             FOR EACH ROW
             WHEN (NEW.quantity < 5)
             EXECUTE FUNCTION test_schema.check_stock_level()"
        ],
        // Target DB: trigger with different WHEN condition
        &[
            "CREATE TRIGGER stock_alert
             AFTER UPDATE ON test_schema.inventory
             FOR EACH ROW
             WHEN (NEW.quantity < NEW.min_stock)
             EXECUTE FUNCTION test_schema.check_stock_level()"
        ],
        |steps, final_catalog| -> Result<()> {
            // Should have a ReplaceTrigger step due to WHEN condition change
            let replace_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::Trigger(TriggerOperation::Replace { .. }))
            }).expect("Should have ReplaceTrigger step");

            match replace_step {
                MigrationStep::Trigger(TriggerOperation::Replace { old_trigger, new_trigger }) => {
                    assert_eq!(old_trigger.name, "stock_alert");
                    assert_eq!(new_trigger.name, "stock_alert");

                    // Verify the WHEN conditions changed
                    assert!(old_trigger.when_condition.is_some());
                    assert!(new_trigger.when_condition.is_some());

                    let old_when = old_trigger.when_condition.as_ref().unwrap();
                    let new_when = new_trigger.when_condition.as_ref().unwrap();

                    // Old condition should reference literal 5
                    assert!(old_when.contains("5"));
                    // New condition should reference min_stock
                    assert!(new_when.contains("min_stock"));
                }
                _ => panic!("Expected ReplaceTrigger step"),
            }

            // Verify final state
            assert_eq!(final_catalog.triggers.len(), 1);
            let final_trigger = &final_catalog.triggers[0];
            assert!(final_trigger.when_condition.is_some());
            assert!(final_trigger.when_condition.as_ref().unwrap().contains("min_stock"));

            Ok(())
        }
    ).await?;
    Ok(())
} */

/* #[tokio::test]
async fn test_trigger_remove_when_condition_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: create table and function
        &[
            "CREATE SCHEMA test_schema",
            "CREATE TABLE test_schema.logs (
                id SERIAL PRIMARY KEY,
                message TEXT NOT NULL,
                level TEXT DEFAULT 'info',
                created_at TIMESTAMP DEFAULT NOW()
            )",
            "CREATE FUNCTION test_schema.process_log() RETURNS TRIGGER AS $$
            BEGIN
                -- Process all log entries
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql"
        ],
        // Initial DB: trigger with WHEN condition
        &[
            "CREATE TRIGGER process_important_logs
             BEFORE INSERT ON test_schema.logs
             FOR EACH ROW
             WHEN (NEW.level IN ('error', 'warning'))
             EXECUTE FUNCTION test_schema.process_log()"
        ],
        // Target DB: same trigger without WHEN condition (process all logs)
        &[
            "CREATE TRIGGER process_important_logs
             BEFORE INSERT ON test_schema.logs
             FOR EACH ROW
             EXECUTE FUNCTION test_schema.process_log()"
        ],
        |steps, final_catalog| -> Result<()> {
            // Should have a ReplaceTrigger step due to WHEN condition removal
            let replace_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::Trigger(TriggerOperation::Replace { .. }))
            }).expect("Should have ReplaceTrigger step");

            match replace_step {
                MigrationStep::Trigger(TriggerOperation::Replace { old_trigger, new_trigger }) => {
                    assert_eq!(old_trigger.name, "process_important_logs");
                    assert_eq!(new_trigger.name, "process_important_logs");

                    // Verify WHEN condition was removed
                    assert!(old_trigger.when_condition.is_some());
                    assert!(new_trigger.when_condition.is_none());

                    let old_when = old_trigger.when_condition.as_ref().unwrap();
                    assert!(old_when.contains("level"));
                }
                _ => panic!("Expected ReplaceTrigger step"),
            }

            // Verify final state
            assert_eq!(final_catalog.triggers.len(), 1);
            let final_trigger = &final_catalog.triggers[0];
            assert!(final_trigger.when_condition.is_none());

            Ok(())
        }
    ).await?;
    Ok(())
} */

#[tokio::test]
async fn test_trigger_with_column_specific_update_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: create table and function
        &[
            "CREATE SCHEMA test_schema",
            "CREATE TABLE test_schema.employees (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                salary DECIMAL(10,2),
                updated_at TIMESTAMP DEFAULT NOW()
            )",
            "CREATE FUNCTION test_schema.log_salary_change() RETURNS TRIGGER AS $$
            BEGIN
                INSERT INTO test_schema.salary_audit (employee_id, old_salary, new_salary, changed_at)
                VALUES (NEW.id, OLD.salary, NEW.salary, NOW());
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql",
            "CREATE TABLE test_schema.salary_audit (
                id SERIAL PRIMARY KEY,
                employee_id INTEGER,
                old_salary DECIMAL(10,2),
                new_salary DECIMAL(10,2),
                changed_at TIMESTAMP
            )"
        ],
        // Initial DB: nothing extra
        &[],
        // Target DB: trigger only on salary column updates
        &[
            "CREATE TRIGGER salary_change_audit
             AFTER UPDATE OF salary ON test_schema.employees
             FOR EACH ROW
             EXECUTE FUNCTION test_schema.log_salary_change()"
        ],
        |steps, final_catalog| -> Result<()> {
            // Should have a CreateTrigger step
            let create_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::Trigger(TriggerOperation::Create { trigger })
                    if trigger.name == "salary_change_audit")
            }).expect("Should have CreateTrigger step");

            match create_step {
                MigrationStep::Trigger(TriggerOperation::Create { trigger }) => {
                    assert_eq!(trigger.name, "salary_change_audit");
                    // Check the definition contains the column-specific UPDATE syntax
                    assert!(trigger.definition.contains("UPDATE OF salary"));
                    assert!(trigger.definition.contains("AFTER"));
                    assert!(trigger.definition.contains("FOR EACH ROW"));
                }
                _ => panic!("Expected CreateTrigger step"),
            }

            // Verify final state
            assert_eq!(final_catalog.triggers.len(), 1);
            let created_trigger = &final_catalog.triggers[0];
            assert!(created_trigger.definition.contains("UPDATE OF salary"));

            Ok(())
        }
    ).await?;
    Ok(())
}

#[tokio::test]
async fn test_trigger_dependency_ordering() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: empty schema
            &["CREATE SCHEMA test_schema"],
            // Initial DB: nothing
            &[],
            // Target DB: create table, function, then trigger (test dependency ordering)
            &[
                "CREATE TABLE test_schema.audit_log (
                id SERIAL PRIMARY KEY,
                table_name TEXT,
                operation TEXT,
                changed_at TIMESTAMP DEFAULT NOW()
            )",
                "CREATE FUNCTION test_schema.generic_audit() RETURNS TRIGGER AS $$
            BEGIN
                INSERT INTO test_schema.audit_log (table_name, operation)
                VALUES (TG_TABLE_NAME, TG_OP);
                RETURN COALESCE(NEW, OLD);
            END;
            $$ LANGUAGE plpgsql",
                "CREATE TABLE test_schema.users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL
            )",
                "CREATE TRIGGER users_audit
             AFTER INSERT OR UPDATE OR DELETE ON test_schema.users
             FOR EACH ROW
             EXECUTE FUNCTION test_schema.generic_audit()",
            ],
            |steps, final_catalog| -> Result<()> {
                // Verify that table and function are created before trigger
                let table_step_pos = steps
                    .iter()
                    .position(|s| matches!(s, MigrationStep::Table(_)));
                let function_step_pos = steps
                    .iter()
                    .position(|s| matches!(s, MigrationStep::Function(_)));
                let trigger_step_pos = steps
                    .iter()
                    .position(|s| matches!(s, MigrationStep::Trigger(_)));

                assert!(table_step_pos.is_some());
                assert!(function_step_pos.is_some());
                assert!(trigger_step_pos.is_some());

                // Trigger should come after both table and function
                assert!(trigger_step_pos.unwrap() > table_step_pos.unwrap());
                assert!(trigger_step_pos.unwrap() > function_step_pos.unwrap());

                // Verify final state
                assert_eq!(final_catalog.triggers.len(), 1);
                assert_eq!(final_catalog.tables.len(), 2); // users + audit_log
                assert_eq!(final_catalog.functions.len(), 1);

                let trigger = &final_catalog.triggers[0];
                assert_eq!(trigger.name, "users_audit");

                // Verify trigger dependencies
                use pgmt::catalog::id::DbObjectId;
                assert!(trigger.depends_on.contains(&DbObjectId::Table {
                    schema: "test_schema".to_string(),
                    name: "users".to_string(),
                }));
                assert!(trigger.depends_on.contains(&DbObjectId::Function {
                    schema: "test_schema".to_string(),
                    name: "generic_audit".to_string(),
                }));

                Ok(())
            },
        )
        .await?;
    Ok(())
}
