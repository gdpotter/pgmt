use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::diff::operations::{AggregateOperation, MigrationStep};

/// Helper SQL to create a state transition function for testing
fn create_sfunc_sql(schema: &str, name: &str) -> String {
    format!(
        "CREATE FUNCTION {schema}.{name}(state text, val text) RETURNS text AS $$
         BEGIN
             IF state IS NULL OR state = '' THEN
                 RETURN val;
             ELSE
                 RETURN state || ', ' || val;
             END IF;
         END;
         $$ LANGUAGE plpgsql"
    )
}

#[tokio::test]
async fn test_create_aggregate_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema and sfunc
            &[
                "CREATE SCHEMA test_schema",
                &create_sfunc_sql("test_schema", "concat_sfunc"),
            ],
            // Initial DB: nothing extra
            &[],
            // Target DB: create aggregate
            &[
                "CREATE AGGREGATE test_schema.group_concat(text) (
                    SFUNC = test_schema.concat_sfunc,
                    STYPE = text,
                    INITCOND = ''
                )",
            ],
            |steps, final_catalog| -> Result<()> {
                // Should have a Create step for the aggregate
                let create_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Aggregate(AggregateOperation::Create { aggregate })
                            if aggregate.name == "group_concat")
                    })
                    .expect("Should have Create Aggregate step");

                match create_step {
                    MigrationStep::Aggregate(AggregateOperation::Create { aggregate }) => {
                        assert_eq!(aggregate.schema, "test_schema");
                        assert_eq!(aggregate.name, "group_concat");
                        assert_eq!(aggregate.arguments, "text");
                        assert!(aggregate.definition.contains("SFUNC"));
                        assert!(aggregate.definition.contains("STYPE"));
                    }
                    _ => panic!("Expected Create Aggregate step"),
                }

                // Verify final state
                assert_eq!(final_catalog.aggregates.len(), 1);
                let created_agg = &final_catalog.aggregates[0];
                assert_eq!(created_agg.schema, "test_schema");
                assert_eq!(created_agg.name, "group_concat");

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_drop_aggregate_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema and sfunc
            &[
                "CREATE SCHEMA test_schema",
                &create_sfunc_sql("test_schema", "concat_sfunc"),
            ],
            // Initial DB: has aggregate
            &[
                "CREATE AGGREGATE test_schema.group_concat(text) (
                    SFUNC = test_schema.concat_sfunc,
                    STYPE = text
                )",
            ],
            // Target DB: aggregate removed
            &[],
            |steps, final_catalog| -> Result<()> {
                // Should have a Drop step
                let drop_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Aggregate(AggregateOperation::Drop { identifier })
                            if identifier.name == "group_concat")
                    })
                    .expect("Should have Drop Aggregate step");

                match drop_step {
                    MigrationStep::Aggregate(AggregateOperation::Drop { identifier }) => {
                        assert_eq!(identifier.schema, "test_schema");
                        assert_eq!(identifier.name, "group_concat");
                        assert_eq!(identifier.arguments, "text");
                    }
                    _ => panic!("Expected Drop Aggregate step"),
                }

                // Aggregates can be recreated from schema, so DROP is not destructive
                assert!(!drop_step.has_destructive_sql());

                // Verify final state - no aggregates
                assert_eq!(final_catalog.aggregates.len(), 0);

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_replace_aggregate_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema and sfunc
            &[
                "CREATE SCHEMA test_schema",
                &create_sfunc_sql("test_schema", "concat_sfunc"),
            ],
            // Initial DB: aggregate with empty INITCOND
            &["CREATE AGGREGATE test_schema.group_concat(text) (
                    SFUNC = test_schema.concat_sfunc,
                    STYPE = text,
                    INITCOND = ''
                )"],
            // Target DB: aggregate with different INITCOND (same name, same args)
            // Note: We create a new aggregate with INITCOND = 'N/A' - the test helper
            // handles this by comparing initial and target catalogs
            &["CREATE AGGREGATE test_schema.group_concat(text) (
                    SFUNC = test_schema.concat_sfunc,
                    STYPE = text,
                    INITCOND = 'N/A'
                )"],
            |steps, final_catalog| -> Result<()> {
                // Should have a Replace step (since definition changed)
                let replace_step = steps
                    .iter()
                    .find(|s| {
                        matches!(
                            s,
                            MigrationStep::Aggregate(AggregateOperation::Replace { .. })
                        )
                    })
                    .expect("Should have Replace Aggregate step");

                match replace_step {
                    MigrationStep::Aggregate(AggregateOperation::Replace {
                        old_aggregate,
                        new_aggregate,
                    }) => {
                        assert_eq!(old_aggregate.initial_value, Some("".to_string()));
                        assert_eq!(new_aggregate.initial_value, Some("N/A".to_string()));
                    }
                    _ => panic!("Expected Replace Aggregate step"),
                }

                // Verify final state
                assert_eq!(final_catalog.aggregates.len(), 1);
                assert_eq!(
                    final_catalog.aggregates[0].initial_value,
                    Some("N/A".to_string())
                );

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_aggregate_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema, sfunc, and aggregate
            &[
                "CREATE SCHEMA test_schema",
                &create_sfunc_sql("test_schema", "concat_sfunc"),
                "CREATE AGGREGATE test_schema.group_concat(text) (
                    SFUNC = test_schema.concat_sfunc,
                    STYPE = text
                )",
            ],
            // Initial DB: nothing extra
            &[],
            // Target DB: add comment
            &["COMMENT ON AGGREGATE test_schema.group_concat(text) IS 'Concatenates text values'"],
            |steps, final_catalog| -> Result<()> {
                // Should have a Comment step
                let comment_step = steps
                    .iter()
                    .find(|s| matches!(s, MigrationStep::Aggregate(AggregateOperation::Comment(_))))
                    .expect("Should have Comment Aggregate step");

                match comment_step {
                    MigrationStep::Aggregate(AggregateOperation::Comment(_)) => {
                        // Comment operation found
                    }
                    _ => panic!("Expected Comment Aggregate step"),
                }

                // Verify final state
                assert_eq!(final_catalog.aggregates.len(), 1);
                assert_eq!(
                    final_catalog.aggregates[0].comment,
                    Some("Concatenates text values".to_string())
                );

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_aggregate_with_finalfunc_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema only
            &["CREATE SCHEMA test_schema"],
            // Initial DB: nothing
            &[],
            // Target DB: aggregate with sfunc and finalfunc
            &[
                "CREATE FUNCTION test_schema.sum_sfunc(state integer, val integer) RETURNS integer AS $$
                 BEGIN RETURN COALESCE(state, 0) + COALESCE(val, 0); END;
                 $$ LANGUAGE plpgsql",
                "CREATE FUNCTION test_schema.avg_final(state integer) RETURNS numeric AS $$
                 BEGIN RETURN state::numeric / 2; END;
                 $$ LANGUAGE plpgsql",
                "CREATE AGGREGATE test_schema.custom_avg(integer) (
                    SFUNC = test_schema.sum_sfunc,
                    STYPE = integer,
                    FINALFUNC = test_schema.avg_final,
                    INITCOND = '0'
                )",
            ],
            |steps, final_catalog| -> Result<()> {
                // Should have Create steps for functions and aggregate
                let agg_create = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Aggregate(AggregateOperation::Create { aggregate })
                            if aggregate.name == "custom_avg")
                    })
                    .expect("Should have Create Aggregate step");

                match agg_create {
                    MigrationStep::Aggregate(AggregateOperation::Create { aggregate }) => {
                        assert_eq!(aggregate.final_func, Some("avg_final".to_string()));
                        assert!(aggregate.definition.contains("FINALFUNC"));
                    }
                    _ => panic!("Expected Create Aggregate step"),
                }

                // Verify final state
                assert_eq!(final_catalog.aggregates.len(), 1);
                let agg = &final_catalog.aggregates[0];
                assert_eq!(agg.final_func, Some("avg_final".to_string()));

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_aggregate_dependency_ordering() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema only
            &["CREATE SCHEMA test_schema"],
            // Initial DB: nothing
            &[],
            // Target DB: sfunc and aggregate (aggregate depends on sfunc)
            &[
                &create_sfunc_sql("test_schema", "my_sfunc"),
                "CREATE AGGREGATE test_schema.my_agg(text) (
                    SFUNC = test_schema.my_sfunc,
                    STYPE = text
                )",
            ],
            |steps, _final_catalog| -> Result<()> {
                // Find indices of function create and aggregate create
                let func_idx = steps
                    .iter()
                    .position(|s| {
                        matches!(s, MigrationStep::Function(op)
                            if matches!(op, pgmt::diff::operations::FunctionOperation::Create { name, .. }
                                if name == "my_sfunc"))
                    })
                    .expect("Should have Create Function step");

                let agg_idx = steps
                    .iter()
                    .position(|s| {
                        matches!(s, MigrationStep::Aggregate(AggregateOperation::Create { aggregate })
                            if aggregate.name == "my_agg")
                    })
                    .expect("Should have Create Aggregate step");

                // Function should come before aggregate
                assert!(
                    func_idx < agg_idx,
                    "Function (index {}) should be created before aggregate (index {})",
                    func_idx,
                    agg_idx
                );

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_aggregate_with_array_state_type_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE SCHEMA test_schema"],
            &[],
            &[
                "CREATE TYPE test_schema.item AS (id integer, name text)",
                "CREATE FUNCTION test_schema.item_collect_sfunc(state test_schema.item[], val test_schema.item) RETURNS test_schema.item[] AS $$
                 BEGIN RETURN COALESCE(state, ARRAY[]::test_schema.item[]) || val; END;
                 $$ LANGUAGE plpgsql",
                "CREATE AGGREGATE test_schema.collect_items(test_schema.item) (
                    SFUNC = test_schema.item_collect_sfunc,
                    STYPE = test_schema.item[]
                )",
            ],
            |steps, final_catalog| -> Result<()> {
                let agg_create = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Aggregate(AggregateOperation::Create { aggregate })
                            if aggregate.name == "collect_items")
                    })
                    .expect("Should have Create Aggregate step");

                match agg_create {
                    MigrationStep::Aggregate(AggregateOperation::Create { aggregate }) => {
                        assert!(aggregate.definition.contains("STYPE = test_schema.item[]"));
                    }
                    _ => panic!("Expected Create Aggregate step"),
                }

                assert_eq!(final_catalog.aggregates.len(), 1);
                let agg = &final_catalog.aggregates[0];
                assert_eq!(agg.state_type, "item");
                assert!(agg.state_type_formatted.contains("item[]"));

                Ok(())
            },
        )
        .await?;
    Ok(())
}
