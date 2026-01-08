use crate::helpers::harness::with_test_db;
use anyhow::Result;
use pgmt::catalog::aggregate::fetch;
use pgmt::catalog::id::{DbObjectId, DependsOn};

#[tokio::test]
async fn test_fetch_basic_aggregate() {
    with_test_db(async |db| {
        // Create state transition function first
        db.execute(
            "CREATE FUNCTION agg_sfunc(state text, val text) RETURNS text AS $$
             BEGIN
                 IF state IS NULL OR state = '' THEN
                     RETURN val;
                 ELSE
                     RETURN state || ', ' || val;
                 END IF;
             END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        // Create the aggregate
        db.execute(
            "CREATE AGGREGATE group_concat(text) (
                SFUNC = agg_sfunc,
                STYPE = text,
                INITCOND = ''
            )",
        )
        .await;

        let aggregates = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(aggregates.len(), 1);
        let agg = &aggregates[0];

        assert_eq!(agg.schema, "public");
        assert_eq!(agg.name, "group_concat");
        assert_eq!(agg.arguments, "text");
        assert_eq!(agg.state_type, "text");
        assert_eq!(agg.state_func, "agg_sfunc");
        assert_eq!(agg.initial_value, Some("".to_string()));
        assert!(agg.final_func.is_none());
        assert!(agg.combine_func.is_none());
        assert!(agg.comment.is_none());
    })
    .await;
}

#[tokio::test]
async fn test_fetch_aggregate_with_finalfunc() {
    with_test_db(async |db| {
        // Create state transition function
        db.execute(
            "CREATE FUNCTION sum_sfunc(state integer, val integer) RETURNS integer AS $$
             BEGIN RETURN COALESCE(state, 0) + COALESCE(val, 0); END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        // Create final function
        db.execute(
            "CREATE FUNCTION avg_final(state integer) RETURNS numeric AS $$
             BEGIN RETURN state::numeric / 2; END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        // Create aggregate with final function
        db.execute(
            "CREATE AGGREGATE custom_avg(integer) (
                SFUNC = sum_sfunc,
                STYPE = integer,
                FINALFUNC = avg_final,
                INITCOND = '0'
            )",
        )
        .await;

        let aggregates = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(aggregates.len(), 1);
        let agg = &aggregates[0];

        assert_eq!(agg.name, "custom_avg");
        assert_eq!(agg.state_func, "sum_sfunc");
        assert_eq!(agg.final_func, Some("avg_final".to_string()));
        assert_eq!(agg.final_func_schema, Some("public".to_string()));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_aggregate_with_comment() {
    with_test_db(async |db| {
        // Create state transition function
        db.execute(
            "CREATE FUNCTION text_concat_sfunc(state text, val text) RETURNS text AS $$
             BEGIN RETURN COALESCE(state, '') || COALESCE(val, ''); END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        // Create aggregate
        db.execute(
            "CREATE AGGREGATE string_agg_custom(text) (
                SFUNC = text_concat_sfunc,
                STYPE = text
            )",
        )
        .await;

        // Add comment
        db.execute("COMMENT ON AGGREGATE string_agg_custom(text) IS 'Custom string aggregation'")
            .await;

        let aggregates = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(aggregates.len(), 1);
        let agg = &aggregates[0];

        assert_eq!(agg.comment, Some("Custom string aggregation".to_string()));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_aggregate_dependencies() -> Result<()> {
    with_test_db(async |db| {
        // Create state transition function in public schema
        db.execute(
            "CREATE FUNCTION my_sfunc(state integer, val integer) RETURNS integer AS $$
             BEGIN RETURN COALESCE(state, 0) + COALESCE(val, 0); END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        // Create aggregate
        db.execute(
            "CREATE AGGREGATE my_sum(integer) (
                SFUNC = my_sfunc,
                STYPE = integer,
                INITCOND = '0'
            )",
        )
        .await;

        let aggregates = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(aggregates.len(), 1);
        let agg = &aggregates[0];

        // Should depend on schema
        let deps = agg.depends_on();
        assert!(deps.contains(&DbObjectId::Schema {
            name: "public".to_string()
        }));

        // Should depend on state transition function
        // The sfunc takes (state, value) so arguments are from the aggregate's query result
        let sfunc_dep = deps
            .iter()
            .find(|d| matches!(d, DbObjectId::Function { name, .. } if name == "my_sfunc"));
        assert!(
            sfunc_dep.is_some(),
            "Should depend on state transition function. Actual deps: {:?}",
            deps
        );

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_fetch_aggregate_in_custom_schema() {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA app").await;

        // Create state function in app schema
        db.execute(
            "CREATE FUNCTION app.count_sfunc(state bigint, val anyelement) RETURNS bigint AS $$
             BEGIN RETURN COALESCE(state, 0) + 1; END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        // Create aggregate in app schema
        db.execute(
            "CREATE AGGREGATE app.custom_count(anyelement) (
                SFUNC = app.count_sfunc,
                STYPE = bigint,
                INITCOND = '0'
            )",
        )
        .await;

        let aggregates = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(aggregates.len(), 1);
        let agg = &aggregates[0];

        assert_eq!(agg.schema, "app");
        assert_eq!(agg.name, "custom_count");
        assert_eq!(agg.state_func_schema, "app");
    })
    .await;
}

#[tokio::test]
async fn test_fetch_aggregate_with_custom_state_type() -> Result<()> {
    with_test_db(async |db| {
        // Create custom composite type for state
        db.execute("CREATE TYPE running_stats AS (sum numeric, count bigint)")
            .await;

        // Create state function using custom type
        db.execute(
            "CREATE FUNCTION stats_sfunc(state running_stats, val numeric) RETURNS running_stats AS $$
             DECLARE
                 result running_stats;
             BEGIN
                 IF state IS NULL THEN
                     result.sum := val;
                     result.count := 1;
                 ELSE
                     result.sum := state.sum + val;
                     result.count := state.count + 1;
                 END IF;
                 RETURN result;
             END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        // Create final function
        db.execute(
            "CREATE FUNCTION stats_final(state running_stats) RETURNS numeric AS $$
             BEGIN RETURN state.sum / state.count; END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        // Create aggregate with custom state type
        db.execute(
            "CREATE AGGREGATE running_avg(numeric) (
                SFUNC = stats_sfunc,
                STYPE = running_stats,
                FINALFUNC = stats_final
            )",
        )
        .await;

        let aggregates = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(aggregates.len(), 1);
        let agg = &aggregates[0];

        assert_eq!(agg.state_type, "running_stats");
        assert_eq!(agg.state_type_schema, "public");

        // Should depend on custom state type
        let deps = agg.depends_on();
        assert!(deps.contains(&DbObjectId::Type {
            schema: "public".to_string(),
            name: "running_stats".to_string(),
        }));

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_fetch_multiple_aggregates() {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA analytics").await;

        // Create first aggregate's function
        db.execute(
            "CREATE FUNCTION sum1_sfunc(state integer, val integer) RETURNS integer AS $$
             BEGIN RETURN COALESCE(state, 0) + COALESCE(val, 0); END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        // Create second aggregate's function
        db.execute(
            "CREATE FUNCTION analytics.sum2_sfunc(state bigint, val bigint) RETURNS bigint AS $$
             BEGIN RETURN COALESCE(state, 0) + COALESCE(val, 0); END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        // Create aggregates
        db.execute(
            "CREATE AGGREGATE public.agg_sum(integer) (SFUNC = sum1_sfunc, STYPE = integer)",
        )
        .await;
        db.execute(
            "CREATE AGGREGATE analytics.big_sum(bigint) (SFUNC = analytics.sum2_sfunc, STYPE = bigint)",
        )
        .await;

        let aggregates = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(aggregates.len(), 2);

        // Should be ordered by schema, then name
        assert_eq!(aggregates[0].schema, "analytics");
        assert_eq!(aggregates[0].name, "big_sum");

        assert_eq!(aggregates[1].schema, "public");
        assert_eq!(aggregates[1].name, "agg_sum");
    })
    .await;
}

#[tokio::test]
async fn test_aggregate_id_method() {
    use pgmt::catalog::aggregate::Aggregate;

    let agg = Aggregate {
        schema: "app".to_string(),
        name: "my_agg".to_string(),
        arguments: "integer".to_string(),
        state_type: "integer".to_string(),
        state_type_schema: "pg_catalog".to_string(),
        state_type_formatted: "integer".to_string(),
        state_func: "int4pl".to_string(),
        state_func_schema: "pg_catalog".to_string(),
        final_func: None,
        final_func_schema: None,
        combine_func: None,
        combine_func_schema: None,
        initial_value: Some("0".to_string()),
        definition: "CREATE AGGREGATE app.my_agg(integer) (...)".to_string(),
        comment: None,
        depends_on: vec![],
    };

    assert_eq!(
        agg.id(),
        DbObjectId::Aggregate {
            schema: "app".to_string(),
            name: "my_agg".to_string(),
            arguments: "integer".to_string(),
        }
    );
}

#[tokio::test]
async fn test_aggregate_with_array_argument_type() {
    with_test_db(async |db| {
        // Create custom type
        db.execute("CREATE TYPE item_status AS ENUM ('pending', 'active', 'completed')")
            .await;

        // Create state function that takes array of custom type
        db.execute(
            "CREATE FUNCTION count_statuses_sfunc(state integer, statuses item_status[]) RETURNS integer AS $$
             BEGIN RETURN COALESCE(state, 0) + COALESCE(array_length(statuses, 1), 0); END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        // Create aggregate with array argument
        db.execute(
            "CREATE AGGREGATE count_all_statuses(item_status[]) (
                SFUNC = count_statuses_sfunc,
                STYPE = integer,
                INITCOND = '0'
            )",
        )
        .await;

        let aggregates = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(aggregates.len(), 1);
        let agg = &aggregates[0];

        assert_eq!(agg.name, "count_all_statuses");
        // Arguments should include array notation
        assert!(
            agg.arguments.contains("[]"),
            "arguments should contain array notation, got: {}",
            agg.arguments
        );
    })
    .await;
}

#[tokio::test]
async fn test_aggregate_with_custom_array_state_type() -> Result<()> {
    with_test_db(async |db| {
        // Create custom type
        db.execute("CREATE TYPE item AS (id integer, name text)")
            .await;

        // Create state function that uses array of custom type
        db.execute(
            "CREATE FUNCTION item_collect_sfunc(state item[], val item) RETURNS item[] AS $$
             BEGIN RETURN COALESCE(state, ARRAY[]::item[]) || val; END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        // Create aggregate with array state type
        db.execute(
            "CREATE AGGREGATE collect_items(item) (
                SFUNC = item_collect_sfunc,
                STYPE = item[]
            )",
        )
        .await;

        let aggregates = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(aggregates.len(), 1);
        let agg = &aggregates[0];

        assert_eq!(agg.name, "collect_items");

        // state_type is element type for dependency tracking
        assert_eq!(agg.state_type, "item");
        // state_type_formatted preserves array brackets
        assert!(agg.state_type_formatted.contains("item[]"));

        // Should depend on base type "item", not internal "_item"
        let deps = agg.depends_on();
        assert!(deps.contains(&DbObjectId::Type {
            schema: "public".to_string(),
            name: "item".to_string()
        }));
        assert!(!deps.contains(&DbObjectId::Type {
            schema: "public".to_string(),
            name: "_item".to_string()
        }));

        Ok(())
    })
    .await
}
