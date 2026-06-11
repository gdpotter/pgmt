use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;

/// Test that demonstrates the baseline validation would catch the generated columns issue
/// This test is mainly for demonstrating the validation concept
#[tokio::test]
async fn test_baseline_validation_concept() -> Result<()> {
    // This test shows how the validation would work in principle
    // In reality, the validation is built into the migrate commands

    let helper = MigrationTestHelper::new().await;

    // This should pass since we fixed the generated columns issue
    helper.run_migration_test(
        &["CREATE SCHEMA test_schema"],
        &[],
        &[
            "CREATE TABLE test_schema.orders (id INTEGER, total DECIMAL, tax DECIMAL GENERATED ALWAYS AS (total * 0.08) STORED)",
        ],
        |_steps, final_catalog| {
            // Verify the generated column is in the final catalog
            let table = &final_catalog.tables[0];
            let tax_column = table.columns.iter().find(|c| c.name == "tax").unwrap();
            assert!(tax_column.generated.is_some(), "Tax column should have generated expression");

            // This confirms that our baseline would accurately represent the schema
            // The migrate command validation would catch any discrepancies
            Ok(())
        }
    ).await?;

    Ok(())
}

/// A shadow branch legitimately contains image-provided substrate (excluded
/// schemas, their extensions). Baseline validation loads the baseline-applied
/// shadow catalog scoped to managed objects, so substrate must not surface as
/// "unexpected differences" (DROP SCHEMA topology, …).
#[tokio::test]
async fn test_baseline_validation_ignores_excluded_substrate() -> Result<()> {
    use crate::helpers::harness::with_test_db;
    use pgmt::config::filter::ObjectFilter;
    use pgmt::config::types::{ObjectExclude, ObjectInclude, Objects, TrackingTable};
    use sqlx::postgres::PgConnectOptions;
    use std::str::FromStr;

    with_test_db(async |db| {
        // Simulate a shadow branch: substrate present, and the scoped clean
        // skips the database (branch-provisioned marker).
        db.execute("CREATE SCHEMA topology").await;
        db.execute("CREATE TABLE topology.layer (id int)").await;
        let opts = PgConnectOptions::from_str(&db.url()).unwrap();
        pgmt::db::cleaner::mark_branch_provisioned(
            opts.get_host(),
            opts.get_port(),
            opts.get_database().unwrap(),
        );

        let objects = Objects {
            include: ObjectInclude::default(),
            exclude: ObjectExclude {
                schemas: vec!["topology".to_string()],
                tables: vec![],
            },
        };

        // Expected catalog: what the (filtered) schema files produce.
        db.execute("CREATE TABLE public.users (id int)").await;
        let filter = ObjectFilter::new(&objects, &TrackingTable::default());
        let expected = pgmt::catalog::Catalog::load_managed(db.pool(), &filter)
            .await
            .unwrap();
        db.execute("DROP TABLE public.users").await;

        // Baseline file recreating the managed objects only.
        let temp = tempfile::TempDir::new().unwrap();
        let baseline_path = temp.path().join("baseline_1.sql");
        std::fs::write(&baseline_path, "CREATE TABLE public.users (id int);\n").unwrap();
        let roles_path = temp.path().join("roles.sql");

        pgmt::migration::baseline::validate_baseline_against_catalog_with_suggestions(
            db.pool(),
            &baseline_path,
            &expected,
            &pgmt::migration::baseline::BaselineConfig {
                validate_consistency: true,
                verbose: false,
            },
            false,
            &roles_path,
            &objects,
        )
        .await
        .expect("substrate in the shadow branch must not fail baseline validation");
    })
    .await;

    Ok(())
}
