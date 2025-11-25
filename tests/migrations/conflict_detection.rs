use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::diff::operations::MigrationStep;

#[tokio::test]
async fn test_migration_validation_no_conflicts() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    // Create a simple schema with a table
    helper
        .run_migration_test(
            &["CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)"],
            &[],
            &[],
            |_steps, _final_catalog| {
                // No differences, so validation should pass
                Ok(())
            },
        )
        .await?;

    // Test that validation passes when schema matches baseline + migrations
    // Note: The actual validation call would be tested in a higher-level integration test
    // since it requires the full CLI setup

    Ok(())
}

#[tokio::test]
async fn test_migration_validation_detects_conflicts() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    // Simulate a conflict scenario:
    // - Initial state has users table (from baseline/migration)
    // - Current schema files add a products table (new change)
    // This should be detected as a difference requiring a new migration

    helper
        .run_migration_test(
            &["CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)"], // Both DBs
            &[],                                                                 // Initial only
            &["CREATE TABLE products (id SERIAL PRIMARY KEY, title TEXT NOT NULL)"], // Target only
            |steps, _final_catalog| {
                // Should detect that products table needs to be added
                assert!(
                    !steps.is_empty(),
                    "Should detect differences requiring migration steps"
                );

                // Verify that the difference is about the products table
                let has_product_table_creation = steps
                    .iter()
                    .any(|step| matches!(step, MigrationStep::Table(_)));
                assert!(
                    has_product_table_creation,
                    "Should detect products table creation needed"
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_migration_validation_with_baseline_reconstruction() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    // Test that validation correctly reconstructs state from baseline + migrations
    // and compares against current schema files

    helper
        .run_migration_test(
            &[
                "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)",
                "CREATE TABLE orders (id SERIAL PRIMARY KEY, user_id INTEGER REFERENCES users(id))",
            ],
            &[],
            &[
                // Current schema has an additional index that wasn't in baseline
                "CREATE INDEX idx_orders_user_id ON orders (user_id)",
            ],
            |steps, _final_catalog| {
                // Should detect the missing index
                assert!(!steps.is_empty(), "Should detect index difference");

                let has_index_creation = steps
                    .iter()
                    .any(|step| matches!(step, MigrationStep::Index(_)));
                assert!(has_index_creation, "Should detect index creation needed");

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_migration_validation_with_complex_changes() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    // Test validation with multiple types of changes (tables, views, functions)
    helper.run_migration_test(
        &[
            "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)",
            "CREATE VIEW active_users AS SELECT * FROM users WHERE email IS NOT NULL",
        ],
        &[],
        &[
            // Current schema has additional function
            "CREATE FUNCTION get_user_count() RETURNS INTEGER LANGUAGE SQL AS 'SELECT COUNT(*) FROM users'",
        ],
        |steps, _final_catalog| {
            // Should detect the missing function
            assert!(!steps.is_empty(), "Should detect function difference");

            let has_function_creation = steps.iter().any(|step| {
                matches!(step, MigrationStep::Function(_))
            });
            assert!(has_function_creation, "Should detect function creation needed");

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_migration_validation_object_filtering() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    // Test that object filtering is applied correctly during validation
    // This ensures the migration tracking table doesn't cause false positives

    helper.run_migration_test(
        &[
            "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)",
            // Include the migration tracking table that should be filtered out
            "CREATE TABLE pgmt_migrations (version BIGINT PRIMARY KEY, description TEXT NOT NULL, applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, checksum TEXT NOT NULL)",
        ],
        &[],
        &[],
        |steps, _final_catalog| {
            // Should not detect differences for the tracking table
            // since it should be filtered out
            let has_tracking_table_operations = steps.iter().any(|step| {
                format!("{:?}", step).contains("pgmt_migrations")
            });

            assert!(!has_tracking_table_operations,
                "Should not detect tracking table operations due to filtering");

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_migration_validation_handles_empty_state() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    // Test validation when there are no baselines or migrations
    // This tests the edge case of a fresh project

    helper
        .run_migration_test(
            &[], // No initial state
            &[], // No initial-only content
            &[
                // Only current schema has content
                "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)",
            ],
            |steps, _final_catalog| {
                // Should detect that users table needs to be added
                assert!(
                    !steps.is_empty(),
                    "Should detect users table creation needed"
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}
