//! Tests for file dependency augmentation in migration scenarios

use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;

#[tokio::test]
async fn test_file_dependency_augmentation_in_migration_flow() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    // Create schema with file dependencies that PostgreSQL might miss
    let schema_setup = vec![
        "CREATE SCHEMA app",
        "CREATE TYPE app.priority AS ENUM ('low', 'medium', 'high')",
        "CREATE TABLE app.users (id SERIAL PRIMARY KEY, name VARCHAR(100))",
    ];

    let initial_only = vec![
        // Initial state has only basic tables
    ];

    let target_only = vec![
        // Add a table that uses the enum - creating an implicit dependency
        "CREATE TABLE app.tasks (id SERIAL, priority app.priority, assigned_to INTEGER)",
        "CREATE VIEW app.user_tasks AS SELECT u.name, t.priority FROM app.users u JOIN app.tasks t ON u.id = t.assigned_to",
    ];

    let _steps = helper
        .run_migration_test(
            &schema_setup,
            &initial_only,
            &target_only,
            |steps, final_catalog| {
                assert!(!steps.is_empty());

                // Check that we have creation steps for the new objects
                let has_table_create = steps
                    .iter()
                    .any(|step| matches!(step, pgmt::diff::operations::MigrationStep::Table(_)));
                let has_view_create = steps
                    .iter()
                    .any(|step| matches!(step, pgmt::diff::operations::MigrationStep::View(_)));

                assert!(has_table_create, "Should have table creation step");
                assert!(has_view_create, "Should have view creation step");

                // Verify final catalog has the expected objects
                let has_tasks_table = final_catalog.tables.iter().any(|t| t.name == "tasks");
                let has_user_tasks_view =
                    final_catalog.views.iter().any(|v| v.name == "user_tasks");

                assert!(has_tasks_table, "Final catalog should contain tasks table");
                assert!(
                    has_user_tasks_view,
                    "Final catalog should contain user_tasks view"
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

// Integration test with schema_ops temporarily disabled due to TestDatabase URL access complexity
// The core functionality is tested in tests/catalog/file_dependencies.rs
