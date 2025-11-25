use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::diff::operations::{ExtensionOperation, MigrationStep, TableOperation};

#[tokio::test]
async fn test_extension_before_dependent_types() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        &[], // both DBs: empty
        &[], // initial only: empty
        &[
            "CREATE EXTENSION IF NOT EXISTS citext",
            "CREATE TABLE users (id SERIAL PRIMARY KEY, email citext)",
        ], // target only: extension with dependent table
        |steps, _catalog| {
            // Find the position of extension and table steps
            let extension_idx = steps.iter().position(|s|
                matches!(s, MigrationStep::Extension(ExtensionOperation::Create { .. }))
            );
            let table_idx = steps.iter().position(|s|
                matches!(s, MigrationStep::Table(TableOperation::Create { .. }))
            );

            assert!(extension_idx.is_some(), "Extension step should exist");
            assert!(table_idx.is_some(), "Table step should exist");

            // Extension MUST come before the table that uses its type
            assert!(
                extension_idx.unwrap() < table_idx.unwrap(),
                "Extension must appear before table that uses its types. Extension at {}, Table at {}",
                extension_idx.unwrap(),
                table_idx.unwrap()
            );

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_multiple_extensions_before_objects() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        &[], // both DBs: empty
        &[], // initial only: empty
        &[
            "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"",
            "CREATE EXTENSION IF NOT EXISTS citext",
            "CREATE TABLE users (id uuid DEFAULT uuid_generate_v4(), email citext)",
            "CREATE TABLE posts (id uuid DEFAULT uuid_generate_v4(), title TEXT)",
        ], // target only: multiple extensions with dependent tables
        |steps, _catalog| {
            // Find all extension and table steps
            let extension_indices: Vec<_> = steps.iter().enumerate()
                .filter_map(|(i, s)| match s {
                    MigrationStep::Extension(ExtensionOperation::Create { .. }) => Some(i),
                    _ => None
                })
                .collect();

            let table_indices: Vec<_> = steps.iter().enumerate()
                .filter_map(|(i, s)| match s {
                    MigrationStep::Table(TableOperation::Create { .. }) => Some(i),
                    _ => None
                })
                .collect();

            assert_eq!(extension_indices.len(), 2, "Should have 2 extension steps");
            assert_eq!(table_indices.len(), 2, "Should have 2 table steps");

            // All extensions must come before all tables
            let last_extension_idx = extension_indices.iter().max().unwrap();
            let first_table_idx = table_indices.iter().min().unwrap();

            assert!(
                last_extension_idx < first_table_idx,
                "All extensions must appear before any tables. Last extension at {}, First table at {}",
                last_extension_idx,
                first_table_idx
            );

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_extension_before_view_with_cast() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        &[], // both DBs: empty
        &[], // initial only: empty
        &[
            "CREATE EXTENSION IF NOT EXISTS citext",
            "CREATE TABLE users (id SERIAL, email TEXT)",
            "CREATE VIEW user_emails AS SELECT id, email::citext as normalized_email FROM users",
        ], // target only: extension with view that casts to its type
        |steps, _catalog| {
            // Find extension and view steps
            let extension_idx = steps.iter().position(|s|
                matches!(s, MigrationStep::Extension(ExtensionOperation::Create { .. }))
            );
            let view_idx = steps.iter().position(|s|
                matches!(s, MigrationStep::View(_))
            );

            assert!(extension_idx.is_some(), "Extension step should exist");
            assert!(view_idx.is_some(), "View step should exist");

            // Extension MUST come before the view that uses its type
            assert!(
                extension_idx.unwrap() < view_idx.unwrap(),
                "Extension must appear before view that casts to its types. Extension at {}, View at {}",
                extension_idx.unwrap(),
                view_idx.unwrap()
            );

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_extension_before_function_using_type() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        &[], // both DBs: empty
        &[], // initial only: empty
        &[
            "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"",
            r#"CREATE FUNCTION generate_id() RETURNS uuid
            LANGUAGE SQL
            AS $$ SELECT uuid_generate_v4() $$"#,
        ], // target only: extension with function using its functionality
        |steps, _catalog| {
            // Find extension and function steps
            let extension_idx = steps.iter().position(|s|
                matches!(s, MigrationStep::Extension(ExtensionOperation::Create { .. }))
            );
            let function_idx = steps.iter().position(|s|
                matches!(s, MigrationStep::Function(_))
            );

            assert!(extension_idx.is_some(), "Extension step should exist");
            assert!(function_idx.is_some(), "Function step should exist");

            // Extension MUST come before the function that uses it
            assert!(
                extension_idx.unwrap() < function_idx.unwrap(),
                "Extension must appear before function that uses it. Extension at {}, Function at {}",
                extension_idx.unwrap(),
                function_idx.unwrap()
            );

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_extension_ordering_with_schemas() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &[], // both DBs: empty
            &[], // initial only: empty
            &[
                "CREATE SCHEMA app",
                "CREATE EXTENSION IF NOT EXISTS citext SCHEMA app",
                "CREATE TABLE app.users (id SERIAL, email app.citext)",
            ], // target only: extension in custom schema
            |steps, _catalog| {
                // Find the position of schema, extension and table steps
                let schema_idx = steps
                    .iter()
                    .position(|s| matches!(s, MigrationStep::Schema(_)));
                let extension_idx = steps.iter().position(|s| {
                    matches!(
                        s,
                        MigrationStep::Extension(ExtensionOperation::Create { .. })
                    )
                });
                let table_idx = steps
                    .iter()
                    .position(|s| matches!(s, MigrationStep::Table(TableOperation::Create { .. })));

                assert!(schema_idx.is_some(), "Schema step should exist");
                assert!(extension_idx.is_some(), "Extension step should exist");
                assert!(table_idx.is_some(), "Table step should exist");

                // Schema should come first (since extension depends on it)
                // Then extension should come before table
                assert!(
                    schema_idx.unwrap() < extension_idx.unwrap(),
                    "Schema must appear before extension installed in it"
                );
                assert!(
                    extension_idx.unwrap() < table_idx.unwrap(),
                    "Extension must appear before table that uses its types"
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_extension_drop_ordering() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        &[], // both DBs: empty
        &[
            "CREATE EXTENSION IF NOT EXISTS citext",
            "CREATE TABLE users (id SERIAL PRIMARY KEY, email citext)",
        ], // initial only: has extension and dependent table
        &[], // target only: empty (dropping everything)
        |steps, _catalog| {
            // When dropping, tables should be dropped before extensions
            let extension_drop_idx = steps.iter().position(|s|
                matches!(s, MigrationStep::Extension(ExtensionOperation::Drop { .. }))
            );
            let table_drop_idx = steps.iter().position(|s|
                matches!(s, MigrationStep::Table(TableOperation::Drop { .. }))
            );

            assert!(extension_drop_idx.is_some(), "Extension drop step should exist");
            assert!(table_drop_idx.is_some(), "Table drop step should exist");

            // Table MUST be dropped before the extension it depends on
            assert!(
                table_drop_idx.unwrap() < extension_drop_idx.unwrap(),
                "Table must be dropped before extension it depends on. Table drop at {}, Extension drop at {}",
                table_drop_idx.unwrap(),
                extension_drop_idx.unwrap()
            );

            Ok(())
        }
    ).await?;

    Ok(())
}
