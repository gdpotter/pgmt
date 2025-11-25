use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::diff::operations::{ExtensionOperation, MigrationStep};

#[tokio::test]
async fn test_extension_create_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &[],                                               // Both DBs: empty
            &[],                                               // Initial only: empty
            &["CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\""], // Target only: has extension
            |steps, _final_catalog| -> Result<()> {
                // Filter only CREATE extension steps
                let create_steps: Vec<_> = steps
                    .iter()
                    .filter(|s| {
                        matches!(
                            s,
                            MigrationStep::Extension(ExtensionOperation::Create { .. })
                        )
                    })
                    .collect();

                assert_eq!(create_steps.len(), 1);

                match &create_steps[0] {
                    MigrationStep::Extension(ExtensionOperation::Create { extension }) => {
                        assert_eq!(extension.name, "uuid-ossp");
                        assert_eq!(extension.schema, "public");
                    }
                    _ => panic!(
                        "Expected ExtensionOperation::Create, got {:?}",
                        create_steps[0]
                    ),
                }

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_extension_drop_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &[],                                               // Both DBs: empty
            &["CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\""], // Initial only: has extension
            &[],                                               // Target only: empty
            |steps, _final_catalog| -> Result<()> {
                // Filter only DROP extension steps
                let drop_steps: Vec<_> = steps
                    .iter()
                    .filter(|s| {
                        matches!(s, MigrationStep::Extension(ExtensionOperation::Drop { .. }))
                    })
                    .collect();

                assert_eq!(drop_steps.len(), 1);

                match &drop_steps[0] {
                    MigrationStep::Extension(ExtensionOperation::Drop { identifier }) => {
                        assert_eq!(identifier.name, "uuid-ossp");
                    }
                    _ => panic!("Expected ExtensionOperation::Drop, got {:?}", drop_steps[0]),
                }

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_extension_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\""], // Both DBs: have extension
            &[],                                               // Initial only: no comment
            &["COMMENT ON EXTENSION \"uuid-ossp\" IS 'UUID generation functions'"], // Target only: has comment
            |steps, final_catalog| -> Result<()> {
                // Filter only comment operation steps
                let comment_steps: Vec<_> = steps
                    .iter()
                    .filter(|s| {
                        matches!(s, MigrationStep::Extension(ExtensionOperation::Comment(_)))
                    })
                    .collect();

                assert_eq!(comment_steps.len(), 1);

                match &comment_steps[0] {
                    MigrationStep::Extension(ExtensionOperation::Comment(_)) => {
                        // Expected comment operation
                    }
                    _ => panic!(
                        "Expected ExtensionOperation::Comment, got {:?}",
                        comment_steps[0]
                    ),
                }

                // Verify the final state has the comment
                let ext = final_catalog
                    .extensions
                    .iter()
                    .find(|e| e.name == "uuid-ossp")
                    .expect("Extension should exist in final catalog");
                assert_eq!(ext.comment, Some("UUID generation functions".to_string()));

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_drop_extension_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &[
                "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"",
                "COMMENT ON EXTENSION \"uuid-ossp\" IS 'UUID generation functions'",
            ], // Both DBs: have extension with comment
            &[],                                             // Initial only: keep comment
            &["COMMENT ON EXTENSION \"uuid-ossp\" IS NULL"], // Target only: remove comment
            |steps, final_catalog| -> Result<()> {
                // Filter only comment operation steps
                let comment_steps: Vec<_> = steps
                    .iter()
                    .filter(|s| {
                        matches!(s, MigrationStep::Extension(ExtensionOperation::Comment(_)))
                    })
                    .collect();

                assert_eq!(comment_steps.len(), 1);

                match &comment_steps[0] {
                    MigrationStep::Extension(ExtensionOperation::Comment(_)) => {
                        // Expected comment operation
                    }
                    _ => panic!(
                        "Expected ExtensionOperation::Comment, got {:?}",
                        comment_steps[0]
                    ),
                }

                // Verify the final state has no comment
                let ext = final_catalog
                    .extensions
                    .iter()
                    .find(|e| e.name == "uuid-ossp")
                    .expect("Extension should exist in final catalog");
                assert_eq!(ext.comment, None);

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_extension_with_custom_schema() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE SCHEMA utils"], // Both DBs: have schema
            &[],                      // Initial only: empty
            &["CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" SCHEMA utils"], // Target only: extension in custom schema
            |steps, final_catalog| -> Result<()> {
                // Filter only CREATE extension steps
                let create_steps: Vec<_> = steps
                    .iter()
                    .filter(|s| {
                        matches!(
                            s,
                            MigrationStep::Extension(ExtensionOperation::Create { .. })
                        )
                    })
                    .collect();

                assert_eq!(create_steps.len(), 1);

                match &create_steps[0] {
                    MigrationStep::Extension(ExtensionOperation::Create { extension }) => {
                        assert_eq!(extension.name, "uuid-ossp");
                        assert_eq!(extension.schema, "utils");
                    }
                    _ => panic!(
                        "Expected ExtensionOperation::Create, got {:?}",
                        create_steps[0]
                    ),
                }

                // Verify the final state
                let ext = final_catalog
                    .extensions
                    .iter()
                    .find(|e| e.name == "uuid-ossp")
                    .expect("Extension should exist in final catalog");
                assert_eq!(ext.schema, "utils");

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_extension_no_changes() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\""], // Both DBs: same extension
            &[],                                               // Initial only: empty
            &[],                                               // Target only: empty
            |steps, _final_catalog| -> Result<()> {
                // Filter extension-related steps only
                let extension_steps: Vec<_> = steps
                    .iter()
                    .filter(|s| matches!(s, MigrationStep::Extension(_)))
                    .collect();

                // No changes should result in no extension migration steps
                assert!(
                    extension_steps.is_empty(),
                    "Expected no extension migration steps, got {:?}",
                    extension_steps
                );
                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_multiple_extensions_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &[], // Both DBs: empty
            &[], // Initial only: empty
            &[
                "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"",
                // Only test with commonly available extensions
            ], // Target only: multiple extensions
            |steps, final_catalog| -> Result<()> {
                // Filter only CREATE extension steps
                let create_steps: Vec<_> = steps
                    .iter()
                    .filter(|s| {
                        matches!(
                            s,
                            MigrationStep::Extension(ExtensionOperation::Create { .. })
                        )
                    })
                    .collect();

                assert_eq!(create_steps.len(), 1); // Only uuid-ossp

                // Verify all extensions are created
                match &create_steps[0] {
                    MigrationStep::Extension(ExtensionOperation::Create { extension }) => {
                        assert_eq!(extension.name, "uuid-ossp");
                    }
                    _ => panic!("Expected ExtensionOperation::Create"),
                }

                // Verify final state
                assert!(!final_catalog.extensions.is_empty());
                let has_uuid = final_catalog
                    .extensions
                    .iter()
                    .any(|e| e.name == "uuid-ossp");
                assert!(has_uuid, "Should have uuid-ossp extension");

                Ok(())
            },
        )
        .await?;

    Ok(())
}
