use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::diff::operations::{CommentOperation, MigrationStep, SequenceOperation};

#[tokio::test]
async fn test_create_sequence_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema
            &["CREATE SCHEMA test_schema"],
            // Initial DB only: nothing extra
            &[],
            // Target DB only: add sequence
            &["CREATE SEQUENCE test_schema.user_id_seq"],
            // Verification closure
            |steps, final_catalog| {
                // Should have CREATE SEQUENCE step
                assert!(!steps.is_empty());
                let create_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::Sequence(SequenceOperation::Create { schema, name, .. })
                    if schema == "test_schema" && name == "user_id_seq")
            }).expect("Should have CreateSequence step");

                match create_step {
                    MigrationStep::Sequence(SequenceOperation::Create { schema, name, .. }) => {
                        assert_eq!(schema, "test_schema");
                        assert_eq!(name, "user_id_seq");
                    }
                    _ => panic!("Expected CreateSequence step"),
                }

                // Verify final state
                let created_sequence = final_catalog
                    .sequences
                    .iter()
                    .find(|s| s.name == "user_id_seq" && s.schema == "test_schema")
                    .expect("Sequence should be created");
                assert_eq!(created_sequence.name, "user_id_seq");
                assert_eq!(created_sequence.schema, "test_schema");

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_drop_sequence_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema
            &["CREATE SCHEMA test_schema"],
            // Initial DB only: has sequence
            &["CREATE SEQUENCE test_schema.old_seq"],
            // Target DB only: nothing extra (sequence missing)
            &[],
            // Verification closure
            |steps, final_catalog| {
                // Should have DROP SEQUENCE step
                assert!(!steps.is_empty());
                assert!(steps.iter().any(|s| {
                matches!(s, MigrationStep::Sequence(SequenceOperation::Drop { schema, name })
                    if schema == "test_schema" && name == "old_seq")
            }), "Should have DropSequence step");

                // Verify final state - sequence should be gone
                let has_sequence = final_catalog
                    .sequences
                    .iter()
                    .any(|s| s.name == "old_seq" && s.schema == "test_schema");
                assert!(!has_sequence);

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_sequence_with_table_serial_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema
            &["CREATE SCHEMA test_schema"],
            // Initial DB only: nothing
            &[],
            // Target DB only: table with SERIAL column
            &["CREATE TABLE test_schema.users (id SERIAL, name TEXT)"],
            // Verification closure
            |steps, final_catalog| {
                // Should have both CREATE SEQUENCE and CREATE TABLE steps
                assert!(steps.len() >= 2);

                let _sequence_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Sequence(SequenceOperation::Create { name, .. })
                    if name == "users_id_seq")
                    })
                    .expect("Should have CreateSequence step for SERIAL column");

                // Verify final state - both table and sequence exist
                assert_eq!(final_catalog.tables.len(), 1);
                let table = &final_catalog.tables[0];
                assert_eq!(table.name, "users");

                // Should have sequence for SERIAL column
                let has_sequence = final_catalog
                    .sequences
                    .iter()
                    .any(|s| s.name == "users_id_seq");
                assert!(has_sequence);

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_sequence_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: schema and sequence
        &[
            "CREATE SCHEMA test_schema",
            "CREATE SEQUENCE test_schema.user_id_seq",
        ],
        // Initial DB only: nothing extra
        &[],
        // Target DB only: add comment
        &["COMMENT ON SEQUENCE test_schema.user_id_seq IS 'User ID sequence'"],
        // Verification closure
        |steps, final_catalog| {
            // Should have SET COMMENT step
            assert!(!steps.is_empty());
            let _comment_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::Sequence(SequenceOperation::Comment(
                    CommentOperation::Set { target, comment }
                )) if target.schema == "test_schema" && target.name == "user_id_seq" && comment == "User ID sequence")
            }).expect("Should have SetComment step");

            // Verify final state
            let sequence = final_catalog.sequences.iter()
                .find(|s| s.name == "user_id_seq" && s.schema == "test_schema")
                .expect("Sequence should exist");
            assert_eq!(sequence.comment, Some("User ID sequence".to_string()));

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_sequence_ownership_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema, sequence, and table
            &[
                "CREATE SCHEMA test_schema",
                "CREATE SEQUENCE test_schema.user_id_seq",
                "CREATE TABLE test_schema.users (id INTEGER, name TEXT)",
            ],
            // Initial DB only: nothing extra (no ownership)
            &[],
            // Target DB only: set ownership
            &["ALTER SEQUENCE test_schema.user_id_seq OWNED BY test_schema.users.id"],
            // Verification closure
            |steps, final_catalog| {
                // Should have ALTER OWNERSHIP step
                assert!(!steps.is_empty());
                let _ownership_step = steps
                    .iter()
                    .find(|s| {
                        matches!(
                            s,
                            MigrationStep::Sequence(SequenceOperation::AlterOwnership { .. })
                        )
                    })
                    .expect("Should have AlterOwnership step");

                // Verify final state
                let sequence = final_catalog
                    .sequences
                    .iter()
                    .find(|s| s.name == "user_id_seq" && s.schema == "test_schema")
                    .expect("Sequence should exist");

                // Check ownership is set in the owned_by field
                assert_eq!(sequence.owned_by, Some("test_schema.users.id".to_string()));

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_serial_sequence_default_grants_no_drift() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema only
            &["CREATE SCHEMA test_schema"],
            // Initial DB only: nothing
            &[],
            // Target DB only: create table with SERIAL column (auto-creates sequence with default grants)
            &["CREATE TABLE test_schema.users (id SERIAL PRIMARY KEY, name TEXT)"],
            // Verification closure
            |steps, final_catalog| {
                // The key test: when diffing a database that has a SERIAL-created sequence
                // against itself, there should be NO grant drift steps
                let grant_steps: Vec<_> = steps
                    .iter()
                    .filter(|s| matches!(s, MigrationStep::Grant(_)))
                    .collect();

                // If there are grant steps, it means we're detecting drift between
                // implicit default grants and explicit grants - which is the bug
                if !grant_steps.is_empty() {
                    panic!("Found unexpected grant drift steps: {:#?}", grant_steps);
                }

                // Verify we have the sequence in the catalog
                final_catalog
                    .sequences
                    .iter()
                    .find(|s| s.name == "users_id_seq" && s.schema == "test_schema")
                    .expect("SERIAL should create sequence");

                // Verify we have synthesized grants for the sequence
                let sequence_grants: Vec<_> = final_catalog
                    .grants
                    .iter()
                    .filter(|g| {
                        matches!(&g.object,
                    pgmt::catalog::grant::ObjectType::Sequence { schema, name }
                    if schema == "test_schema" && name == "users_id_seq")
                    })
                    .collect();

                assert!(
                    !sequence_grants.is_empty(),
                    "Should have grants for SERIAL sequence"
                );

                // Should have owner grants (USAGE, SELECT, UPDATE)
                let owner_grants = sequence_grants
                    .iter()
                    .find(|g| matches!(g.grantee, pgmt::catalog::grant::GranteeType::Role(_)))
                    .expect("Should have owner grants for sequence");

                // Verify expected privileges for sequence owner
                // SERIAL sequences only get USAGE privilege by default (not SELECT/UPDATE like explicit grants)
                assert!(
                    owner_grants.privileges.contains(&"USAGE".to_string()),
                    "Should have USAGE privilege"
                );

                // Verify that this grant is correctly identified as an owner grant
                // (This is why it doesn't generate migration steps - owner grants are implicit in PostgreSQL)
                use pgmt::catalog::grant::GranteeType;
                if let GranteeType::Role(role_name) = &owner_grants.grantee {
                    assert_eq!(
                        role_name, &owner_grants.object_owner,
                        "Grant should be to the object owner"
                    );
                } else {
                    panic!("Expected owner grant to be to a role, not PUBLIC");
                }

                Ok(())
            },
        )
        .await?;

    Ok(())
}
