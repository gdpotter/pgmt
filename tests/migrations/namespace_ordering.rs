//! Ordering tests for objects that share a PostgreSQL name-space across object
//! types. These collisions have no `pg_depend` edge between the dropped and
//! created object, so the migration pipeline must derive a drop-before-create
//! ordering from the shared namespace slot. The helper applies the migration to
//! a real database, so an incorrect order also fails by erroring at apply time
//! (e.g. "relation already exists").

use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::diff::operations::{
    ConstraintOperation, IndexOperation, MigrationStep, OperationKind, TableOperation,
};

/// Dropping a UNIQUE constraint and creating a plain index with the *same* name.
/// The constraint's backing index occupies the same pg_class slot as the new
/// index, so the constraint must be dropped first.
#[tokio::test]
async fn test_drop_constraint_create_index_same_name() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE TABLE items (id SERIAL PRIMARY KEY, email TEXT, name TEXT)"],
            &["ALTER TABLE items ADD CONSTRAINT shared_name UNIQUE (email)"],
            &["CREATE INDEX shared_name ON items (name)"],
            |steps, _catalog| {
                let drop_idx = steps.iter().position(|s| {
                    matches!(s, MigrationStep::Constraint(ConstraintOperation::Drop(_)))
                });
                let create_idx = steps
                    .iter()
                    .position(|s| matches!(s, MigrationStep::Index(IndexOperation::Create(_))));

                let drop_idx = drop_idx.expect("constraint drop step should exist");
                let create_idx = create_idx.expect("index create step should exist");

                assert!(
                    drop_idx < create_idx,
                    "constraint `shared_name` must be dropped before index `shared_name` is created (drop at {drop_idx}, create at {create_idx})"
                );
                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Dropping a table and creating a view with the same name. Both live in the
/// pg_class relation namespace.
#[tokio::test]
async fn test_drop_table_create_view_same_name() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE TABLE source (id INT, label TEXT)"],
            &["CREATE TABLE widget (id INT)"],
            &["CREATE VIEW widget AS SELECT id FROM source"],
            |steps, _catalog| {
                let drop_idx = steps.iter().position(|s| {
                    matches!(s, MigrationStep::Table(TableOperation::Drop { .. }))
                });
                let create_idx = steps
                    .iter()
                    .position(|s| matches!(s, MigrationStep::View(_)));

                let drop_idx = drop_idx.expect("table drop step should exist");
                let create_idx = create_idx.expect("view create step should exist");

                assert!(
                    drop_idx < create_idx,
                    "table `widget` must be dropped before view `widget` is created (drop at {drop_idx}, create at {create_idx})"
                );
                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Dropping a standalone type and creating a domain with the same name. Both
/// live in the pg_type namespace.
#[tokio::test]
async fn test_drop_type_create_domain_same_name() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &[],
            &["CREATE TYPE status AS ENUM ('a', 'b')"],
            &["CREATE DOMAIN status AS TEXT"],
            |steps, _catalog| {
                let drop_idx = steps.iter().position(|s| {
                    matches!(s, MigrationStep::Type(_)) && s.operation_kind() == OperationKind::Drop
                });
                let create_idx = steps.iter().position(|s| {
                    matches!(s, MigrationStep::Domain(_))
                        && s.operation_kind() == OperationKind::Create
                });

                let drop_idx = drop_idx.expect("type drop step should exist");
                let create_idx = create_idx.expect("domain create step should exist");

                assert!(
                    drop_idx < create_idx,
                    "type `status` must be dropped before domain `status` is created (drop at {drop_idx}, create at {create_idx})"
                );
                Ok(())
            },
        )
        .await?;

    Ok(())
}
