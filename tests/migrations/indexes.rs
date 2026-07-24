use crate::helpers::harness::with_test_db;
use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::catalog::Catalog;
use pgmt::diff::operations::{
    CollationOperation, CommentOperation, IndexOperation, MigrationStep, TypeOperation,
};
use pgmt::diff::plan;

#[tokio::test]
async fn test_index_create_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: table
            &["CREATE TABLE users (id SERIAL, email VARCHAR, name VARCHAR)"],
            // Initial DB only: nothing extra
            &[],
            // Target DB only: add index
            &["CREATE INDEX idx_users_email ON users (email)"],
            // Verification closure
            |steps, final_catalog| {
                // Should have CREATE INDEX step
                assert!(!steps.is_empty());
                let create_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Index(IndexOperation::Create(index))
                    if index.name == "idx_users_email")
                    })
                    .expect("Should have CreateIndex step");

                match create_step {
                    MigrationStep::Index(IndexOperation::Create(index)) => {
                        assert_eq!(index.name, "idx_users_email");
                        assert_eq!(index.table_name, "users");
                        assert_eq!(index.columns.len(), 1);
                        assert_eq!(index.columns[0].expression, "email");
                    }
                    _ => panic!("Expected CreateIndex step"),
                }

                // Verify final state
                let created_index = final_catalog
                    .indexes
                    .iter()
                    .find(|i| i.name == "idx_users_email")
                    .expect("Index should be created");

                assert_eq!(created_index.name, "idx_users_email");
                assert_eq!(created_index.table_name, "users");
                assert_eq!(created_index.columns.len(), 1);
                assert_eq!(created_index.columns[0].expression, "email");
                assert!(!created_index.is_unique);

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_index_drop_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: table
            &["CREATE TABLE users (id SERIAL, email VARCHAR)"],
            // Initial DB only: has index
            &["CREATE INDEX idx_users_email ON users (email)"],
            // Target DB only: nothing extra (index missing)
            &[],
            // Verification closure
            |steps, final_catalog| {
                // Should have DROP INDEX step
                assert!(!steps.is_empty());
                assert!(
                    steps.iter().any(|s| {
                        matches!(s, MigrationStep::Index(IndexOperation::Drop { name, .. })
                    if name == "idx_users_email")
                    }),
                    "Should have DropIndex step"
                );

                // Verify final state - index should be gone
                let has_index = final_catalog
                    .indexes
                    .iter()
                    .any(|i| i.name == "idx_users_email");
                assert!(!has_index);

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_unique_index_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: table
            &["CREATE TABLE users (id SERIAL, email VARCHAR UNIQUE)"],
            // Initial DB only: nothing extra
            &[],
            // Target DB only: add unique index explicitly
            &["CREATE UNIQUE INDEX idx_users_email_unique ON users (email)"],
            // Verification closure
            |steps, final_catalog| {
                // Should have CREATE INDEX step
                assert!(!steps.is_empty());
                let create_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Index(IndexOperation::Create(index))
                    if index.name == "idx_users_email_unique")
                    })
                    .expect("Should have CreateIndex step");

                match create_step {
                    MigrationStep::Index(IndexOperation::Create(index)) => {
                        assert_eq!(index.name, "idx_users_email_unique");
                        assert!(index.is_unique);
                    }
                    _ => panic!("Expected CreateIndex step"),
                }

                // Verify final state
                let created_index = final_catalog
                    .indexes
                    .iter()
                    .find(|i| i.name == "idx_users_email_unique")
                    .expect("Unique index should be created");
                assert!(created_index.is_unique);

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_expression_index_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: table
            &["CREATE TABLE users (id SERIAL, email VARCHAR, name VARCHAR)"],
            // Initial DB only: nothing extra
            &[],
            // Target DB only: add expression index
            &["CREATE INDEX idx_users_email_lower ON users (LOWER(email))"],
            // Verification closure
            |steps, final_catalog| {
                // Should have CREATE INDEX step
                assert!(!steps.is_empty());
                let create_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Index(IndexOperation::Create(index))
                    if index.name == "idx_users_email_lower")
                    })
                    .expect("Should have CreateIndex step");

                match create_step {
                    MigrationStep::Index(IndexOperation::Create(index)) => {
                        assert_eq!(index.name, "idx_users_email_lower");
                        assert_eq!(index.table_name, "users");
                        assert_eq!(index.columns.len(), 1);
                        assert!(index.columns[0].expression.contains("lower"));
                    }
                    _ => panic!("Expected CreateIndex step"),
                }

                // Verify final state
                let created_index = final_catalog
                    .indexes
                    .iter()
                    .find(|i| i.name == "idx_users_email_lower")
                    .expect("Expression index should be created");
                assert!(created_index.columns[0].expression.contains("lower"));

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_index_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: table and index
            &[
                "CREATE TABLE users (id SERIAL, email VARCHAR)",
                "CREATE INDEX idx_users_email ON users (email)",
            ],
            // Initial DB only: nothing extra
            &[],
            // Target DB only: add comment
            &["COMMENT ON INDEX idx_users_email IS 'Email lookup index'"],
            // Verification closure
            |steps, final_catalog| {
                // Should have SET COMMENT step
                assert!(!steps.is_empty());
                let _comment_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Comment(CommentOperation::Set { target, comment }) if target.name() == "idx_users_email" && comment == "Email lookup index")
                    })
                    .expect("Should have SetComment step");

                // Verify final state
                let index = final_catalog
                    .indexes
                    .iter()
                    .find(|i| i.name == "idx_users_email")
                    .expect("Index should exist");
                assert_eq!(index.comment, Some("Email lookup index".to_string()));

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_multi_column_index_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: table
        &["CREATE TABLE users (id SERIAL, first_name VARCHAR, last_name VARCHAR, email VARCHAR)"],
        // Initial DB only: nothing extra
        &[],
        // Target DB only: add multi-column index
        &["CREATE INDEX idx_users_name ON users (last_name, first_name)"],
        // Verification closure
        |steps, final_catalog| {
            // Should have CREATE INDEX step
            assert!(!steps.is_empty());
            let create_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::Index(IndexOperation::Create(index))
                    if index.name == "idx_users_name")
            }).expect("Should have CreateIndex step");

            match create_step {
                MigrationStep::Index(IndexOperation::Create(index)) => {
                    assert_eq!(index.name, "idx_users_name");
                    assert_eq!(index.table_name, "users");
                    assert_eq!(index.columns.len(), 2);
                    assert_eq!(index.columns[0].expression, "last_name");
                    assert_eq!(index.columns[1].expression, "first_name");
                }
                _ => panic!("Expected CreateIndex step"),
            }

            // Verify final state
            let created_index = final_catalog.indexes.iter()
                .find(|i| i.name == "idx_users_name")
                .expect("Multi-column index should be created");
            assert_eq!(created_index.columns.len(), 2);

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_index_cascade_on_type_change() -> Result<()> {
    with_test_db(async |initial_db| {
        with_test_db(async |target_db| {
            // Initial: composite type, table with that type, expression index
            initial_db
                .execute("CREATE TYPE dimensions AS (width INT, height INT)")
                .await;
            initial_db
                .execute("CREATE TABLE products (id SERIAL PRIMARY KEY, dims dimensions)")
                .await;
            initial_db
                .execute("CREATE INDEX idx_dims ON products (((dims).width))")
                .await;

            // Target: composite type gains a field
            target_db
                .execute("CREATE TYPE dimensions AS (width INT, height INT, depth INT)")
                .await;
            target_db
                .execute("CREATE TABLE products (id SERIAL PRIMARY KEY, dims dimensions)")
                .await;
            target_db
                .execute("CREATE INDEX idx_dims ON products (((dims).width))")
                .await;

            let initial_catalog = Catalog::load_unfiltered(initial_db.pool()).await?;
            let target_catalog = Catalog::load_unfiltered(target_db.pool()).await?;

            let steps = plan(&initial_catalog, &target_catalog)?;

            // The type change should cascade to the index
            let drop_index_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::Index(IndexOperation::Drop { name, .. })
                        if name == "idx_dims")
                })
                .expect("Should have DROP INDEX for idx_dims");

            let create_index_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::Index(IndexOperation::Create(index))
                        if index.name == "idx_dims")
                })
                .expect("Should have CREATE INDEX for idx_dims");

            let drop_type_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::Type(TypeOperation::Drop { name, .. })
                        if name == "dimensions")
                })
                .expect("Should have DROP TYPE for dimensions");

            let create_type_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::Type(TypeOperation::Create { name, .. })
                        if name == "dimensions")
                })
                .expect("Should have CREATE TYPE for dimensions");

            // Index must be dropped before type, and created after type
            assert!(
                drop_index_pos < drop_type_pos,
                "DROP INDEX should come before DROP TYPE"
            );
            assert!(
                create_type_pos < create_index_pos,
                "CREATE TYPE should come before CREATE INDEX"
            );

            Ok(())
        })
        .await
    })
    .await
}

/// An explicit per-key COLLATE override must survive the full pipeline: the
/// index carries a dependency on the collation, so CREATE COLLATION is ordered
/// before CREATE INDEX, and the applied index round-trips with the override.
#[tokio::test]
async fn test_index_collation_override_orders_after_collation() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &[],
            &[],
            &[
                "CREATE COLLATION german_ci (provider = icu, locale = 'de-u-ks-level2', deterministic = false)",
                "CREATE TABLE users (name text)",
                "CREATE INDEX idx_users_name_ci ON users (name COLLATE german_ci)",
            ],
            |steps, final_catalog| {
                let create_collation = steps
                    .iter()
                    .position(|s| {
                        matches!(s, MigrationStep::Collation(CollationOperation::Create { .. }))
                    })
                    .expect("Should have CREATE COLLATION step");
                let create_index = steps
                    .iter()
                    .position(|s| {
                        matches!(s, MigrationStep::Index(IndexOperation::Create(index))
                            if index.name == "idx_users_name_ci")
                    })
                    .expect("Should have CREATE INDEX step");
                assert!(
                    create_collation < create_index,
                    "CREATE COLLATION must come before the index that overrides to it"
                );

                // The fresh-database apply inside run_migration_test proves the
                // plan is complete; verify the round-tripped state too.
                let idx = final_catalog
                    .indexes
                    .iter()
                    .find(|i| i.name == "idx_users_name_ci")
                    .expect("Index should be created");
                let collation = idx.columns[0]
                    .collation
                    .as_ref()
                    .expect("index keeps its collation override");
                assert_eq!(collation.schema, "public");
                assert_eq!(collation.name, "german_ci");

                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// An index on a column that itself has a custom collation, with no per-key
/// override, must not grow a phantom COLLATE clause: the fetched key records
/// no collation and identical databases diff to an empty plan.
#[tokio::test]
async fn test_index_on_collated_column_no_spurious_diff() -> Result<()> {
    let helper = MigrationTestHelper::new().await;
    let (initial_db, target_db) = helper.setup_migration_test().await;

    for db in [&initial_db, &target_db] {
        db.execute(
            "CREATE COLLATION german_ci (provider = icu, locale = 'de-u-ks-level2', deterministic = false)",
        )
        .await;
        db.execute("CREATE TABLE users (name text COLLATE german_ci)")
            .await;
        db.execute("CREATE INDEX idx_users_name ON users (name)")
            .await;
    }

    let initial_catalog = Catalog::load_unfiltered(initial_db.pool()).await?;
    let target_catalog = Catalog::load_unfiltered(target_db.pool()).await?;

    let idx = initial_catalog
        .indexes
        .iter()
        .find(|i| i.name == "idx_users_name")
        .expect("index should be in catalog");
    assert_eq!(
        idx.columns[0].collation, None,
        "inherited column collation must not be recorded on the index key"
    );

    let steps = helper
        .run_migration_pipeline(&initial_catalog, &target_catalog)
        .await?;
    assert!(
        steps.is_empty(),
        "identical databases must produce an empty plan, got: {steps:?}"
    );

    initial_db.cleanup().await;
    target_db.cleanup().await;
    Ok(())
}

/// Changing an attribute of the collation an index key overrides to must
/// cascade: the index is dropped before the collation and recreated after it,
/// the plan applies cleanly, and re-diffing the applied state is empty. The
/// table's column is plain text, so this exercises the index-only dependency
/// path (the table itself never depends on the collation).
#[tokio::test]
async fn test_collation_change_recreates_overriding_index() -> Result<()> {
    let helper = MigrationTestHelper::new().await;
    let (initial_db, target_db) = helper.setup_migration_test().await;

    initial_db
        .execute("CREATE COLLATION german_ci (provider = icu, locale = 'de-u-ks-level2', deterministic = false)")
        .await;
    target_db
        .execute("CREATE COLLATION german_ci (provider = icu, locale = 'de-u-ks-level2', deterministic = true)")
        .await;
    for db in [&initial_db, &target_db] {
        db.execute("CREATE TABLE users (name text)").await;
        db.execute("CREATE INDEX idx_users_name_ci ON users (name COLLATE german_ci)")
            .await;
    }

    let initial_catalog = Catalog::load_unfiltered(initial_db.pool()).await?;
    let target_catalog = Catalog::load_unfiltered(target_db.pool()).await?;
    let steps = helper
        .run_migration_pipeline(&initial_catalog, &target_catalog)
        .await?;

    let position = |pred: &dyn Fn(&MigrationStep) -> bool, what: &str| {
        steps
            .iter()
            .position(pred)
            .unwrap_or_else(|| panic!("expected {what} step in plan: {steps:?}"))
    };
    let drop_index = position(
        &|s| matches!(s, MigrationStep::Index(IndexOperation::Drop { name, .. }) if name == "idx_users_name_ci"),
        "DROP INDEX",
    );
    let drop_collation = position(
        &|s| matches!(s, MigrationStep::Collation(CollationOperation::Drop { name, .. }) if name == "german_ci"),
        "DROP COLLATION",
    );
    let create_collation = position(
        &|s| matches!(s, MigrationStep::Collation(CollationOperation::Create { collation }) if collation.name == "german_ci"),
        "CREATE COLLATION",
    );
    let create_index = position(
        &|s| matches!(s, MigrationStep::Index(IndexOperation::Create(index)) if index.name == "idx_users_name_ci"),
        "CREATE INDEX",
    );
    assert!(
        drop_index < drop_collation,
        "dependent index must be dropped before the collation"
    );
    assert!(
        drop_collation < create_collation,
        "collation drop must precede its recreate"
    );
    assert!(
        create_collation < create_index,
        "index must be recreated after the collation"
    );

    // Apply to the real initial database, then re-diff: must be empty.
    helper.execute_migration(&initial_db, &steps).await?;
    let final_catalog = Catalog::load_unfiltered(initial_db.pool()).await?;
    assert!(final_catalog.collations[0].deterministic);
    let rediff = helper
        .run_migration_pipeline(&final_catalog, &target_catalog)
        .await?;
    assert!(
        rediff.is_empty(),
        "re-diff after applying the cascade must be empty, got: {rediff:?}"
    );

    initial_db.cleanup().await;
    target_db.cleanup().await;
    Ok(())
}
