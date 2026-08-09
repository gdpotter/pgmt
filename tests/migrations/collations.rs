use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::catalog::Catalog;
use pgmt::catalog::collation::CollationProvider;
use pgmt::catalog::id::DbObjectId;
use pgmt::catalog::target::AttrTarget;
use pgmt::diff::operations::{
    CollationOperation, CommentOperation, DomainOperation, MigrationStep, SqlRenderer,
    TableOperation,
};

/// Index of the first step matching a predicate, for ordering assertions.
fn position(steps: &[MigrationStep], pred: impl Fn(&MigrationStep) -> bool) -> usize {
    steps
        .iter()
        .position(pred)
        .expect("expected step not found in plan")
}

#[tokio::test]
async fn test_create_collation_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE SCHEMA test_schema"],
            &[],
            &[
                "CREATE COLLATION test_schema.case_insensitive (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
            ],
            |steps, final_catalog| -> Result<()> {
                let create_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Collation(CollationOperation::Create { collation })
                        if collation.name == "case_insensitive")
                    })
                    .expect("Should have Create Collation step");

                let sql = &create_step.to_sql()[0].sql;
                assert!(sql.contains("CREATE COLLATION \"test_schema\".\"case_insensitive\""));
                assert!(sql.contains("provider = icu"));
                assert!(sql.contains("locale = 'und-u-ks-level2'"));
                assert!(sql.contains("deterministic = false"));

                assert_eq!(final_catalog.collations.len(), 1);
                let created = &final_catalog.collations[0];
                assert_eq!(created.schema, "test_schema");
                assert_eq!(created.name, "case_insensitive");
                assert_eq!(created.provider, CollationProvider::Icu);
                assert!(!created.deterministic);
                assert_eq!(created.locale.as_deref(), Some("und-u-ks-level2"));

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_drop_collation_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE SCHEMA test_schema"],
            &["CREATE COLLATION test_schema.old_coll (locale = 'C')"],
            &[],
            |steps, final_catalog| -> Result<()> {
                let drop_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Collation(CollationOperation::Drop { name, .. })
                        if name == "old_coll")
                    })
                    .expect("Should have Drop Collation step");

                assert_eq!(
                    drop_step.to_sql()[0].sql,
                    "DROP COLLATION \"test_schema\".\"old_coll\";"
                );

                // Collations can be recreated from schema files, so DROP is not destructive
                assert!(!drop_step.has_destructive_sql());

                assert_eq!(final_catalog.collations.len(), 0);
                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_collation_attribute_change_recreates() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE SCHEMA test_schema"],
            &[
                "CREATE COLLATION test_schema.ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
            ],
            &[
                "CREATE COLLATION test_schema.ci (provider = icu, locale = 'und-u-ks-level1', deterministic = false)",
            ],
            |steps, final_catalog| -> Result<()> {
                let drop_idx = position(steps, |s| {
                    matches!(s, MigrationStep::Collation(CollationOperation::Drop { .. }))
                });
                let create_idx = position(steps, |s| {
                    matches!(s, MigrationStep::Collation(CollationOperation::Create { .. }))
                });
                assert!(drop_idx < create_idx, "DROP must precede recreate");

                assert_eq!(final_catalog.collations.len(), 1);
                assert_eq!(
                    final_catalog.collations[0].locale.as_deref(),
                    Some("und-u-ks-level1")
                );
                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_collation_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE COLLATION posixy (locale = 'C')"],
            &[],
            &["COMMENT ON COLLATION posixy IS 'plain byte order'"],
            |steps, final_catalog| -> Result<()> {
                let comment_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Comment(CommentOperation::Set { target, .. })
                        if *target == AttrTarget::object(DbObjectId::Collation {
                            schema: "public".to_string(),
                            name: "posixy".to_string(),
                        }))
                    })
                    .expect("Should have Comment Set step");

                assert_eq!(
                    comment_step.to_sql()[0].sql,
                    "COMMENT ON COLLATION \"public\".\"posixy\" IS 'plain byte order';"
                );

                assert_eq!(
                    final_catalog.collations[0].comment.as_deref(),
                    Some("plain byte order")
                );
                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_drop_collation_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE COLLATION posixy (locale = 'C')"],
            &["COMMENT ON COLLATION posixy IS 'plain byte order'"],
            &[],
            |steps, final_catalog| -> Result<()> {
                let comment_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Comment(CommentOperation::Drop { target })
                        if *target == AttrTarget::object(DbObjectId::Collation {
                            schema: "public".to_string(),
                            name: "posixy".to_string(),
                        }))
                    })
                    .expect("Should have Comment Drop step");

                assert_eq!(
                    comment_step.to_sql()[0].sql,
                    "COMMENT ON COLLATION \"public\".\"posixy\" IS NULL;"
                );

                assert!(final_catalog.collations[0].comment.is_none());
                Ok(())
            },
        )
        .await?;
    Ok(())
}

/// Regression test for issue #15: a schema declaring a custom collation plus a
/// domain using it validated fine against the shadow but generated a plan with
/// no CREATE COLLATION step, so applying to a fresh target failed with
/// `collation "..." for encoding "UTF8" does not exist`.
#[tokio::test]
async fn test_collation_used_by_domain_orders_collation_first() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &[],
            &[],
            &[
                "CREATE COLLATION case_insensitive (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
                "CREATE DOMAIN email AS text COLLATE case_insensitive",
            ],
            |steps, final_catalog| -> Result<()> {
                let collation_idx = position(steps, |s| {
                    matches!(s, MigrationStep::Collation(CollationOperation::Create { .. }))
                });
                let domain_idx = position(steps, |s| matches!(s, MigrationStep::Domain(_)));
                assert!(
                    collation_idx < domain_idx,
                    "CREATE COLLATION must come before the domain that uses it"
                );

                // The fresh-database apply inside run_migration_test proves the
                // plan is complete; verify the round-tripped state too.
                assert_eq!(final_catalog.collations.len(), 1);
                assert_eq!(final_catalog.domains.len(), 1);
                let collation = final_catalog.domains[0]
                    .collation
                    .as_ref()
                    .expect("domain keeps its collation");
                assert_eq!(collation.schema, "public");
                assert_eq!(collation.name, "case_insensitive");
                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_cross_schema_collation_domain() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &[],
            &[],
            &[
                "CREATE SCHEMA coll_schema",
                "CREATE SCHEMA domain_schema",
                "CREATE COLLATION coll_schema.ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
                "CREATE DOMAIN domain_schema.email AS text COLLATE coll_schema.ci",
            ],
            |steps, final_catalog| -> Result<()> {
                let collation_idx = position(steps, |s| {
                    matches!(s, MigrationStep::Collation(CollationOperation::Create { .. }))
                });
                let domain_idx = position(steps, |s| matches!(s, MigrationStep::Domain(_)));
                assert!(collation_idx < domain_idx);

                let domain_step = &steps[domain_idx];
                assert!(
                    domain_step.to_sql()[0]
                        .sql
                        .contains("COLLATE \"coll_schema\".\"ci\""),
                    "COLLATE clause must be schema-qualified: {}",
                    domain_step.to_sql()[0].sql
                );

                let collation = final_catalog.domains[0]
                    .collation
                    .as_ref()
                    .expect("domain keeps its collation");
                assert_eq!(collation.schema, "coll_schema");
                assert_eq!(collation.name, "ci");
                Ok(())
            },
        )
        .await?;
    Ok(())
}

/// Two same-named collations in different schemas must resolve to the right
/// one. A bare-name fetch (the old behavior) cannot tell them apart.
#[tokio::test]
async fn test_same_named_collations_in_different_schemas() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &[],
            &[],
            &[
                "CREATE SCHEMA schema_a",
                "CREATE SCHEMA schema_b",
                "CREATE COLLATION schema_a.ci (locale = 'C')",
                "CREATE COLLATION schema_b.ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
                "CREATE DOMAIN public.email AS text COLLATE schema_b.ci",
            ],
            |_steps, final_catalog| -> Result<()> {
                assert_eq!(final_catalog.collations.len(), 2);

                let collation = final_catalog.domains[0]
                    .collation
                    .as_ref()
                    .expect("domain keeps its collation");
                assert_eq!(collation.schema, "schema_b");
                assert_eq!(collation.name, "ci");

                // The domain's dependency points at the right collation.
                assert!(final_catalog.domains[0].depends_on.contains(
                    &DbObjectId::Collation {
                        schema: "schema_b".to_string(),
                        name: "ci".to_string()
                    }
                ));
                Ok(())
            },
        )
        .await?;
    Ok(())
}

/// Changing an immutable collation attribute (here: determinism) while a domain
/// references the collation must cascade: the domain is dropped before the
/// collation and recreated after it, the plan applies cleanly to a real
/// database, and re-diffing the applied state against the target is empty.
#[tokio::test]
async fn test_collation_change_cascades_dependent_domain() -> Result<()> {
    let helper = MigrationTestHelper::new().await;
    let (initial_db, target_db) = helper.setup_migration_test().await;

    initial_db
        .execute("CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)")
        .await;
    initial_db
        .execute("CREATE DOMAIN email AS text COLLATE ci")
        .await;

    target_db
        .execute("CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level2', deterministic = true)")
        .await;
    target_db
        .execute("CREATE DOMAIN email AS text COLLATE ci")
        .await;

    let initial_catalog = Catalog::load_unfiltered(initial_db.pool()).await?;
    let target_catalog = Catalog::load_unfiltered(target_db.pool()).await?;
    let steps = helper
        .run_migration_pipeline(&initial_catalog, &target_catalog)
        .await?;

    let drop_domain = position(
        &steps,
        |s| matches!(s, MigrationStep::Domain(DomainOperation::Drop { name, .. }) if name == "email"),
    );
    let drop_collation = position(
        &steps,
        |s| matches!(s, MigrationStep::Collation(CollationOperation::Drop { name, .. }) if name == "ci"),
    );
    let create_collation = position(
        &steps,
        |s| matches!(s, MigrationStep::Collation(CollationOperation::Create { collation }) if collation.name == "ci"),
    );
    let create_domain = position(
        &steps,
        |s| matches!(s, MigrationStep::Domain(DomainOperation::Create { name, .. }) if name == "email"),
    );
    assert!(
        drop_domain < drop_collation,
        "dependent domain must be dropped before the collation"
    );
    assert!(
        drop_collation < create_collation,
        "collation drop must precede its recreate"
    );
    assert!(
        create_collation < create_domain,
        "domain must be recreated after the collation"
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

/// The deep chain: collation <- domain <- table column using the domain.
/// Changing the collation must transitively cascade through the existing
/// machinery: the table (which references the domain) and the domain are both
/// drop/recreated around the collation, the plan applies cleanly, and a
/// re-diff is empty. Like every recreate cascade, the table is fully
/// recreated.
#[tokio::test]
async fn test_collation_change_cascades_through_domain_to_table() -> Result<()> {
    let helper = MigrationTestHelper::new().await;
    let (initial_db, target_db) = helper.setup_migration_test().await;

    initial_db
        .execute("CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)")
        .await;
    initial_db
        .execute("CREATE DOMAIN email AS text COLLATE ci")
        .await;
    initial_db
        .execute("CREATE TABLE users (id integer PRIMARY KEY, address email)")
        .await;

    target_db
        .execute("CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level1', deterministic = false)")
        .await;
    target_db
        .execute("CREATE DOMAIN email AS text COLLATE ci")
        .await;
    target_db
        .execute("CREATE TABLE users (id integer PRIMARY KEY, address email)")
        .await;

    let initial_catalog = Catalog::load_unfiltered(initial_db.pool()).await?;
    let target_catalog = Catalog::load_unfiltered(target_db.pool()).await?;
    let steps = helper
        .run_migration_pipeline(&initial_catalog, &target_catalog)
        .await?;

    let drop_table = position(
        &steps,
        |s| matches!(s, MigrationStep::Table(TableOperation::Drop { name, .. }) if name == "users"),
    );
    let drop_domain = position(
        &steps,
        |s| matches!(s, MigrationStep::Domain(DomainOperation::Drop { name, .. }) if name == "email"),
    );
    let drop_collation = position(
        &steps,
        |s| matches!(s, MigrationStep::Collation(CollationOperation::Drop { name, .. }) if name == "ci"),
    );
    let create_collation = position(
        &steps,
        |s| matches!(s, MigrationStep::Collation(CollationOperation::Create { collation }) if collation.name == "ci"),
    );
    let create_domain = position(
        &steps,
        |s| matches!(s, MigrationStep::Domain(DomainOperation::Create { name, .. }) if name == "email"),
    );
    let create_table = position(
        &steps,
        |s| matches!(s, MigrationStep::Table(TableOperation::Create { name, .. }) if name == "users"),
    );
    assert!(drop_table < drop_domain, "table drops before its domain");
    assert!(
        drop_domain < drop_collation,
        "domain drops before collation"
    );
    assert!(
        create_collation < create_domain,
        "collation recreated first"
    );
    assert!(create_domain < create_table, "table recreated last");

    helper.execute_migration(&initial_db, &steps).await?;
    let final_catalog = Catalog::load_unfiltered(initial_db.pool()).await?;
    assert_eq!(
        final_catalog.collations[0].locale.as_deref(),
        Some("und-u-ks-level1")
    );
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

/// A comment-only change on a collation is attached state, not structure: it
/// must not drop/recreate the collation or cascade into dependents.
#[tokio::test]
async fn test_collation_comment_only_change_does_not_cascade() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &[
                "CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
                "CREATE DOMAIN email AS text COLLATE ci",
                "CREATE TABLE users (id integer PRIMARY KEY, address email)",
            ],
            &[],
            &["COMMENT ON COLLATION ci IS 'case-insensitive'"],
            |steps, final_catalog| -> Result<()> {
                assert!(
                    !steps.iter().any(|s| matches!(s, MigrationStep::Collation(_))),
                    "comment-only change must not touch the collation structurally"
                );
                assert!(
                    !steps
                        .iter()
                        .any(|s| matches!(s, MigrationStep::Domain(_) | MigrationStep::Table(_))),
                    "comment-only change must not cascade into dependents"
                );
                assert!(steps.iter().any(|s| matches!(
                    s,
                    MigrationStep::Comment(CommentOperation::Set { .. })
                )));

                assert_eq!(
                    final_catalog.collations[0].comment.as_deref(),
                    Some("case-insensitive")
                );
                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_mixed_case_collation_name_quotes_correctly() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &[],
            &[],
            &[
                "CREATE COLLATION \"CaseSensitive\" (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
                "CREATE DOMAIN email AS text COLLATE \"CaseSensitive\"",
            ],
            |steps, final_catalog| -> Result<()> {
                let create_step = steps
                    .iter()
                    .find(|s| matches!(s, MigrationStep::Collation(CollationOperation::Create { .. })))
                    .expect("Should have Create Collation step");
                assert!(
                    create_step.to_sql()[0]
                        .sql
                        .contains("CREATE COLLATION \"public\".\"CaseSensitive\"")
                );

                assert_eq!(final_catalog.collations[0].name, "CaseSensitive");
                let collation = final_catalog.domains[0]
                    .collation
                    .as_ref()
                    .expect("domain keeps its collation");
                assert_eq!(collation.name, "CaseSensitive");
                Ok(())
            },
        )
        .await?;
    Ok(())
}
