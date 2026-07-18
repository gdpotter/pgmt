use crate::helpers::harness::with_test_db;
use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::catalog::Catalog;
use pgmt::catalog::domain::fetch;
use pgmt::diff::domains::diff;
use pgmt::diff::operations::{
    CommentOperation, DomainOperation, MigrationStep, SqlRenderer, TypeOperation,
};
use pgmt::diff::plan;
use pgmt::render::Safety;

#[tokio::test]
async fn test_create_domain_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: just the schema
            &["CREATE SCHEMA test_schema"],
            // Initial DB: nothing extra
            &[],
            // Target DB: create domain
            &["CREATE DOMAIN test_schema.positive_int AS INTEGER CHECK (VALUE > 0)"],
            |steps, final_catalog| -> Result<()> {
                // Should have a Create step
                let create_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Domain(DomainOperation::Create { name, .. })
                        if name == "positive_int")
                    })
                    .expect("Should have Create Domain step");

                match create_step {
                    MigrationStep::Domain(DomainOperation::Create {
                        schema,
                        name,
                        definition,
                    }) => {
                        assert_eq!(schema, "test_schema");
                        assert_eq!(name, "positive_int");
                        assert!(definition.contains("AS integer"));
                        assert!(definition.contains("CHECK"));
                    }
                    _ => panic!("Expected Create Domain step"),
                }

                // Verify final state
                assert_eq!(final_catalog.domains.len(), 1);
                let created_domain = &final_catalog.domains[0];
                assert_eq!(created_domain.schema, "test_schema");
                assert_eq!(created_domain.name, "positive_int");
                assert_eq!(created_domain.base_type, "integer");
                assert_eq!(created_domain.check_constraints.len(), 1);

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_drop_domain_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: just the schema
            &["CREATE SCHEMA test_schema"],
            // Initial DB: has domain
            &["CREATE DOMAIN test_schema.user_id AS INTEGER"],
            // Target DB: domain removed
            &[],
            |steps, final_catalog| -> Result<()> {
                // Should have a Drop step
                let drop_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Domain(DomainOperation::Drop { name, .. })
                        if name == "user_id")
                    })
                    .expect("Should have Drop Domain step");

                match drop_step {
                    MigrationStep::Domain(DomainOperation::Drop { schema, name }) => {
                        assert_eq!(schema, "test_schema");
                        assert_eq!(name, "user_id");
                    }
                    _ => panic!("Expected Drop Domain step"),
                }

                // Domains can be recreated from schema, so DROP is not destructive
                assert!(!drop_step.has_destructive_sql());

                // Verify final state - no domains
                assert_eq!(final_catalog.domains.len(), 0);

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_domain_set_not_null_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: has domain without NOT NULL
            &[
                "CREATE SCHEMA test_schema",
                "CREATE DOMAIN test_schema.user_id AS INTEGER",
            ],
            // Initial DB: nothing extra
            &[],
            // Target DB: add NOT NULL
            &["ALTER DOMAIN test_schema.user_id SET NOT NULL"],
            |steps, final_catalog| -> Result<()> {
                // Should have AlterSetNotNull step
                let alter_step = steps
                    .iter()
                    .find(|s| {
                        matches!(
                            s,
                            MigrationStep::Domain(DomainOperation::AlterSetNotNull { .. })
                        )
                    })
                    .expect("Should have AlterSetNotNull step");

                match alter_step {
                    MigrationStep::Domain(DomainOperation::AlterSetNotNull { schema, name }) => {
                        assert_eq!(schema, "test_schema");
                        assert_eq!(name, "user_id");
                    }
                    _ => panic!("Expected AlterSetNotNull step"),
                }

                // Verify final state
                assert_eq!(final_catalog.domains.len(), 1);
                assert!(final_catalog.domains[0].not_null);

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_domain_drop_not_null_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: has domain with NOT NULL
            &[
                "CREATE SCHEMA test_schema",
                "CREATE DOMAIN test_schema.required_text AS TEXT NOT NULL",
            ],
            // Initial DB: nothing extra
            &[],
            // Target DB: remove NOT NULL
            &["ALTER DOMAIN test_schema.required_text DROP NOT NULL"],
            |steps, final_catalog| -> Result<()> {
                // Should have AlterDropNotNull step
                let alter_step = steps
                    .iter()
                    .find(|s| {
                        matches!(
                            s,
                            MigrationStep::Domain(DomainOperation::AlterDropNotNull { .. })
                        )
                    })
                    .expect("Should have AlterDropNotNull step");

                match alter_step {
                    MigrationStep::Domain(DomainOperation::AlterDropNotNull { schema, name }) => {
                        assert_eq!(schema, "test_schema");
                        assert_eq!(name, "required_text");
                    }
                    _ => panic!("Expected AlterDropNotNull step"),
                }

                // Verify final state
                assert_eq!(final_catalog.domains.len(), 1);
                assert!(!final_catalog.domains[0].not_null);

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_domain_set_default_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: has domain without default
            &[
                "CREATE SCHEMA test_schema",
                "CREATE DOMAIN test_schema.status AS TEXT",
            ],
            // Initial DB: nothing extra
            &[],
            // Target DB: add default
            &["ALTER DOMAIN test_schema.status SET DEFAULT 'pending'"],
            |steps, final_catalog| -> Result<()> {
                // Should have AlterSetDefault step
                let alter_step = steps
                    .iter()
                    .find(|s| {
                        matches!(
                            s,
                            MigrationStep::Domain(DomainOperation::AlterSetDefault { .. })
                        )
                    })
                    .expect("Should have AlterSetDefault step");

                match alter_step {
                    MigrationStep::Domain(DomainOperation::AlterSetDefault {
                        schema,
                        name,
                        default,
                    }) => {
                        assert_eq!(schema, "test_schema");
                        assert_eq!(name, "status");
                        assert!(default.contains("pending"));
                    }
                    _ => panic!("Expected AlterSetDefault step"),
                }

                // Verify final state
                assert_eq!(final_catalog.domains.len(), 1);
                assert!(final_catalog.domains[0].default.is_some());

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_domain_drop_default_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: has domain with default
            &[
                "CREATE SCHEMA test_schema",
                "CREATE DOMAIN test_schema.status AS TEXT DEFAULT 'active'",
            ],
            // Initial DB: nothing extra
            &[],
            // Target DB: remove default
            &["ALTER DOMAIN test_schema.status DROP DEFAULT"],
            |steps, final_catalog| -> Result<()> {
                // Should have AlterDropDefault step
                let alter_step = steps
                    .iter()
                    .find(|s| {
                        matches!(
                            s,
                            MigrationStep::Domain(DomainOperation::AlterDropDefault { .. })
                        )
                    })
                    .expect("Should have AlterDropDefault step");

                match alter_step {
                    MigrationStep::Domain(DomainOperation::AlterDropDefault { schema, name }) => {
                        assert_eq!(schema, "test_schema");
                        assert_eq!(name, "status");
                    }
                    _ => panic!("Expected AlterDropDefault step"),
                }

                // Verify final state
                assert_eq!(final_catalog.domains.len(), 1);
                assert!(final_catalog.domains[0].default.is_none());

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_domain_add_constraint_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: has domain without constraints
            &[
                "CREATE SCHEMA test_schema",
                "CREATE DOMAIN test_schema.positive_int AS INTEGER",
            ],
            // Initial DB: nothing extra
            &[],
            // Target DB: add constraint
            &["ALTER DOMAIN test_schema.positive_int ADD CONSTRAINT positive_check CHECK (VALUE > 0)"],
            |steps, final_catalog| -> Result<()> {
                // Should have AddConstraint step
                let add_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Domain(DomainOperation::AddConstraint { .. }))
                    })
                    .expect("Should have AddConstraint step");

                match add_step {
                    MigrationStep::Domain(DomainOperation::AddConstraint {
                        schema,
                        name,
                        constraint_name,
                        expression,
                    }) => {
                        assert_eq!(schema, "test_schema");
                        assert_eq!(name, "positive_int");
                        assert_eq!(constraint_name, "positive_check");
                        assert!(expression.contains("VALUE > 0"));
                    }
                    _ => panic!("Expected AddConstraint step"),
                }

                // Verify final state
                assert_eq!(final_catalog.domains.len(), 1);
                assert_eq!(final_catalog.domains[0].check_constraints.len(), 1);
                assert_eq!(
                    final_catalog.domains[0].check_constraints[0].name,
                    "positive_check"
                );

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_domain_drop_constraint_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: has domain with constraint
            &[
                "CREATE SCHEMA test_schema",
                "CREATE DOMAIN test_schema.positive_int AS INTEGER",
                "ALTER DOMAIN test_schema.positive_int ADD CONSTRAINT positive_check CHECK (VALUE > 0)",
            ],
            // Initial DB: nothing extra
            &[],
            // Target DB: drop constraint
            &["ALTER DOMAIN test_schema.positive_int DROP CONSTRAINT positive_check"],
            |steps, final_catalog| -> Result<()> {
                // Should have DropConstraint step
                let drop_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Domain(DomainOperation::DropConstraint { .. }))
                    })
                    .expect("Should have DropConstraint step");

                match drop_step {
                    MigrationStep::Domain(DomainOperation::DropConstraint {
                        schema,
                        name,
                        constraint_name,
                    }) => {
                        assert_eq!(schema, "test_schema");
                        assert_eq!(name, "positive_int");
                        assert_eq!(constraint_name, "positive_check");
                    }
                    _ => panic!("Expected DropConstraint step"),
                }

                // Verify final state
                assert_eq!(final_catalog.domains.len(), 1);
                assert_eq!(final_catalog.domains[0].check_constraints.len(), 0);

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_domain_change_constraint_expression_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    // Changing a CHECK constraint's expression (keeping the same name) must
    // DROP the old constraint before re-ADDing it. Both are ALTER steps on the
    // same domain id, so the ordering pass has to keep the drop ahead of the
    // add — otherwise applying the migration fails with "constraint ... already
    // exists". Regression test: the full pipeline applies the steps to a fresh
    // DB, so a flipped order panics during apply, before verification runs.
    let steps = helper
        .run_migration_test(
            // Both DBs: domain with the original constraint
            &[
                "CREATE SCHEMA test_schema",
                "CREATE DOMAIN test_schema.numbersonly AS TEXT CHECK (VALUE ~ '^\\d+$')",
            ],
            // Initial DB: nothing extra
            &[],
            // Target DB: same constraint name, different expression
            &[
                "ALTER DOMAIN test_schema.numbersonly DROP CONSTRAINT numbersonly_check",
                "ALTER DOMAIN test_schema.numbersonly ADD CONSTRAINT numbersonly_check CHECK (VALUE ~ '^[\\d+\\.]$')",
            ],
            |steps, final_catalog| -> Result<()> {
                // The DROP CONSTRAINT must be ordered before the matching ADD.
                let drop_pos = steps
                    .iter()
                    .position(|s| {
                        matches!(s, MigrationStep::Domain(DomainOperation::DropConstraint { constraint_name, .. })
                            if constraint_name == "numbersonly_check")
                    })
                    .expect("Should have DropConstraint step");
                let add_pos = steps
                    .iter()
                    .position(|s| {
                        matches!(s, MigrationStep::Domain(DomainOperation::AddConstraint { constraint_name, .. })
                            if constraint_name == "numbersonly_check")
                    })
                    .expect("Should have AddConstraint step");

                assert!(
                    drop_pos < add_pos,
                    "DROP CONSTRAINT (pos {drop_pos}) must come before ADD CONSTRAINT (pos {add_pos})"
                );

                // Final state reflects the new expression.
                assert_eq!(final_catalog.domains.len(), 1);
                let constraints = &final_catalog.domains[0].check_constraints;
                assert_eq!(constraints.len(), 1);
                assert_eq!(constraints[0].name, "numbersonly_check");
                assert!(constraints[0].expression.contains("[\\d+\\.]"));

                Ok(())
            },
        )
        .await?;

    // Exactly the two constraint ALTERs, nothing spurious.
    let constraint_steps = steps
        .iter()
        .filter(|s| {
            matches!(
                s,
                MigrationStep::Domain(
                    DomainOperation::DropConstraint { .. } | DomainOperation::AddConstraint { .. }
                )
            )
        })
        .count();
    assert_eq!(constraint_steps, 2);

    Ok(())
}

#[tokio::test]
async fn test_domain_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: has domain without comment
            &[
                "CREATE SCHEMA test_schema",
                "CREATE DOMAIN test_schema.email AS TEXT CHECK (VALUE ~ '@')",
            ],
            // Initial DB: nothing extra
            &[],
            // Target DB: add comment
            &["COMMENT ON DOMAIN test_schema.email IS 'Email address format'"],
            |steps, final_catalog| -> Result<()> {
                // Should have SetComment step
                let comment_step = steps
                    .iter()
                    .find(|s| matches!(s, MigrationStep::Comment(CommentOperation::Set { .. })))
                    .expect("Should have SetComment step");

                match comment_step {
                    MigrationStep::Comment(CommentOperation::Set { target, comment }) => {
                        assert_eq!(target.schema(), "test_schema");
                        assert_eq!(target.name(), "email");
                        assert_eq!(comment, "Email address format");
                    }
                    _ => panic!("Expected SetComment step"),
                }

                // Verify final state
                assert_eq!(final_catalog.domains.len(), 1);
                assert_eq!(
                    final_catalog.domains[0].comment,
                    Some("Email address format".to_string())
                );

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_domain_drop_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: has domain with comment
            &[
                "CREATE SCHEMA test_schema",
                "CREATE DOMAIN test_schema.email AS TEXT",
                "COMMENT ON DOMAIN test_schema.email IS 'Email address'",
            ],
            // Initial DB: nothing extra
            &[],
            // Target DB: remove comment
            &["COMMENT ON DOMAIN test_schema.email IS NULL"],
            |steps, final_catalog| -> Result<()> {
                // Should have DropComment step
                let comment_step = steps
                    .iter()
                    .find(|s| matches!(s, MigrationStep::Comment(CommentOperation::Drop { .. })))
                    .expect("Should have DropComment step");

                match comment_step {
                    MigrationStep::Comment(CommentOperation::Drop { target }) => {
                        assert_eq!(target.schema(), "test_schema");
                        assert_eq!(target.name(), "email");
                    }
                    _ => panic!("Expected DropComment step"),
                }

                // Verify final state
                assert_eq!(final_catalog.domains.len(), 1);
                assert!(final_catalog.domains[0].comment.is_none());

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_domain_base_type_change_drop_recreate() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: just schema
            &["CREATE SCHEMA test_schema"],
            // Initial DB: domain with TEXT base type
            &["CREATE DOMAIN test_schema.user_id AS TEXT"],
            // Target DB: domain with INTEGER base type
            &["CREATE DOMAIN test_schema.user_id AS INTEGER"],
            |steps, final_catalog| -> Result<()> {
                // Should have Drop then Create steps since base type changed
                let drop_step = steps.iter().find(|s| {
                    matches!(s, MigrationStep::Domain(DomainOperation::Drop { name, .. })
                        if name == "user_id")
                });
                let create_step = steps.iter().find(|s| {
                    matches!(s, MigrationStep::Domain(DomainOperation::Create { name, .. })
                        if name == "user_id")
                });

                assert!(drop_step.is_some(), "Should have Drop step");
                assert!(create_step.is_some(), "Should have Create step");

                // Verify final state
                assert_eq!(final_catalog.domains.len(), 1);
                assert_eq!(final_catalog.domains[0].base_type, "integer");

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_domain_with_array_base_type() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: just schema
            &["CREATE SCHEMA test_schema"],
            // Initial DB: nothing
            &[],
            // Target DB: domain with array base type
            &["CREATE DOMAIN test_schema.int_list AS INTEGER[] NOT NULL CHECK (cardinality(VALUE) > 0)"],
            |steps, final_catalog| -> Result<()> {
                // Should have Create step
                let create_step = steps.iter().find(|s| {
                    matches!(s, MigrationStep::Domain(DomainOperation::Create { name, .. })
                        if name == "int_list")
                }).expect("Should have Create step");

                match create_step {
                    MigrationStep::Domain(DomainOperation::Create { definition, .. }) => {
                        // Should have proper array notation
                        assert!(definition.contains("integer[]"));
                        assert!(definition.contains("NOT NULL"));
                        assert!(definition.contains("CHECK"));
                    }
                    _ => panic!("Expected Create step"),
                }

                // Verify final state
                assert_eq!(final_catalog.domains.len(), 1);
                assert_eq!(final_catalog.domains[0].base_type, "integer[]");
                assert!(final_catalog.domains[0].not_null);
                assert_eq!(final_catalog.domains[0].check_constraints.len(), 1);

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_domain_with_custom_type_dependency() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: schema and custom type
            &[
                "CREATE SCHEMA test_schema",
                "CREATE TYPE test_schema.priority AS ENUM ('low', 'medium', 'high')",
            ],
            // Initial DB: nothing extra
            &[],
            // Target DB: domain using custom type
            &["CREATE DOMAIN test_schema.required_priority AS test_schema.priority NOT NULL DEFAULT 'medium'"],
            |steps, final_catalog| -> Result<()> {
                // Should have Create step
                let create_step = steps.iter().find(|s| {
                    matches!(s, MigrationStep::Domain(DomainOperation::Create { name, .. })
                        if name == "required_priority")
                }).expect("Should have Create step");

                match create_step {
                    MigrationStep::Domain(DomainOperation::Create { definition, .. }) => {
                        assert!(definition.contains("priority"));
                        assert!(definition.contains("NOT NULL"));
                        assert!(definition.contains("DEFAULT"));
                    }
                    _ => panic!("Expected Create step"),
                }

                // Verify final state
                assert_eq!(final_catalog.domains.len(), 1);
                let domain = &final_catalog.domains[0];
                assert_eq!(domain.name, "required_priority");
                assert!(domain.not_null);

                // Should have dependency on the type
                use pgmt::catalog::id::DbObjectId;
                assert!(domain.depends_on.contains(&DbObjectId::Type {
                    schema: "test_schema".to_string(),
                    name: "priority".to_string(),
                }));

                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_domain_multiple_changes_in_one_migration() {
    with_test_db(async |source_db| {
        with_test_db(async |target_db| {
            // Source: domain with all features
            source_db.execute("CREATE DOMAIN test_domain AS INTEGER NOT NULL DEFAULT 1").await;
            source_db.execute("ALTER DOMAIN test_domain ADD CONSTRAINT positive CHECK (VALUE > 0)").await;
            source_db.execute("COMMENT ON DOMAIN test_domain IS 'Test domain'").await;

            // Target: different configuration
            target_db.execute("CREATE DOMAIN test_domain AS INTEGER DEFAULT 100").await;
            target_db.execute("ALTER DOMAIN test_domain ADD CONSTRAINT range_check CHECK (VALUE BETWEEN 0 AND 1000)").await;

            let source_domains = fetch(&mut *source_db.conn().await).await.unwrap();
            let target_domains = fetch(&mut *target_db.conn().await).await.unwrap();

            let steps = diff(target_domains.first(), source_domains.first());

            // Should have multiple structural steps (comments are emitted by the
            // central comment pass in the full pipeline, not by `domains::diff`):
            // - AlterSetNotNull (adding NOT NULL)
            // - AlterSetDefault (changing default from 100 to 1)
            // - DropConstraint (removing range_check)
            // - AddConstraint (adding positive)

            assert!(steps.iter().any(|s| matches!(s, MigrationStep::Domain(DomainOperation::AlterSetNotNull { .. }))));
            assert!(steps.iter().any(|s| matches!(s, MigrationStep::Domain(DomainOperation::AlterSetDefault { .. }))));
            assert!(steps.iter().any(|s| matches!(s, MigrationStep::Domain(DomainOperation::DropConstraint { constraint_name, .. }) if constraint_name == "range_check")));
            assert!(steps.iter().any(|s| matches!(s, MigrationStep::Domain(DomainOperation::AddConstraint { constraint_name, .. }) if constraint_name == "positive")));
        })
        .await
    })
    .await;
}

#[tokio::test]
async fn test_domain_sql_rendering() {
    // Test that the SQL rendering is correct
    let create_op = DomainOperation::Create {
        schema: "app".to_string(),
        name: "positive_int".to_string(),
        definition: "AS integer NOT NULL CHECK (VALUE > 0)".to_string(),
    };
    let sql = create_op.to_sql();
    assert_eq!(sql.len(), 1);
    assert!(sql[0].sql.contains("CREATE DOMAIN"));
    assert!(sql[0].sql.contains("\"app\".\"positive_int\""));
    assert_eq!(sql[0].safety, Safety::Safe);

    let drop_op = DomainOperation::Drop {
        schema: "app".to_string(),
        name: "positive_int".to_string(),
    };
    let sql = drop_op.to_sql();
    assert_eq!(sql.len(), 1);
    assert!(sql[0].sql.contains("DROP DOMAIN"));
    // Domains can be recreated from schema, so DROP is not destructive
    assert_eq!(sql[0].safety, Safety::Safe);

    let set_not_null = DomainOperation::AlterSetNotNull {
        schema: "app".to_string(),
        name: "user_id".to_string(),
    };
    let sql = set_not_null.to_sql();
    assert!(sql[0].sql.contains("SET NOT NULL"));

    let drop_not_null = DomainOperation::AlterDropNotNull {
        schema: "app".to_string(),
        name: "user_id".to_string(),
    };
    let sql = drop_not_null.to_sql();
    assert!(sql[0].sql.contains("DROP NOT NULL"));

    let set_default = DomainOperation::AlterSetDefault {
        schema: "app".to_string(),
        name: "status".to_string(),
        default: "'active'".to_string(),
    };
    let sql = set_default.to_sql();
    assert!(sql[0].sql.contains("SET DEFAULT"));

    let drop_default = DomainOperation::AlterDropDefault {
        schema: "app".to_string(),
        name: "status".to_string(),
    };
    let sql = drop_default.to_sql();
    assert!(sql[0].sql.contains("DROP DEFAULT"));

    let add_constraint = DomainOperation::AddConstraint {
        schema: "app".to_string(),
        name: "positive_int".to_string(),
        constraint_name: "positive".to_string(),
        expression: "CHECK (VALUE > 0)".to_string(),
    };
    let sql = add_constraint.to_sql();
    assert!(sql[0].sql.contains("ADD CONSTRAINT"));
    assert!(sql[0].sql.contains("\"positive\""));

    let drop_constraint = DomainOperation::DropConstraint {
        schema: "app".to_string(),
        name: "positive_int".to_string(),
        constraint_name: "positive".to_string(),
    };
    let sql = drop_constraint.to_sql();
    assert!(sql[0].sql.contains("DROP CONSTRAINT"));
}

#[tokio::test]
async fn test_domain_cascade_on_base_type_change() -> Result<()> {
    with_test_db(async |initial_db| {
        with_test_db(async |target_db| {
            // Initial: composite type + domain based on it
            initial_db
                .execute("CREATE TYPE address AS (street TEXT, city TEXT)")
                .await;
            initial_db
                .execute("CREATE DOMAIN mailing_address AS address")
                .await;

            // Target: composite type gains a field
            target_db
                .execute("CREATE TYPE address AS (street TEXT, city TEXT, zip TEXT)")
                .await;
            target_db
                .execute("CREATE DOMAIN mailing_address AS address")
                .await;

            let initial_catalog = Catalog::load_unfiltered(initial_db.pool()).await?;
            let target_catalog = Catalog::load_unfiltered(target_db.pool()).await?;

            let steps = plan(&initial_catalog, &target_catalog)?;

            // Should have DROP + CREATE for domain (cascade) and type
            let drop_domain_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::Domain(DomainOperation::Drop { name, .. })
                        if name == "mailing_address")
                })
                .expect("Should have DROP DOMAIN for mailing_address");

            let create_domain_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::Domain(DomainOperation::Create { name, .. })
                        if name == "mailing_address")
                })
                .expect("Should have CREATE DOMAIN for mailing_address");

            let drop_type_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::Type(TypeOperation::Drop { name, .. })
                        if name == "address")
                })
                .expect("Should have DROP TYPE for address");

            let create_type_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::Type(TypeOperation::Create { name, .. })
                        if name == "address")
                })
                .expect("Should have CREATE TYPE for address");

            // Domain must be dropped before type, and created after type
            assert!(
                drop_domain_pos < drop_type_pos,
                "DROP DOMAIN should come before DROP TYPE"
            );
            assert!(
                create_type_pos < create_domain_pos,
                "CREATE TYPE should come before CREATE DOMAIN"
            );

            Ok(())
        })
        .await
    })
    .await
}
