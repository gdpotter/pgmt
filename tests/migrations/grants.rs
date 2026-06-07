//! Grant migration tests

use crate::helpers::harness::with_test_db;
use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::catalog::Catalog;
use pgmt::config::filter::ObjectFilter;
use pgmt::config::types::{ObjectExclude, ObjectInclude, Objects, TrackingTable};
use pgmt::diff::operations::{GrantOperation, MigrationStep, SqlRenderer};

#[tokio::test]
async fn test_grant_table_privilege_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: table
        &["CREATE TABLE users (id SERIAL, name VARCHAR)"],
        // Initial DB only: nothing extra (no grants)
        &[],
        // Target DB only: add grant
        &["GRANT SELECT, INSERT ON TABLE users TO test_app_user"],
        // Verification closure
        |steps, final_catalog| {
            // Verify migration steps
            assert!(!steps.is_empty());
            let _grant_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::Grant(GrantOperation::Grant { grant })
                    if grant.grantee == pgmt::catalog::grant::GranteeType::Role("test_app_user".to_string()))
            }).expect("Should have Grant step");

            // Verify final state
            let grant = final_catalog.grants.iter()
                .find(|g| {
                    matches!(g.grantee, pgmt::catalog::grant::GranteeType::Role(ref name) if name == "test_app_user") &&
                    matches!(g.target.object, pgmt::catalog::id::DbObjectId::Table { ref schema, ref name } if schema == "public" && name == "users")
                })
                .expect("Grant should be created");

            assert!(grant.privileges.contains(&"SELECT".to_string()));
            assert!(grant.privileges.contains(&"INSERT".to_string()));
            assert!(!grant.with_grant_option);

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_grant_procedure_privilege_migration() -> Result<()> {
    // Procedures are a distinct DbObjectId variant; a grant on one must render
    // `ON PROCEDURE` (not `ON FUNCTION`). Applying to a fresh DB also fails at
    // apply time if the wrong keyword is emitted.
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &[
                "CREATE SCHEMA test_schema",
                "CREATE PROCEDURE test_schema.do_thing(x integer) LANGUAGE plpgsql AS $$ BEGIN END $$",
            ],
            // Initial DB only: no grant
            &[],
            // Target DB only: grant EXECUTE on the procedure
            &["GRANT EXECUTE ON PROCEDURE test_schema.do_thing(integer) TO test_app_user"],
            |steps, final_catalog| {
                use pgmt::catalog::grant::GranteeType;
                use pgmt::catalog::id::DbObjectId;

                // The grant step for test_app_user must render the PROCEDURE keyword.
                let grant_step = steps
                    .iter()
                    .find(|s| {
                        matches!(s, MigrationStep::Grant(GrantOperation::Grant { grant })
                            if matches!(&grant.grantee, GranteeType::Role(n) if n == "test_app_user")
                                && matches!(&grant.target.object, DbObjectId::Procedure { .. }))
                    })
                    .expect("Should have a procedure grant step for test_app_user");

                let sql = grant_step.to_sql();
                assert!(
                    sql.iter().any(|r| r.sql.contains("ON PROCEDURE")),
                    "expected GRANT ... ON PROCEDURE, got: {:?}",
                    sql.iter().map(|r| &r.sql).collect::<Vec<_>>()
                );

                // Round-trip: the grant exists on the procedure after applying.
                let exists = final_catalog.grants.iter().any(|g| {
                    matches!(&g.grantee, GranteeType::Role(n) if n == "test_app_user")
                        && matches!(&g.target.object, DbObjectId::Procedure { name, .. } if name == "do_thing")
                });
                assert!(exists, "procedure grant should exist after migration");

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_grant_column_privilege_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: table
            &["CREATE TABLE users (id SERIAL, email TEXT, ssn TEXT)"],
            // Initial DB only: no grants
            &[],
            // Target DB only: column-level grants
            &[
                "GRANT SELECT (email), UPDATE (email) ON users TO test_app_user",
                "GRANT SELECT (ssn) ON users TO test_read_only",
            ],
            |steps, final_catalog| {
                use pgmt::catalog::grant::GranteeType;

                // Column grants are folded into one GrantColumns op per relation;
                // email's SELECT and UPDATE land in the same op.
                let has_column_grant_step = steps.iter().any(|s| {
                    matches!(s, MigrationStep::Grant(GrantOperation::GrantColumns(cg))
                        if matches!(&cg.grantee, GranteeType::Role(n) if n == "test_app_user")
                            && cg.privilege_columns.get("SELECT").is_some_and(|cols| cols.contains("email"))
                            && cg.privilege_columns.get("UPDATE").is_some_and(|cols| cols.contains("email")))
                });
                assert!(has_column_grant_step, "Should have a folded column GRANT step");

                // Round-trip: the migration was applied to a fresh DB, so the
                // re-fetched catalog must reflect the column grant.
                let email_grant = final_catalog
                    .grants
                    .iter()
                    .find(|g| {
                        matches!(&g.grantee, GranteeType::Role(n) if n == "test_app_user")
                            && g.target.column_name() == Some("email")
                            && g.target.schema_and_name()
                                == ("public".to_string(), "users".to_string())
                    })
                    .expect("email column grant should exist after migration");
                assert!(email_grant.privileges.contains(&"SELECT".to_string()));
                assert!(email_grant.privileges.contains(&"UPDATE".to_string()));

                let ssn_grant_exists = final_catalog.grants.iter().any(|g| {
                    matches!(&g.grantee, GranteeType::Role(n) if n == "test_read_only")
                        && g.target.column_name() == Some("ssn")
                });
                assert!(
                    ssn_grant_exists,
                    "ssn column grant should exist after migration"
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_revoke_column_privilege_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: table
            &["CREATE TABLE users (id SERIAL, email TEXT)"],
            // Initial DB only: has column grant
            &["GRANT SELECT (email) ON users TO test_app_user"],
            // Target DB only: nothing (grant removed)
            &[],
            |steps, final_catalog| {
                use pgmt::catalog::grant::GranteeType;

                let has_revoke_step = steps.iter().any(|s| {
                    matches!(s, MigrationStep::Grant(GrantOperation::RevokeColumns(cg))
                        if cg.privilege_columns.get("SELECT").is_some_and(|cols| cols.contains("email")))
                });
                assert!(has_revoke_step, "Should have a folded column REVOKE step");

                let grant_exists = final_catalog.grants.iter().any(|g| {
                    matches!(&g.grantee, GranteeType::Role(n) if n == "test_app_user")
                        && g.target.column_name() == Some("email")
                });
                assert!(
                    !grant_exists,
                    "column grant should be revoked after migration"
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_revoke_table_privilege_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: table
        &["CREATE TABLE users (id SERIAL, name VARCHAR)"],
        // Initial DB only: has grant
        &["GRANT SELECT ON TABLE users TO test_app_user"],
        // Target DB only: nothing extra (grant missing)
        &[],
        // Verification closure
        |steps, final_catalog| {
            // Verify migration steps
            assert!(!steps.is_empty());
            let _revoke_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::Grant(GrantOperation::Revoke { grant })
                    if grant.grantee == pgmt::catalog::grant::GranteeType::Role("test_app_user".to_string()))
            }).expect("Should have Revoke step");

            // Verify final state
            let grant_exists = final_catalog.grants.iter()
                .any(|g| {
                    matches!(g.grantee, pgmt::catalog::grant::GranteeType::Role(ref name) if name == "test_app_user") &&
                    matches!(g.target.object, pgmt::catalog::id::DbObjectId::Table { ref schema, ref name } if schema == "public" && name == "users")
                });

            assert!(!grant_exists, "Grant should be revoked");

            Ok(())
        }
    ).await?;

    Ok(())
}

/// Adding privileges to an existing grant should emit only the added privileges
/// as a single GRANT — no REVOKE of the privileges that already existed.
/// Going from `SELECT, INSERT` to `SELECT, INSERT, UPDATE` emits just
/// `GRANT UPDATE` rather than revoking and re-granting everything.
#[tokio::test]
async fn test_grant_privilege_superset_migration() -> Result<()> {
    use pgmt::catalog::grant::GranteeType;

    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: table
            &["CREATE TABLE users (id SERIAL, name VARCHAR)"],
            // Initial DB only: a subset of the privileges
            &["GRANT SELECT, INSERT ON TABLE users TO test_app_user"],
            // Target DB only: the same privileges plus UPDATE
            &["GRANT SELECT, INSERT, UPDATE ON TABLE users TO test_app_user"],
            |steps, final_catalog| {
                let is_app_user = |g: &pgmt::catalog::grant::Grant| {
                    matches!(&g.grantee, GranteeType::Role(n) if n == "test_app_user")
                };

                // No REVOKE should be generated — the existing privileges are untouched.
                let revoke_steps: Vec<_> = steps
                    .iter()
                    .filter(|s| matches!(s, MigrationStep::Grant(GrantOperation::Revoke { grant }) if is_app_user(grant)))
                    .collect();
                assert!(
                    revoke_steps.is_empty(),
                    "Adding a privilege must not revoke existing ones, got: {revoke_steps:?}"
                );

                // Exactly one GRANT, carrying only the newly added UPDATE privilege.
                let grant_steps: Vec<_> = steps
                    .iter()
                    .filter_map(|s| match s {
                        MigrationStep::Grant(GrantOperation::Grant { grant }) if is_app_user(grant) => Some(grant),
                        _ => None,
                    })
                    .collect();
                assert_eq!(grant_steps.len(), 1, "Expected a single GRANT step");
                assert_eq!(
                    grant_steps[0].privileges,
                    vec!["UPDATE".to_string()],
                    "GRANT should carry only the added privilege"
                );

                // Round-trip: final state has all three privileges.
                let grant = final_catalog
                    .grants
                    .iter()
                    .find(|g| is_app_user(g)
                        && matches!(&g.target.object, pgmt::catalog::id::DbObjectId::Table { name, .. } if name == "users"))
                    .expect("grant should exist after migration");
                for priv_name in ["SELECT", "INSERT", "UPDATE"] {
                    assert!(
                        grant.privileges.contains(&priv_name.to_string()),
                        "expected {priv_name} after migration, got {:?}",
                        grant.privileges
                    );
                }

                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// When privileges both gain and lose members, emit only the delta: one REVOKE
/// for the dropped privilege and one GRANT for the added one, leaving the
/// overlapping privilege untouched. `SELECT, INSERT` -> `SELECT, UPDATE` emits
/// `REVOKE INSERT` + `GRANT UPDATE` and never touches SELECT.
#[tokio::test]
async fn test_grant_privilege_delta_migration() -> Result<()> {
    use pgmt::catalog::grant::GranteeType;

    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE TABLE users (id SERIAL, name VARCHAR)"],
            &["GRANT SELECT, INSERT ON TABLE users TO test_app_user"],
            &["GRANT SELECT, UPDATE ON TABLE users TO test_app_user"],
            |steps, final_catalog| {
                let is_app_user = |g: &pgmt::catalog::grant::Grant| {
                    matches!(&g.grantee, GranteeType::Role(n) if n == "test_app_user")
                };

                let revoke_steps: Vec<_> = steps
                    .iter()
                    .filter_map(|s| match s {
                        MigrationStep::Grant(GrantOperation::Revoke { grant }) if is_app_user(grant) => Some(grant),
                        _ => None,
                    })
                    .collect();
                let grant_steps: Vec<_> = steps
                    .iter()
                    .filter_map(|s| match s {
                        MigrationStep::Grant(GrantOperation::Grant { grant }) if is_app_user(grant) => Some(grant),
                        _ => None,
                    })
                    .collect();

                assert_eq!(revoke_steps.len(), 1, "Expected a single REVOKE step");
                assert_eq!(revoke_steps[0].privileges, vec!["INSERT".to_string()]);

                assert_eq!(grant_steps.len(), 1, "Expected a single GRANT step");
                assert_eq!(grant_steps[0].privileges, vec!["UPDATE".to_string()]);

                // SELECT (present in both states) must not appear in any step.
                assert!(
                    !revoke_steps[0].privileges.contains(&"SELECT".to_string())
                        && !grant_steps[0].privileges.contains(&"SELECT".to_string()),
                    "the overlapping SELECT privilege must be left untouched"
                );

                // Round-trip: final state is exactly {SELECT, UPDATE}.
                let grant = final_catalog
                    .grants
                    .iter()
                    .find(|g| is_app_user(g)
                        && matches!(&g.target.object, pgmt::catalog::id::DbObjectId::Table { name, .. } if name == "users"))
                    .expect("grant should exist after migration");
                assert!(grant.privileges.contains(&"SELECT".to_string()));
                assert!(grant.privileges.contains(&"UPDATE".to_string()));
                assert!(
                    !grant.privileges.contains(&"INSERT".to_string()),
                    "INSERT should have been revoked"
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_grant_with_grant_option_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: table
        &["CREATE TABLE users (id SERIAL, name VARCHAR)"],
        // Initial DB only: nothing extra (no grants)
        &[],
        // Target DB only: add grant with grant option
        &["GRANT UPDATE ON TABLE users TO test_admin_user WITH GRANT OPTION"],
        // Verification closure
        |steps, final_catalog| {
            // Verify migration steps
            assert!(!steps.is_empty());

            // Check that the generated SQL contains WITH GRANT OPTION
            let grant_step_found = steps.iter().any(|step| {
                if let MigrationStep::Grant(GrantOperation::Grant { .. }) = step {
                    let sql_list = step.to_sql();
                    sql_list.iter().any(|rendered| {
                        rendered.sql.contains("GRANT UPDATE") && rendered.sql.contains("WITH GRANT OPTION")
                    })
                } else {
                    false
                }
            });
            assert!(grant_step_found, "Should have Grant step with WITH GRANT OPTION");

            // Verify final state
            let grant = final_catalog.grants.iter()
                .find(|g| {
                    matches!(g.grantee, pgmt::catalog::grant::GranteeType::Role(ref name) if name == "test_admin_user") &&
                    matches!(g.target.object, pgmt::catalog::id::DbObjectId::Table { ref schema, ref name } if schema == "public" && name == "users")
                })
                .expect("Grant should be created");

            assert!(grant.privileges.contains(&"UPDATE".to_string()));
            assert!(grant.with_grant_option);

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_public_grant_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: schema
        &["CREATE SCHEMA test_schema"],
        // Initial DB only: nothing extra (no grants)
        &[],
        // Target DB only: add public grant
        &["GRANT USAGE ON SCHEMA test_schema TO PUBLIC"],
        // Verification closure
        |steps, final_catalog| {
            // Verify migration steps
            assert!(!steps.is_empty());

            // Check that the generated SQL grants to PUBLIC
            let public_grant_step_found = steps.iter().any(|step| {
                if let MigrationStep::Grant(GrantOperation::Grant { .. }) = step {
                    let sql_list = step.to_sql();
                    sql_list.iter().any(|rendered| {
                        rendered.sql.contains("GRANT USAGE") && rendered.sql.contains("TO PUBLIC")
                    })
                } else {
                    false
                }
            });
            assert!(public_grant_step_found, "Should have Grant step to PUBLIC");

            // Verify final state
            let grant = final_catalog.grants.iter()
                .find(|g| {
                    matches!(g.grantee, pgmt::catalog::grant::GranteeType::Public) &&
                    matches!(g.target.object, pgmt::catalog::id::DbObjectId::Schema { ref name } if name == "test_schema")
                })
                .expect("Public grant should be created");

            assert!(grant.privileges.contains(&"USAGE".to_string()));

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_grant_view_privilege_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: table and view
        &[
            "CREATE TABLE users (id SERIAL, name VARCHAR)",
            "CREATE VIEW active_users AS SELECT * FROM users WHERE name IS NOT NULL"
        ],
        // Initial DB only: nothing extra (no grants)
        &[],
        // Target DB only: add grant on view
        &["GRANT SELECT ON active_users TO test_app_user"],
        // Verification closure
        |steps, final_catalog| {
            // Verify migration steps
            assert!(!steps.is_empty());
            let grant_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::Grant(GrantOperation::Grant { grant })
                    if grant.grantee == pgmt::catalog::grant::GranteeType::Role("test_app_user".to_string()))
            }).expect("Should have Grant step");

            // Check the generated SQL - should NOT contain "ON VIEW" or "ON TABLE"
            let sql_list = grant_step.to_sql();
            let grant_sql = &sql_list[0].sql;

            // Should be: GRANT SELECT ON "public"."active_users" TO test_app_user;
            // Should NOT be: GRANT SELECT ON VIEW "public"."active_users" TO test_app_user;
            // Should NOT be: GRANT SELECT ON TABLE "public"."active_users" TO test_app_user;
            assert!(!grant_sql.contains("ON VIEW"), "Grant on view should not contain 'ON VIEW'");
            assert!(!grant_sql.contains("ON TABLE"), "Grant on view should not contain 'ON TABLE'");
            assert!(grant_sql.contains("ON \"public\".\"active_users\""), "Should reference the view directly");

            // Verify final state
            let grant = final_catalog.grants.iter()
                .find(|g| {
                    matches!(g.grantee, pgmt::catalog::grant::GranteeType::Role(ref name) if name == "test_app_user") &&
                    matches!(g.target.object, pgmt::catalog::id::DbObjectId::View { ref schema, ref name } if schema == "public" && name == "active_users")
                })
                .expect("Grant should be created");

            assert!(grant.privileges.contains(&"SELECT".to_string()));

            Ok(())
        }
    ).await?;

    Ok(())
}

/// Test that migrating from default privileges to revoked defaults generates REVOKE
/// This is the key test for the default ACL handling fix:
/// - Initial DB: function with default PUBLIC EXECUTE (NULL ACL)
/// - Target DB: function with REVOKE PUBLIC EXECUTE (explicit ACL)
/// - Expected: Migration generates REVOKE EXECUTE ON FUNCTION ... FROM PUBLIC
#[tokio::test]
async fn test_revoke_default_public_execute_on_function() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: function
            &["CREATE FUNCTION test_func() RETURNS INT AS $$ SELECT 1; $$ LANGUAGE SQL"],
            // Initial DB only: nothing extra (uses default PUBLIC EXECUTE)
            &[],
            // Target DB only: revoke the default PUBLIC EXECUTE
            &["REVOKE EXECUTE ON FUNCTION test_func() FROM PUBLIC"],
            // Verification closure
            |steps, final_catalog| {
                // Should have a REVOKE step for PUBLIC EXECUTE
                let revoke_step = steps.iter().find(|s| {
                    matches!(s, MigrationStep::Grant(GrantOperation::Revoke { grant })
                    if matches!(&grant.grantee, pgmt::catalog::grant::GranteeType::Public)
                        && matches!(&grant.target.object, pgmt::catalog::id::DbObjectId::Function { name, .. } if name == "test_func"))
                });

                assert!(
                    revoke_step.is_some(),
                    "Should have REVOKE step for PUBLIC EXECUTE on function. Steps: {:?}",
                    steps
                );

                // Verify the REVOKE SQL is correct
                if let Some(step) = revoke_step {
                    let sql_list = step.to_sql();
                    let revoke_sql = &sql_list[0].sql;
                    assert!(
                        revoke_sql.contains("REVOKE")
                            && revoke_sql.contains("EXECUTE")
                            && revoke_sql.contains("PUBLIC"),
                        "Should generate correct REVOKE SQL: {}",
                        revoke_sql
                    );
                }

                // Verify final state: no PUBLIC grant on the function
                let public_grant_exists = final_catalog.grants.iter().any(|g| {
                    matches!(&g.grantee, pgmt::catalog::grant::GranteeType::Public)
                        && matches!(&g.target.object, pgmt::catalog::id::DbObjectId::Function { name, .. } if name == "test_func")
                });

                assert!(
                    !public_grant_exists,
                    "PUBLIC should not have EXECUTE on test_func after migration"
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Test that grants in excluded schemas are filtered from the catalog.
/// This verifies the fix for the issue where schema exclusion wasn't applied to grants.
#[tokio::test]
async fn test_grants_filtered_by_excluded_schema() {
    with_test_db(async |db| {
        // Create an excluded schema with a function
        db.execute("CREATE SCHEMA excluded_schema").await;
        db.execute(
            "CREATE FUNCTION excluded_schema.notify_func() RETURNS VOID AS $$ SELECT 1; $$ LANGUAGE SQL",
        )
        .await;
        // By default, PUBLIC has EXECUTE on functions - this creates the grant

        // Also create a function in public schema for comparison
        db.execute("CREATE FUNCTION public.my_func() RETURNS VOID AS $$ SELECT 1; $$ LANGUAGE SQL")
            .await;

        // Load the catalog
        let catalog = Catalog::load(db.pool()).await.unwrap();

        // Verify grants exist in the unfiltered catalog for both schemas
        let excluded_grants_before = catalog
            .grants
            .iter()
            .filter(|g| {
                matches!(
                    &g.target.object,
                    pgmt::catalog::id::DbObjectId::Function { schema, .. }
                    if schema == "excluded_schema"
                )
            })
            .count();
        assert!(
            excluded_grants_before > 0,
            "Should have grants in excluded_schema before filtering"
        );

        let public_grants_before = catalog
            .grants
            .iter()
            .filter(|g| {
                matches!(
                    &g.target.object,
                    pgmt::catalog::id::DbObjectId::Function { schema, name, .. }
                    if schema == "public" && name == "my_func"
                )
            })
            .count();
        assert!(
            public_grants_before > 0,
            "Should have grants on public.my_func before filtering"
        );

        // Create filter that excludes the schema
        let filter = ObjectFilter::new(
            &Objects {
                include: ObjectInclude {
                    schemas: vec![],
                    tables: vec![],
                },
                exclude: ObjectExclude {
                    schemas: vec!["excluded_schema".to_string()],
                    tables: vec![],
                },
            },
            &TrackingTable {
                schema: "public".to_string(),
                name: "pgmt_migrations".to_string(),
            },
        );

        // Apply filter
        let filtered_catalog = filter.filter_catalog(catalog);

        // Verify grants in excluded schema are filtered out
        let excluded_grants_after = filtered_catalog
            .grants
            .iter()
            .filter(|g| {
                matches!(
                    &g.target.object,
                    pgmt::catalog::id::DbObjectId::Function { schema, .. }
                    if schema == "excluded_schema"
                )
            })
            .count();
        assert_eq!(
            excluded_grants_after, 0,
            "Should NOT have grants in excluded_schema after filtering"
        );

        // Verify grants in public schema are still present
        let public_grants_after = filtered_catalog
            .grants
            .iter()
            .filter(|g| {
                matches!(
                    &g.target.object,
                    pgmt::catalog::id::DbObjectId::Function { schema, name, .. }
                    if schema == "public" && name == "my_func"
                )
            })
            .count();
        assert!(
            public_grants_after > 0,
            "Should still have grants on public.my_func after filtering"
        );
    })
    .await;
}

/// Test that no spurious REVOKE is generated when both catalogs have the same explicit ACL.
/// This tests the fix for the bug where diff_grants() would generate REVOKEs for objects
/// that already had explicit ACL in both old and new catalogs.
#[tokio::test]
async fn test_no_spurious_revoke_when_both_have_explicit_acl() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Both DBs: function with explicit REVOKE (same state)
            &[
                "CREATE FUNCTION test_func() RETURNS INT AS $$ SELECT 1; $$ LANGUAGE SQL",
                "REVOKE EXECUTE ON FUNCTION test_func() FROM PUBLIC",
            ],
            // Initial DB only: nothing extra
            &[],
            // Target DB only: nothing extra (same as initial)
            &[],
            |steps, _final_catalog| {
                // Should have NO grant-related steps since both have same explicit ACL
                let grant_steps: Vec<_> = steps
                    .iter()
                    .filter(|s| matches!(s, MigrationStep::Grant(_)))
                    .collect();

                assert!(
                    grant_steps.is_empty(),
                    "Expected no grant steps when both catalogs have same explicit ACL, got {:?}",
                    grant_steps
                );
                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Column grants on the same relation fold into a single statement: many
/// `GRANT INSERT (col) ON t` become one `GRANT INSERT (a, b, c) ON t`, with
/// per-column privilege sets preserved. This is the headline win behind phase 2.
#[tokio::test]
async fn test_column_grants_folded_into_single_statement() -> Result<()> {
    use pgmt::catalog::grant::GranteeType;

    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE TABLE t (id SERIAL, a TEXT, b TEXT, c TEXT)"],
            &[],
            // a, b: INSERT+UPDATE; c: INSERT only — three column grants on one table.
            &[
                "GRANT INSERT (a), UPDATE (a) ON t TO test_app_user",
                "GRANT INSERT (b), UPDATE (b) ON t TO test_app_user",
                "GRANT INSERT (c) ON t TO test_app_user",
            ],
            |steps, final_catalog| {
                // Exactly one folded column-grant op for this grantee/relation.
                let folded: Vec<_> = steps
                    .iter()
                    .filter_map(|s| match s {
                        MigrationStep::Grant(GrantOperation::GrantColumns(cg))
                            if matches!(&cg.grantee, GranteeType::Role(n) if n == "test_app_user") =>
                        {
                            Some(cg)
                        }
                        _ => None,
                    })
                    .collect();
                assert_eq!(
                    folded.len(),
                    1,
                    "all column grants on one relation should fold into a single op, got {}",
                    folded.len()
                );

                let cg = folded[0];
                // INSERT covers a, b, c; UPDATE covers a, b.
                let insert: Vec<&String> = cg.privilege_columns["INSERT"].iter().collect();
                let update: Vec<&String> = cg.privilege_columns["UPDATE"].iter().collect();
                assert_eq!(insert, vec!["a", "b", "c"]);
                assert_eq!(update, vec!["a", "b"]);

                // It renders as a single GRANT statement listing the columns.
                let sql = &MigrationStep::Grant(GrantOperation::GrantColumns(cg.clone())).to_sql()[0].sql;
                assert!(sql.starts_with("GRANT "), "got: {sql}");
                assert_eq!(sql.matches("GRANT").count(), 1, "should be one statement: {sql}");
                assert!(sql.contains(r#"INSERT ("a", "b", "c")"#), "got: {sql}");
                assert!(sql.contains(r#"UPDATE ("a", "b")"#), "got: {sql}");

                // Round-trip: every column grant exists after applying.
                for (col, has_update) in [("a", true), ("b", true), ("c", false)] {
                    let g = final_catalog
                        .grants
                        .iter()
                        .find(|g| matches!(&g.grantee, GranteeType::Role(n) if n == "test_app_user")
                            && g.target.column_name() == Some(col))
                        .unwrap_or_else(|| panic!("column grant on {col} should exist"));
                    assert!(g.privileges.contains(&"INSERT".to_string()));
                    assert_eq!(g.privileges.contains(&"UPDATE".to_string()), has_update);
                }

                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// REVOKEs of column grants fold the same way GRANTs do.
#[tokio::test]
async fn test_column_revokes_folded_into_single_statement() -> Result<()> {
    use pgmt::catalog::grant::GranteeType;

    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE TABLE t (id SERIAL, a TEXT, b TEXT)"],
            // Initial DB has two column grants; target has none.
            &[
                "GRANT SELECT (a) ON t TO test_app_user",
                "GRANT SELECT (b) ON t TO test_app_user",
            ],
            &[],
            |steps, final_catalog| {
                let folded: Vec<_> = steps
                    .iter()
                    .filter_map(|s| match s {
                        MigrationStep::Grant(GrantOperation::RevokeColumns(cg))
                            if matches!(&cg.grantee, GranteeType::Role(n) if n == "test_app_user") =>
                        {
                            Some(cg)
                        }
                        _ => None,
                    })
                    .collect();
                assert_eq!(folded.len(), 1, "column revokes should fold into one op");

                let cols: Vec<&String> = folded[0].privilege_columns["SELECT"].iter().collect();
                assert_eq!(cols, vec!["a", "b"]);

                let sql = &MigrationStep::Grant(GrantOperation::RevokeColumns(folded[0].clone()))
                    .to_sql()[0]
                    .sql;
                assert!(sql.starts_with("REVOKE "), "got: {sql}");
                assert!(sql.contains(r#"SELECT ("a", "b")"#), "got: {sql}");

                // Round-trip: no column grants remain for this grantee.
                let any_left = final_catalog.grants.iter().any(|g| {
                    matches!(&g.grantee, GranteeType::Role(n) if n == "test_app_user")
                        && g.target.column_name().is_some()
                });
                assert!(!any_left, "column grants should be revoked");

                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// A folded column REVOKE must be ordered before the DROP of the table it sits
/// on. This exercises the `rep_id` ordering hook: the merged op borrows a
/// constituent grant's identity so the dependency edge to the relation resolves
/// in the drop direction (revoke -> relation), just like an unfolded revoke.
#[tokio::test]
async fn test_folded_column_revoke_ordered_before_table_drop() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            // Initial DB has the table with column grants; target drops the table.
            &[],
            &[
                "CREATE TABLE doomed (id SERIAL, a TEXT, b TEXT)",
                "GRANT SELECT (a) ON doomed TO test_app_user",
                "GRANT SELECT (b) ON doomed TO test_app_user",
            ],
            &[],
            |steps, _final_catalog| {
                use pgmt::diff::operations::TableOperation;

                let revoke_pos = steps.iter().position(|s| {
                    matches!(s, MigrationStep::Grant(GrantOperation::RevokeColumns(_)))
                });
                let drop_pos = steps.iter().position(|s| {
                    matches!(s, MigrationStep::Table(TableOperation::Drop { name, .. }) if name == "doomed")
                });

                let revoke_pos = revoke_pos.expect("expected a folded column REVOKE step");
                let drop_pos = drop_pos.expect("expected a DROP TABLE step");
                assert!(
                    revoke_pos < drop_pos,
                    "REVOKE (pos {revoke_pos}) must come before DROP TABLE (pos {drop_pos})"
                );

                Ok(())
            },
        )
        .await?;

    Ok(())
}
