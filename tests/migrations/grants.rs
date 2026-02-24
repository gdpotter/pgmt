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
                    matches!(g.object, pgmt::catalog::grant::ObjectType::Table { ref schema, ref name } if schema == "public" && name == "users")
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
                    matches!(g.object, pgmt::catalog::grant::ObjectType::Table { ref schema, ref name } if schema == "public" && name == "users")
                });

            assert!(!grant_exists, "Grant should be revoked");

            Ok(())
        }
    ).await?;

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
                    matches!(g.object, pgmt::catalog::grant::ObjectType::Table { ref schema, ref name } if schema == "public" && name == "users")
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
                    matches!(g.object, pgmt::catalog::grant::ObjectType::Schema { ref name } if name == "test_schema")
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
                    matches!(g.object, pgmt::catalog::grant::ObjectType::View { ref schema, ref name } if schema == "public" && name == "active_users")
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
                        && matches!(&grant.object, pgmt::catalog::grant::ObjectType::Function { name, .. } if name == "test_func"))
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
                        && matches!(&g.object, pgmt::catalog::grant::ObjectType::Function { name, .. } if name == "test_func")
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
                    &g.object,
                    pgmt::catalog::grant::ObjectType::Function { schema, .. }
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
                    &g.object,
                    pgmt::catalog::grant::ObjectType::Function { schema, name, .. }
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
                    &g.object,
                    pgmt::catalog::grant::ObjectType::Function { schema, .. }
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
                    &g.object,
                    pgmt::catalog::grant::ObjectType::Function { schema, name, .. }
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
