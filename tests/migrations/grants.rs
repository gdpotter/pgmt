//! Grant migration tests

use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
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
            println!("Generated grant SQL: {}", grant_sql);

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
