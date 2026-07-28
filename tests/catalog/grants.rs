use crate::helpers::harness::with_test_db;

use pgmt::catalog::Catalog;
use pgmt::catalog::grant::GranteeType;
use pgmt::catalog::id::{DbObjectId, DependsOn};

#[tokio::test]
async fn test_fetch_table_grants() {
    with_test_db(async |db| {
        // Create test table and grant privileges
        db.execute("CREATE SCHEMA test_schema").await;
        db.execute("CREATE TABLE test_schema.users (id SERIAL, name VARCHAR)")
            .await;
        db.execute("GRANT SELECT, INSERT ON test_schema.users TO test_app_user")
            .await;
        db.execute("GRANT SELECT ON test_schema.users TO test_read_only")
            .await;

        // Fetch and verify grants
        let grants = Catalog::load_unfiltered(db.pool()).await.unwrap().grants;

        // Find grants for our test table
        let table_grants: Vec<_> = grants
            .iter()
            .filter(|g| {
                matches!(&g.target.object, DbObjectId::Table { schema, name }
                if schema == "test_schema" && name == "users")
            })
            .collect();

        assert!(table_grants.len() >= 2, "Should have at least 2 grants");

        // Check test_app_user grant
        let app_user_grant = table_grants
            .iter()
            .find(|g| matches!(&g.grantee, GranteeType::Role(name) if name == "test_app_user"))
            .expect("Should have grant for test_app_user");

        assert!(app_user_grant.privileges.contains(&"SELECT".to_string()));
        assert!(app_user_grant.privileges.contains(&"INSERT".to_string()));

        // Check test_read_only grant
        let read_only_grant = table_grants
            .iter()
            .find(|g| matches!(&g.grantee, GranteeType::Role(name) if name == "test_read_only"))
            .expect("Should have grant for test_read_only");

        assert!(read_only_grant.privileges.contains(&"SELECT".to_string()));
        assert!(!read_only_grant.privileges.contains(&"INSERT".to_string()));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_column_grants() {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA test_col").await;
        db.execute("CREATE TABLE test_col.users (id SERIAL, email TEXT, ssn TEXT)")
            .await;
        // Column-level grants: SELECT+UPDATE on email, SELECT on ssn.
        db.execute("GRANT SELECT (email), UPDATE (email) ON test_col.users TO test_app_user")
            .await;
        db.execute("GRANT SELECT (ssn) ON test_col.users TO test_read_only")
            .await;

        let grants = Catalog::load_unfiltered(db.pool()).await.unwrap().grants;

        let column_grants: Vec<_> = grants
            .iter()
            .filter(|g| {
                g.target.column_name().is_some()
                    && g.target.schema_and_name() == ("test_col".to_string(), "users".to_string())
            })
            .collect();

        // email/test_app_user and ssn/test_read_only
        let email_grant = column_grants
            .iter()
            .find(|g| {
                g.target.column_name() == Some("email")
                    && matches!(&g.grantee, GranteeType::Role(n) if n == "test_app_user")
            })
            .expect("Should have column grant on email for test_app_user");
        assert!(email_grant.privileges.contains(&"SELECT".to_string()));
        assert!(email_grant.privileges.contains(&"UPDATE".to_string()));

        let ssn_grant = column_grants
            .iter()
            .find(|g| {
                g.target.column_name() == Some("ssn")
                    && matches!(&g.grantee, GranteeType::Role(n) if n == "test_read_only")
            })
            .expect("Should have column grant on ssn for test_read_only");
        assert_eq!(ssn_grant.privileges, vec!["SELECT".to_string()]);

        // Column grant depends on its parent table for ordering.
        assert!(email_grant.depends_on().contains(&DbObjectId::Table {
            schema: "test_col".to_string(),
            name: "users".to_string(),
        }));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_schema_grants() {
    with_test_db(async |db| {
        // Create test schema and grant usage
        db.execute("CREATE SCHEMA test_grants_schema").await;
        db.execute("GRANT USAGE ON SCHEMA test_grants_schema TO test_app_user")
            .await;

        // Fetch and verify grants
        let grants = Catalog::load_unfiltered(db.pool()).await.unwrap().grants;

        // Find grants for our test schema
        let schema_grants: Vec<_> = grants
            .iter()
            .filter(|g| {
                matches!(&g.target.object, DbObjectId::Schema { name }
                if name == "test_grants_schema")
            })
            .collect();

        assert!(!schema_grants.is_empty(), "Should have schema grants");

        let usage_grant = schema_grants
            .iter()
            .find(|g| matches!(&g.grantee, GranteeType::Role(name) if name == "test_app_user"))
            .expect("Should have USAGE grant for test_app_user");

        assert!(usage_grant.privileges.contains(&"USAGE".to_string()));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_public_grants() {
    with_test_db(async |db| {
        // Create test table and grant to PUBLIC
        db.execute("CREATE SCHEMA test_public_schema").await;
        db.execute("CREATE TABLE test_public_schema.public_table (id SERIAL)")
            .await;
        db.execute("GRANT SELECT ON test_public_schema.public_table TO PUBLIC")
            .await;

        // Fetch and verify grants
        let grants = Catalog::load_unfiltered(db.pool()).await.unwrap().grants;

        // Find grants for our test table
        let public_grants: Vec<_> = grants
            .iter()
            .filter(|g| {
                matches!(&g.target.object, DbObjectId::Table { schema, name }
                if schema == "test_public_schema" && name == "public_table")
            })
            .filter(|g| matches!(&g.grantee, GranteeType::Public))
            .collect();

        assert!(!public_grants.is_empty(), "Should have PUBLIC grants");

        let public_grant = &public_grants[0];
        assert!(public_grant.privileges.contains(&"SELECT".to_string()));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_grant_with_grant_option() {
    with_test_db(async |db| {
        // Create test table and grant with GRANT OPTION
        db.execute("CREATE SCHEMA test_grant_option_schema").await;
        db.execute("CREATE TABLE test_grant_option_schema.admin_table (id SERIAL)")
            .await;
        db.execute(
            "GRANT SELECT ON test_grant_option_schema.admin_table TO test_admin_user WITH GRANT OPTION",
        )
        .await;

        // Fetch and verify grants
        let grants = Catalog::load_unfiltered(db.pool()).await.unwrap().grants;

        // Find grants for our test table
        let admin_grants: Vec<_> = grants
            .iter()
            .filter(|g| {
                matches!(&g.target.object, DbObjectId::Table { schema, name }
                if schema == "test_grant_option_schema" && name == "admin_table")
            })
            .filter(|g| matches!(&g.grantee, GranteeType::Role(name) if name == "test_admin_user"))
            .collect();

        assert!(!admin_grants.is_empty(), "Should have admin grants");

        let admin_grant = &admin_grants[0];
        assert!(admin_grant.privileges.contains(&"SELECT".to_string()));
        assert!(
            admin_grant.with_grant_option,
            "Should have WITH GRANT OPTION"
        );
    })
    .await;
}

#[tokio::test]
async fn test_fetch_function_grants() {
    with_test_db(async |db| {
        // Create test function and grant execute
        db.execute("CREATE SCHEMA test_func_schema").await;
        db.execute("CREATE FUNCTION test_func_schema.test_func() RETURNS INTEGER AS $$ BEGIN RETURN 42; END; $$ LANGUAGE plpgsql").await;
        db.execute("GRANT EXECUTE ON FUNCTION test_func_schema.test_func() TO test_app_user")
            .await;

        // Fetch and verify grants
        let grants = Catalog::load_unfiltered(db.pool()).await.unwrap().grants;

        // Find grants for our test function
        let function_grants: Vec<_> = grants
            .iter()
            .filter(|g| {
                matches!(&g.target.object, DbObjectId::Function { schema, name, .. }
                if schema == "test_func_schema" && name == "test_func")
            })
            .collect();

        assert!(!function_grants.is_empty(), "Should have function grants");
        let grant = &function_grants[0];
        assert!(grant.privileges.contains(&"EXECUTE".to_string()));
    })
    .await;
}

#[tokio::test]
async fn test_grant_dependencies() {
    with_test_db(async |db| {
        // Create test objects and grants
        db.execute("CREATE SCHEMA test_dep_schema").await;
        db.execute("CREATE TABLE test_dep_schema.dep_table (id SERIAL)")
            .await;
        db.execute("GRANT SELECT ON test_dep_schema.dep_table TO test_app_user")
            .await;

        // Fetch and verify grants
        let grants = Catalog::load_unfiltered(db.pool()).await.unwrap().grants;

        // Find grant for our test table
        let table_grant = grants
            .iter()
            .find(|g| {
                matches!(&g.target.object, DbObjectId::Table { schema, name }
                if schema == "test_dep_schema" && name == "dep_table")
            })
            .expect("Should have table grant");

        // Verify dependencies
        assert_eq!(
            table_grant.depends_on().len(),
            1,
            "Grant should depend on target object"
        );

        // The dependency should be on the table
        let table_dep = &table_grant.depends_on()[0];
        assert!(
            matches!(table_dep, pgmt::catalog::id::DbObjectId::Table { schema, name }
            if schema == "test_dep_schema" && name == "dep_table")
        );
    })
    .await;
}

/// Test that function grants with custom types have consistent argument formatting
/// with the function itself. This tests the search_path consistency fix.
#[tokio::test]
async fn test_function_grant_with_custom_type_arguments_match() {
    with_test_db(async |db| {
        // Create a custom enum type and a function using it
        db.execute("CREATE SCHEMA test_custom_type_schema").await;
        db.execute("CREATE TYPE test_custom_type_schema.status_enum AS ENUM ('active', 'inactive', 'pending')")
            .await;
        db.execute(
            "CREATE FUNCTION test_custom_type_schema.process_status(s test_custom_type_schema.status_enum)
             RETURNS TEXT AS $$ BEGIN RETURN s::text; END; $$ LANGUAGE plpgsql",
        )
        .await;
        db.execute(
            "GRANT EXECUTE ON FUNCTION test_custom_type_schema.process_status(test_custom_type_schema.status_enum) TO test_app_user",
        )
        .await;

        // Load the full catalog (which sets consistent search_path)
        let catalog = Catalog::load_unfiltered(db.pool()).await.unwrap();

        // Find the function
        let function = catalog
            .functions
            .iter()
            .find(|f| f.schema == "test_custom_type_schema" && f.name == "process_status")
            .expect("Should find process_status function");

        // Find grants for this function
        let function_grants: Vec<_> = catalog
            .grants
            .iter()
            .filter(|g| {
                matches!(&g.target.object, DbObjectId::Function { schema, name, .. }
                if schema == "test_custom_type_schema" && name == "process_status")
            })
            .collect();

        assert!(
            !function_grants.is_empty(),
            "Should have at least one grant for the function"
        );

        // The key assertion: Grant's arguments should match function's arguments
        // Before the fix, these could differ in schema qualification (e.g., "public.status_enum" vs "status_enum")
        for grant in function_grants {
            if let DbObjectId::Function { arguments, .. } = &grant.target.object {
                assert_eq!(
                    arguments, &function.arguments,
                    "Grant arguments '{}' should match function arguments '{}'",
                    arguments, function.arguments
                );

                // The grant's dependency should match the function's ID exactly
                let function_id = function.id();
                assert!(
                    grant.depends_on().contains(&function_id),
                    "Grant should depend on function with matching ID. Grant depends on {:?}, function ID is {:?}",
                    grant.depends_on(),
                    function_id
                );
            }
        }
    })
    .await;
}

/// Test that grants track whether they come from default ACL or explicit grants
/// A function created without explicit grants should have is_default_acl = true
/// A function with REVOKE PUBLIC should have is_default_acl = false
#[tokio::test]
async fn test_function_grant_is_default_acl() {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA test_default_acl_schema").await;

        // Create two functions:
        // 1. func_with_defaults - no explicit grants, uses PostgreSQL defaults (PUBLIC has EXECUTE)
        db.execute(
            "CREATE FUNCTION test_default_acl_schema.func_with_defaults()
             RETURNS INT AS $$ SELECT 1; $$ LANGUAGE SQL",
        )
        .await;

        // 2. func_revoked - explicitly revoke PUBLIC EXECUTE
        db.execute(
            "CREATE FUNCTION test_default_acl_schema.func_revoked()
             RETURNS INT AS $$ SELECT 2; $$ LANGUAGE SQL",
        )
        .await;
        db.execute("REVOKE EXECUTE ON FUNCTION test_default_acl_schema.func_revoked() FROM PUBLIC")
            .await;

        // Fetch grants
        let grants = Catalog::load_unfiltered(db.pool()).await.unwrap().grants;

        // Find PUBLIC grant for func_with_defaults - should have is_default_acl = true
        let default_grant = grants
            .iter()
            .find(|g| {
                matches!(&g.target.object, DbObjectId::Function { schema, name, .. }
                if schema == "test_default_acl_schema" && name == "func_with_defaults")
                    && matches!(&g.grantee, GranteeType::Public)
            })
            .expect("func_with_defaults should have PUBLIC EXECUTE grant from defaults");

        assert!(
            default_grant.is_default_acl,
            "Grant from default ACL should have is_default_acl = true"
        );

        // func_revoked should NOT have a PUBLIC grant (it was revoked)
        let revoked_public_grant = grants.iter().find(|g| {
            matches!(&g.target.object, DbObjectId::Function { schema, name, .. }
            if schema == "test_default_acl_schema" && name == "func_revoked")
                && matches!(&g.grantee, GranteeType::Public)
        });

        assert!(
            revoked_public_grant.is_none(),
            "func_revoked should not have PUBLIC grant after REVOKE"
        );

        // Also verify that any grants on func_revoked have is_default_acl = false
        // (because the ACL is no longer NULL - it's been explicitly modified)
        let revoked_grants: Vec<_> = grants
            .iter()
            .filter(|g| {
                matches!(&g.target.object, DbObjectId::Function { schema, name, .. }
                if schema == "test_default_acl_schema" && name == "func_revoked")
            })
            .collect();

        for grant in revoked_grants {
            assert!(
                !grant.is_default_acl,
                "Grants on object with explicit ACL should have is_default_acl = false"
            );
        }
    })
    .await;
}

/// Test that Catalog::contains_id works correctly for all object types
#[tokio::test]
async fn test_catalog_contains_id() {
    with_test_db(async |db| {
        // Create various objects
        db.execute("CREATE SCHEMA test_contains_schema").await;
        db.execute("CREATE TYPE test_contains_schema.my_enum AS ENUM ('a', 'b')").await;
        db.execute("CREATE TABLE test_contains_schema.my_table (id SERIAL)").await;
        db.execute("CREATE FUNCTION test_contains_schema.my_func() RETURNS INT AS $$ SELECT 1; $$ LANGUAGE SQL").await;

        let catalog = Catalog::load_unfiltered(db.pool()).await.unwrap();

        // Test contains_id for schema
        assert!(catalog.contains_id(&DbObjectId::Schema {
            name: "test_contains_schema".to_string()
        }));
        assert!(!catalog.contains_id(&DbObjectId::Schema {
            name: "nonexistent_schema".to_string()
        }));

        // Test contains_id for type
        assert!(catalog.contains_id(&DbObjectId::Type {
            schema: "test_contains_schema".to_string(),
            name: "my_enum".to_string()
        }));
        assert!(!catalog.contains_id(&DbObjectId::Type {
            schema: "test_contains_schema".to_string(),
            name: "nonexistent_type".to_string()
        }));

        // Test contains_id for table
        assert!(catalog.contains_id(&DbObjectId::Table {
            schema: "test_contains_schema".to_string(),
            name: "my_table".to_string()
        }));
        assert!(!catalog.contains_id(&DbObjectId::Table {
            schema: "test_contains_schema".to_string(),
            name: "nonexistent_table".to_string()
        }));

        // Test contains_id for function (need to match arguments exactly)
        // The function has no arguments, so arguments should be empty string
        assert!(catalog.contains_id(&DbObjectId::Function {
            schema: "test_contains_schema".to_string(),
            name: "my_func".to_string(),
            arguments: "".to_string()
        }));
        assert!(!catalog.contains_id(&DbObjectId::Function {
            schema: "test_contains_schema".to_string(),
            name: "my_func".to_string(),
            arguments: "integer".to_string()  // Wrong arguments
        }));
    })
    .await;
}

/// A user type whose name begins with an underscore is a user type, not an
/// array type. Grants on it were once dropped by a `typname NOT LIKE '\_%'`
/// filter that took the leading underscore of PostgreSQL's array-type naming
/// for a rule; arrays are now simply absent from the catalog, so no name
/// pattern is consulted and `_internal_status` keeps its privileges.
#[tokio::test]
async fn test_grants_on_underscore_named_type_are_tracked() {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA test_underscore_type").await;
        db.execute("CREATE TYPE test_underscore_type._internal_status AS ENUM ('new', 'done')")
            .await;
        db.execute("GRANT USAGE ON TYPE test_underscore_type._internal_status TO test_app_user")
            .await;

        let grants = Catalog::load_unfiltered(db.pool()).await.unwrap().grants;

        let usage = grants
            .iter()
            .find(|g| {
                matches!(&g.target.object, DbObjectId::Type { schema, name }
                if schema == "test_underscore_type" && name == "_internal_status")
                    && matches!(&g.grantee, GranteeType::Role(n) if n == "test_app_user")
            })
            .expect("underscore-named type should keep its USAGE grant");
        assert!(usage.privileges.contains(&"USAGE".to_string()));

        // The array type PostgreSQL created alongside it is not an object pgmt
        // manages, so nothing carries its privileges either.
        assert!(
            !grants.iter().any(|g| {
                matches!(&g.target.object, DbObjectId::Type { name, .. }
                if name == "__internal_status")
            }),
            "the array type should not appear in the catalog's grants"
        );
    })
    .await;
}

/// Range types are catalog objects like any other user type, and their
/// privileges are tracked. Grant fetching once listed only enums, domains and
/// composites, so a range type's ACL was invisible and a `GRANT USAGE` on one
/// was silently lost.
#[tokio::test]
async fn test_grants_on_range_types_are_tracked() {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA test_range_type").await;
        db.execute("CREATE TYPE test_range_type.int_span AS RANGE (subtype = int4)")
            .await;
        db.execute("GRANT USAGE ON TYPE test_range_type.int_span TO test_app_user")
            .await;

        let catalog = Catalog::load_unfiltered(db.pool()).await.unwrap();

        assert!(
            catalog
                .types
                .iter()
                .any(|t| t.schema == "test_range_type" && t.name == "int_span"),
            "the range type itself should be in the catalog"
        );

        let usage = catalog
            .grants
            .iter()
            .find(|g| {
                matches!(&g.target.object, DbObjectId::Type { schema, name }
                if schema == "test_range_type" && name == "int_span")
                    && matches!(&g.grantee, GranteeType::Role(n) if n == "test_app_user")
            })
            .expect("range type should keep its USAGE grant");
        assert!(usage.privileges.contains(&"USAGE".to_string()));
    })
    .await;
}

/// A relation's row type is not a type anyone grants on: it exists because the
/// relation does. Which `pg_type` rows are relation row types is decided by
/// `pg_class.reltype` pointing at them, so a table's row type is recognised as
/// one whatever it is called, and a standalone composite alongside it keeps its
/// own privileges.
#[tokio::test]
async fn test_relation_row_types_are_not_granted_on() {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA test_row_type").await;
        db.execute("CREATE TABLE test_row_type.thing (id INT)")
            .await;
        db.execute("CREATE TYPE test_row_type.point2d AS (x INT, y INT)")
            .await;
        db.execute("GRANT USAGE ON TYPE test_row_type.point2d TO test_app_user")
            .await;
        db.execute("GRANT SELECT ON test_row_type.thing TO test_app_user")
            .await;

        let grants = Catalog::load_unfiltered(db.pool()).await.unwrap().grants;

        assert!(
            grants.iter().any(|g| {
                matches!(&g.target.object, DbObjectId::Type { schema, name }
                if schema == "test_row_type" && name == "point2d")
                    && matches!(&g.grantee, GranteeType::Role(n) if n == "test_app_user")
            }),
            "the standalone composite type should keep its USAGE grant"
        );
        assert!(
            grants.iter().any(|g| {
                matches!(&g.target.object, DbObjectId::Table { schema, name }
                if schema == "test_row_type" && name == "thing")
            }),
            "the table should keep its SELECT grant"
        );
        assert!(
            !grants.iter().any(|g| {
                matches!(&g.target.object, DbObjectId::Type { schema, name }
                if schema == "test_row_type" && name == "thing")
            }),
            "the table's row type must not appear as a grantable type"
        );
    })
    .await;
}
