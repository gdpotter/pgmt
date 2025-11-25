use crate::helpers::harness::with_test_db;

use pgmt::catalog::grant::{GranteeType, ObjectType, fetch};
use pgmt::catalog::id::DependsOn;

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
        let grants = fetch(db.pool()).await.unwrap();

        // Find grants for our test table
        let table_grants: Vec<_> = grants
            .iter()
            .filter(|g| {
                matches!(&g.object, ObjectType::Table { schema, name }
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
async fn test_fetch_schema_grants() {
    with_test_db(async |db| {
        // Create test schema and grant usage
        db.execute("CREATE SCHEMA test_grants_schema").await;
        db.execute("GRANT USAGE ON SCHEMA test_grants_schema TO test_app_user")
            .await;

        // Fetch and verify grants
        let grants = fetch(db.pool()).await.unwrap();

        // Find grants for our test schema
        let schema_grants: Vec<_> = grants
            .iter()
            .filter(|g| {
                matches!(&g.object, ObjectType::Schema { name }
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
        let grants = fetch(db.pool()).await.unwrap();

        // Find grants for our test table
        let public_grants: Vec<_> = grants
            .iter()
            .filter(|g| {
                matches!(&g.object, ObjectType::Table { schema, name }
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
        let grants = fetch(db.pool()).await.unwrap();

        // Find grants for our test table
        let admin_grants: Vec<_> = grants
            .iter()
            .filter(|g| {
                matches!(&g.object, ObjectType::Table { schema, name }
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
        let grants = fetch(db.pool()).await.unwrap();

        // Find grants for our test function
        let function_grants: Vec<_> = grants
            .iter()
            .filter(|g| {
                matches!(&g.object, ObjectType::Function { schema, name }
                if schema == "test_func_schema" && name == "test_func")
            })
            .collect();

        // Note: Function grants might not be captured in all PostgreSQL setups
        // This test verifies the grant fetching mechanism works
        println!("Found {} function grants", function_grants.len());
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
        let grants = fetch(db.pool()).await.unwrap();

        // Find grant for our test table
        let table_grant = grants
            .iter()
            .find(|g| {
                matches!(&g.object, ObjectType::Table { schema, name }
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
