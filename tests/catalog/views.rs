use crate::helpers::harness::with_test_db;
use pgmt::catalog::id::{DbObjectId, DependsOn};
use pgmt::catalog::view::fetch;

#[tokio::test]
async fn test_fetch_basic_view() {
    with_test_db(async |db| {
        db.execute(
            "CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT
            )",
        )
        .await;

        db.execute(
            "CREATE VIEW active_users AS
             SELECT id, name, email FROM users WHERE email IS NOT NULL",
        )
        .await;

        let views = fetch(db.pool()).await.unwrap();

        assert_eq!(views.len(), 1);
        let view = &views[0];

        assert_eq!(view.schema, "public");
        assert_eq!(view.name, "active_users");
        assert!(!view.definition.is_empty());
        assert_eq!(view.columns.len(), 3);

        assert_eq!(view.columns[0].name, "id");
        assert_eq!(view.columns[0].type_, Some("integer".to_string()));

        assert_eq!(view.columns[1].name, "name");
        assert_eq!(view.columns[1].type_, Some("text".to_string()));

        assert_eq!(view.columns[2].name, "email");
        assert_eq!(view.columns[2].type_, Some("text".to_string()));

        assert_eq!(view.depends_on().len(), 1);
        assert!(view.depends_on().contains(&DbObjectId::Table {
            schema: "public".to_string(),
            name: "users".to_string()
        }));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_view_with_custom_types() {
    with_test_db(async |db| {
        db.execute("CREATE TYPE status AS ENUM ('active', 'inactive')")
            .await;

        db.execute(
            "CREATE TABLE accounts (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                status status NOT NULL
            )",
        )
        .await;

        db.execute(
            "CREATE VIEW active_accounts AS
             SELECT id, name FROM accounts WHERE status = 'active'",
        )
        .await;

        let views = fetch(db.pool()).await.unwrap();

        assert_eq!(views.len(), 1);
        let view = &views[0];

        assert_eq!(view.schema, "public");
        assert_eq!(view.name, "active_accounts");

        assert!(view.depends_on().contains(&DbObjectId::Table {
            schema: "public".to_string(),
            name: "accounts".to_string()
        }));
        assert!(view.depends_on().contains(&DbObjectId::Type {
            schema: "public".to_string(),
            name: "status".to_string()
        }));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_view_with_function_dependency() {
    with_test_db(async |db| {
        db.execute(
            "CREATE OR REPLACE FUNCTION upper_name(input TEXT)
             RETURNS TEXT AS $$
             BEGIN
                 RETURN UPPER(input);
             END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        db.execute(
            "CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL
            )",
        )
        .await;

        db.execute(
            "CREATE VIEW users_upper AS
             SELECT id, upper_name(name) as name FROM users",
        )
        .await;

        let views = fetch(db.pool()).await.unwrap();

        assert_eq!(views.len(), 1);
        let view = &views[0];

        assert_eq!(view.schema, "public");
        assert_eq!(view.name, "users_upper");

        assert!(view.depends_on().contains(&DbObjectId::Table {
            schema: "public".to_string(),
            name: "users".to_string()
        }));
        assert!(view.depends_on().contains(&DbObjectId::Function {
            schema: "public".to_string(),
            name: "upper_name".to_string()
        }));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_nested_views() {
    with_test_db(async |db| {
        db.execute(
            "CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                age INTEGER
            )",
        )
        .await;

        db.execute(
            "CREATE VIEW adult_users AS
             SELECT id, name FROM users WHERE age >= 18",
        )
        .await;

        db.execute(
            "CREATE VIEW adult_users_summary AS
             SELECT COUNT(*) as total FROM adult_users",
        )
        .await;

        let mut views = fetch(db.pool()).await.unwrap();
        views.sort_by(|a, b| a.name.cmp(&b.name));

        assert_eq!(views.len(), 2);

        let adult_users = &views[0];
        assert_eq!(adult_users.name, "adult_users");
        assert!(adult_users.depends_on().contains(&DbObjectId::Table {
            schema: "public".to_string(),
            name: "users".to_string()
        }));

        let summary = &views[1];
        assert_eq!(summary.name, "adult_users_summary");
        assert!(summary.depends_on().contains(&DbObjectId::View {
            schema: "public".to_string(),
            name: "adult_users".to_string()
        }));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_views_different_schemas() {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA app").await;
        db.execute("CREATE SCHEMA reporting").await;

        db.execute(
            "CREATE TABLE public.users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL
            )",
        )
        .await;

        db.execute(
            "CREATE TABLE app.products (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                price DECIMAL(10,2)
            )",
        )
        .await;

        db.execute(
            "CREATE VIEW public.user_list AS
             SELECT name FROM public.users",
        )
        .await;

        db.execute(
            "CREATE VIEW reporting.product_summary AS
             SELECT COUNT(*) as total, AVG(price) as avg_price FROM app.products",
        )
        .await;

        let mut views = fetch(db.pool()).await.unwrap();
        views.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));

        assert_eq!(views.len(), 2);

        let user_list = views
            .iter()
            .find(|v| v.schema == "public" && v.name == "user_list")
            .unwrap();
        assert!(user_list.depends_on().contains(&DbObjectId::Table {
            schema: "public".to_string(),
            name: "users".to_string()
        }));

        let product_summary = views
            .iter()
            .find(|v| v.schema == "reporting" && v.name == "product_summary")
            .unwrap();
        assert!(product_summary.depends_on().contains(&DbObjectId::Table {
            schema: "app".to_string(),
            name: "products".to_string()
        }));
    })
    .await;
}

#[tokio::test]
async fn test_view_id_and_dependencies() {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA test_schema").await;
        db.execute(
            "CREATE TABLE test_schema.test_table (
                id SERIAL PRIMARY KEY,
                name TEXT
            )",
        )
        .await;

        db.execute(
            "CREATE VIEW test_schema.test_view AS
             SELECT * FROM test_schema.test_table",
        )
        .await;

        let views = fetch(db.pool()).await.unwrap();

        assert_eq!(views.len(), 1);
        let view = &views[0];

        // Test view.id() method
        assert_eq!(
            view.id(),
            DbObjectId::View {
                schema: "test_schema".to_string(),
                name: "test_view".to_string()
            }
        );

        // Test view.depends_on method
        let deps = view.depends_on();
        assert_eq!(deps.len(), 2);

        // Should depend on both the table and the schema
        assert!(deps.contains(&DbObjectId::Table {
            schema: "test_schema".to_string(),
            name: "test_table".to_string()
        }));
        assert!(deps.contains(&DbObjectId::Schema {
            name: "test_schema".to_string()
        }));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_view_no_dependencies() {
    with_test_db(async |db| {
        // Create a view with no table dependencies (uses only constants)
        db.execute(
            "CREATE VIEW constants AS
             SELECT 1 as one, 'hello' as greeting, NOW() as current_time",
        )
        .await;

        let views = fetch(db.pool()).await.unwrap();

        assert_eq!(views.len(), 1);
        let view = &views[0];

        assert_eq!(view.schema, "public");
        assert_eq!(view.name, "constants");
        assert_eq!(view.columns.len(), 3);

        // Should have no dependencies on tables/types/functions
        assert_eq!(view.depends_on().len(), 0);
    })
    .await;
}

#[tokio::test]
async fn test_fetch_view_with_comment() {
    with_test_db(async |db| {
        // Create a table and view with comment
        db.execute(
            "CREATE TABLE products (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                price DECIMAL(10,2)
            )",
        )
        .await;

        db.execute(
            "CREATE VIEW expensive_products AS
             SELECT id, name, price FROM products WHERE price > 100.00",
        )
        .await;

        db.execute("COMMENT ON VIEW expensive_products IS 'View showing products over $100'")
            .await;

        let views = fetch(db.pool()).await.unwrap();

        assert_eq!(views.len(), 1);
        let view = &views[0];

        assert_eq!(view.schema, "public");
        assert_eq!(view.name, "expensive_products");
        assert_eq!(
            view.comment,
            Some("View showing products over $100".to_string())
        );
    })
    .await;
}

#[tokio::test]
async fn test_fetch_view_with_custom_type_array_dependency() {
    with_test_db(async |db| {
        // Create custom type
        db.execute("CREATE TYPE priority AS ENUM ('low', 'medium', 'high')")
            .await;

        // Create table with array of custom type
        db.execute(
            "CREATE TABLE tasks (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                priorities priority[] NOT NULL
            )",
        )
        .await;

        // Create view that directly references the custom type in a WHERE clause
        // This forces PostgreSQL to create a direct dependency on the enum type
        db.execute(
            "CREATE VIEW task_summary AS
             SELECT id, title, priorities FROM tasks WHERE 'high'::priority = ANY(priorities)",
        )
        .await;

        let views = fetch(db.pool()).await.unwrap();

        assert_eq!(views.len(), 1);
        let view = &views[0];

        assert_eq!(view.schema, "public");
        assert_eq!(view.name, "task_summary");

        // The bug: should depend on "priority", not "_priority"
        // By using 'high'::priority in the WHERE clause, we force a direct dependency
        assert!(view.depends_on().contains(&DbObjectId::Type {
            schema: "public".to_string(),
            name: "priority".to_string() // Should be "priority", not "_priority"
        }));

        // Should also depend on the table
        assert!(view.depends_on().contains(&DbObjectId::Table {
            schema: "public".to_string(),
            name: "tasks".to_string()
        }));

        // Verify we don't have the incorrect "_priority" dependency
        assert!(!view.depends_on().contains(&DbObjectId::Type {
            schema: "public".to_string(),
            name: "_priority".to_string() // This should NOT be present
        }));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_view_with_array_type_cast_dependency() {
    with_test_db(async |db| {
        // Create custom type
        db.execute("CREATE TYPE status AS ENUM ('active', 'inactive', 'pending')")
            .await;

        // Create table with array column
        db.execute(
            "CREATE TABLE items (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                statuses status[] NOT NULL
            )",
        )
        .await;

        // Create view that casts an array value - this should create dependency on the base type
        db.execute(
            "CREATE VIEW active_items AS
             SELECT id, name FROM items WHERE ARRAY['active']::status[] <@ statuses",
        )
        .await;

        let views = fetch(db.pool()).await.unwrap();

        assert_eq!(views.len(), 1);
        let view = &views[0];

        assert_eq!(view.schema, "public");
        assert_eq!(view.name, "active_items");

        // Should depend on "status", not "_status" (the array type name)
        assert!(view.depends_on().contains(&DbObjectId::Type {
            schema: "public".to_string(),
            name: "status".to_string() // Should be "status", not "_status"
        }));

        // Should also depend on the table
        assert!(view.depends_on().contains(&DbObjectId::Table {
            schema: "public".to_string(),
            name: "items".to_string()
        }));

        // Verify we don't have the incorrect "_status" dependency
        assert!(!view.depends_on().contains(&DbObjectId::Type {
            schema: "public".to_string(),
            name: "_status".to_string() // This should NOT be present
        }));
    })
    .await;
}
