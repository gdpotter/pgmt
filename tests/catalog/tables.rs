use crate::helpers::harness::with_test_db;
use pgmt::catalog::id::DependsOn;

use pgmt::catalog::id::DbObjectId;
use pgmt::catalog::table::fetch;

#[tokio::test]
async fn test_fetch_basic_table() {
    with_test_db(async |db| {
        db.execute(
            "CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT
        )",
        )
        .await;

        let tables = fetch(db.pool()).await.unwrap();

        assert_eq!(tables.len(), 1);
        let table = &tables[0];

        assert_eq!(table.schema, "public");
        assert_eq!(table.name, "users");
        assert_eq!(table.columns.len(), 3);

        assert_eq!(table.columns[0].name, "id");
        assert_eq!(table.columns[0].data_type, "integer");
        assert!(table.columns[0].not_null);
        assert!(table.columns[0].default.is_some());

        assert_eq!(table.columns[1].name, "name");
        assert_eq!(table.columns[1].data_type, "text");
        assert!(table.columns[1].not_null);

        assert_eq!(table.columns[2].name, "email");
        assert_eq!(table.columns[2].data_type, "text");
        assert!(!table.columns[2].not_null);

        assert!(table.primary_key.is_some());
        let pk = table.primary_key.as_ref().unwrap();
        assert_eq!(pk.name, "users_pkey");
        assert_eq!(pk.columns, vec!["id"]);

        // Table depends on schema and sequence (for SERIAL column)
        assert_eq!(table.depends_on().len(), 2);
        assert!(table.depends_on().contains(&DbObjectId::Schema {
            name: "public".to_string()
        }));
        assert!(table.depends_on().contains(&DbObjectId::Sequence {
            schema: "public".to_string(),
            name: "users_id_seq".to_string()
        }));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_table_without_primary_key() {
    with_test_db(async |db| {
        db.execute(
            "CREATE TABLE logs (
            timestamp TIMESTAMP NOT NULL,
            level TEXT NOT NULL,
            message TEXT
        )",
        )
        .await;

        let tables = fetch(db.pool()).await.unwrap();

        assert_eq!(tables.len(), 1);
        let table = &tables[0];

        assert_eq!(table.schema, "public");
        assert_eq!(table.name, "logs");
        assert_eq!(table.columns.len(), 3);
        assert!(table.primary_key.is_none());
    })
    .await;
}

#[tokio::test]
async fn test_fetch_compound_primary_key() {
    with_test_db(async |db| {
        db.execute(
            "CREATE TABLE order_items (
                order_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                PRIMARY KEY (order_id, product_id)
            )",
        )
        .await;

        let tables = fetch(db.pool()).await.unwrap();

        assert_eq!(tables.len(), 1);
        let table = &tables[0];

        assert_eq!(table.schema, "public");
        assert_eq!(table.name, "order_items");

        assert!(table.primary_key.is_some());
        let pk = table.primary_key.as_ref().unwrap();
        assert_eq!(pk.name, "order_items_pkey");
        assert_eq!(pk.columns, vec!["order_id", "product_id"]);
    })
    .await;
}

#[tokio::test]
async fn test_fetch_table_with_custom_types() {
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

        let tables = fetch(db.pool()).await.unwrap();

        assert_eq!(tables.len(), 1);
        let table = &tables[0];

        assert_eq!(table.schema, "public");
        assert_eq!(table.name, "accounts");

        let status_col = table.columns.iter().find(|c| c.name == "status").unwrap();
        assert_eq!(status_col.data_type, "\"public\".\"status\"");
        assert!(status_col.not_null);

        assert_eq!(table.depends_on().len(), 3);
        assert!(table.depends_on().contains(&DbObjectId::Schema {
            name: "public".to_string()
        }));
        assert!(table.depends_on().contains(&DbObjectId::Type {
            schema: "public".to_string(),
            name: "status".to_string()
        }));
        assert!(table.depends_on().contains(&DbObjectId::Sequence {
            schema: "public".to_string(),
            name: "accounts_id_seq".to_string()
        }));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_table_with_generated_column() {
    with_test_db(async |db| {
        db.execute(
            "CREATE TABLE products (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                discounted_price DECIMAL(10,2) GENERATED ALWAYS AS (price * 0.9) STORED
            )",
        )
        .await;

        let tables = fetch(db.pool()).await.unwrap();

        assert_eq!(tables.len(), 1);
        let table = &tables[0];

        assert_eq!(table.schema, "public");
        assert_eq!(table.name, "products");

        let gen_col = table
            .columns
            .iter()
            .find(|c| c.name == "discounted_price")
            .unwrap();
        assert_eq!(gen_col.data_type, "numeric(10,2)");
        assert!(gen_col.generated.is_some());
        assert!(gen_col.default.is_none()); // Generated columns don't have defaults
    })
    .await;
}

#[tokio::test]
async fn test_fetch_multiple_tables_different_schemas() {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA app").await;
        db.execute("CREATE SCHEMA audit").await;

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
            "CREATE TABLE audit.log_entries (
                id SERIAL PRIMARY KEY,
                action TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT NOW()
            )",
        )
        .await;

        let mut tables = fetch(db.pool()).await.unwrap();
        tables.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));

        assert_eq!(tables.len(), 3);

        let users_table = &tables
            .iter()
            .find(|t| t.schema == "public" && t.name == "users")
            .unwrap();
        assert_eq!(users_table.columns.len(), 2);
        assert!(users_table.primary_key.is_some());

        let products_table = &tables
            .iter()
            .find(|t| t.schema == "app" && t.name == "products")
            .unwrap();
        assert_eq!(products_table.columns.len(), 3);
        assert!(products_table.primary_key.is_some());
        assert_eq!(products_table.depends_on().len(), 2);
        assert!(products_table.depends_on().contains(&DbObjectId::Schema {
            name: "app".to_string()
        }));
        assert!(products_table.depends_on().contains(&DbObjectId::Sequence {
            schema: "app".to_string(),
            name: "products_id_seq".to_string()
        }));

        let log_table = &tables
            .iter()
            .find(|t| t.schema == "audit" && t.name == "log_entries")
            .unwrap();
        assert_eq!(log_table.columns.len(), 3);
        assert!(log_table.primary_key.is_some());
        assert_eq!(log_table.depends_on().len(), 2);
        assert!(log_table.depends_on().contains(&DbObjectId::Schema {
            name: "audit".to_string()
        }));
        assert!(log_table.depends_on().contains(&DbObjectId::Sequence {
            schema: "audit".to_string(),
            name: "log_entries_id_seq".to_string()
        }));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_table_with_function_dependency() {
    with_test_db(async |db| {
        db.execute(
            "CREATE OR REPLACE FUNCTION calculate_hash(input TEXT)
             RETURNS TEXT AS $$
             BEGIN
                 RETURN md5(input);
             END;
             $$ LANGUAGE plpgsql IMMUTABLE",
        )
        .await;

        db.execute(
            "CREATE TABLE documents (
                id SERIAL PRIMARY KEY,
                content TEXT NOT NULL,
                content_hash TEXT GENERATED ALWAYS AS (calculate_hash(content)) STORED
            )",
        )
        .await;

        let tables = fetch(db.pool()).await.unwrap();

        assert_eq!(tables.len(), 1);
        let table = &tables[0];

        assert_eq!(table.schema, "public");
        assert_eq!(table.name, "documents");

        assert!(table.depends_on().contains(&DbObjectId::Function {
            schema: "public".to_string(),
            name: "calculate_hash".to_string()
        }));

        let hash_col = table
            .columns
            .iter()
            .find(|c| c.name == "content_hash")
            .unwrap();
        assert!(hash_col.depends_on.contains(&DbObjectId::Function {
            schema: "public".to_string(),
            name: "calculate_hash".to_string()
        }));
    })
    .await;
}

#[tokio::test]
async fn test_table_id_and_dependencies() {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA test_schema").await;
        db.execute(
            "CREATE TABLE test_schema.test_table (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL
            )",
        )
        .await;

        let tables = fetch(db.pool()).await.unwrap();

        assert_eq!(tables.len(), 1);
        let table = &tables[0];

        assert_eq!(
            table.id(),
            DbObjectId::Table {
                schema: "test_schema".to_string(),
                name: "test_table".to_string()
            }
        );

        // Table depends on schema and sequence (for SERIAL column)
        let deps = table.depends_on();
        assert_eq!(deps.len(), 2);
        assert!(deps.contains(&DbObjectId::Schema {
            name: "test_schema".to_string()
        }));
        assert!(deps.contains(&DbObjectId::Sequence {
            schema: "test_schema".to_string(),
            name: "test_table_id_seq".to_string()
        }));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_table_with_serial_columns() {
    with_test_db(async |db| {
        db.execute(
            "CREATE TABLE products (
                id SERIAL PRIMARY KEY,
                big_id BIGSERIAL NOT NULL,
                small_id SMALLSERIAL NOT NULL,
                name TEXT NOT NULL
            )",
        )
        .await;

        let tables = fetch(db.pool()).await.unwrap();

        assert_eq!(tables.len(), 1);
        let table = &tables[0];

        assert_eq!(table.schema, "public");
        assert_eq!(table.name, "products");
        assert_eq!(table.columns.len(), 4);

        assert_eq!(table.columns[0].name, "id");
        assert_eq!(table.columns[0].data_type, "integer");
        assert!(table.columns[0].not_null);
        assert!(table.columns[0].default.is_some());

        // Verify SERIAL column depends on its underlying sequence
        let has_sequence_dep = table.columns[0].depends_on.iter().any(|dep| {
            matches!(dep, DbObjectId::Sequence { schema, name }
                if schema == "public" && name == "products_id_seq")
        });
        assert!(
            has_sequence_dep,
            "SERIAL column should depend on its sequence"
        );

        // Table should also depend on the sequence
        let table_has_sequence_dep = table.depends_on().iter().any(|dep| {
            matches!(dep, DbObjectId::Sequence { schema, name }
                if schema == "public" && name == "products_id_seq")
        });
        assert!(
            table_has_sequence_dep,
            "Table with SERIAL should depend on sequence"
        );

        assert_eq!(table.columns[1].name, "big_id");
        assert_eq!(table.columns[1].data_type, "bigint");
        assert!(table.columns[1].not_null);
        assert!(table.columns[1].default.is_some());

        assert_eq!(table.columns[2].name, "small_id");
        assert_eq!(table.columns[2].data_type, "smallint");
        assert!(table.columns[2].not_null);
        assert!(table.columns[2].default.is_some());

        assert_eq!(table.columns[3].name, "name");
        assert_eq!(table.columns[3].data_type, "text");
        assert!(table.columns[3].not_null);
    })
    .await;
}

#[tokio::test]
async fn test_fetch_table_with_array_columns() {
    with_test_db(async |db| {
        db.execute(
            "CREATE TABLE documents (
                id SERIAL PRIMARY KEY,
                tags TEXT[] NOT NULL,
                scores INTEGER[],
                matrix INTEGER[][],
                keywords VARCHAR(50)[]
            )",
        )
        .await;

        let tables = fetch(db.pool()).await.unwrap();

        assert_eq!(tables.len(), 1);
        let table = &tables[0];

        assert_eq!(table.schema, "public");
        assert_eq!(table.name, "documents");
        assert_eq!(table.columns.len(), 5);

        let tags_col = table.columns.iter().find(|c| c.name == "tags").unwrap();
        assert_eq!(tags_col.data_type, "text[]");
        assert!(tags_col.not_null);

        let scores_col = table.columns.iter().find(|c| c.name == "scores").unwrap();
        assert_eq!(scores_col.data_type, "integer[]");
        assert!(!scores_col.not_null);

        let matrix_col = table.columns.iter().find(|c| c.name == "matrix").unwrap();
        assert_eq!(matrix_col.data_type, "integer[]");
        assert!(!matrix_col.not_null);

        let keywords_col = table.columns.iter().find(|c| c.name == "keywords").unwrap();
        assert_eq!(keywords_col.data_type, "character varying(50)[]");
        assert!(!keywords_col.not_null);
    })
    .await;
}

#[tokio::test]
async fn test_fetch_empty_table() {
    with_test_db(async |db| {
        // PostgreSQL allows tables with no columns
        db.execute("CREATE TABLE empty_table ()").await;

        let tables = fetch(db.pool()).await.unwrap();

        assert_eq!(tables.len(), 1);
        let table = &tables[0];

        assert_eq!(table.schema, "public");
        assert_eq!(table.name, "empty_table");
        assert_eq!(table.columns.len(), 0);
        assert!(table.primary_key.is_none());

        assert_eq!(
            table.depends_on(),
            vec![DbObjectId::Schema {
                name: "public".to_string()
            }]
        );
    })
    .await;
}

#[tokio::test]
async fn test_fetch_table_with_custom_array_types() {
    with_test_db(async |db| {
        db.execute("CREATE TYPE priority AS ENUM ('low', 'medium', 'high')")
            .await;
        db.execute(
            "CREATE TABLE tasks (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                priorities priority[] NOT NULL
            )",
        )
        .await;

        let tables = fetch(db.pool()).await.unwrap();

        assert_eq!(tables.len(), 1);
        let table = &tables[0];

        assert_eq!(table.schema, "public");
        assert_eq!(table.name, "tasks");

        let priorities_col = table
            .columns
            .iter()
            .find(|c| c.name == "priorities")
            .unwrap();
        assert_eq!(priorities_col.data_type, "\"public\".\"priority\"[]");
        assert!(priorities_col.not_null);

        assert_eq!(table.depends_on().len(), 3);
        assert!(table.depends_on().contains(&DbObjectId::Schema {
            name: "public".to_string()
        }));
        assert!(table.depends_on().contains(&DbObjectId::Type {
            schema: "public".to_string(),
            name: "priority".to_string()
        }));
        assert!(table.depends_on().contains(&DbObjectId::Sequence {
            schema: "public".to_string(),
            name: "tasks_id_seq".to_string()
        }));
    })
    .await;
}

#[tokio::test]
async fn test_column_ordering() {
    with_test_db(async |db| {
        db.execute(
            "CREATE TABLE test_order (
                z_last TEXT,
                a_first INTEGER,
                m_middle BOOLEAN,
                b_second TIMESTAMP,
                y_almost_last DECIMAL(10,2)
            )",
        )
        .await;

        let tables = fetch(db.pool()).await.unwrap();

        assert_eq!(tables.len(), 1);
        let table = &tables[0];

        assert_eq!(table.schema, "public");
        assert_eq!(table.name, "test_order");
        assert_eq!(table.columns.len(), 5);

        // Columns are in definition order, not alphabetical
        assert_eq!(table.columns[0].name, "z_last");
        assert_eq!(table.columns[0].data_type, "text");

        assert_eq!(table.columns[1].name, "a_first");
        assert_eq!(table.columns[1].data_type, "integer");

        assert_eq!(table.columns[2].name, "m_middle");
        assert_eq!(table.columns[2].data_type, "boolean");

        assert_eq!(table.columns[3].name, "b_second");
        assert_eq!(table.columns[3].data_type, "timestamp without time zone");

        assert_eq!(table.columns[4].name, "y_almost_last");
        assert_eq!(table.columns[4].data_type, "numeric(10,2)");
    })
    .await;
}

#[tokio::test]
async fn test_fetch_table_with_comment() {
    with_test_db(async |db| {
        db.execute(
            "CREATE TABLE products (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                price DECIMAL(10,2)
            )",
        )
        .await;
        db.execute("COMMENT ON TABLE products IS 'Product catalog table'")
            .await;

        let tables = fetch(db.pool()).await.unwrap();

        assert_eq!(tables.len(), 1);
        let table = &tables[0];

        assert_eq!(table.schema, "public");
        assert_eq!(table.name, "products");
        assert_eq!(table.comment, Some("Product catalog table".to_string()));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_table_with_column_comments() {
    with_test_db(async |db| {
        db.execute(
            "CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )",
        )
        .await;
        db.execute("COMMENT ON COLUMN users.id IS 'Unique user identifier'")
            .await;
        db.execute("COMMENT ON COLUMN users.name IS 'Full name of the user'")
            .await;
        db.execute("COMMENT ON COLUMN users.email IS 'Email address for notifications'")
            .await;

        let tables = fetch(db.pool()).await.unwrap();

        assert_eq!(tables.len(), 1);
        let table = &tables[0];

        assert_eq!(table.schema, "public");
        assert_eq!(table.name, "users");

        assert_eq!(table.columns[0].name, "id");
        assert_eq!(
            table.columns[0].comment,
            Some("Unique user identifier".to_string())
        );

        assert_eq!(table.columns[1].name, "name");
        assert_eq!(
            table.columns[1].comment,
            Some("Full name of the user".to_string())
        );

        assert_eq!(table.columns[2].name, "email");
        assert_eq!(
            table.columns[2].comment,
            Some("Email address for notifications".to_string())
        );

        assert_eq!(table.columns[3].name, "created_at");
        assert_eq!(table.columns[3].comment, None);
    })
    .await;
}

#[tokio::test]
async fn test_fetch_table_and_column_comments() {
    with_test_db(async |db| {
        db.execute(
            "CREATE TABLE orders (
                id SERIAL PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                total DECIMAL(10,2) NOT NULL,
                status TEXT DEFAULT 'pending'
            )",
        )
        .await;
        db.execute("COMMENT ON TABLE orders IS 'Customer order records'")
            .await;
        db.execute("COMMENT ON COLUMN orders.customer_id IS 'Reference to customer table'")
            .await;
        db.execute("COMMENT ON COLUMN orders.total IS 'Order total amount in USD'")
            .await;

        let tables = fetch(db.pool()).await.unwrap();

        assert_eq!(tables.len(), 1);
        let table = &tables[0];

        assert_eq!(table.schema, "public");
        assert_eq!(table.name, "orders");
        assert_eq!(table.comment, Some("Customer order records".to_string()));

        let customer_id_col = table
            .columns
            .iter()
            .find(|c| c.name == "customer_id")
            .unwrap();
        assert_eq!(
            customer_id_col.comment,
            Some("Reference to customer table".to_string())
        );

        let total_col = table.columns.iter().find(|c| c.name == "total").unwrap();
        assert_eq!(
            total_col.comment,
            Some("Order total amount in USD".to_string())
        );

        let id_col = table.columns.iter().find(|c| c.name == "id").unwrap();
        assert_eq!(id_col.comment, None);

        let status_col = table.columns.iter().find(|c| c.name == "status").unwrap();
        assert_eq!(status_col.comment, None);
    })
    .await;
}
