use crate::helpers::harness::with_test_db;
use pgmt::catalog::id::DependsOn;
use pgmt::catalog::index::IndexType;

#[tokio::test]
async fn test_fetch_basic_indexes() {
    with_test_db(async |db| {
        db.execute(
            "CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                email VARCHAR,
                name VARCHAR NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )",
        )
        .await;

        db.execute("CREATE INDEX idx_users_name ON users (name)")
            .await;
        db.execute("CREATE UNIQUE INDEX idx_users_email_unique ON users (email)")
            .await;
        db.execute("CREATE INDEX idx_users_created_partial ON users (created_at) WHERE created_at > '2020-01-01'").await;

        let indexes = pgmt::catalog::index::fetch(&mut *db.conn().await).await.unwrap();

        // 3 created indexes (excluding primary key which is filtered out)
        assert_eq!(indexes.len(), 3);

        let name_index = indexes.iter().find(|i| i.name == "idx_users_name").unwrap();
        assert_eq!(name_index.table_name, "users");
        assert_eq!(name_index.index_type, IndexType::Btree);
        assert!(!name_index.is_unique);
        assert_eq!(name_index.columns.len(), 1);
        assert_eq!(name_index.columns[0].expression, "name");

        let email_index = indexes
            .iter()
            .find(|i| i.name == "idx_users_email_unique")
            .unwrap();
        assert!(email_index.is_unique);
        assert_eq!(email_index.columns[0].expression, "email");

        let partial_index = indexes
            .iter()
            .find(|i| i.name == "idx_users_created_partial")
            .unwrap();
        assert!(partial_index.predicate.is_some());
        assert!(
            partial_index
                .predicate
                .as_ref()
                .unwrap()
                .contains("2020-01-01")
        );
    }).await;
}

#[tokio::test]
async fn test_fetch_index_with_comment() {
    with_test_db(async |db| {
        db.execute("CREATE TABLE products (id SERIAL, sku VARCHAR)")
            .await;

        db.execute("CREATE INDEX idx_products_sku ON products (sku)")
            .await;
        db.execute("COMMENT ON INDEX idx_products_sku IS 'Index for fast SKU lookups'")
            .await;

        let indexes = pgmt::catalog::index::fetch(&mut *db.conn().await)
            .await
            .unwrap();

        let sku_index = indexes
            .iter()
            .find(|i| i.name == "idx_products_sku")
            .unwrap();
        assert_eq!(
            sku_index.comment,
            Some("Index for fast SKU lookups".to_string())
        );
    })
    .await;
}

#[tokio::test]
async fn test_fetch_index_with_include_columns() {
    with_test_db(async |db| {
        // Create test table
        db.execute(
            "CREATE TABLE orders (
                id SERIAL PRIMARY KEY,
                customer_id INTEGER,
                total_amount DECIMAL,
                status VARCHAR,
                created_at TIMESTAMP
            )",
        )
        .await;

        // Create index with INCLUDE columns (covering index)
        db.execute(
            "CREATE INDEX idx_orders_customer_covering
             ON orders (customer_id)
             INCLUDE (total_amount, created_at)",
        )
        .await;

        let indexes = pgmt::catalog::index::fetch(&mut *db.conn().await)
            .await
            .unwrap();

        let covering_index = indexes
            .iter()
            .find(|i| i.name == "idx_orders_customer_covering")
            .unwrap();
        assert_eq!(covering_index.columns.len(), 1);
        assert_eq!(covering_index.columns[0].expression, "customer_id");
        assert_eq!(covering_index.include_columns.len(), 2);
        assert!(
            covering_index
                .include_columns
                .contains(&"total_amount".to_string())
        );
        assert!(
            covering_index
                .include_columns
                .contains(&"created_at".to_string())
        );
    })
    .await;
}

#[tokio::test]
async fn test_fetch_expression_index() {
    with_test_db(async |db| {
        // Create test table
        db.execute("CREATE TABLE users (id SERIAL, email VARCHAR)")
            .await;

        // Create expression index
        db.execute("CREATE INDEX idx_users_email_lower ON users (lower(email))")
            .await;

        let indexes = pgmt::catalog::index::fetch(&mut *db.conn().await)
            .await
            .unwrap();

        let expr_index = indexes
            .iter()
            .find(|i| i.name == "idx_users_email_lower")
            .unwrap();
        assert_eq!(expr_index.columns.len(), 1);
        // The expression should be the full expression (PostgreSQL may add type casts)
        assert!(expr_index.columns[0].expression.contains("lower"));
        assert!(expr_index.columns[0].expression.contains("email"));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_multicolumn_index() {
    with_test_db(async |db| {
        // Create test table
        db.execute(
            "CREATE TABLE log_entries (
                id SERIAL,
                user_id INTEGER,
                action VARCHAR,
                created_at TIMESTAMP
            )",
        )
        .await;

        // Create multi-column index with ordering
        db.execute(
            "CREATE INDEX idx_log_user_time
             ON log_entries (user_id ASC, created_at DESC)",
        )
        .await;

        let indexes = pgmt::catalog::index::fetch(&mut *db.conn().await)
            .await
            .unwrap();

        let multi_index = indexes
            .iter()
            .find(|i| i.name == "idx_log_user_time")
            .unwrap();
        assert_eq!(multi_index.columns.len(), 2);

        // First column should be user_id with ASC ordering
        assert_eq!(multi_index.columns[0].expression, "user_id");
        assert_eq!(multi_index.columns[0].ordering, Some("ASC".to_string()));

        // Second column should be created_at with DESC ordering
        assert_eq!(multi_index.columns[1].expression, "created_at");
        assert_eq!(multi_index.columns[1].ordering, Some("DESC".to_string()));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_gin_index() {
    with_test_db(async |db| {
        // Create test table
        db.execute("CREATE TABLE documents (id SERIAL, content TEXT)")
            .await;

        // Create GIN index for full-text search
        db.execute(
            "CREATE INDEX idx_documents_content_gin
             ON documents USING gin (to_tsvector('english', content))",
        )
        .await;

        let indexes = pgmt::catalog::index::fetch(&mut *db.conn().await)
            .await
            .unwrap();

        let gin_index = indexes
            .iter()
            .find(|i| i.name == "idx_documents_content_gin")
            .unwrap();
        assert_eq!(gin_index.index_type, IndexType::Gin);
        assert_eq!(gin_index.columns.len(), 1);
        // Should not have ordering/nulls ordering for GIN indexes
        assert_eq!(gin_index.columns[0].ordering, None);
        assert_eq!(gin_index.columns[0].nulls_ordering, None);
    })
    .await;
}

#[tokio::test]
async fn test_fetch_index_dependencies() {
    with_test_db(async |db| {
        // Create a custom type
        db.execute("CREATE TYPE priority AS ENUM ('low', 'medium', 'high')")
            .await;

        // Create test table using the custom type
        db.execute(
            "CREATE TABLE tasks (
                id SERIAL,
                title VARCHAR,
                priority_level priority
            )",
        )
        .await;

        // Create index on the custom type column
        db.execute("CREATE INDEX idx_tasks_priority ON tasks (priority_level)")
            .await;

        let indexes = pgmt::catalog::index::fetch(&mut *db.conn().await)
            .await
            .unwrap();

        let priority_index = indexes
            .iter()
            .find(|i| i.name == "idx_tasks_priority")
            .unwrap();

        // Should depend on the table and potentially the custom type
        assert!(!priority_index.depends_on().is_empty());

        // Should include dependency on the tasks table
        let table_dep = priority_index.depends_on().iter().find(|dep| {
            matches!(dep,
                pgmt::catalog::id::DbObjectId::Table { schema, name }
                if schema == "public" && name == "tasks"
            )
        });
        assert!(table_dep.is_some());
    })
    .await;
}

#[tokio::test]
async fn test_fetch_index_with_storage_parameters() {
    with_test_db(async |db| {
        // Create test table
        db.execute("CREATE TABLE large_table (id SERIAL, data TEXT)")
            .await;

        // Create index with storage parameters
        db.execute(
            "CREATE INDEX idx_large_table_data
             ON large_table (data)
             WITH (fillfactor = 70)",
        )
        .await;

        let indexes = pgmt::catalog::index::fetch(&mut *db.conn().await)
            .await
            .unwrap();

        let param_index = indexes
            .iter()
            .find(|i| i.name == "idx_large_table_data")
            .unwrap();

        // Should have storage parameters
        assert!(!param_index.storage_parameters.is_empty());
        let fillfactor_param = param_index
            .storage_parameters
            .iter()
            .find(|(key, _)| key == "fillfactor");
        assert!(fillfactor_param.is_some());
        assert_eq!(fillfactor_param.unwrap().1, "70");
    })
    .await;
}

#[tokio::test]
async fn test_fetch_indexes_across_schemas() {
    with_test_db(async |db| {
        // Create additional schema
        db.execute("CREATE SCHEMA test_schema").await;

        // Create tables in different schemas
        db.execute("CREATE TABLE public.public_table (id SERIAL, name VARCHAR)")
            .await;
        db.execute("CREATE TABLE test_schema.schema_table (id SERIAL, name VARCHAR)")
            .await;

        // Create indexes in different schemas
        db.execute("CREATE INDEX idx_public ON public.public_table (name)")
            .await;
        db.execute("CREATE INDEX idx_schema ON test_schema.schema_table (name)")
            .await;

        let indexes = pgmt::catalog::index::fetch(&mut *db.conn().await)
            .await
            .unwrap();

        // Should have indexes from both schemas
        let public_index = indexes
            .iter()
            .find(|i| i.schema == "public" && i.name == "idx_public");
        let schema_index = indexes
            .iter()
            .find(|i| i.schema == "test_schema" && i.name == "idx_schema");

        assert!(public_index.is_some());
        assert!(schema_index.is_some());

        // Verify table references
        assert_eq!(public_index.unwrap().table_name, "public_table");
        assert_eq!(schema_index.unwrap().table_name, "schema_table");
    })
    .await;
}

#[tokio::test]
async fn test_primary_key_indexes_excluded() {
    with_test_db(async |db| {
        // Create table with primary key (which creates an index automatically)
        db.execute(
            "CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                email VARCHAR UNIQUE,
                name VARCHAR NOT NULL
            )",
        )
        .await;

        // Create an explicit index
        db.execute("CREATE INDEX idx_users_name ON users (name)")
            .await;

        let indexes = pgmt::catalog::index::fetch(&mut *db.conn().await)
            .await
            .unwrap();

        // Should only have 1 index: the explicit idx_users_name index.
        // The primary key index is excluded (managed as part of PRIMARY KEY constraint).
        // The unique email index is also excluded (managed through UNIQUE constraint).
        assert_eq!(indexes.len(), 1);

        // Verify the primary key index is not in the results
        let has_primary_key_index = indexes.iter().any(|i| i.name.contains("pkey"));
        assert!(
            !has_primary_key_index,
            "Primary key indexes should be excluded from catalog fetch"
        );

        // Verify we got the expected index
        let name_index = indexes.iter().find(|i| i.name == "idx_users_name");
        assert!(
            name_index.is_some(),
            "Explicit index should be present in catalog"
        );
    })
    .await;
}

#[tokio::test]
async fn test_unique_index_with_fk_reference_not_excluded() {
    with_test_db(async |db| {
        // Create referenced table with compound unique index
        db.execute(
            "CREATE TABLE parent (
                id SERIAL PRIMARY KEY,
                tenant_id INT NOT NULL,
                code VARCHAR(50) NOT NULL
            )",
        )
        .await;
        db.execute("CREATE UNIQUE INDEX idx_parent_tenant_code ON parent (tenant_id, code)")
            .await;

        // Create child table with FK referencing the unique index columns
        db.execute(
            "CREATE TABLE child (
                id SERIAL PRIMARY KEY,
                parent_tenant_id INT NOT NULL,
                parent_code VARCHAR(50) NOT NULL,
                CONSTRAINT fk_child_parent
                    FOREIGN KEY (parent_tenant_id, parent_code)
                    REFERENCES parent (tenant_id, code)
            )",
        )
        .await;

        let indexes = pgmt::catalog::index::fetch(&mut *db.conn().await)
            .await
            .unwrap();

        // The standalone unique index should NOT be excluded just because a FK references it.
        // FKs reference indexes but don't own them - only unique/exclusion constraints own their backing index.
        let idx = indexes.iter().find(|i| i.name == "idx_parent_tenant_code");
        assert!(
            idx.is_some(),
            "Unique index should not be excluded when FK references it"
        );
    })
    .await;
}
