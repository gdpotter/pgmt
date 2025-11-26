use pgmt::catalog::Catalog;
use pgmt::catalog::id::DbObjectId;
use pgmt::catalog::table::{Column, Table};
use pgmt::catalog::view::View;
use pgmt::schema_generator::{SchemaGenerator, SchemaGeneratorConfig};
use std::collections::BTreeMap;
use std::fs;
use tempfile::TempDir;

#[test]
fn test_schema_generator_basic() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().to_path_buf();

    // Create a simple catalog with one table
    let table = Table::new(
        "public".to_string(),
        "users".to_string(),
        vec![
            Column {
                name: "id".to_string(),
                data_type: "integer".to_string(),
                default: None,
                not_null: true,
                generated: None,
                comment: None,
                depends_on: vec![],
            },
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                default: None,
                not_null: false,
                generated: None,
                comment: None,
                depends_on: vec![],
            },
        ],
        None,   // primary_key
        None,   // comment
        vec![], // depends_on
    );

    let catalog = Catalog {
        schemas: vec![],
        tables: vec![table],
        views: vec![],
        types: vec![],
        domains: vec![],
        functions: vec![],
        aggregates: vec![],
        sequences: vec![],
        indexes: vec![],
        constraints: vec![],
        triggers: vec![],
        extensions: vec![],
        grants: vec![],
        forward_deps: BTreeMap::new(),
        reverse_deps: BTreeMap::new(),
    };

    let config = SchemaGeneratorConfig::default();
    let generator = SchemaGenerator::new(catalog, output_path.clone(), config);

    // Generate schema files
    generator.generate_files().unwrap();

    // Verify that the tables directory and users.sql file were created
    assert!(output_path.join("tables").exists());
    assert!(output_path.join("tables/users.sql").exists());

    // Read the content and verify it contains CREATE TABLE
    let content = fs::read_to_string(output_path.join("tables/users.sql")).unwrap();
    assert!(content.contains("CREATE TABLE"));
    assert!(content.contains("users"));
    assert!(content.contains("id"));
    assert!(content.contains("name"));
}

#[test]
fn test_file_organization_by_object_type() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().to_path_buf();

    // Create a catalog with various object types
    let table = Table::new(
        "public".to_string(),
        "products".to_string(),
        vec![Column {
            name: "id".to_string(),
            data_type: "integer".to_string(),
            default: None,
            not_null: true,
            generated: None,
            comment: None,
            depends_on: vec![],
        }],
        None,   // primary_key
        None,   // comment
        vec![], // depends_on
    );

    let view = View {
        schema: "public".to_string(),
        name: "active_products".to_string(),
        definition: "SELECT * FROM products WHERE active = true".to_string(),
        columns: vec![], // Empty for test
        comment: None,
        depends_on: vec![DbObjectId::Table {
            schema: "public".to_string(),
            name: "products".to_string(),
        }],
    };

    // Build forward dependencies for the view
    let mut forward_deps = BTreeMap::new();
    forward_deps.insert(
        view.id(),
        vec![DbObjectId::Table {
            schema: "public".to_string(),
            name: "products".to_string(),
        }],
    );

    let catalog = Catalog {
        schemas: vec![],
        tables: vec![table],
        views: vec![view],
        types: vec![],
        domains: vec![],
        functions: vec![],
        aggregates: vec![],
        sequences: vec![],
        indexes: vec![],
        constraints: vec![],
        triggers: vec![],
        extensions: vec![],
        grants: vec![],
        forward_deps,
        reverse_deps: BTreeMap::new(),
    };

    let config = SchemaGeneratorConfig::default();
    let generator = SchemaGenerator::new(catalog, output_path.clone(), config);

    // Generate schema files
    generator.generate_files().unwrap();

    // Verify proper file organization
    assert!(output_path.join("tables").exists());
    assert!(output_path.join("tables/products.sql").exists());
    assert!(output_path.join("views").exists());
    assert!(output_path.join("views/active_products.sql").exists());

    // Verify content
    let table_content = fs::read_to_string(output_path.join("tables/products.sql")).unwrap();
    assert!(table_content.contains("CREATE TABLE"));
    assert!(table_content.contains("products"));

    let view_content = fs::read_to_string(output_path.join("views/active_products.sql")).unwrap();
    assert!(view_content.contains("CREATE VIEW"));
    assert!(view_content.contains("active_products"));

    // Verify that the view has a dependency on the table
    assert!(view_content.contains("-- require: tables/products.sql"));
}

#[test]
fn test_config_filtering() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().to_path_buf();

    // Create a simple catalog
    let table = Table::new(
        "public".to_string(),
        "test_table".to_string(),
        vec![Column {
            name: "id".to_string(),
            data_type: "integer".to_string(),
            default: None,
            not_null: true,
            generated: None,
            comment: None,
            depends_on: vec![],
        }],
        None,   // primary_key
        None,   // comment
        vec![], // depends_on
    );

    let catalog = Catalog {
        schemas: vec![],
        tables: vec![table],
        views: vec![],
        types: vec![],
        domains: vec![],
        functions: vec![],
        aggregates: vec![],
        sequences: vec![],
        indexes: vec![],
        constraints: vec![],
        triggers: vec![],
        extensions: vec![],
        grants: vec![],
        forward_deps: BTreeMap::new(),
        reverse_deps: BTreeMap::new(),
    };

    // Test with comments disabled
    let config = SchemaGeneratorConfig {
        include_comments: false,
        include_grants: true,
        include_triggers: true,
        include_extensions: true,
    };

    let generator = SchemaGenerator::new(catalog, output_path.clone(), config);
    generator.generate_files().unwrap();

    // Basic functionality should still work
    assert!(output_path.join("tables").exists());
    assert!(output_path.join("tables/test_table.sql").exists());
}

#[test]
fn test_no_self_referential_dependencies() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().to_path_buf();

    // Create a table with some related objects that would normally create dependencies
    let table = Table::new(
        "public".to_string(),
        "users".to_string(),
        vec![Column {
            name: "id".to_string(),
            data_type: "integer".to_string(),
            default: None,
            not_null: true,
            generated: None,
            comment: None,
            depends_on: vec![],
        }],
        None,   // primary_key
        None,   // comment
        vec![], // depends_on
    );

    // Create an index on the table
    let index = pgmt::catalog::index::Index {
        schema: "public".to_string(),
        name: "idx_users_id".to_string(),
        table_schema: "public".to_string(),
        table_name: "users".to_string(),
        index_type: pgmt::catalog::index::IndexType::Btree,
        is_unique: false,
        is_clustered: false,
        is_valid: true,
        columns: vec![pgmt::catalog::index::IndexColumn {
            expression: "id".to_string(),
            collation: None,
            opclass: None,
            ordering: None,
            nulls_ordering: None,
        }],
        include_columns: vec![],
        predicate: None,
        tablespace: None,
        storage_parameters: vec![],
        comment: None,
        depends_on: vec![DbObjectId::Table {
            schema: "public".to_string(),
            name: "users".to_string(),
        }],
    };

    // Set up forward dependencies: index depends on table
    let mut forward_deps = BTreeMap::new();
    forward_deps.insert(
        index.id(),
        vec![DbObjectId::Table {
            schema: "public".to_string(),
            name: "users".to_string(),
        }],
    );

    let catalog = Catalog {
        schemas: vec![],
        tables: vec![table],
        views: vec![],
        types: vec![],
        domains: vec![],
        functions: vec![],
        aggregates: vec![],
        sequences: vec![],
        indexes: vec![index],
        constraints: vec![],
        triggers: vec![],
        extensions: vec![],
        grants: vec![],
        forward_deps,
        reverse_deps: BTreeMap::new(),
    };

    let config = SchemaGeneratorConfig::default();
    let generator = SchemaGenerator::new(catalog, output_path.clone(), config);

    // Generate schema files
    generator.generate_files().unwrap();

    // Verify that the table file was created and contains both table and index
    assert!(output_path.join("tables").exists());
    assert!(output_path.join("tables/users.sql").exists());

    let content = fs::read_to_string(output_path.join("tables/users.sql")).unwrap();

    // Should contain both CREATE TABLE and CREATE INDEX
    assert!(content.contains("CREATE TABLE"));
    assert!(content.contains("CREATE INDEX"));

    // Should NOT contain a require statement pointing to itself
    assert!(!content.contains("-- require: tables/users.sql"));

    // Should not contain any require statements since there are no external dependencies
    assert!(!content.contains("-- require:"));
}

#[test]
fn test_gist_index_rendering() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().to_path_buf();

    // Create a table
    let table = Table::new(
        "public".to_string(),
        "customers".to_string(),
        vec![
            Column {
                name: "id".to_string(),
                data_type: "integer".to_string(),
                default: Some("nextval('customers_id_seq'::regclass)".to_string()),
                not_null: true,
                generated: None,
                comment: None,
                depends_on: vec![],
            },
            Column {
                name: "first_name".to_string(),
                data_type: "text".to_string(),
                default: None,
                not_null: false,
                generated: None,
                comment: None,
                depends_on: vec![],
            },
            Column {
                name: "last_name".to_string(),
                data_type: "text".to_string(),
                default: None,
                not_null: false,
                generated: None,
                comment: None,
                depends_on: vec![],
            },
        ],
        None,
        None,
        vec![],
    );

    // Create a GiST index with operator class
    let gist_index = pgmt::catalog::index::Index {
        schema: "public".to_string(),
        name: "customers_search_idx".to_string(),
        table_schema: "public".to_string(),
        table_name: "customers".to_string(),
        index_type: pgmt::catalog::index::IndexType::Gist,
        is_unique: false,
        is_clustered: false,
        is_valid: true,
        columns: vec![pgmt::catalog::index::IndexColumn {
            expression: "(first_name || ' ' || last_name)".to_string(),
            collation: None,
            opclass: Some("gist_trgm_ops".to_string()),
            ordering: None,
            nulls_ordering: None,
        }],
        include_columns: vec![],
        predicate: None,
        tablespace: None,
        storage_parameters: vec![],
        comment: None,
        depends_on: vec![DbObjectId::Table {
            schema: "public".to_string(),
            name: "customers".to_string(),
        }],
    };

    let catalog = Catalog {
        schemas: vec![],
        tables: vec![table],
        views: vec![],
        types: vec![],
        domains: vec![],
        functions: vec![],
        aggregates: vec![],
        sequences: vec![],
        indexes: vec![gist_index],
        constraints: vec![],
        triggers: vec![],
        extensions: vec![],
        grants: vec![],
        forward_deps: BTreeMap::new(),
        reverse_deps: BTreeMap::new(),
    };

    let config = SchemaGeneratorConfig::default();
    let generator = SchemaGenerator::new(catalog, output_path.clone(), config);

    // Generate schema files
    generator.generate_files().unwrap();

    // Verify that the table file contains the GiST index with proper syntax
    let table_file = output_path.join("tables/customers.sql");
    assert!(table_file.exists());

    let content = fs::read_to_string(&table_file).unwrap();

    // Verify the index includes USING clause and operator class
    assert!(
        content.contains("USING gist"),
        "Index should include 'USING gist' clause"
    );
    assert!(
        content.contains("gist_trgm_ops"),
        "Index should include 'gist_trgm_ops' operator class"
    );
    assert!(content.contains("CREATE INDEX"));
    assert!(content.contains("customers_search_idx"));
}

#[test]
fn test_detailed_config_variations() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().to_path_buf();

    // Create a simple table
    let table = Table::new(
        "public".to_string(),
        "test_table".to_string(),
        vec![Column {
            name: "id".to_string(),
            data_type: "integer".to_string(),
            default: None,
            not_null: true,
            generated: None,
            comment: Some("Primary key".to_string()),
            depends_on: vec![],
        }],
        None,
        Some("Test table comment".to_string()),
        vec![],
    );

    let catalog = Catalog {
        schemas: vec![],
        tables: vec![table],
        views: vec![],
        types: vec![],
        domains: vec![],
        functions: vec![],
        aggregates: vec![],
        sequences: vec![],
        indexes: vec![],
        constraints: vec![],
        triggers: vec![],
        extensions: vec![],
        grants: vec![],
        forward_deps: BTreeMap::new(),
        reverse_deps: BTreeMap::new(),
    };

    // Test with all features disabled
    let minimal_config = SchemaGeneratorConfig {
        include_comments: false,
        include_grants: false,
        include_triggers: false,
        include_extensions: false,
    };

    let generator = SchemaGenerator::new(catalog, output_path.clone(), minimal_config);
    generator.generate_files().unwrap();

    // Should still create basic structure and content
    assert!(output_path.join("tables").exists());
    assert!(output_path.join("tables/test_table.sql").exists());

    let content = fs::read_to_string(output_path.join("tables/test_table.sql")).unwrap();
    assert!(content.contains("CREATE TABLE"));
    assert!(content.contains("test_table"));
}

#[test]
fn test_empty_catalog() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().to_path_buf();

    let catalog = Catalog::empty();
    let config = SchemaGeneratorConfig::default();
    let generator = SchemaGenerator::new(catalog, output_path.clone(), config);

    // Should not fail with empty catalog
    generator.generate_files().unwrap();

    // Should create directory structure even if empty
    assert!(output_path.join("tables").exists());
    assert!(output_path.join("views").exists());
    assert!(output_path.join("functions").exists());
}
