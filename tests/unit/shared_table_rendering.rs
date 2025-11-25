//! Tests for shared table rendering consistency between schema generator and diff operations

use anyhow::Result;
use pgmt::catalog::table::{Column, PrimaryKey, Table};
use pgmt::diff::operations::SqlRenderer;
use pgmt::diff::operations::table::TableOperation;
use pgmt::render::sql::render_create_table;

/// Test that schema generator and diff operations produce identical CREATE TABLE SQL
#[test]
fn test_shared_table_rendering_consistency() -> Result<()> {
    // Create a test table with various features
    let table = Table::new(
        "public".to_string(),
        "test_table".to_string(),
        vec![
            Column {
                name: "id".to_string(),
                data_type: "integer".to_string(),
                default: Some("nextval('test_table_id_seq'::regclass)".to_string()),
                not_null: true,
                generated: None,
                comment: None,
                depends_on: vec![],
            },
            Column {
                name: "email".to_string(),
                data_type: "text".to_string(),
                default: None,
                not_null: true,
                generated: None,
                comment: None,
                depends_on: vec![],
            },
            Column {
                name: "full_name".to_string(),
                data_type: "text".to_string(),
                default: None,
                not_null: false,
                generated: Some("first_name || ' ' || last_name".to_string()),
                comment: None,
                depends_on: vec![],
            },
            Column {
                name: "created_at".to_string(),
                data_type: "timestamp with time zone".to_string(),
                default: Some("CURRENT_TIMESTAMP".to_string()),
                not_null: true,
                generated: None,
                comment: None,
                depends_on: vec![],
            },
        ],
        Some(PrimaryKey {
            name: "test_table_pkey".to_string(),
            columns: vec!["id".to_string()],
        }),
        None,
        vec![],
    );

    // Get SQL from shared rendering function (used by schema generator)
    let schema_generator_sql = render_create_table(&table);

    // Get SQL from diff operations
    let table_operation = TableOperation::Create {
        schema: table.schema.clone(),
        name: table.name.clone(),
        columns: table.columns.clone(),
        primary_key: table.primary_key.clone(),
    };

    let diff_operation_sqls = table_operation.to_sql();
    assert_eq!(diff_operation_sqls.len(), 1);
    let diff_operation_sql = &diff_operation_sqls[0].sql;

    // Verify they produce identical SQL
    assert_eq!(
        schema_generator_sql, *diff_operation_sql,
        "Schema generator and diff operations must produce identical CREATE TABLE SQL"
    );

    // Verify the expected SQL structure
    let expected_features = vec![
        "CREATE TABLE \"public\".\"test_table\"",
        "\"id\" integer DEFAULT nextval('test_table_id_seq'::regclass) NOT NULL",
        "\"email\" text NOT NULL",
        "\"full_name\" text GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED",
        "\"created_at\" timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL",
        "CONSTRAINT \"test_table_pkey\" PRIMARY KEY (\"id\")",
    ];

    for feature in expected_features {
        assert!(
            schema_generator_sql.contains(feature),
            "SQL should contain '{}', but got: {}",
            feature,
            schema_generator_sql
        );
    }

    Ok(())
}

/// Test consistency for simple table without primary key
#[test]
fn test_shared_table_rendering_simple() -> Result<()> {
    let table = Table::new(
        "app".to_string(),
        "logs".to_string(),
        vec![
            Column {
                name: "message".to_string(),
                data_type: "text".to_string(),
                default: None,
                not_null: false,
                generated: None,
                comment: None,
                depends_on: vec![],
            },
            Column {
                name: "level".to_string(),
                data_type: "text".to_string(),
                default: Some("'info'".to_string()),
                not_null: true,
                generated: None,
                comment: None,
                depends_on: vec![],
            },
        ],
        None, // No primary key
        None,
        vec![],
    );

    // Get SQL from both sources
    let schema_generator_sql = render_create_table(&table);

    let table_operation = TableOperation::Create {
        schema: table.schema.clone(),
        name: table.name.clone(),
        columns: table.columns.clone(),
        primary_key: None,
    };
    let diff_operation_sql = &table_operation.to_sql()[0].sql;

    // Verify identical output
    assert_eq!(schema_generator_sql, *diff_operation_sql);

    // Verify expected structure
    assert!(schema_generator_sql.contains("CREATE TABLE \"app\".\"logs\""));
    assert!(schema_generator_sql.contains("\"message\" text,"));
    assert!(schema_generator_sql.contains("\"level\" text DEFAULT 'info' NOT NULL"));
    assert!(!schema_generator_sql.contains("CONSTRAINT")); // No primary key

    Ok(())
}

/// Test consistency for compound primary key
#[test]
fn test_shared_table_rendering_compound_key() -> Result<()> {
    let table = Table::new(
        "public".to_string(),
        "user_permissions".to_string(),
        vec![
            Column {
                name: "user_id".to_string(),
                data_type: "uuid".to_string(),
                default: None,
                not_null: true,
                generated: None,
                comment: None,
                depends_on: vec![],
            },
            Column {
                name: "permission_id".to_string(),
                data_type: "uuid".to_string(),
                default: None,
                not_null: true,
                generated: None,
                comment: None,
                depends_on: vec![],
            },
        ],
        Some(PrimaryKey {
            name: "user_permissions_pkey".to_string(),
            columns: vec!["user_id".to_string(), "permission_id".to_string()],
        }),
        None,
        vec![],
    );

    // Get SQL from both sources
    let schema_generator_sql = render_create_table(&table);

    let table_operation = TableOperation::Create {
        schema: table.schema.clone(),
        name: table.name.clone(),
        columns: table.columns.clone(),
        primary_key: table.primary_key.clone(),
    };
    let diff_operation_sql = &table_operation.to_sql()[0].sql;

    // Verify identical output
    assert_eq!(schema_generator_sql, *diff_operation_sql);

    // Verify compound primary key
    assert!(schema_generator_sql.contains(
        "CONSTRAINT \"user_permissions_pkey\" PRIMARY KEY (\"user_id\", \"permission_id\")"
    ));

    Ok(())
}
