use anyhow::Result;
use sqlx::Row;
use std::fs;
use tempfile::TempDir;

// use pgmt::db::schema_executor::SchemaExecutor;
use crate::helpers::harness::with_test_db;

#[tokio::test]
async fn test_schema_executor_reports_specific_file_on_error() -> Result<()> {
    with_test_db(async |db| {
        // Create a temporary schema directory with a file containing a syntax error
        let temp_dir = TempDir::new()?;
        let schema_dir = temp_dir.path();

        // Create a valid schema file first
        let valid_schema = r#"CREATE SCHEMA test_schema;"#;
        fs::write(schema_dir.join("01_schema.sql"), valid_schema)?;

        // Create a file with a syntax error (invalid data type)
        let invalid_schema = r#"CREATE TABLE test_schema.users (
    id SERIAL PRIMARY KEY,
    email TEXTT NOT NULL  -- TEXTT is not a valid type
);"#;
        fs::write(schema_dir.join("02_users.sql"), invalid_schema)?;

        // Test that the processor provides detailed error information
        use pgmt::db::schema_processor::{SchemaProcessor, SchemaProcessorConfig};
        let processor_config = SchemaProcessorConfig {
            verbose: true,
            clean_before_apply: false,
        };
        let processor = SchemaProcessor::new(db.pool().clone(), processor_config);
        let result = processor.process_schema_directory(schema_dir).await;

        assert!(result.is_err());
        let error_str = result.unwrap_err().to_string();

        // Verify the error message contains specific file information
        assert!(
            error_str.contains("02_users.sql"),
            "Error should mention the specific file that failed"
        );
        assert!(
            error_str.contains("TEXTT"),
            "Error should mention the problematic SQL content"
        );
        assert!(
            error_str.contains("🐘 Database Error:"),
            "Error should include PostgreSQL error details"
        );

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_schema_executor_succeeds_with_valid_files() -> Result<()> {
    with_test_db(async |db| {
        // Create a temporary schema directory with valid schema files
        let temp_dir = TempDir::new()?;
        let schema_dir = temp_dir.path();

        // Create valid schema files
        fs::write(schema_dir.join("01_schema.sql"), "CREATE SCHEMA test_app;")?;
        fs::write(
            schema_dir.join("02_table.sql"),
            r#"CREATE TABLE test_app.users (
        id SERIAL PRIMARY KEY,
        email TEXT NOT NULL UNIQUE,
        created_at TIMESTAMP DEFAULT NOW()
    );"#,
        )?;
        fs::write(
            schema_dir.join("03_view.sql"),
            r#"CREATE VIEW test_app.active_users AS
        SELECT id, email FROM test_app.users WHERE created_at > NOW() - INTERVAL '30 days';"#,
        )?;

        // Test that the processor succeeds with valid files
        use pgmt::db::schema_processor::{SchemaProcessor, SchemaProcessorConfig};
        let processor_config = SchemaProcessorConfig {
            verbose: true,
            clean_before_apply: false,
        };
        let processor = SchemaProcessor::new(db.pool().clone(), processor_config);
        let result = processor.process_schema_directory(schema_dir).await;

        assert!(result.is_ok(), "Should succeed with valid schema files");

        // Verify the schema was actually applied
        let tables = sqlx::query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'test_app' ORDER BY table_name")
            .fetch_all(db.pool())
            .await?;

        // Should have at least the users table (there might be additional system tables)
        assert!(!tables.is_empty());

        let table_names: Vec<String> = tables
            .iter()
            .map(|row| row.get::<String, _>("table_name"))
            .collect();

        assert!(
            table_names.contains(&"users".to_string()),
            "Should contain users table. Found tables: {:?}",
            table_names
        );

        Ok(())
    }).await
}

#[tokio::test]
async fn test_schema_executor_handles_dependency_order() -> Result<()> {
    with_test_db(async |db| {
        // Create a temporary schema directory with files that have dependencies
        let temp_dir = TempDir::new()?;
        let schema_dir = temp_dir.path();

        // Create schema files with explicit dependencies
        fs::write(schema_dir.join("schema.sql"), "CREATE SCHEMA app;")?;

        fs::write(
            schema_dir.join("types.sql"),
            r#"-- require: schema.sql
CREATE TYPE app.user_status AS ENUM ('active', 'inactive', 'suspended');"#,
        )?;

        fs::write(
            schema_dir.join("tables.sql"),
            r#"-- require: schema.sql, types.sql
CREATE TABLE app.users (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    status app.user_status DEFAULT 'active'
);"#,
        )?;

        // Test that the processor respects dependency order
        use pgmt::db::schema_processor::{SchemaProcessor, SchemaProcessorConfig};
        let processor_config = SchemaProcessorConfig {
            verbose: true,
            clean_before_apply: false,
        };
        let processor = SchemaProcessor::new(db.pool().clone(), processor_config);
        let result = processor.process_schema_directory(schema_dir).await;

        assert!(
            result.is_ok(),
            "Should succeed when dependencies are properly ordered"
        );

        // Verify the schema was applied correctly
        let enums = sqlx::query("SELECT typname FROM pg_type WHERE typname = 'user_status'")
            .fetch_all(db.pool())
            .await?;

        assert_eq!(enums.len(), 1);

        Ok(())
    })
    .await
}

#[test]
fn test_error_display_formatting() {
    use pgmt::db::schema_executor::SqlExecutionError;

    let error = SqlExecutionError {
        source_context: "tables/users.sql".to_string(),
        sql_content: "CREATE TABLE users (id SERIAL, email TEXTT)".to_string(),
        line_number: Some(2),
        postgres_error: "type \"textt\" does not exist".to_string(),
        suggestion: Some("Check for typos in data type names".to_string()),
        troubleshooting_tips: vec![],
        dependencies_info: None,
    };

    let display = format!("{}", error);

    // Verify all expected components are present
    assert!(display.contains("❌ Failed to apply schema file 'tables/users.sql'"));
    assert!(display.contains("Line 2"));
    assert!(display.contains("CREATE TABLE users"));
    assert!(display.contains("🐘 Database Error"));
    assert!(display.contains("type \"textt\" does not exist"));
    assert!(display.contains("💡 Suggestion: Check for typos in data type names"));
}
