use anyhow::Result;
use std::fs;
use tempfile::TempDir;

use crate::helpers::harness::with_test_db;

#[tokio::test]
async fn test_apply_schema_to_shadow_enhanced_error_reporting() -> Result<()> {
    with_test_db(async |shadow_db| {
        // Create a temporary schema directory with a file containing a syntax error
        let temp_dir = TempDir::new()?;
        let schema_dir = temp_dir.path().join("schema");
        fs::create_dir_all(&schema_dir)?;

        // Write a schema file with a deliberate syntax error
        let invalid_schema = r#"-- Test schema with syntax error
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email TEXTT NOT NULL  -- Error: TEXTT should be TEXT
);"#;
        fs::write(schema_dir.join("users.sql"), invalid_schema)?;

        // Test the schema processor directly (this is what apply command uses)
        use pgmt::db::schema_processor::{SchemaProcessor, SchemaProcessorConfig};
        let processor_config = SchemaProcessorConfig {
            verbose: true,
            clean_before_apply: false, // DB is already clean
            ..Default::default()
        };
        let processor = SchemaProcessor::new(shadow_db.pool().clone(), processor_config);
        let result = processor.process_schema_directory(&schema_dir).await;

        assert!(
            result.is_err(),
            "Schema application should fail with syntax error"
        );

        let error_message = result.unwrap_err().to_string();

        // Verify the error message contains enhanced information
        assert!(
            error_message.contains("users.sql"),
            "Should mention the specific file that failed. Error: {}",
            error_message
        );
        assert!(
            error_message.contains("TEXTT"),
            "Should mention the problematic SQL content. Error: {}",
            error_message
        );
        assert!(
            error_message.contains("🐘 Database Error:"),
            "Should include PostgreSQL error details. Error: {}",
            error_message
        );

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_schema_executor_succeeds_with_valid_schema() -> Result<()> {
    with_test_db(async |shadow_db| {
        // Create a temporary schema directory with valid schema files
        let temp_dir = TempDir::new()?;
        let schema_dir = temp_dir.path().join("schema");
        fs::create_dir_all(&schema_dir)?;

        // Write valid schema files
        fs::write(schema_dir.join("01_schema.sql"), "CREATE SCHEMA app;")?;
        fs::write(
            schema_dir.join("02_users.sql"),
            r#"CREATE TABLE app.users (
        id SERIAL PRIMARY KEY,
        email TEXT NOT NULL UNIQUE,
        created_at TIMESTAMP DEFAULT NOW()
    );"#,
        )?;

        // Test the schema processor
        use pgmt::db::schema_processor::{SchemaProcessor, SchemaProcessorConfig};
        let processor_config = SchemaProcessorConfig {
            verbose: true,
            clean_before_apply: false,
            ..Default::default()
        };
        let processor = SchemaProcessor::new(shadow_db.pool().clone(), processor_config);
        let result = processor.process_schema_directory(&schema_dir).await;

        assert!(
            result.is_ok(),
            "Schema executor should succeed with valid schema: {:?}",
            result
        );

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_schema_executor_dependency_error_reporting() -> Result<()> {
    with_test_db(async |shadow_db| {
        // Create a temporary schema directory with files that have dependency order issues
        let temp_dir = TempDir::new()?;
        let schema_dir = temp_dir.path().join("schema");
        fs::create_dir_all(&schema_dir)?;

        // Write schema files where one references a table that doesn't exist yet
        fs::write(
            schema_dir.join("01_view.sql"),
            r#"-- This will fail because users table doesn't exist yet
CREATE VIEW user_emails AS SELECT id, email FROM users;"#,
        )?;

        fs::write(
            schema_dir.join("02_users.sql"),
            r#"CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        email TEXT NOT NULL
    );"#,
        )?;

        // Test the schema processor
        use pgmt::db::schema_processor::{SchemaProcessor, SchemaProcessorConfig};
        let processor_config = SchemaProcessorConfig {
            verbose: true,
            clean_before_apply: false,
            ..Default::default()
        };
        let processor = SchemaProcessor::new(shadow_db.pool().clone(), processor_config);
        let result = processor.process_schema_directory(&schema_dir).await;

        assert!(
            result.is_err(),
            "Schema executor should fail with dependency error"
        );

        let error_message = result.unwrap_err().to_string();

        // Verify the error message contains enhanced information
        assert!(
            error_message.contains("01_view.sql"),
            "Should mention the specific file that failed. Error: {}",
            error_message
        );
        assert!(
            error_message.contains("users"),
            "Should mention the missing table. Error: {}",
            error_message
        );

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_migrate_apply_enhanced_error_reporting() -> Result<()> {
    with_test_db(async |shadow_db| {
        // Create a temporary migration file with syntax error
        let temp_dir = TempDir::new()?;
        let migration_path = temp_dir.path().join("V001__test_migration.sql");

        let invalid_migration_sql = r#"-- Test migration with syntax error
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email TEXTT NOT NULL  -- Error: TEXTT should be TEXT
);"#;
        fs::write(&migration_path, invalid_migration_sql)?;

        // Test the enhanced SQL executor for migrations
        use pgmt::db::schema_executor::SchemaExecutor;
        let result = SchemaExecutor::execute_sql_with_enhanced_errors(
            shadow_db.pool(),
            &migration_path,
            invalid_migration_sql,
        )
        .await;

        assert!(
            result.is_err(),
            "Migration execution should fail with syntax error"
        );

        let error_message = result.unwrap_err().to_string();

        // Verify the error message contains enhanced information
        assert!(
            error_message.contains("V001__test_migration.sql"),
            "Should mention the specific migration file that failed. Error: {}",
            error_message
        );
        assert!(
            error_message.contains("TEXTT"),
            "Should mention the problematic SQL content. Error: {}",
            error_message
        );
        assert!(
            error_message.contains("🐘 Database Error:"),
            "Should include PostgreSQL error details. Error: {}",
            error_message
        );

        Ok(())
    })
    .await
}
