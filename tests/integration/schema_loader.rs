use anyhow::Result;
use pgmt::schema_loader::{SchemaFile, SchemaLoader, SchemaLoaderConfig};
use std::fs;
use tempfile::TempDir;

/// Integration tests for the schema loader functionality
/// These tests verify that the schema loader correctly handles multi-file schemas
/// and integrates properly with the overall pgmt system
///
/// Helper to find a file's position in the ordered list
fn find_file_index(files: &[SchemaFile], name: &str) -> usize {
    files
        .iter()
        .position(|f| f.relative_path.contains(name))
        .unwrap_or_else(|| panic!("File {} not found", name))
}

#[tokio::test]
async fn test_schema_loader_multi_file_with_dependencies() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let schema_dir = temp_dir.path().join("schema");
    fs::create_dir_all(&schema_dir)?;
    fs::create_dir_all(schema_dir.join("schemas"))?;
    fs::create_dir_all(schema_dir.join("types"))?;
    fs::create_dir_all(schema_dir.join("tables"))?;
    fs::create_dir_all(schema_dir.join("views"))?;

    // Create schema files with dependencies

    // 1. Schema definition (no dependencies)
    fs::write(
        schema_dir.join("schemas").join("app.sql"),
        "CREATE SCHEMA app;",
    )?;

    // 2. Custom types (requires schema)
    fs::write(
        schema_dir.join("types").join("user_status.sql"),
        "-- require: schemas/app.sql\nCREATE TYPE app.user_status AS ENUM ('active', 'inactive', 'pending');",
    )?;

    // 3. Tables (requires schema and types)
    fs::write(
        schema_dir.join("tables").join("users.sql"),
        r#"-- require: schemas/app.sql, types/user_status.sql
CREATE TABLE app.users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    status app.user_status DEFAULT 'pending'
);"#,
    )?;

    // 4. Views (requires tables)
    fs::write(
        schema_dir.join("views").join("active_users.sql"),
        r#"-- require: tables/users.sql
CREATE VIEW app.active_users AS
SELECT id, name, email
FROM app.users
WHERE status = 'active';"#,
    )?;

    let config = SchemaLoaderConfig::new(schema_dir);
    let loader = SchemaLoader::new(config);

    let files = loader.load_ordered_schema_files()?;

    // Verify that the files are in the correct order
    let schema_idx = find_file_index(&files, "schemas/app.sql");
    let types_idx = find_file_index(&files, "types/user_status.sql");
    let tables_idx = find_file_index(&files, "tables/users.sql");
    let views_idx = find_file_index(&files, "views/active_users.sql");

    assert!(schema_idx < types_idx, "Schema should come before types");
    assert!(schema_idx < tables_idx, "Schema should come before tables");
    assert!(types_idx < tables_idx, "Types should come before tables");
    assert!(tables_idx < views_idx, "Tables should come before views");

    // Verify content is included
    assert!(
        files
            .iter()
            .any(|f| f.content.contains("CREATE SCHEMA app"))
    );
    assert!(
        files
            .iter()
            .any(|f| f.content.contains("CREATE TYPE app.user_status"))
    );
    assert!(
        files
            .iter()
            .any(|f| f.content.contains("CREATE TABLE app.users"))
    );
    assert!(
        files
            .iter()
            .any(|f| f.content.contains("CREATE VIEW app.active_users"))
    );

    Ok(())
}

#[tokio::test]
async fn test_schema_loader_alphabetical_order_without_dependencies() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let schema_dir = temp_dir.path().join("schema");
    fs::create_dir_all(&schema_dir)?;
    fs::create_dir_all(schema_dir.join("tables"))?;

    // Create files that should be ordered alphabetically
    fs::write(
        schema_dir.join("tables").join("zebra.sql"),
        "CREATE TABLE zebra (id INT);",
    )?;

    fs::write(
        schema_dir.join("tables").join("apple.sql"),
        "CREATE TABLE apple (id INT);",
    )?;

    fs::write(
        schema_dir.join("tables").join("beta.sql"),
        "CREATE TABLE beta (id INT);",
    )?;

    let config = SchemaLoaderConfig::new(schema_dir);
    let loader = SchemaLoader::new(config);

    let files = loader.load_ordered_schema_files()?;

    // Verify alphabetical ordering
    let apple_idx = find_file_index(&files, "apple.sql");
    let beta_idx = find_file_index(&files, "beta.sql");
    let zebra_idx = find_file_index(&files, "zebra.sql");

    assert!(
        apple_idx < beta_idx,
        "apple.sql should come before beta.sql"
    );
    assert!(
        beta_idx < zebra_idx,
        "beta.sql should come before zebra.sql"
    );

    Ok(())
}

#[tokio::test]
async fn test_schema_loader_complex_dependency_chain() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let schema_dir = temp_dir.path().join("schema");
    fs::create_dir_all(&schema_dir)?;

    // Create a complex dependency chain: a -> b -> c -> d
    fs::write(schema_dir.join("a.sql"), "CREATE SCHEMA a;")?;

    fs::write(
        schema_dir.join("b.sql"),
        "-- require: a.sql\nCREATE SCHEMA b;",
    )?;

    fs::write(
        schema_dir.join("c.sql"),
        "-- require: b.sql\nCREATE SCHEMA c;",
    )?;

    fs::write(
        schema_dir.join("d.sql"),
        "-- require: c.sql\nCREATE SCHEMA d;",
    )?;

    // Add another file that depends on multiple previous files
    fs::write(
        schema_dir.join("final.sql"),
        "-- require: a.sql, c.sql\nCREATE TABLE final_table (id INT);",
    )?;

    let config = SchemaLoaderConfig::new(schema_dir);
    let loader = SchemaLoader::new(config);

    let files = loader.load_ordered_schema_files()?;

    // Verify correct ordering
    let a_idx = find_file_index(&files, "a.sql");
    let b_idx = find_file_index(&files, "b.sql");
    let c_idx = find_file_index(&files, "c.sql");
    let d_idx = find_file_index(&files, "d.sql");
    let final_idx = find_file_index(&files, "final.sql");

    assert!(a_idx < b_idx);
    assert!(b_idx < c_idx);
    assert!(c_idx < d_idx);
    assert!(a_idx < final_idx);
    assert!(c_idx < final_idx);

    Ok(())
}

#[tokio::test]
async fn test_schema_loader_error_on_circular_dependency() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let schema_dir = temp_dir.path().join("schema");
    fs::create_dir_all(&schema_dir)?;

    // Create circular dependency: a -> b -> a
    fs::write(
        schema_dir.join("a.sql"),
        "-- require: b.sql\nCREATE TABLE a (id INT);",
    )?;

    fs::write(
        schema_dir.join("b.sql"),
        "-- require: a.sql\nCREATE TABLE b (id INT);",
    )?;

    let config = SchemaLoaderConfig::new(schema_dir);
    let loader = SchemaLoader::new(config);

    let result = loader.load_ordered_schema_files();
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Circular dependency")
    );

    Ok(())
}

#[tokio::test]
async fn test_schema_loader_error_on_missing_dependency() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let schema_dir = temp_dir.path().join("schema");
    fs::create_dir_all(&schema_dir)?;

    // Create file that depends on missing file
    fs::write(
        schema_dir.join("table.sql"),
        "-- require: missing.sql\nCREATE TABLE users (id INT);",
    )?;

    let config = SchemaLoaderConfig::new(schema_dir);
    let loader = SchemaLoader::new(config);

    let result = loader.load_ordered_schema_files();
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Missing dependency")
    );

    Ok(())
}

#[tokio::test]
async fn test_schema_loader_multiple_dependencies_on_one_line() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let schema_dir = temp_dir.path().join("schema");
    fs::create_dir_all(&schema_dir)?;

    // Create dependencies
    fs::write(schema_dir.join("base.sql"), "CREATE SCHEMA base;")?;
    fs::write(
        schema_dir.join("types.sql"),
        "-- require: base.sql\nCREATE TYPE base.status AS ENUM ('active');",
    )?;
    fs::write(
        schema_dir.join("functions.sql"),
        "-- require: base.sql\nCREATE FUNCTION base.get_status() RETURNS TEXT AS $$ SELECT 'active' $$ LANGUAGE SQL;",
    )?;

    // Create file with multiple dependencies
    fs::write(
        schema_dir.join("complex.sql"),
        "-- require: base.sql, types.sql, functions.sql\nCREATE TABLE base.complex (id INT, status base.status);",
    )?;

    let config = SchemaLoaderConfig::new(schema_dir);
    let loader = SchemaLoader::new(config);

    let files = loader.load_ordered_schema_files()?;

    let base_idx = find_file_index(&files, "base.sql");
    let types_idx = find_file_index(&files, "types.sql");
    let functions_idx = find_file_index(&files, "functions.sql");
    let complex_idx = find_file_index(&files, "complex.sql");

    // Complex should come after all its dependencies
    assert!(base_idx < complex_idx);
    assert!(types_idx < complex_idx);
    assert!(functions_idx < complex_idx);

    Ok(())
}

#[tokio::test]
async fn test_schema_loader_normalizes_paths_without_sql_extension() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let schema_dir = temp_dir.path().join("schema");
    fs::create_dir_all(&schema_dir)?;

    // Create dependencies
    fs::write(schema_dir.join("base.sql"), "CREATE SCHEMA base;")?;

    // Create file that references dependency without .sql extension
    fs::write(
        schema_dir.join("table.sql"),
        "-- require: base\nCREATE TABLE base.users (id INT);",
    )?;

    let config = SchemaLoaderConfig::new(schema_dir);
    let loader = SchemaLoader::new(config);

    let files = loader.load_ordered_schema_files()?;

    let base_idx = find_file_index(&files, "base.sql");
    let table_idx = find_file_index(&files, "table.sql");

    assert!(base_idx < table_idx);

    Ok(())
}
