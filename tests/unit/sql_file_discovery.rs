/// Unit tests for SQL file discovery functionality
/// Tests the discovery and ordering of SQL migration files from various sources
use anyhow::Result;
use std::fs;
use tempfile::TempDir;

/// Test enhanced SQL file discovery functionality with various migration patterns
#[test]
fn test_enhanced_sql_file_discovery() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let sql_dir = temp_dir.path().join("sql_files");
    fs::create_dir_all(&sql_dir)?;

    // Create test SQL files
    fs::write(sql_dir.join("001_schema.sql"), "CREATE SCHEMA test;")?;
    fs::write(
        sql_dir.join("002_tables.sql"),
        "CREATE TABLE test.users (id SERIAL);",
    )?;
    fs::write(sql_dir.join("README.txt"), "This should be ignored")?;

    // Test file discovery
    let files = pgmt::db::sql_executor::discover_sql_files_ordered(&sql_dir)?;

    assert_eq!(files.len(), 2);

    let file_names: Vec<String> = files
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
        .collect();

    assert_eq!(file_names, vec!["001_schema.sql", "002_tables.sql"]);

    Ok(())
}

/// Test Prisma-style migration discovery
#[test]
fn test_prisma_style_migration_discovery() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let prisma_dir = temp_dir.path().join("prisma/migrations");

    // Create Prisma-style structure
    let migration1 = prisma_dir.join("20231201120000_init");
    let migration2 = prisma_dir.join("20231201130000_add_users");

    fs::create_dir_all(&migration1)?;
    fs::create_dir_all(&migration2)?;

    fs::write(migration1.join("migration.sql"), "CREATE SCHEMA app;")?;
    fs::write(
        migration2.join("migration.sql"),
        "CREATE TABLE app.users (id SERIAL);",
    )?;

    let files = pgmt::db::sql_executor::discover_sql_files_ordered(&prisma_dir)?;
    assert_eq!(files.len(), 2);

    // Verify chronological ordering
    let file_paths: Vec<String> = files
        .iter()
        .map(|p| p.to_string_lossy().to_string())
        .collect();

    assert!(file_paths[0].contains("20231201120000_init"));
    assert!(file_paths[1].contains("20231201130000_add_users"));

    Ok(())
}

/// Test mixed migration structure discovery
#[test]
fn test_mixed_migration_structure_discovery() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let migrations_dir = temp_dir.path().join("migrations");
    fs::create_dir_all(&migrations_dir)?;

    // Direct SQL files (Flyway-style)
    fs::write(migrations_dir.join("V1__initial.sql"), "CREATE SCHEMA app;")?;
    fs::write(
        migrations_dir.join("V3__final.sql"),
        "ALTER TABLE app.users ADD COLUMN email TEXT;",
    )?;

    // Directory-based migration (Prisma-style)
    let migration2 = migrations_dir.join("20231201130000_add_users");
    fs::create_dir_all(&migration2)?;
    fs::write(
        migration2.join("migration.sql"),
        "CREATE TABLE app.users (id SERIAL);",
    )?;

    // Rails-style up/down
    let migration4 = migrations_dir.join("004_add_indexes");
    fs::create_dir_all(&migration4)?;
    fs::write(
        migration4.join("up.sql"),
        "CREATE INDEX idx_users ON app.users (id);",
    )?;
    fs::write(migration4.join("down.sql"), "DROP INDEX idx_users;")?;

    let files = pgmt::db::sql_executor::discover_sql_files_ordered(&migrations_dir)?;
    assert_eq!(files.len(), 5); // V1, V3, migration.sql, up.sql, down.sql

    let file_names: Vec<String> = files
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
        .collect();

    assert!(file_names.contains(&"V1__initial.sql".to_string()));
    assert!(file_names.contains(&"V3__final.sql".to_string()));
    assert!(file_names.contains(&"migration.sql".to_string()));
    assert!(file_names.contains(&"up.sql".to_string()));
    assert!(file_names.contains(&"down.sql".to_string()));

    Ok(())
}

/// Test error handling for nonexistent directories
#[test]
fn test_nonexistent_directory_error() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let nonexistent = temp_dir.path().join("does_not_exist");

    let result = pgmt::db::sql_executor::discover_sql_files_ordered(&nonexistent);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Directory does not exist")
    );

    Ok(())
}

/// Test empty directory handling (should warn but not fail)
#[test]
fn test_empty_directory_handling() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let empty_dir = temp_dir.path().join("empty_migrations");
    fs::create_dir_all(&empty_dir)?;

    // Should succeed but return empty list
    let files = pgmt::db::sql_executor::discover_sql_files_ordered(&empty_dir)?;
    assert_eq!(files.len(), 0);

    Ok(())
}

/// Test alternative SQL file extensions
#[test]
fn test_alternative_sql_extensions() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let migrations_dir = temp_dir.path().join("migrations");
    fs::create_dir_all(&migrations_dir)?;

    // Various SQL extensions
    fs::write(migrations_dir.join("001_schema.sql"), "CREATE SCHEMA app;")?;
    fs::write(
        migrations_dir.join("002_tables.psql"),
        "CREATE TABLE app.users (id SERIAL);",
    )?;
    fs::write(
        migrations_dir.join("003_indexes.pgsql"),
        "CREATE INDEX idx_users ON app.users (id);",
    )?;

    let files = pgmt::db::sql_executor::discover_sql_files_ordered(&migrations_dir)?;
    assert_eq!(files.len(), 3);

    let file_names: Vec<String> = files
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
        .collect();

    assert!(file_names.contains(&"001_schema.sql".to_string()));
    assert!(file_names.contains(&"002_tables.psql".to_string()));
    assert!(file_names.contains(&"003_indexes.pgsql".to_string()));

    Ok(())
}
