use anyhow::Result;
use pgmt::db::sql_executor::discover_sql_files_ordered;
use std::fs;
use tempfile::TempDir;

/// Test enhanced migration directory discovery with various real-world patterns

#[test]
fn test_prisma_migration_structure() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let migrations_dir = temp_dir.path().join("prisma/migrations");

    // Create Prisma-style migration structure
    // migrations/
    //   20231201120000_init/
    //     migration.sql
    //   20231201130000_add_users/
    //     migration.sql
    //   20231201140000_add_indexes/
    //     migration.sql

    let migration1 = migrations_dir.join("20231201120000_init");
    let migration2 = migrations_dir.join("20231201130000_add_users");
    let migration3 = migrations_dir.join("20231201140000_add_indexes");

    fs::create_dir_all(&migration1)?;
    fs::create_dir_all(&migration2)?;
    fs::create_dir_all(&migration3)?;

    fs::write(migration1.join("migration.sql"), "CREATE SCHEMA app;")?;
    fs::write(
        migration2.join("migration.sql"),
        "CREATE TABLE app.users (id SERIAL PRIMARY KEY);",
    )?;
    fs::write(
        migration3.join("migration.sql"),
        "CREATE INDEX idx_users_id ON app.users (id);",
    )?;

    let files = discover_sql_files_ordered(&migrations_dir)?;

    assert_eq!(files.len(), 3);

    // Verify alphabetical ordering by directory name
    let file_paths: Vec<String> = files
        .iter()
        .map(|p| p.to_string_lossy().to_string())
        .collect();

    assert!(file_paths[0].contains("20231201120000_init"));
    assert!(file_paths[1].contains("20231201130000_add_users"));
    assert!(file_paths[2].contains("20231201140000_add_indexes"));

    Ok(())
}

#[test]
fn test_flyway_migration_structure() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let migrations_dir = temp_dir.path().join("db/migration");
    fs::create_dir_all(&migrations_dir)?;

    // Create Flyway-style migrations
    // V1__Initial_schema.sql
    // V2__Add_users_table.sql
    // V3__Add_indexes.sql

    fs::write(
        migrations_dir.join("V1__Initial_schema.sql"),
        "CREATE SCHEMA app;",
    )?;
    fs::write(
        migrations_dir.join("V2__Add_users_table.sql"),
        "CREATE TABLE app.users (id SERIAL);",
    )?;
    fs::write(
        migrations_dir.join("V3__Add_indexes.sql"),
        "CREATE INDEX idx_users ON app.users (id);",
    )?;

    let files = discover_sql_files_ordered(&migrations_dir)?;

    assert_eq!(files.len(), 3);

    let file_names: Vec<String> = files
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
        .collect();

    assert_eq!(
        file_names,
        vec![
            "V1__Initial_schema.sql",
            "V2__Add_users_table.sql",
            "V3__Add_indexes.sql"
        ]
    );

    Ok(())
}

#[test]
fn test_mixed_migration_structure() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let base_dir = temp_dir.path().join("migrations");
    fs::create_dir_all(&base_dir)?;

    // Create mixed structure with various patterns
    // migrations/
    //   001_initial.sql (direct file)
    //   002_users/
    //     up.sql
    //     down.sql
    //   20231201_add_indexes/
    //     migration.sql
    //   V3__final_schema.sql (direct file)

    fs::write(base_dir.join("001_initial.sql"), "CREATE SCHEMA app;")?;

    let users_dir = base_dir.join("002_users");
    fs::create_dir_all(&users_dir)?;
    fs::write(
        users_dir.join("up.sql"),
        "CREATE TABLE app.users (id SERIAL);",
    )?;
    fs::write(users_dir.join("down.sql"), "DROP TABLE app.users;")?;

    let indexes_dir = base_dir.join("20231201_add_indexes");
    fs::create_dir_all(&indexes_dir)?;
    fs::write(
        indexes_dir.join("migration.sql"),
        "CREATE INDEX idx_users ON app.users (id);",
    )?;

    fs::write(
        base_dir.join("V3__final_schema.sql"),
        "ALTER TABLE app.users ADD COLUMN email TEXT;",
    )?;

    let files = discover_sql_files_ordered(&base_dir)?;

    assert_eq!(files.len(), 5);

    let file_names: Vec<String> = files
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
        .collect();

    // Should be sorted alphabetically by full path
    assert!(file_names.contains(&"001_initial.sql".to_string()));
    assert!(file_names.contains(&"up.sql".to_string()));
    assert!(file_names.contains(&"down.sql".to_string()));
    assert!(file_names.contains(&"migration.sql".to_string()));
    assert!(file_names.contains(&"V3__final_schema.sql".to_string()));

    Ok(())
}

#[test]
fn test_rails_migration_structure() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let migrations_dir = temp_dir.path().join("db/migrate");
    fs::create_dir_all(&migrations_dir)?;

    // Create Rails-style migrations
    // 20231201120000_create_users.rb (should be ignored)
    // 20231201120000_create_users.sql (custom SQL version)
    // 20231201130000_add_indexes.sql

    fs::write(
        migrations_dir.join("20231201120000_create_users.rb"),
        "# Ruby migration file",
    )?;
    fs::write(
        migrations_dir.join("20231201120000_create_users.sql"),
        "CREATE TABLE users (id SERIAL);",
    )?;
    fs::write(
        migrations_dir.join("20231201130000_add_indexes.sql"),
        "CREATE INDEX idx_users ON users (id);",
    )?;

    let files = discover_sql_files_ordered(&migrations_dir)?;

    // Should only find SQL files, not Ruby files
    assert_eq!(files.len(), 2);

    let file_names: Vec<String> = files
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
        .collect();

    assert!(file_names.contains(&"20231201120000_create_users.sql".to_string()));
    assert!(file_names.contains(&"20231201130000_add_indexes.sql".to_string()));
    assert!(!file_names.iter().any(|f| f.contains(".rb")));

    Ok(())
}

#[test]
fn test_empty_directory_warning() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let empty_dir = temp_dir.path().join("empty_migrations");
    fs::create_dir_all(&empty_dir)?;

    // Should succeed but provide warning (captured in stderr)
    let files = discover_sql_files_ordered(&empty_dir)?;
    assert_eq!(files.len(), 0);

    Ok(())
}

#[test]
fn test_directory_with_no_sql_files() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let dir = temp_dir.path().join("no_sql_files");
    fs::create_dir_all(&dir)?;

    // Create non-SQL files
    fs::write(dir.join("README.md"), "# Migration documentation")?;
    fs::write(dir.join("config.yaml"), "version: 1")?;
    fs::write(dir.join("script.py"), "print('hello')")?;

    // Create subdirectories that look like migrations but have no SQL
    let migration_dir = dir.join("20231201120000_init");
    fs::create_dir_all(&migration_dir)?;
    fs::write(migration_dir.join("notes.txt"), "Migration notes")?;

    let files = discover_sql_files_ordered(&dir)?;
    assert_eq!(files.len(), 0);

    Ok(())
}

#[test]
fn test_nonexistent_directory() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let nonexistent = temp_dir.path().join("does_not_exist");

    let result = discover_sql_files_ordered(&nonexistent);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Directory does not exist")
    );

    Ok(())
}

#[test]
fn test_file_instead_of_directory() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let file_path = temp_dir.path().join("not_a_directory.sql");
    fs::write(&file_path, "CREATE TABLE test (id SERIAL);")?;

    let result = discover_sql_files_ordered(&file_path);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Path is not a directory")
    );

    Ok(())
}

#[test]
fn test_deeply_nested_migrations() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let base_dir = temp_dir.path().join("complex/nested/migration/structure");
    fs::create_dir_all(&base_dir)?;

    // Create deeply nested structure
    // complex/nested/migration/structure/
    //   001_schemas/
    //     app.sql
    //     auth.sql
    //   002_tables/
    //     users/
    //       create.sql
    //       indexes.sql
    //     posts/
    //       create.sql

    let schemas_dir = base_dir.join("001_schemas");
    fs::create_dir_all(&schemas_dir)?;
    fs::write(schemas_dir.join("app.sql"), "CREATE SCHEMA app;")?;
    fs::write(schemas_dir.join("auth.sql"), "CREATE SCHEMA auth;")?;

    let users_dir = base_dir.join("002_tables/users");
    fs::create_dir_all(&users_dir)?;
    fs::write(
        users_dir.join("create.sql"),
        "CREATE TABLE app.users (id SERIAL);",
    )?;
    fs::write(
        users_dir.join("indexes.sql"),
        "CREATE INDEX idx_users ON app.users (id);",
    )?;

    let posts_dir = base_dir.join("002_tables/posts");
    fs::create_dir_all(&posts_dir)?;
    fs::write(
        posts_dir.join("create.sql"),
        "CREATE TABLE app.posts (id SERIAL);",
    )?;

    let files = discover_sql_files_ordered(&base_dir)?;

    assert_eq!(files.len(), 5);

    // Verify all files were found
    let file_names: Vec<String> = files
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
        .collect();

    assert!(file_names.contains(&"app.sql".to_string()));
    assert!(file_names.contains(&"auth.sql".to_string()));
    assert!(file_names.contains(&"create.sql".to_string()));
    assert!(file_names.contains(&"indexes.sql".to_string()));
    // Note: there are two create.sql files, so count should be correct

    Ok(())
}

#[test]
fn test_alternative_sql_extensions() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let migrations_dir = temp_dir.path().join("migrations");
    fs::create_dir_all(&migrations_dir)?;

    // Test various SQL file extensions
    fs::write(migrations_dir.join("001_schema.sql"), "CREATE SCHEMA app;")?;
    fs::write(
        migrations_dir.join("002_tables.psql"),
        "CREATE TABLE app.users (id SERIAL);",
    )?;
    fs::write(
        migrations_dir.join("003_indexes.pgsql"),
        "CREATE INDEX idx_users ON app.users (id);",
    )?;

    let files = discover_sql_files_ordered(&migrations_dir)?;

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

#[test]
fn test_migration_files_without_extensions() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let migrations_dir = temp_dir.path().join("migrations");
    fs::create_dir_all(&migrations_dir)?;

    // Create files that should be recognized as SQL based on content
    fs::write(migrations_dir.join("001_schema.up"), "CREATE SCHEMA app;")?;
    fs::write(migrations_dir.join("001_schema.down"), "DROP SCHEMA app;")?;
    fs::write(
        migrations_dir.join("migration_001"),
        "CREATE TABLE app.users (id SERIAL PRIMARY KEY);",
    )?;

    // Create a non-SQL file with similar name (should be ignored)
    fs::write(
        migrations_dir.join("readme_migration"),
        "This is documentation",
    )?;

    let files = discover_sql_files_ordered(&migrations_dir)?;

    // Should find the files with SQL content
    assert_eq!(files.len(), 3);

    let file_names: Vec<String> = files
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
        .collect();

    assert!(file_names.contains(&"001_schema.up".to_string()));
    assert!(file_names.contains(&"001_schema.down".to_string()));
    assert!(file_names.contains(&"migration_001".to_string()));
    assert!(!file_names.contains(&"readme_migration".to_string()));

    Ok(())
}

#[test]
fn test_uuid_based_migration_directories() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let migrations_dir = temp_dir.path().join("migrations");
    fs::create_dir_all(&migrations_dir)?;

    // Create UUID-based migration directories (some tools use these)
    let uuid1 = "550e8400-e29b-41d4-a716-446655440000";
    let uuid2 = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";

    let migration1 = migrations_dir.join(uuid1);
    let migration2 = migrations_dir.join(uuid2);

    fs::create_dir_all(&migration1)?;
    fs::create_dir_all(&migration2)?;

    fs::write(migration1.join("up.sql"), "CREATE SCHEMA app;")?;
    fs::write(
        migration2.join("up.sql"),
        "CREATE TABLE app.users (id SERIAL);",
    )?;

    let files = discover_sql_files_ordered(&migrations_dir)?;

    assert_eq!(files.len(), 2);

    Ok(())
}

#[test]
fn test_very_large_migration_directory() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let migrations_dir = temp_dir.path().join("migrations");
    fs::create_dir_all(&migrations_dir)?;

    // Create a large number of migrations to test performance and output truncation
    for i in 1..=20 {
        let migration_dir = migrations_dir.join(format!("{:03}_migration_{}", i, i));
        fs::create_dir_all(&migration_dir)?;
        fs::write(
            migration_dir.join("migration.sql"),
            format!("-- Migration {}\nCREATE TABLE table_{} (id SERIAL);", i, i),
        )?;
    }

    let files = discover_sql_files_ordered(&migrations_dir)?;

    assert_eq!(files.len(), 20);

    // Verify ordering
    let first_file = files[0].to_string_lossy();
    let last_file = files[19].to_string_lossy();

    assert!(first_file.contains("001_migration_1"));
    assert!(last_file.contains("020_migration_20"));

    Ok(())
}
