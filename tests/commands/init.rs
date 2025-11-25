use anyhow::Result;
use pgmt::commands::init::import::ImportSource;
use pgmt::commands::init::project::{create_project_structure, generate_config_file};
use pgmt::commands::init::{BaselineCreationConfig, InitOptions, ObjectManagementConfig};
use pgmt::db::sql_executor::discover_sql_files_ordered;
use pgmt::prompts::ShadowDatabaseInput;
use std::env;
use std::fs;
use std::path::PathBuf;

#[test]
fn test_import_source_variants() {
    let dir_import = ImportSource::Directory(PathBuf::from("./migrations"));
    let file_import = ImportSource::SqlFile(PathBuf::from("./dump.sql"));
    let db_import = ImportSource::Database("postgres://localhost/test".to_string());

    match dir_import {
        ImportSource::Directory(path) => assert_eq!(path, PathBuf::from("./migrations")),
        _ => panic!("Expected Directory variant"),
    }

    match file_import {
        ImportSource::SqlFile(path) => assert_eq!(path, PathBuf::from("./dump.sql")),
        _ => panic!("Expected SqlFile variant"),
    }

    match db_import {
        ImportSource::Database(url) => assert_eq!(url, "postgres://localhost/test"),
        _ => panic!("Expected Database variant"),
    }
}

#[test]
fn test_object_management_config_default() {
    let config = ObjectManagementConfig::default();
    assert!(config.comments);
    assert!(config.grants);
    assert!(config.triggers);
    assert!(config.extensions);
}

#[test]
fn test_object_management_config_custom() {
    let config = ObjectManagementConfig {
        comments: false,
        grants: true,
        triggers: false,
        extensions: true,
    };

    assert!(!config.comments);
    assert!(config.grants);
    assert!(!config.triggers);
    assert!(config.extensions);
}

#[test]
fn test_discover_sql_files_ordered() -> Result<()> {
    let temp_dir = env::temp_dir().join("pgmt_test_sql_discovery");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir)?;

    // Create test SQL files in non-alphabetical order
    fs::write(
        temp_dir.join("03_views.sql"),
        "CREATE VIEW test_view AS SELECT 1;",
    )?;
    fs::write(temp_dir.join("01_schema.sql"), "CREATE SCHEMA test;")?;
    fs::write(
        temp_dir.join("02_tables.sql"),
        "CREATE TABLE test.users (id SERIAL);",
    )?;
    fs::write(
        temp_dir.join("04_data.sql"),
        "INSERT INTO test.users DEFAULT VALUES;",
    )?;

    // Create a subdirectory with more files
    let sub_dir = temp_dir.join("functions");
    fs::create_dir_all(&sub_dir)?;
    fs::write(
        sub_dir.join("01_helpers.sql"),
        "CREATE FUNCTION test_fn() RETURNS INT AS $$ SELECT 1 $$ LANGUAGE SQL;",
    )?;

    // Create a non-SQL file that should be ignored
    fs::write(temp_dir.join("README.txt"), "This should be ignored")?;

    let files = discover_sql_files_ordered(&temp_dir)?;

    // Should find 5 SQL files total
    assert_eq!(files.len(), 5);

    // Verify alphabetical ordering
    let file_names: Vec<String> = files
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
        .collect();

    assert_eq!(file_names[0], "01_schema.sql");
    assert_eq!(file_names[1], "02_tables.sql");
    assert_eq!(file_names[2], "03_views.sql");
    assert_eq!(file_names[3], "04_data.sql");
    assert_eq!(file_names[4], "01_helpers.sql");

    // Cleanup
    fs::remove_dir_all(&temp_dir)?;
    Ok(())
}

#[test]
fn test_discover_sql_files_empty_directory() -> Result<()> {
    let temp_dir = env::temp_dir().join("pgmt_test_empty_dir");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir)?;

    let files = discover_sql_files_ordered(&temp_dir)?;
    assert_eq!(files.len(), 0);

    // Cleanup
    fs::remove_dir_all(&temp_dir)?;
    Ok(())
}

#[test]
fn test_create_project_structure() -> Result<()> {
    let temp_dir = env::temp_dir().join("pgmt_test_project_structure");
    let _ = fs::remove_dir_all(&temp_dir);

    let schema_dir = PathBuf::from("schema");
    create_project_structure(&temp_dir, &schema_dir)?;

    // Verify main directories
    assert!(temp_dir.exists());
    assert!(temp_dir.join("schema").exists());
    assert!(temp_dir.join("migrations").exists());
    assert!(temp_dir.join("schema_baselines").exists());

    // Verify schema subdirectories
    assert!(temp_dir.join("schema/tables").exists());
    assert!(temp_dir.join("schema/views").exists());
    assert!(temp_dir.join("schema/functions").exists());

    // Project structure creation is complete

    // Cleanup
    fs::remove_dir_all(&temp_dir)?;
    Ok(())
}

#[test]
fn test_create_project_structure_custom_schema_dir() -> Result<()> {
    let temp_dir = env::temp_dir().join("pgmt_test_custom_schema");
    let _ = fs::remove_dir_all(&temp_dir);

    let schema_dir = PathBuf::from("custom_schema");
    create_project_structure(&temp_dir, &schema_dir)?;

    // Verify custom schema directory structure
    assert!(temp_dir.join("custom_schema").exists());
    assert!(temp_dir.join("custom_schema/tables").exists());
    assert!(temp_dir.join("custom_schema/views").exists());
    assert!(temp_dir.join("custom_schema/functions").exists());

    // Cleanup
    fs::remove_dir_all(&temp_dir)?;
    Ok(())
}

#[test]
fn test_create_project_structure_existing_gitignore() -> Result<()> {
    let temp_dir = env::temp_dir().join("pgmt_test_existing_gitignore");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir)?;

    // Pre-create a .gitignore with some content
    let existing_content = "# Existing content\n*.log\n";
    fs::write(temp_dir.join(".gitignore"), existing_content)?;

    let schema_dir = PathBuf::from("schema");
    create_project_structure(&temp_dir, &schema_dir)?;

    // Verify that existing .gitignore was not overwritten
    let gitignore_content = fs::read_to_string(temp_dir.join(".gitignore"))?;
    assert_eq!(gitignore_content, existing_content);

    // Cleanup
    fs::remove_dir_all(&temp_dir)?;
    Ok(())
}

#[test]
fn test_generate_config_file_auto_shadow() -> Result<()> {
    let temp_dir = env::temp_dir().join("pgmt_test_config_auto");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir)?;

    let options = InitOptions {
        project_dir: temp_dir.clone(),
        dev_database_url: "postgres://localhost/myapp_dev".to_string(),
        shadow_config: ShadowDatabaseInput::Auto,
        shadow_pg_version: None,
        schema_dir: PathBuf::from("schema"),
        import_source: None,
        object_config: ObjectManagementConfig::default(),
        baseline_config: BaselineCreationConfig::default(),
        tracking_table: pgmt::config::types::TrackingTable::default(),
        roles_file: None,
    };

    generate_config_file(&options, &temp_dir)?;

    let config_path = temp_dir.join("pgmt.yaml");
    assert!(config_path.exists());

    let config_content = fs::read_to_string(config_path)?;

    // Verify config content
    assert!(config_content.contains("dev_url: postgres://localhost/myapp_dev"));
    assert!(config_content.contains("auto: true"));
    assert!(config_content.contains("schema_dir: schema"));
    assert!(config_content.contains("migrations_dir: migrations"));
    assert!(config_content.contains("baselines_dir: schema_baselines"));
    assert!(config_content.contains("comments: true"));
    assert!(config_content.contains("grants: true"));
    assert!(config_content.contains("triggers: true"));
    assert!(config_content.contains("extensions: true"));

    // Cleanup
    fs::remove_dir_all(&temp_dir)?;
    Ok(())
}

#[test]
fn test_generate_config_file_manual_shadow() -> Result<()> {
    let temp_dir = env::temp_dir().join("pgmt_test_config_manual");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir)?;

    let options = InitOptions {
        project_dir: temp_dir.clone(),
        dev_database_url: "postgres://localhost/myapp_dev".to_string(),
        shadow_config: ShadowDatabaseInput::Manual("postgres://localhost/myapp_shadow".to_string()),
        shadow_pg_version: None,
        schema_dir: PathBuf::from("custom_schema"),
        import_source: None,
        object_config: ObjectManagementConfig {
            comments: false,
            grants: true,
            triggers: false,
            extensions: true,
        },
        baseline_config: BaselineCreationConfig::default(),
        tracking_table: pgmt::config::types::TrackingTable::default(),
        roles_file: None,
    };

    generate_config_file(&options, &temp_dir)?;

    let config_path = temp_dir.join("pgmt.yaml");
    assert!(config_path.exists());

    let config_content = fs::read_to_string(config_path)?;

    // Verify manual shadow config
    assert!(config_content.contains("auto: false"));
    assert!(config_content.contains("url: postgres://localhost/myapp_shadow"));
    assert!(config_content.contains("schema_dir: custom_schema"));

    // Verify object management config
    assert!(config_content.contains("comments: false"));
    assert!(config_content.contains("grants: true"));
    assert!(config_content.contains("triggers: false"));
    assert!(config_content.contains("extensions: true"));

    // Cleanup
    fs::remove_dir_all(&temp_dir)?;
    Ok(())
}

#[test]
fn test_init_options_debug() {
    // Test that InitOptions can be debugged (has Debug trait)
    let options = InitOptions {
        project_dir: PathBuf::from("/tmp/test"),
        dev_database_url: "postgres://localhost/test".to_string(),
        shadow_config: ShadowDatabaseInput::Auto,
        shadow_pg_version: None,
        schema_dir: PathBuf::from("schema"),
        import_source: Some(ImportSource::Directory(PathBuf::from("migrations"))),
        object_config: ObjectManagementConfig::default(),
        baseline_config: BaselineCreationConfig::default(),
        tracking_table: pgmt::config::types::TrackingTable::default(),
        roles_file: None,
    };

    let debug_output = format!("{:?}", options);
    assert!(debug_output.contains("InitOptions"));
    assert!(debug_output.contains("postgres://localhost/test"));
    assert!(debug_output.contains("schema"));
}
