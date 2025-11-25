use anyhow::Result;
use pgmt::commands::init::import::ImportSource;
use pgmt::commands::init::{BaselineCreationConfig, InitOptions, ObjectManagementConfig};
use pgmt::prompts::ShadowDatabaseInput;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

use crate::helpers::harness::with_test_db;

/// Integration tests for the init command end-to-end workflow
/// These tests validate the complete init process including:
/// - Project structure creation
/// - Configuration file generation
/// - Schema import from various sources
/// - Co-located file organization
/// - Database catalog loading and validation
/// - Context-aware object management

#[tokio::test]
async fn test_init_workflow_minimal_setup() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let project_path = temp_dir.path().to_path_buf();

    let options = InitOptions {
        project_dir: project_path.clone(),
        dev_database_url: "postgres://localhost/test_minimal".to_string(),
        shadow_config: ShadowDatabaseInput::Auto,
        shadow_pg_version: None,
        schema_dir: PathBuf::from("schema"),
        import_source: None, // No schema import
        object_config: ObjectManagementConfig::default(),
        baseline_config: BaselineCreationConfig::default(),
        tracking_table: pgmt::config::types::TrackingTable::default(),
        roles_file: None,
    };

    pgmt::commands::init::project::create_project_structure(
        &options.project_dir,
        &options.schema_dir,
    )?;
    pgmt::commands::init::project::generate_config_file(&options, &options.project_dir)?;

    assert!(project_path.exists());
    assert!(project_path.join("schema").exists());
    assert!(project_path.join("schema/tables").exists());
    assert!(project_path.join("schema/views").exists());
    assert!(project_path.join("schema/functions").exists());
    assert!(project_path.join("migrations").exists());
    assert!(project_path.join("schema_baselines").exists());

    let config_path = project_path.join("pgmt.yaml");
    assert!(config_path.exists());

    let config_content = fs::read_to_string(config_path)?;
    assert!(config_content.contains("dev_url: postgres://localhost/test_minimal"));
    assert!(config_content.contains("auto: true"));
    assert!(config_content.contains("schema_dir: schema"));
    assert!(config_content.contains("comments: true"));
    assert!(config_content.contains("grants: true"));
    assert!(config_content.contains("triggers: true"));
    assert!(config_content.contains("extensions: true"));

    Ok(())
}

#[tokio::test]
async fn test_init_workflow_custom_configuration() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let project_path = temp_dir.path().to_path_buf();

    let options = InitOptions {
        project_dir: project_path.clone(),
        dev_database_url: "postgres://user:pass@localhost:5432/custom_db".to_string(),
        shadow_config: ShadowDatabaseInput::Manual(
            "postgres://localhost/custom_shadow".to_string(),
        ),
        shadow_pg_version: None,
        schema_dir: PathBuf::from("db_schema"),
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

    // Execute the init workflow
    pgmt::commands::init::project::create_project_structure(
        &options.project_dir,
        &options.schema_dir,
    )?;
    pgmt::commands::init::project::generate_config_file(&options, &options.project_dir)?;

    // Verify custom schema directory structure
    assert!(project_path.join("db_schema").exists());
    assert!(project_path.join("db_schema/tables").exists());
    assert!(project_path.join("db_schema/views").exists());
    assert!(project_path.join("db_schema/functions").exists());

    // Verify custom configuration
    let config_content = fs::read_to_string(project_path.join("pgmt.yaml"))?;
    assert!(config_content.contains("dev_url: postgres://user:pass@localhost:5432/custom_db"));
    assert!(config_content.contains("auto: false"));
    assert!(config_content.contains("url: postgres://localhost/custom_shadow"));
    assert!(config_content.contains("schema_dir: db_schema"));
    assert!(config_content.contains("comments: false"));
    assert!(config_content.contains("grants: true"));
    assert!(config_content.contains("triggers: false"));
    assert!(config_content.contains("extensions: true"));

    Ok(())
}

#[tokio::test]
async fn test_init_workflow_with_sql_file_import() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let project_path = temp_dir.path().to_path_buf();

    // Create a test SQL file to import
    let sql_file = temp_dir.path().join("existing_schema.sql");
    let sql_content = r#"
-- Sample SQL file for import testing
CREATE SCHEMA app;

CREATE TYPE app.priority AS ENUM ('low', 'medium', 'high');

CREATE TABLE app.users (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    priority app.priority DEFAULT 'medium'
);

CREATE VIEW app.active_users AS
SELECT id, email FROM app.users WHERE priority != 'low';

CREATE FUNCTION app.get_user_count()
RETURNS INTEGER AS $$
    SELECT COUNT(*) FROM app.users;
$$ LANGUAGE SQL;

CREATE INDEX idx_users_email ON app.users (email);

COMMENT ON TABLE app.users IS 'User accounts table';
COMMENT ON COLUMN app.users.email IS 'User email address';
"#;
    fs::write(&sql_file, sql_content)?;

    let options = InitOptions {
        project_dir: project_path.clone(),
        dev_database_url: "postgres://localhost/test_import".to_string(),
        shadow_config: ShadowDatabaseInput::Auto,
        shadow_pg_version: None,
        schema_dir: PathBuf::from("schema"),
        import_source: Some(ImportSource::SqlFile(sql_file)),
        object_config: ObjectManagementConfig::default(),
        baseline_config: BaselineCreationConfig::default(),
        tracking_table: pgmt::config::types::TrackingTable::default(),
        roles_file: None,
    };

    // Execute init workflow
    pgmt::commands::init::project::create_project_structure(
        &options.project_dir,
        &options.schema_dir,
    )?;
    pgmt::commands::init::project::generate_config_file(&options, &options.project_dir)?;

    // For this test, we verify the structure was created
    // The actual SQL import would require database connectivity
    // which is tested separately in database integration tests

    // Verify basic structure
    assert!(project_path.join("schema").exists());
    assert!(project_path.join("pgmt.yaml").exists());

    // Verify import source was properly configured
    // (The actual import logic would be tested with a real database)

    Ok(())
}

#[tokio::test]
async fn test_init_workflow_with_directory_import() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let project_path = temp_dir.path().to_path_buf();

    // Create a test directory structure to import
    let import_dir = temp_dir.path().join("existing_migrations");
    fs::create_dir_all(&import_dir)?;

    // Create some test SQL files in alphabetical order
    fs::write(
        import_dir.join("001_schemas.sql"),
        "CREATE SCHEMA auth;\nCREATE SCHEMA billing;",
    )?;
    fs::write(
        import_dir.join("002_types.sql"),
        "CREATE TYPE auth.role AS ENUM ('user', 'admin');",
    )?;
    fs::write(
        import_dir.join("003_tables.sql"),
        "CREATE TABLE auth.users (id SERIAL, role auth.role);",
    )?;
    fs::write(
        import_dir.join("004_indexes.sql"),
        "CREATE INDEX idx_users_role ON auth.users (role);",
    )?;

    let options = InitOptions {
        project_dir: project_path.clone(),
        dev_database_url: "postgres://localhost/test_dir_import".to_string(),
        shadow_config: ShadowDatabaseInput::Auto,
        shadow_pg_version: None,
        schema_dir: PathBuf::from("schema"),
        import_source: Some(ImportSource::Directory(import_dir.clone())),
        object_config: ObjectManagementConfig::default(),
        baseline_config: BaselineCreationConfig::default(),
        tracking_table: pgmt::config::types::TrackingTable::default(),
        roles_file: None,
    };

    // Execute init workflow
    pgmt::commands::init::project::create_project_structure(
        &options.project_dir,
        &options.schema_dir,
    )?;
    pgmt::commands::init::project::generate_config_file(&options, &options.project_dir)?;

    // Test the enhanced SQL file discovery functionality
    let discovered_files = pgmt::db::sql_executor::discover_sql_files_ordered(&import_dir)?;
    assert_eq!(discovered_files.len(), 4);

    // Verify files are in alphabetical order
    let file_names: Vec<String> = discovered_files
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
        .collect();

    assert_eq!(
        file_names,
        vec![
            "001_schemas.sql",
            "002_types.sql",
            "003_tables.sql",
            "004_indexes.sql"
        ]
    );

    // Verify project structure was created
    assert!(project_path.join("schema").exists());
    assert!(project_path.join("pgmt.yaml").exists());

    Ok(())
}

#[tokio::test]
async fn test_init_workflow_with_prisma_style_migrations() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let project_path = temp_dir.path().to_path_buf();

    // Create Prisma-style migration directory structure
    let prisma_migrations = temp_dir.path().join("prisma/migrations");

    let migration1 = prisma_migrations.join("20231201120000_init");
    let migration2 = prisma_migrations.join("20231201130000_add_users");
    let migration3 = prisma_migrations.join("20231201140000_add_indexes");

    fs::create_dir_all(&migration1)?;
    fs::create_dir_all(&migration2)?;
    fs::create_dir_all(&migration3)?;

    fs::write(migration1.join("migration.sql"), "CREATE SCHEMA app;")?;
    fs::write(
        migration2.join("migration.sql"),
        "CREATE TABLE app.users (id SERIAL PRIMARY KEY, email TEXT);",
    )?;
    fs::write(
        migration3.join("migration.sql"),
        "CREATE INDEX idx_users_email ON app.users (email);",
    )?;

    let options = InitOptions {
        project_dir: project_path.clone(),
        dev_database_url: "postgres://localhost/test_prisma_import".to_string(),
        shadow_config: ShadowDatabaseInput::Auto,
        shadow_pg_version: None,
        schema_dir: PathBuf::from("schema"),
        import_source: Some(ImportSource::Directory(prisma_migrations.clone())),
        object_config: ObjectManagementConfig::default(),
        baseline_config: BaselineCreationConfig::default(),
        tracking_table: pgmt::config::types::TrackingTable::default(),
        roles_file: None,
    };

    // Execute init workflow
    pgmt::commands::init::project::create_project_structure(
        &options.project_dir,
        &options.schema_dir,
    )?;
    pgmt::commands::init::project::generate_config_file(&options, &options.project_dir)?;

    // Test enhanced discovery with Prisma structure
    let discovered_files = pgmt::db::sql_executor::discover_sql_files_ordered(&prisma_migrations)?;
    assert_eq!(discovered_files.len(), 3);

    // Verify chronological ordering by timestamp
    let file_paths: Vec<String> = discovered_files
        .iter()
        .map(|p| p.to_string_lossy().to_string())
        .collect();

    assert!(file_paths[0].contains("20231201120000_init"));
    assert!(file_paths[1].contains("20231201130000_add_users"));
    assert!(file_paths[2].contains("20231201140000_add_indexes"));

    Ok(())
}

#[tokio::test]
async fn test_init_workflow_with_mixed_migration_structure() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let project_path = temp_dir.path().to_path_buf();

    // Create complex mixed migration structure
    let migrations_dir = temp_dir.path().join("db/migrations");
    fs::create_dir_all(&migrations_dir)?;

    // Direct SQL files
    fs::write(
        migrations_dir.join("V1__initial_schema.sql"),
        "CREATE SCHEMA app;",
    )?;
    fs::write(
        migrations_dir.join("V4__final_changes.sql"),
        "ALTER TABLE app.users ADD COLUMN created_at TIMESTAMP;",
    )?;

    // Timestamped directories (Prisma-style)
    let migration2 = migrations_dir.join("20231201130000_add_users");
    fs::create_dir_all(&migration2)?;
    fs::write(
        migration2.join("migration.sql"),
        "CREATE TABLE app.users (id SERIAL PRIMARY KEY);",
    )?;

    // Rails-style directory
    let migration3 = migrations_dir.join("003_add_indexes");
    fs::create_dir_all(&migration3)?;
    fs::write(
        migration3.join("up.sql"),
        "CREATE INDEX idx_users_id ON app.users (id);",
    )?;
    fs::write(migration3.join("down.sql"), "DROP INDEX idx_users_id;")?;

    let options = InitOptions {
        project_dir: project_path.clone(),
        dev_database_url: "postgres://localhost/test_mixed_import".to_string(),
        shadow_config: ShadowDatabaseInput::Auto,
        shadow_pg_version: None,
        schema_dir: PathBuf::from("schema"),
        import_source: Some(ImportSource::Directory(migrations_dir.clone())),
        object_config: ObjectManagementConfig::default(),
        baseline_config: BaselineCreationConfig::default(),
        tracking_table: pgmt::config::types::TrackingTable::default(),
        roles_file: None,
    };

    // Execute init workflow
    pgmt::commands::init::project::create_project_structure(
        &options.project_dir,
        &options.schema_dir,
    )?;
    pgmt::commands::init::project::generate_config_file(&options, &options.project_dir)?;

    // Test discovery of mixed structure
    let discovered_files = pgmt::db::sql_executor::discover_sql_files_ordered(&migrations_dir)?;
    assert_eq!(discovered_files.len(), 5); // V1, V4, migration.sql, up.sql, down.sql

    let file_names: Vec<String> = discovered_files
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
        .collect();

    assert!(file_names.contains(&"V1__initial_schema.sql".to_string()));
    assert!(file_names.contains(&"V4__final_changes.sql".to_string()));
    assert!(file_names.contains(&"migration.sql".to_string()));
    assert!(file_names.contains(&"up.sql".to_string()));
    assert!(file_names.contains(&"down.sql".to_string()));

    Ok(())
}

#[tokio::test]
async fn test_init_workflow_with_empty_migration_directory() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let project_path = temp_dir.path().to_path_buf();

    // Create empty migration directory
    let empty_migrations = temp_dir.path().join("empty_migrations");
    fs::create_dir_all(&empty_migrations)?;

    let options = InitOptions {
        project_dir: project_path.clone(),
        dev_database_url: "postgres://localhost/test_empty_import".to_string(),
        shadow_config: ShadowDatabaseInput::Auto,
        shadow_pg_version: None,
        schema_dir: PathBuf::from("schema"),
        import_source: Some(ImportSource::Directory(empty_migrations.clone())),
        object_config: ObjectManagementConfig::default(),
        baseline_config: BaselineCreationConfig::default(),
        tracking_table: pgmt::config::types::TrackingTable::default(),
        roles_file: None,
    };

    // Execute init workflow - should succeed even with empty directory
    pgmt::commands::init::project::create_project_structure(
        &options.project_dir,
        &options.schema_dir,
    )?;
    pgmt::commands::init::project::generate_config_file(&options, &options.project_dir)?;

    // Test discovery with empty directory (should warn but not fail)
    let discovered_files = pgmt::db::sql_executor::discover_sql_files_ordered(&empty_migrations)?;
    assert_eq!(discovered_files.len(), 0);

    // Project should still be created successfully
    assert!(project_path.join("schema").exists());
    assert!(project_path.join("pgmt.yaml").exists());

    Ok(())
}

#[tokio::test]
async fn test_init_workflow_preserves_existing_gitignore() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let project_path = temp_dir.path().to_path_buf();
    fs::create_dir_all(&project_path)?;

    // Pre-create a .gitignore with existing content
    let existing_gitignore_content =
        "# Existing project gitignore\n*.log\ntarget/\nnode_modules/\n";
    fs::write(project_path.join(".gitignore"), existing_gitignore_content)?;

    let options = InitOptions {
        project_dir: project_path.clone(),
        dev_database_url: "postgres://localhost/test_preserve".to_string(),
        shadow_config: ShadowDatabaseInput::Auto,
        shadow_pg_version: None,
        schema_dir: PathBuf::from("schema"),
        import_source: None,
        object_config: ObjectManagementConfig::default(),
        baseline_config: BaselineCreationConfig::default(),
        tracking_table: pgmt::config::types::TrackingTable::default(),
        roles_file: None,
    };

    // Execute init workflow
    pgmt::commands::init::project::create_project_structure(
        &options.project_dir,
        &options.schema_dir,
    )?;
    pgmt::commands::init::project::generate_config_file(&options, &options.project_dir)?;

    // Verify that existing .gitignore was preserved
    let final_gitignore_content = fs::read_to_string(project_path.join(".gitignore"))?;
    assert_eq!(final_gitignore_content, existing_gitignore_content);

    // Verify other directories were still created
    assert!(project_path.join("schema").exists());
    assert!(project_path.join("migrations").exists());
    assert!(project_path.join("pgmt.yaml").exists());

    Ok(())
}

#[tokio::test]
async fn test_init_workflow_nested_project_directory() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let project_path = temp_dir
        .path()
        .join("level1")
        .join("level2")
        .join("my_project");

    let options = InitOptions {
        project_dir: project_path.clone(),
        dev_database_url: "postgres://localhost/test_nested".to_string(),
        shadow_config: ShadowDatabaseInput::Auto,
        shadow_pg_version: None,
        schema_dir: PathBuf::from("db_structure"),
        import_source: None,
        object_config: ObjectManagementConfig::default(),
        baseline_config: BaselineCreationConfig::default(),
        tracking_table: pgmt::config::types::TrackingTable::default(),
        roles_file: None,
    };

    // Execute init workflow - should create nested directories
    pgmt::commands::init::project::create_project_structure(
        &options.project_dir,
        &options.schema_dir,
    )?;
    pgmt::commands::init::project::generate_config_file(&options, &options.project_dir)?;

    // Verify nested structure was created
    assert!(project_path.exists());
    assert!(project_path.join("db_structure").exists());
    assert!(project_path.join("db_structure/tables").exists());
    assert!(project_path.join("db_structure/views").exists());
    assert!(project_path.join("db_structure/functions").exists());
    assert!(project_path.join("migrations").exists());
    assert!(project_path.join("schema_baselines").exists());
    assert!(project_path.join("pgmt.yaml").exists());

    Ok(())
}

#[tokio::test]
async fn test_init_workflow_error_handling() -> Result<()> {
    // Test error handling for various invalid scenarios

    // Test 1: Invalid project directory (read-only parent)
    if cfg!(unix) {
        let temp_dir = TempDir::new()?;
        let readonly_dir = temp_dir.path().join("readonly");
        fs::create_dir_all(&readonly_dir)?;

        // Make directory read-only
        let mut perms = fs::metadata(&readonly_dir)?.permissions();
        perms.set_readonly(true);
        fs::set_permissions(&readonly_dir, perms)?;

        let invalid_project = readonly_dir.join("should_fail");
        let options = InitOptions {
            project_dir: invalid_project,
            dev_database_url: "postgres://localhost/test".to_string(),
            shadow_config: ShadowDatabaseInput::Auto,
            shadow_pg_version: None,
            schema_dir: PathBuf::from("schema"),
            import_source: None,
            object_config: ObjectManagementConfig::default(),
            baseline_config: BaselineCreationConfig::default(),
            tracking_table: pgmt::config::types::TrackingTable::default(),
            roles_file: None,
        };

        // This should fail gracefully
        let result = pgmt::commands::init::project::create_project_structure(
            &options.project_dir,
            &options.schema_dir,
        );
        assert!(result.is_err());
    }

    // Test 2: Invalid import source (non-existent file)
    let temp_dir = TempDir::new()?;
    let project_path = temp_dir.path().join("test_project");
    let non_existent_file = temp_dir.path().join("does_not_exist.sql");

    let options = InitOptions {
        project_dir: project_path.clone(),
        dev_database_url: "postgres://localhost/test".to_string(),
        shadow_config: ShadowDatabaseInput::Auto,
        shadow_pg_version: None,
        schema_dir: PathBuf::from("schema"),
        import_source: Some(ImportSource::SqlFile(non_existent_file)),
        object_config: ObjectManagementConfig::default(),
        baseline_config: BaselineCreationConfig::default(),
        tracking_table: pgmt::config::types::TrackingTable::default(),
        roles_file: None,
    };

    // Project structure creation should still work
    let result = pgmt::commands::init::project::create_project_structure(
        &options.project_dir,
        &options.schema_dir,
    );
    assert!(result.is_ok());

    // But importing the non-existent file would fail in the import step
    // (This would be caught in the actual import logic)

    Ok(())
}

// ============================================================================
// Database Integration Tests
// ============================================================================
// The following tests require a live PostgreSQL database and test:
// - Catalog loading from database
// - Schema generation from catalog
// - Object counting for context-aware prompts
// - Dependency tracking
// ============================================================================

#[tokio::test]
async fn test_init_catalog_loading_for_validation() -> Result<()> {
    with_test_db(async |db| {
        // Create a simple, valid schema
        db.execute(
            r#"
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            );

            CREATE INDEX idx_users_email ON users (email);

            CREATE VIEW active_users AS
            SELECT id, email FROM users WHERE email IS NOT NULL;

            COMMENT ON TABLE users IS 'User accounts';
            "#,
        )
        .await;

        // TEST: Catalog loading works correctly for init validation
        let catalog = pgmt::catalog::Catalog::load(db.pool()).await?;

        // Verify catalog has expected objects for validation
        assert_eq!(catalog.tables.len(), 1, "Should have 1 table");
        assert_eq!(catalog.indexes.len(), 1, "Should have 1 index");
        assert_eq!(catalog.views.len(), 1, "Should have 1 view");

        // Verify comment was captured
        let users_table = catalog.tables.iter().find(|t| t.name == "users");
        assert!(users_table.is_some(), "users table should exist");
        assert_eq!(
            users_table.unwrap().comment.as_deref(),
            Some("User accounts"),
            "Table comment should be captured"
        );

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_init_schema_generation_from_catalog() -> Result<()> {
    with_test_db(async |db| {
        // Create schema with multiple object types
        db.execute(
            r#"
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

            CREATE TABLE users (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                email TEXT UNIQUE NOT NULL
            );

            CREATE TABLE posts (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                user_id UUID REFERENCES users(id),
                title TEXT NOT NULL
            );

            CREATE INDEX idx_posts_user_id ON posts (user_id);

            CREATE VIEW user_post_count AS
            SELECT u.id, u.email, COUNT(p.id) as post_count
            FROM users u
            LEFT JOIN posts p ON p.user_id = u.id
            GROUP BY u.id, u.email;
            "#,
        )
        .await;

        // Load catalog
        let catalog = pgmt::catalog::Catalog::load(db.pool()).await?;

        // TEST: Schema generator can create files from catalog
        let temp_dir = TempDir::new()?;
        let schema_path = temp_dir.path().join("schema");

        let generator = pgmt::schema_generator::SchemaGenerator::new(
            catalog,
            schema_path.clone(),
            pgmt::schema_generator::SchemaGeneratorConfig {
                include_comments: true,
                include_grants: true,
                include_triggers: true,
                include_extensions: true,
            },
        );

        generator.generate_files()?;

        // Verify schema files were generated
        // Extensions are written to a single file, not a directory
        assert!(
            schema_path.join("extensions.sql").exists(),
            "extensions.sql file should exist"
        );
        assert!(
            schema_path.join("tables").exists(),
            "Tables directory should exist"
        );
        assert!(
            schema_path.join("views").exists(),
            "Views directory should exist"
        );

        // Verify extensions.sql has content
        let extensions_content = std::fs::read_to_string(schema_path.join("extensions.sql"))?;
        assert!(
            extensions_content.contains("uuid-ossp"),
            "extensions.sql should contain uuid-ossp"
        );

        let table_files: Vec<_> = std::fs::read_dir(schema_path.join("tables"))?
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(table_files.len(), 2, "Should have 2 table files");

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_init_object_counting_for_context_prompts() -> Result<()> {
    with_test_db(async |db| {
        // Create database with various object types for counting
        db.execute(
            r#"
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
            CREATE EXTENSION IF NOT EXISTS "pgcrypto";

            CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT);
            CREATE TABLE posts (id SERIAL PRIMARY KEY, user_id INT);
            CREATE TABLE comments (id SERIAL PRIMARY KEY, post_id INT);

            COMMENT ON TABLE users IS 'User accounts';
            COMMENT ON TABLE posts IS 'User posts';

            CREATE OR REPLACE FUNCTION trigger_set_timestamp()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            ALTER TABLE users ADD COLUMN updated_at TIMESTAMP;

            CREATE TRIGGER update_timestamp
            BEFORE UPDATE ON users
            FOR EACH ROW
            EXECUTE FUNCTION trigger_set_timestamp();

            GRANT SELECT ON users TO PUBLIC;
            GRANT INSERT ON posts TO PUBLIC;
            "#,
        )
        .await;

        // Load catalog
        let catalog = pgmt::catalog::Catalog::load(db.pool()).await?;

        // TEST: Verify object counts for context-aware prompts
        assert_eq!(catalog.tables.len(), 3, "Should have 3 tables");
        assert!(
            catalog.extensions.len() >= 2,
            "Should have at least 2 extensions"
        );

        // Count commentable objects (for context-aware prompt)
        let comment_count = catalog
            .tables
            .iter()
            .filter(|t| t.comment.is_some())
            .count();
        assert_eq!(comment_count, 2, "Should have 2 tables with comments");

        // Verify triggers exist (for context-aware prompt)
        assert!(
            !catalog.triggers.is_empty(),
            "Should have at least 1 trigger"
        );

        // Verify functions exist
        assert!(
            !catalog.functions.is_empty(),
            "Should have at least 1 function (trigger_set_timestamp)"
        );

        // Verify grants exist (for context-aware prompt)
        assert!(catalog.grants.len() >= 2, "Should have at least 2 grants");

        // TEST: These counts would be shown in context-aware prompts like:
        // - Comments (3 commentable objects)
        // - Grants (2+ permissions)
        // - Triggers (1+ triggers)
        // - Extensions (2+ extensions)

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_init_handles_complex_dependencies() -> Result<()> {
    with_test_db(async |db| {
        // Create schema with dependencies that need proper ordering
        db.execute(
            r#"
            -- Extension first
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

            -- Custom type
            CREATE TYPE priority AS ENUM ('low', 'medium', 'high');

            -- Table using custom type
            CREATE TABLE tasks (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                title TEXT NOT NULL,
                priority priority DEFAULT 'medium'
            );

            -- View depending on table
            CREATE VIEW high_priority_tasks AS
            SELECT id, title FROM tasks WHERE priority = 'high';

            -- Function depending on table
            CREATE FUNCTION count_high_priority_tasks()
            RETURNS INTEGER AS $$
                SELECT COUNT(*)::INTEGER FROM high_priority_tasks;
            $$ LANGUAGE SQL;
            "#,
        )
        .await;

        // Load catalog
        let catalog = pgmt::catalog::Catalog::load(db.pool()).await?;

        // TEST: Verify dependency tracking
        assert!(!catalog.extensions.is_empty(), "Should have extension");
        assert_eq!(catalog.types.len(), 1, "Should have 1 custom type");
        assert_eq!(catalog.tables.len(), 1, "Should have 1 table");
        assert_eq!(catalog.views.len(), 1, "Should have 1 view");
        assert!(
            !catalog.functions.is_empty(),
            "Should have at least 1 function"
        );

        // Verify the custom type is captured
        let priority_type = catalog.types.iter().find(|t| t.name == "priority");
        assert!(priority_type.is_some(), "priority type should exist");

        // TEST: When generating schema files, these dependencies should be
        // properly ordered: extension → type → table → view → function

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_init_empty_database_has_no_objects() -> Result<()> {
    with_test_db(async |db| {
        // TEST: Empty database (only system objects)
        let catalog = pgmt::catalog::Catalog::load(db.pool()).await?;

        // Verify empty catalog (system objects are filtered out)
        assert_eq!(catalog.tables.len(), 0, "Should have no user tables");
        assert_eq!(catalog.views.len(), 0, "Should have no user views");
        assert_eq!(catalog.types.len(), 0, "Should have no user types");
        assert_eq!(catalog.triggers.len(), 0, "Should have no user triggers");
        assert_eq!(catalog.grants.len(), 0, "Should have no user grants");

        // This represents a new project scenario where no baseline is needed

        Ok(())
    })
    .await
}

/// Test that SQL file import applies roles.sql before the schema file
/// This ensures GRANT statements in the schema don't fail due to missing roles
#[tokio::test]
async fn test_init_sql_file_import_with_roles() -> Result<()> {
    use pgmt::commands::init::import::import_schema;
    use pgmt::config::types::ShadowDatabase;

    let temp_dir = TempDir::new()?;

    // Create a roles.sql file with a unique role name
    // Uses DO block for idempotency (same pattern as other tests)
    let roles_file = temp_dir.path().join("roles.sql");
    fs::write(
        &roles_file,
        r#"-- Roles for import test
DO $$ BEGIN CREATE ROLE import_test_role; EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$;"#,
    )?;

    // Create a schema.sql file with a table and GRANT to that role
    let schema_file = temp_dir.path().join("schema.sql");
    fs::write(
        &schema_file,
        r#"-- Schema with grants that require roles.sql
CREATE TABLE import_test_items (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

GRANT SELECT ON import_test_items TO import_test_role;
"#,
    )?;

    // Import the schema with the roles file
    // This should apply roles.sql BEFORE schema.sql, allowing the GRANT to succeed
    let catalog = import_schema(
        ImportSource::SqlFile(schema_file),
        &ShadowDatabase::Auto,
        Some(roles_file.as_path()),
    )
    .await?;

    // Verify the import succeeded and catalog contains expected objects
    assert_eq!(catalog.tables.len(), 1, "Should have 1 table");
    assert_eq!(
        catalog.tables[0].name, "import_test_items",
        "Table should be import_test_items"
    );

    // Verify the grant was captured (this proves the role existed when GRANT ran)
    assert!(
        !catalog.grants.is_empty(),
        "Should have grants (proves roles.sql was applied first)"
    );
    let has_import_test_grant = catalog.grants.iter().any(|g| {
        matches!(&g.grantee, pgmt::catalog::grant::GranteeType::Role(name) if name == "import_test_role")
    });
    assert!(
        has_import_test_grant,
        "Should have grant to import_test_role"
    );

    Ok(())
}
