use anyhow::Result;
use std::path::{Path, PathBuf};

use crate::catalog::Catalog;
use crate::db::connection::connect_with_retry;
use crate::db::sql_executor::{SqlExecutorConfig, discover_sql_files_ordered, execute_sql_file};

/// Import schema from a directory of SQL files
pub async fn import_from_directory(
    dir: PathBuf,
    shadow_config: &crate::config::types::ShadowDatabase,
    roles_file: Option<&Path>,
) -> Result<Catalog> {
    let shadow_url = shadow_config.get_connection_string().await?;

    // Discover SQL files in alphabetical order
    let sql_files = discover_sql_files_ordered(&dir)?;

    println!(
        "🔄 Found {} SQL files. Executing in order...",
        sql_files.len()
    );

    // Connect to shadow database with retry logic
    let pool = connect_with_retry(&shadow_url).await?;

    // Apply roles file first if it exists (roles must exist before GRANTs)
    if let Some(roles_path) = roles_file
        && roles_path.exists()
    {
        println!("   📋 Applying roles from: {}", roles_path.display());
        crate::schema_ops::apply_roles_file(&pool, roles_path).await?;
    }

    // Configure SQL executor for import scenario
    let executor_config = SqlExecutorConfig {
        initialize_session: true,
        verbose: true,
    };

    // Execute each file - errors bubble up with line numbers already formatted
    for file in &sql_files {
        execute_sql_file(&pool, file, &executor_config).await?;
    }

    // Extract catalog
    let catalog = Catalog::load(&pool).await?;

    // Cleanup
    pool.close().await;
    // Note: Docker containers are automatically cleaned up based on their auto_cleanup configuration

    Ok(catalog)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_import_directory_basic_structure() {
        let temp_dir = env::temp_dir().join("pgmt_test_import_directory");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        // Create test SQL files
        std::fs::write(temp_dir.join("01_schema.sql"), "CREATE SCHEMA test;").unwrap();
        std::fs::write(
            temp_dir.join("02_tables.sql"),
            "CREATE TABLE test.users (id SERIAL);",
        )
        .unwrap();

        // Test file discovery
        let files = discover_sql_files_ordered(&temp_dir).unwrap();
        assert_eq!(files.len(), 2);
        assert!(
            files[0]
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("01_")
        );
        assert!(
            files[1]
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("02_")
        );

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
