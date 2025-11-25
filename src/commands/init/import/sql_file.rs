use anyhow::Result;
use std::path::PathBuf;

use crate::catalog::Catalog;
use crate::db::connection::connect_with_retry;
use crate::db::sql_executor::{SqlExecutorConfig, execute_sql_file};

/// Import schema from a single SQL file
pub async fn import_from_sql_file(
    file: PathBuf,
    shadow_config: &crate::config::types::ShadowDatabase,
) -> Result<Catalog> {
    let shadow_url = shadow_config.get_connection_string().await?;

    println!("🔄 Executing SQL file: {}", file.display());

    // Connect to shadow database with retry logic
    let pool = connect_with_retry(&shadow_url).await?;

    // Configure SQL executor for import scenario
    let executor_config = SqlExecutorConfig {
        initialize_session: true,
        verbose: true,
        continue_on_error: true, // Best effort for imports
    };

    // Execute the file
    execute_sql_file(&pool, &file, &executor_config).await?;

    // Extract catalog
    let catalog = Catalog::load(&pool).await?;

    // Cleanup
    pool.close().await;
    // Note: Docker containers are automatically cleaned up based on their auto_cleanup configuration

    Ok(catalog)
}

/// Validate that a SQL file exists and is readable
pub fn validate_sql_file(file_path: &PathBuf) -> Result<()> {
    if !file_path.exists() {
        return Err(anyhow::anyhow!(
            "SQL file does not exist: {}",
            file_path.display()
        ));
    }

    if !file_path.is_file() {
        return Err(anyhow::anyhow!(
            "Path is not a file: {}",
            file_path.display()
        ));
    }

    // Check if file has SQL content (basic validation)
    let content = std::fs::read_to_string(file_path)
        .map_err(|e| anyhow::anyhow!("Cannot read SQL file {}: {}", file_path.display(), e))?;

    if content.trim().is_empty() {
        return Err(anyhow::anyhow!(
            "SQL file is empty: {}",
            file_path.display()
        ));
    }

    // Basic SQL content validation
    let content_lower = content.to_lowercase();
    if !content_lower.contains("create")
        && !content_lower.contains("insert")
        && !content_lower.contains("alter")
    {
        eprintln!(
            "⚠️  Warning: SQL file '{}' may not contain valid SQL statements",
            file_path.display()
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_validate_sql_file() {
        let temp_dir = env::temp_dir().join("pgmt_test_sql_file_validation");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        // Test valid SQL file
        let valid_file = temp_dir.join("valid.sql");
        std::fs::write(&valid_file, "CREATE TABLE test (id SERIAL);").unwrap();
        assert!(validate_sql_file(&valid_file).is_ok());

        // Test empty SQL file
        let empty_file = temp_dir.join("empty.sql");
        std::fs::write(&empty_file, "").unwrap();
        assert!(validate_sql_file(&empty_file).is_err());

        // Test non-existent file
        let missing_file = temp_dir.join("missing.sql");
        assert!(validate_sql_file(&missing_file).is_err());

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_validate_sql_file_with_warning() {
        let temp_dir = env::temp_dir().join("pgmt_test_sql_file_warning");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        // Test file with non-SQL content (should pass but with warning)
        let questionable_file = temp_dir.join("questionable.sql");
        std::fs::write(&questionable_file, "This is not SQL content").unwrap();
        assert!(validate_sql_file(&questionable_file).is_ok());

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
