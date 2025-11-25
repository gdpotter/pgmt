pub mod database;
pub mod directory;
pub mod sql_file;

use anyhow::Result;
use std::path::{Path, PathBuf};

use crate::catalog::Catalog;

pub use database::import_from_database;
pub use directory::import_from_directory;
pub use sql_file::{import_from_sql_file, validate_sql_file};

/// Import source options for schema initialization
#[derive(Debug, Clone)]
pub enum ImportSource {
    Directory(PathBuf), // Directory of SQL files
    SqlFile(PathBuf),   // Single SQL dump file
    Database(String),   // Existing database URL
}

impl ImportSource {}

/// Import a schema from various sources into a temporary shadow database
/// and return the catalog representation
pub async fn import_schema(
    source: ImportSource,
    shadow_config: &crate::config::types::ShadowDatabase,
) -> Result<Catalog> {
    match source {
        ImportSource::Directory(dir) => {
            validate_directory_source(&dir)?;
            import_from_directory(dir, shadow_config).await
        }
        ImportSource::SqlFile(file) => {
            validate_sql_file(&file)?;
            import_from_sql_file(file, shadow_config).await
        }
        ImportSource::Database(url) => {
            validate_database_url(&url)?;
            import_from_database(url).await
        }
    }
}

/// Validate that a directory source is accessible and contains SQL files
fn validate_directory_source(dir: &Path) -> Result<()> {
    if !dir.exists() {
        return Err(anyhow::anyhow!(
            "Directory does not exist: {}",
            dir.display()
        ));
    }

    if !dir.is_dir() {
        return Err(anyhow::anyhow!(
            "Path is not a directory: {}",
            dir.display()
        ));
    }

    // Check if directory contains any SQL files
    let sql_files = crate::db::sql_executor::discover_sql_files_ordered(dir)?;
    if sql_files.is_empty() {
        return Err(anyhow::anyhow!(
            "Directory '{}' does not contain any SQL files",
            dir.display()
        ));
    }

    // Silent validation - message already shown during prompt
    Ok(())
}

/// Basic validation of database URL format
fn validate_database_url(url: &str) -> Result<()> {
    if url.trim().is_empty() {
        return Err(anyhow::anyhow!("Database URL cannot be empty"));
    }

    if !url.starts_with("postgres://") && !url.starts_with("postgresql://") {
        return Err(anyhow::anyhow!(
            "Invalid database URL format. Expected postgres:// or postgresql:// scheme"
        ));
    }

    // Basic URL format validation
    if !url.contains("://") {
        return Err(anyhow::anyhow!(
            "Invalid database URL format: missing protocol"
        ));
    }

    println!("✅ Database URL validation passed");
    Ok(())
}

/// Get a human-readable description of the import source
impl ImportSource {
    pub fn description(&self) -> String {
        match self {
            ImportSource::Directory(dir) => format!("Directory: {}", dir.display()),
            ImportSource::SqlFile(file) => format!("SQL file: {}", file.display()),
            ImportSource::Database(url) => {
                // Simple URL masking - just show the basic structure without password
                if let Some(at_pos) = url.find('@') {
                    if let Some(colon_pos) = url[..at_pos].rfind(':') {
                        // URL has user:password@host format - mask the password
                        let prefix = &url[..colon_pos];
                        let suffix = &url[at_pos..];
                        format!("Database: {}:***{}", prefix, suffix)
                    } else {
                        format!("Database: {}", url)
                    }
                } else {
                    format!("Database: {}", url)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_import_source_description() {
        let dir_source = ImportSource::Directory(PathBuf::from("/tmp/sql"));
        assert!(dir_source.description().contains("Directory"));

        let file_source = ImportSource::SqlFile(PathBuf::from("/tmp/dump.sql"));
        assert!(file_source.description().contains("SQL file"));

        let db_source = ImportSource::Database("postgres://localhost/test".to_string());
        assert!(db_source.description().contains("Database"));
    }

    #[test]
    fn test_validate_database_url() {
        // Valid URLs
        assert!(validate_database_url("postgres://localhost/test").is_ok());
        assert!(validate_database_url("postgresql://user:pass@host:5432/db").is_ok());

        // Invalid URLs
        assert!(validate_database_url("").is_err());
        assert!(validate_database_url("mysql://localhost/test").is_err());
        assert!(validate_database_url("not a url").is_err());
    }

    #[test]
    fn test_validate_directory_source() {
        let temp_dir = env::temp_dir().join(format!(
            "pgmt_test_validate_directory_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&temp_dir);

        // Test non-existent directory
        assert!(validate_directory_source(&temp_dir).is_err());

        // Test empty directory
        std::fs::create_dir_all(&temp_dir).unwrap();
        assert!(validate_directory_source(&temp_dir).is_err());

        // Test directory with SQL files
        std::fs::write(temp_dir.join("test.sql"), "CREATE TABLE test (id INT);").unwrap();
        assert!(validate_directory_source(&temp_dir).is_ok());

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
