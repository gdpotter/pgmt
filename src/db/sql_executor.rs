use anyhow::Result;
use sqlx::PgPool;
use std::path::Path;

use super::connection::initialize_database_session;
use super::error_context::SqlErrorContext;

/// Configuration for SQL file execution
#[derive(Debug, Clone)]
pub struct SqlExecutorConfig {
    /// Whether to initialize database session before execution
    pub initialize_session: bool,
    /// Whether to provide verbose output during execution
    pub verbose: bool,
}

impl Default for SqlExecutorConfig {
    fn default() -> Self {
        Self {
            initialize_session: true,
            verbose: true,
        }
    }
}

/// Execute a SQL file against a database using PostgreSQL's native parsing
pub async fn execute_sql_file(
    pool: &PgPool,
    file_path: &Path,
    config: &SqlExecutorConfig,
) -> Result<()> {
    let content = std::fs::read_to_string(file_path)
        .map_err(|e| anyhow::anyhow!("Failed to read SQL file {}: {}", file_path.display(), e))?;

    execute_sql_content(pool, &content, file_path, config).await
}

/// Execute SQL content with proper error handling and session management
pub async fn execute_sql_content(
    pool: &PgPool,
    content: &str,
    source: &Path,
    config: &SqlExecutorConfig,
) -> Result<()> {
    let trimmed_content = content.trim();
    if trimmed_content.is_empty() {
        if config.verbose {
            println!("   ⏭️  Skipping empty file: {}", source.display());
        }
        return Ok(());
    }

    if config.initialize_session {
        initialize_database_session(pool).await?;
    }
    match sqlx::raw_sql(content).execute(pool).await {
        Ok(result) => {
            if config.verbose {
                println!(
                    "   ✅ Executed {} (affected {} rows)",
                    source.display(),
                    result.rows_affected()
                );
            }
            Ok(())
        }
        Err(e) => Err(format_sql_error(&e, source, content)),
    }
}

/// Format SQL error with rich context from PostgreSQL
///
/// Always returns a formatted error with line number context when available.
/// The caller decides what to do with the error (fail or continue).
fn format_sql_error(e: &sqlx::Error, source: &Path, content: &str) -> anyhow::Error {
    let ctx = SqlErrorContext::from_sqlx_error(e, content);
    anyhow::anyhow!("{}", ctx.format(&source.display().to_string(), content))
}

/// Discover SQL files in a directory recursively with enhanced migration directory support
/// Handles various migration structures like Prisma, Flyway, Liquibase, etc.
pub fn discover_sql_files_ordered(dir: &Path) -> Result<Vec<std::path::PathBuf>> {
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

    let mut files = Vec::new();
    let mut migration_dirs = Vec::new();
    let mut total_entries = 0;

    fn collect_sql_files_recursive(
        dir: &Path,
        files: &mut Vec<std::path::PathBuf>,
        migration_dirs: &mut Vec<std::path::PathBuf>,
        total_entries: &mut usize,
    ) -> Result<()> {
        if !dir.is_dir() {
            return Ok(());
        }

        let mut entries: Vec<_> = std::fs::read_dir(dir)?
            .filter_map(|entry| entry.ok())
            .collect();

        *total_entries += entries.len();

        entries.sort_by_key(|a| a.file_name());

        for entry in entries {
            let path = entry.path();

            if path.is_dir() {
                if is_migration_directory(&path) {
                    migration_dirs.push(path.clone());
                }
                collect_sql_files_recursive(&path, files, migration_dirs, total_entries)?;
            } else if is_sql_file(&path) {
                files.push(path);
            }
        }

        Ok(())
    }

    collect_sql_files_recursive(dir, &mut files, &mut migration_dirs, &mut total_entries)?;
    if files.is_empty() && total_entries == 0 {
        eprintln!("⚠️  Warning: The directory '{}' is empty.", dir.display());
        eprintln!("   No files or subdirectories found to import.");
    } else if files.is_empty() && total_entries > 0 {
        eprintln!(
            "⚠️  Warning: No SQL files found in '{}' (searched {} items recursively).",
            dir.display(),
            total_entries
        );
        eprintln!("   The directory contains files but no .sql files were discovered.");

        if !migration_dirs.is_empty() {
            eprintln!(
                "   Found {} potential migration directories:",
                migration_dirs.len()
            );
            for (i, migration_dir) in migration_dirs.iter().enumerate().take(5) {
                eprintln!("     - {}", migration_dir.display());
                if i == 4 && migration_dirs.len() > 5 {
                    eprintln!("     ... and {} more", migration_dirs.len() - 5);
                    break;
                }
            }
        }
    } else {
        eprintln!("✅ Found {} SQL files in '{}'", files.len(), dir.display());
        if !migration_dirs.is_empty() {
            eprintln!(
                "   Discovered {} migration directories (Prisma/Flyway style)",
                migration_dirs.len()
            );
        }

        // Show a sample of discovered files for confirmation
        if files.len() <= 5 {
            eprintln!(
                "   Files: {}",
                files
                    .iter()
                    .map(|f| f.file_name().unwrap().to_string_lossy())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        } else {
            eprintln!(
                "   Sample files: {}, ... and {} more",
                files
                    .iter()
                    .take(3)
                    .map(|f| f.file_name().unwrap().to_string_lossy())
                    .collect::<Vec<_>>()
                    .join(", "),
                files.len() - 3
            );
        }
    }

    Ok(files)
}

/// Check if a directory contains SQL files (indicating it's likely a migration directory)
fn is_migration_directory(dir: &Path) -> bool {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if is_sql_file(&entry.path()) {
                return true;
            }
        }
    }
    false
}

/// Check if a file is a SQL file (including common migration file patterns)
fn is_sql_file(path: &Path) -> bool {
    if let Some(extension) = path.extension().and_then(|e| e.to_str()) {
        // Standard SQL files
        if extension.eq_ignore_ascii_case("sql") {
            return true;
        }

        // Some migration tools use different extensions
        if extension.eq_ignore_ascii_case("psql") || extension.eq_ignore_ascii_case("pgsql") {
            return true;
        }
    }

    // Some migration files might not have extensions but have SQL-like names
    if let Some(filename) = path.file_name().and_then(|n| n.to_str())
        && (filename.ends_with(".up")
            || filename.ends_with(".down")
            || filename.contains("migration")
            || filename.contains("schema"))
    {
        // Additional validation: check if file contains SQL keywords
        if let Ok(content) = std::fs::read_to_string(path) {
            let content_lower = content.to_lowercase();
            if content_lower.contains("create")
                || content_lower.contains("alter")
                || content_lower.contains("drop")
                || content_lower.contains("insert")
                || content_lower.contains("select")
            {
                return true;
            }
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_sql_executor_config_default() {
        let config = SqlExecutorConfig::default();
        assert!(config.initialize_session);
        assert!(config.verbose);
    }

    #[test]
    fn test_is_sql_file() {
        assert!(is_sql_file(Path::new("test.sql")));
        assert!(is_sql_file(Path::new("test.SQL")));
        assert!(is_sql_file(Path::new("test.psql")));
        assert!(is_sql_file(Path::new("test.pgsql")));
        assert!(!is_sql_file(Path::new("test.txt")));
        assert!(!is_sql_file(Path::new("test")));
    }

    #[test]
    fn test_discover_sql_files_ordered() {
        let temp_dir = env::temp_dir().join("pgmt_test_sql_discovery");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        // Create test SQL files
        std::fs::write(temp_dir.join("02_tables.sql"), "CREATE TABLE test;").unwrap();
        std::fs::write(temp_dir.join("01_schema.sql"), "CREATE SCHEMA test;").unwrap();
        std::fs::write(temp_dir.join("03_data.sql"), "INSERT INTO test VALUES (1);").unwrap();

        let files = discover_sql_files_ordered(&temp_dir).unwrap();

        assert_eq!(files.len(), 3);
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
        assert!(
            files[2]
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("03_")
        );

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
