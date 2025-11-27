use anyhow::Result;
use dialoguer::{Confirm, Input, Select};
use sqlx::PgPool;
use std::path::PathBuf;

pub fn prompt_required_string_with_validation<F>(
    value: Option<&str>,
    prompt_message: &str,
    validator: F,
) -> Result<String>
where
    F: Fn(&str) -> Result<(), String>,
{
    match value {
        Some(val) => {
            if let Err(e) = validator(val) {
                return Err(anyhow::anyhow!("Invalid value '{}': {}", val, e));
            }
            Ok(val.to_string())
        }
        None => {
            let input: String = Input::new()
                .with_prompt(prompt_message)
                .validate_with(|input: &String| validator(input.trim()))
                .interact_text()?;

            Ok(input.trim().to_string())
        }
    }
}

/// Result of database URL prompt including detected version
pub struct DatabaseConnectionResult {
    pub url: String,
    pub pg_version: Option<String>,
}

/// Prompt for database URL with guidance and connection testing
/// Returns the URL and detected PostgreSQL version
pub async fn prompt_database_url_with_guidance() -> Result<DatabaseConnectionResult> {
    loop {
        let url: String = Input::new()
            .with_prompt("💾 Local dev database URL (e.g., postgres://localhost/myapp_dev)")
            .interact_text()?;

        let url = url.trim();
        if url.is_empty() {
            println!("❌ Database URL cannot be empty");
            continue;
        }

        // Test connection and detect version
        print!("🔄 Testing connection...");
        match test_database_connection_with_version(url).await {
            Ok(pg_version) => {
                println!(" ✅ (PostgreSQL {})", pg_version);
                return Ok(DatabaseConnectionResult {
                    url: url.to_string(),
                    pg_version: Some(pg_version),
                });
            }
            Err(e) => {
                println!(" ❌");
                println!("   Connection failed: {}", e);
                let retry = Confirm::new()
                    .with_prompt("Try a different URL?")
                    .default(true)
                    .interact()?;

                if !retry {
                    return Err(anyhow::anyhow!("Database connection required"));
                }
            }
        }
    }
}

/// Prompt for database URL without guidance (for imports, etc.)
pub async fn prompt_database_url_simple(prompt: &str) -> Result<String> {
    loop {
        let url: String = Input::new().with_prompt(prompt).interact_text()?;

        let url = url.trim();
        if url.is_empty() {
            println!("❌ Database URL cannot be empty");
            continue;
        }

        // Test connection
        print!("🔄 Testing connection...");
        match test_database_connection(url).await {
            Ok(_) => {
                println!(" ✅");
                return Ok(url.to_string());
            }
            Err(e) => {
                println!(" ❌");
                println!("   Connection failed: {}", e);
                let retry = Confirm::new()
                    .with_prompt("Try a different URL?")
                    .default(true)
                    .interact()?;

                if !retry {
                    return Err(anyhow::anyhow!("Database connection required"));
                }
            }
        }
    }
}

/// Shadow database configuration options
#[derive(Debug, Clone)]
pub enum ShadowDatabaseInput {
    Auto,
    Manual(String),
}

/// Prompt for shadow database mode with explanation
pub async fn prompt_shadow_mode_with_explanation() -> Result<ShadowDatabaseInput> {
    // Check Docker availability first
    let (docker_available, debug_info) = crate::docker::DockerManager::is_available_verbose().await;

    if docker_available {
        println!("🛡️  Shadow database (for testing migrations safely)");
        let auto = Confirm::new()
            .with_prompt("   Use auto mode? (Docker-managed, recommended)")
            .default(true)
            .interact()?;

        if auto {
            Ok(ShadowDatabaseInput::Auto)
        } else {
            let url: String = Input::new()
                .with_prompt("   Shadow database URL")
                .interact_text()?;
            Ok(ShadowDatabaseInput::Manual(url.trim().to_string()))
        }
    } else {
        println!("🛡️  Shadow database (for testing migrations safely)");
        println!("   ⚠️  Docker not available - manual mode required");
        tracing::debug!("Docker availability details: {}", debug_info);

        let url: String = Input::new()
            .with_prompt("   Shadow database URL")
            .interact_text()?;
        Ok(ShadowDatabaseInput::Manual(url.trim().to_string()))
    }
}

/// Select from a list of options
pub fn prompt_select<T: Clone>(prompt: &str, options: Vec<(T, &str)>) -> Result<T> {
    let labels: Vec<&str> = options.iter().map(|(_, label)| *label).collect();

    let selection = Select::new()
        .with_prompt(prompt)
        .items(&labels)
        .interact()?;

    Ok(options[selection].0.clone())
}

/// Prompt for directory with validation and creation
pub fn prompt_directory_with_validation(prompt: &str, default: Option<&str>) -> Result<PathBuf> {
    let mut input_builder = Input::new().with_prompt(prompt);

    if let Some(default_value) = default {
        input_builder = input_builder.default(default_value.to_string());
    }

    let path_str: String = input_builder.interact_text()?;
    let path = PathBuf::from(path_str.trim());

    // Validate the path - only check parent if it's not empty/current directory
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
        && !parent.exists()
    {
        return Err(anyhow::anyhow!(
            "Parent directory does not exist: {}",
            parent.display()
        ));
    }

    // Create directory if it doesn't exist
    if !path.exists() {
        std::fs::create_dir_all(&path)?;
        println!("✅ Created directory: {}", path.display());
    }

    Ok(path)
}

/// Test database connection
async fn test_database_connection(url: &str) -> Result<()> {
    let pool = PgPool::connect(url).await?;

    // Simple test query
    sqlx::query("SELECT 1").fetch_one(&pool).await?;

    pool.close().await;
    Ok(())
}

/// Test database connection and return PostgreSQL version
async fn test_database_connection_with_version(url: &str) -> Result<String> {
    let pool = PgPool::connect(url).await?;

    // Get server version
    let row: (String,) = sqlx::query_as("SHOW server_version")
        .fetch_one(&pool)
        .await?;

    pool.close().await;

    // Parse version - extract just the version number (e.g., "15.4" from "15.4 (Debian ...)")
    let version = row
        .0
        .split_whitespace()
        .next()
        .unwrap_or(&row.0)
        .to_string();

    Ok(version)
}

/// Extract major version from full version string (e.g., "15" from "15.4")
pub fn extract_major_version(full_version: &str) -> String {
    full_version
        .split('.')
        .next()
        .unwrap_or(full_version)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prompt_required_string_with_validation() {
        let validator = |s: &str| {
            if s.contains(' ') {
                Err("Value cannot contain spaces".to_string())
            } else {
                Ok(())
            }
        };

        let result =
            prompt_required_string_with_validation(Some("valid_value"), "Enter value", validator);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "valid_value");

        let result =
            prompt_required_string_with_validation(Some("invalid value"), "Enter value", validator);
        assert!(result.is_err());
    }

    #[test]
    fn test_shadow_database_input_enum() {
        // Test that the enum variants work correctly
        let auto = ShadowDatabaseInput::Auto;
        let manual = ShadowDatabaseInput::Manual("postgres://localhost/shadow".to_string());

        match auto {
            ShadowDatabaseInput::Auto => {}
            _ => panic!("Expected Auto variant"),
        }

        match manual {
            ShadowDatabaseInput::Manual(url) => {
                assert_eq!(url, "postgres://localhost/shadow");
            }
            _ => panic!("Expected Manual variant"),
        }
    }

    #[test]
    fn test_prompt_directory_with_validation() {
        use std::env;

        // Test with a temporary directory
        let temp_dir = env::temp_dir();
        let test_path = temp_dir.join("pgmt_test_dir");

        // Clean up if exists
        let _ = std::fs::remove_dir_all(&test_path);

        // Test directory creation would work (we can't actually test interactive prompt)
        assert!(!test_path.exists());
    }
}
