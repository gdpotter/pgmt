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
    /// Extensions installed in the database, fetched over the same validation
    /// connection (used for shadow-image guidance without reconnecting).
    pub extensions: Vec<String>,
}

/// Prompt for database URL with guidance and connection testing, with optional default
/// Returns the URL and detected PostgreSQL version
pub async fn prompt_database_url_with_guidance_and_default(
    default_url: Option<String>,
) -> Result<DatabaseConnectionResult> {
    loop {
        let url: String = if let Some(ref default) = default_url {
            Input::new()
                .with_prompt("💾 Local dev database URL (e.g., postgres://localhost/myapp_dev)")
                .default(default.clone())
                .interact_text()?
        } else {
            Input::new()
                .with_prompt("💾 Local dev database URL (e.g., postgres://localhost/myapp_dev)")
                .interact_text()?
        };

        let url = url.trim();
        if url.is_empty() {
            println!("❌ Database URL cannot be empty");
            continue;
        }

        // Test connection and detect version
        print!("🔄 Testing connection...");
        match test_database_connection_with_version(url).await {
            Ok((pg_version, extensions)) => {
                println!(" ✅ (PostgreSQL {})", pg_version);
                return Ok(DatabaseConnectionResult {
                    url: url.to_string(),
                    pg_version: Some(pg_version),
                    extensions,
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
    /// Docker-managed shadow on the stock `postgres` image (optionally a specific
    /// major version via `shadow_pg_version`/detected version).
    Auto,
    /// Docker-managed shadow on a specific image (e.g. `postgis/postgis:16-3.5`),
    /// optionally pinned to a `platform` for single-arch images.
    Docker {
        image: String,
        platform: Option<String>,
    },
    /// An externally-managed shadow database reached by URL.
    Manual(String),
}

/// Which menu entry to pre-select: the existing config's mode wins, except
/// that a config which would fail (auto/no config while the source DB uses
/// extensions the stock image lacks) is steered to the custom-image option.
fn shadow_mode_default(
    existing: Option<&crate::config::types::ShadowDatabaseInput>,
    nonstandard_extensions: &[String],
) -> usize {
    let existing_docker = existing.and_then(|s| s.docker.as_ref());
    let existing_url = existing.and_then(|s| s.url.as_ref());
    if existing_docker.is_some() {
        1
    } else if existing_url.is_some() {
        2
    } else if !nonstandard_extensions.is_empty() {
        1
    } else {
        0
    }
}

/// Prompt for shadow database mode with explanation.
///
/// `nonstandard_extensions` lists extensions installed in the source database
/// that the stock `postgres` image does not ship (e.g. postgis). When non-empty
/// we warn and pre-select the custom-image option, since an auto shadow would
/// fail at the first migration.
///
/// `existing` (re-init) pre-selects the configured mode and pre-fills the
/// image/platform/url inputs.
pub async fn prompt_shadow_mode_with_explanation(
    nonstandard_extensions: &[String],
    existing: Option<&crate::config::types::ShadowDatabaseInput>,
) -> Result<ShadowDatabaseInput> {
    // Check Docker availability first
    let (docker_available, debug_info) = crate::docker::DockerManager::is_available_verbose().await;

    println!("🛡️  Shadow database (for testing migrations safely)");

    let existing_docker = existing.and_then(|s| s.docker.as_ref());
    let existing_url = existing.and_then(|s| s.url.as_ref());

    if !docker_available {
        println!("   ⚠️  Docker not available - manual mode required");
        tracing::debug!("Docker availability details: {}", debug_info);

        let mut url_input = Input::new().with_prompt("   Shadow database URL");
        if let Some(url) = existing_url {
            url_input = url_input.default(url.clone());
        }
        let url: String = url_input.interact_text()?;
        return Ok(ShadowDatabaseInput::Manual(url.trim().to_string()));
    }

    if !nonstandard_extensions.is_empty() {
        println!(
            "   💡 Extensions not in the stock postgres image: {}",
            nonstandard_extensions.join(", ")
        );
        println!("      You'll likely need a shadow image that includes them.");
        println!("      See https://docs.pgmt.dev/docs/reference/configuration");
    }

    let choices = vec![
        "Auto (Docker-managed stock postgres, recommended)",
        "Docker with a specific image (e.g. PostGIS)",
        "External database URL",
    ];
    let selection = Select::new()
        .with_prompt("   Shadow database mode")
        .items(&choices)
        .default(shadow_mode_default(existing, nonstandard_extensions))
        .interact()?;

    match selection {
        0 => Ok(ShadowDatabaseInput::Auto),
        1 => {
            let mut image_input =
                Input::new().with_prompt("   Shadow Docker image (e.g. postgis/postgis:16-3.5)");
            if let Some(image) = existing_docker.and_then(|d| d.image.as_ref()) {
                image_input = image_input.default(image.clone());
            }
            let image: String = image_input.interact_text()?;

            let mut platform_input = Input::new()
                .with_prompt("   Platform (blank = host default, e.g. linux/amd64)")
                .allow_empty(true);
            if let Some(platform) = existing_docker.and_then(|d| d.platform.as_ref()) {
                platform_input = platform_input.default(platform.clone());
            }
            let platform: String = platform_input.interact_text()?;
            let platform = platform.trim();

            Ok(ShadowDatabaseInput::Docker {
                image: image.trim().to_string(),
                platform: (!platform.is_empty()).then(|| platform.to_string()),
            })
        }
        _ => {
            let mut url_input = Input::new().with_prompt("   Shadow database URL");
            if let Some(url) = existing_url {
                url_input = url_input.default(url.clone());
            }
            let url: String = url_input.interact_text()?;
            Ok(ShadowDatabaseInput::Manual(url.trim().to_string()))
        }
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

/// Test database connection and return PostgreSQL version plus installed
/// extensions (fetched here so callers don't need a second connection).
async fn test_database_connection_with_version(url: &str) -> Result<(String, Vec<String>)> {
    let pool = PgPool::connect(url).await?;

    // Get server version
    let row: (String,) = sqlx::query_as("SHOW server_version")
        .fetch_one(&pool)
        .await?;
    let extensions = fetch_installed_extensions(&pool).await;

    pool.close().await;

    // Parse version - extract just the version number (e.g., "15.4" from "15.4 (Debian ...)")
    let version = row
        .0
        .split_whitespace()
        .next()
        .unwrap_or(&row.0)
        .to_string();

    Ok((version, extensions))
}

/// Best-effort list of installed extensions; empty on query error. Used for
/// shadow-image guidance during init.
pub async fn fetch_installed_extensions(pool: &PgPool) -> Vec<String> {
    let rows: Result<Vec<(String,)>, _> =
        sqlx::query_as("SELECT extname FROM pg_extension ORDER BY extname")
            .fetch_all(pool)
            .await;
    rows.map(|r| r.into_iter().map(|(n,)| n).collect())
        .unwrap_or_default()
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
    fn test_shadow_mode_default() {
        use crate::config::types::{
            ShadowDatabaseInput as ShadowConfigInput, ShadowDockerInput,
        };

        let docker = ShadowConfigInput {
            docker: Some(ShadowDockerInput {
                image: Some("postgis/postgis:17-3.5".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let url = ShadowConfigInput {
            auto: Some(false),
            url: Some("postgres://localhost/shadow".to_string()),
            ..Default::default()
        };
        let auto = ShadowConfigInput {
            auto: Some(true),
            ..Default::default()
        };
        let postgis = vec!["postgis".to_string()];

        // Existing config's mode wins.
        assert_eq!(shadow_mode_default(Some(&docker), &[]), 1);
        assert_eq!(shadow_mode_default(Some(&docker), &postgis), 1);
        assert_eq!(shadow_mode_default(Some(&url), &[]), 2);

        // An auto config that would fail on nonstandard extensions is steered
        // to the custom-image option; otherwise auto stays the default.
        assert_eq!(shadow_mode_default(Some(&auto), &postgis), 1);
        assert_eq!(shadow_mode_default(Some(&auto), &[]), 0);
        assert_eq!(shadow_mode_default(None, &postgis), 1);
        assert_eq!(shadow_mode_default(None, &[]), 0);
    }

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

        let docker = ShadowDatabaseInput::Docker {
            image: "postgis/postgis:16-3.5".to_string(),
            platform: Some("linux/amd64".to_string()),
        };
        match docker {
            ShadowDatabaseInput::Docker { image, platform } => {
                assert_eq!(image, "postgis/postgis:16-3.5");
                assert_eq!(platform.as_deref(), Some("linux/amd64"));
            }
            _ => panic!("Expected Docker variant"),
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
