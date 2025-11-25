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

/// Prompt for database URL with guidance and connection testing
pub async fn prompt_database_url_with_guidance() -> Result<String> {
    let explanation = "
📊 Development Database
   This is where you test your application locally during development.
   Examples:
   • postgres://localhost/myapp_dev
   • postgres://user:pass@localhost:5432/myapp_dev";

    println!("{}", explanation);

    loop {
        let url: String = Input::new()
            .with_prompt("💾 Database URL")
            .interact_text()?;

        let url = url.trim();
        if url.is_empty() {
            println!("❌ Database URL cannot be empty");
            continue;
        }

        // Test connection
        println!("🔄 Testing connection...");
        match test_database_connection(url).await {
            Ok(_) => {
                println!("✅ Connection successful!");
                return Ok(url.to_string());
            }
            Err(e) => {
                println!("❌ Connection failed: {}", e);
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
    let explanation = "
🛡️  Shadow Database Configuration
   pgmt uses a temporary database to safely test schema changes before applying them.
   This ensures your migrations are correct and won't break your development database.";

    println!("{}", explanation);

    // Check Docker availability first
    let (docker_available, debug_info) = crate::docker::DockerManager::is_available_verbose().await;

    if docker_available {
        let auto_explanation = "
   ✅ Auto mode (recommended): pgmt automatically creates and manages
      a temporary PostgreSQL database using Docker. No setup required.

   🔧 Manual mode: You provide a specific database URL that pgmt can use
      for testing. The database will be cleaned before each use.";
        println!("{}", auto_explanation);

        let auto = Confirm::new()
            .with_prompt("🔧 Use auto mode (recommended)?")
            .default(true)
            .interact()?;

        if auto {
            Ok(ShadowDatabaseInput::Auto)
        } else {
            let url: String = Input::new()
                .with_prompt("Shadow database URL")
                .interact_text()?;
            Ok(ShadowDatabaseInput::Manual(url.trim().to_string()))
        }
    } else {
        println!("⚠️  Docker not available: Auto mode requires Docker to be running.\n");
        println!("{}", debug_info);
        println!("💡 Troubleshooting tips:");
        println!("   • Make sure Docker Desktop is running");
        println!(
            "   • On macOS: Try 'export DOCKER_HOST=unix:///Users/$USER/.docker/run/docker.sock'"
        );
        println!("   • Check Docker Desktop settings or restart Docker");
        println!("   • For Colima users: Make sure Colima is running with 'colima start'");
        println!();

        let docker_error_explanation = "
   🔧 Manual mode: You'll need to specify your own shadow database URL.
      This can be any PostgreSQL database that pgmt can use temporarily.";
        println!("{}", docker_error_explanation);

        let url: String = Input::new()
            .with_prompt("Shadow database URL")
            .interact_text()?;
        Ok(ShadowDatabaseInput::Manual(url.trim().to_string()))
    }
}

/// Simple yes/no prompt
pub fn prompt_yes_no(prompt: &str, default: bool) -> Result<bool> {
    Confirm::new()
        .with_prompt(prompt)
        .default(default)
        .interact()
        .map_err(Into::into)
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
