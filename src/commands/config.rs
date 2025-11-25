use crate::config::Config;
use anyhow::{Result, anyhow};
use serde_json;
use serde_yaml;
use std::path::Path;

/// Config subcommands
#[derive(Debug, Clone, clap::Subcommand)]
pub enum ConfigCommands {
    /// Get a configuration value
    Get {
        /// Configuration key (e.g., databases.dev, migration.tracking_table.name)
        key: String,

        /// Output format
        #[arg(long, default_value = "text")]
        format: OutputFormat,
    },

    /// Set a configuration value
    Set {
        /// Configuration key
        key: String,

        /// New value
        value: String,

        /// Configuration file to update
        #[arg(long, default_value = "pgmt.yaml")]
        config_file: String,
    },

    /// List all configuration values
    List {
        /// Output format
        #[arg(long, default_value = "yaml")]
        format: OutputFormat,
    },

    /// Validate configuration file
    Validate {
        /// Configuration file to validate
        #[arg(long, default_value = "pgmt.yaml")]
        config_file: String,
    },
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
    Yaml,
}

/// Execute config command
pub async fn cmd_config(config: &Config, subcommand: Option<ConfigCommands>) -> Result<()> {
    match subcommand {
        Some(ConfigCommands::Get { key, format }) => {
            let value = get_config_value(config, &key)?;
            print_value(&value, &format);
            Ok(())
        }

        Some(ConfigCommands::Set {
            key,
            value,
            config_file,
        }) => {
            set_config_value(&config_file, &key, &value)?;
            println!("✅ Configuration updated: {} = {}", key, value);
            Ok(())
        }

        Some(ConfigCommands::List { format }) => {
            list_config_values(config, &format)?;
            Ok(())
        }

        Some(ConfigCommands::Validate { config_file }) => {
            validate_config_file(&config_file)?;
            println!("✅ Configuration file '{}' is valid", config_file);
            Ok(())
        }

        None => {
            // No subcommand provided, show help
            println!("pgmt config - Manage pgmt configuration");
            println!();
            println!("Usage:");
            println!("  pgmt config get <KEY>           Get a configuration value");
            println!("  pgmt config set <KEY> <VALUE>   Set a configuration value");
            println!("  pgmt config list                List all configuration values");
            println!("  pgmt config validate            Validate configuration file");
            println!();
            println!("Examples:");
            println!("  pgmt config get databases.dev");
            println!("  pgmt config set migration.tracking_table.name my_migrations");
            println!("  pgmt config list --format json");
            Ok(())
        }
    }
}

/// Get a configuration value by key
fn get_config_value(config: &Config, key: &str) -> Result<String> {
    let parts: Vec<&str> = key.split('.').collect();

    match parts.as_slice() {
        ["databases", "dev"] => Ok(config.databases.dev.clone()),
        ["databases", "target"] => Ok(config
            .databases
            .target
            .clone()
            .unwrap_or_else(|| "(not set)".to_string())),

        ["directories", "schema"] => Ok(config.directories.schema.clone()),
        ["directories", "migrations"] => Ok(config.directories.migrations.clone()),
        ["directories", "baselines"] => Ok(config.directories.baselines.clone()),
        ["directories", "roles"] => Ok(config.directories.roles.clone()),

        ["migration", "default_mode"] => Ok(config.migration.default_mode.clone()),
        ["migration", "validate_baseline_consistency"] => {
            Ok(config.migration.validate_baseline_consistency.to_string())
        }
        ["migration", "create_baselines_by_default"] => {
            Ok(config.migration.create_baselines_by_default.to_string())
        }
        ["migration", "tracking_table", "schema"] => {
            Ok(config.migration.tracking_table.schema.clone())
        }
        ["migration", "tracking_table", "name"] => Ok(config.migration.tracking_table.name.clone()),

        ["objects", "comments"] => Ok(config.objects.comments.to_string()),
        ["objects", "grants"] => Ok(config.objects.grants.to_string()),
        ["objects", "triggers"] => Ok(config.objects.triggers.to_string()),
        ["objects", "extensions"] => Ok(config.objects.extensions.to_string()),

        ["docker", "auto_cleanup"] => Ok(config.docker.auto_cleanup.to_string()),
        ["docker", "check_system_identifier"] => {
            Ok(config.docker.check_system_identifier.to_string())
        }

        _ => Err(anyhow!("Unknown configuration key: {}", key)),
    }
}

/// Set a configuration value
fn set_config_value(config_file: &str, key: &str, value: &str) -> Result<()> {
    use crate::config::ConfigInput;

    // Read existing config file
    let config_path = Path::new(config_file);
    if !config_path.exists() {
        return Err(anyhow!("Configuration file '{}' not found", config_file));
    }

    let config_str = std::fs::read_to_string(config_path)?;
    let mut config_input: ConfigInput = serde_yaml::from_str(&config_str)?;

    // Parse the key and update the appropriate field
    let parts: Vec<&str> = key.split('.').collect();

    match parts.as_slice() {
        ["databases", "dev"] => {
            config_input
                .databases
                .get_or_insert_with(Default::default)
                .dev_url = Some(value.to_string());
        }
        ["databases", "target"] => {
            config_input
                .databases
                .get_or_insert_with(Default::default)
                .target_url = Some(value.to_string());
        }

        ["directories", "schema"] => {
            config_input
                .directories
                .get_or_insert_with(Default::default)
                .schema_dir = Some(value.to_string());
        }
        ["directories", "migrations"] => {
            config_input
                .directories
                .get_or_insert_with(Default::default)
                .migrations_dir = Some(value.to_string());
        }
        ["directories", "baselines"] => {
            config_input
                .directories
                .get_or_insert_with(Default::default)
                .baselines_dir = Some(value.to_string());
        }

        ["migration", "default_mode"] => {
            config_input
                .migration
                .get_or_insert_with(Default::default)
                .default_mode = Some(value.to_string());
        }
        ["migration", "validate_baseline_consistency"] => {
            let bool_val = value
                .parse::<bool>()
                .map_err(|_| anyhow!("Invalid boolean value: {}", value))?;
            config_input
                .migration
                .get_or_insert_with(Default::default)
                .validate_baseline_consistency = Some(bool_val);
        }
        ["migration", "create_baselines_by_default"] => {
            let bool_val = value
                .parse::<bool>()
                .map_err(|_| anyhow!("Invalid boolean value: {}", value))?;
            config_input
                .migration
                .get_or_insert_with(Default::default)
                .create_baselines_by_default = Some(bool_val);
        }
        ["migration", "tracking_table", "schema"] => {
            config_input
                .migration
                .get_or_insert_with(Default::default)
                .tracking_table
                .get_or_insert_with(Default::default)
                .schema = Some(value.to_string());
        }
        ["migration", "tracking_table", "name"] => {
            config_input
                .migration
                .get_or_insert_with(Default::default)
                .tracking_table
                .get_or_insert_with(Default::default)
                .name = Some(value.to_string());
        }

        ["objects", "comments"] => {
            let bool_val = value
                .parse::<bool>()
                .map_err(|_| anyhow!("Invalid boolean value: {}", value))?;
            config_input
                .objects
                .get_or_insert_with(Default::default)
                .comments = Some(bool_val);
        }
        ["objects", "grants"] => {
            let bool_val = value
                .parse::<bool>()
                .map_err(|_| anyhow!("Invalid boolean value: {}", value))?;
            config_input
                .objects
                .get_or_insert_with(Default::default)
                .grants = Some(bool_val);
        }
        ["objects", "triggers"] => {
            let bool_val = value
                .parse::<bool>()
                .map_err(|_| anyhow!("Invalid boolean value: {}", value))?;
            config_input
                .objects
                .get_or_insert_with(Default::default)
                .triggers = Some(bool_val);
        }
        ["objects", "extensions"] => {
            let bool_val = value
                .parse::<bool>()
                .map_err(|_| anyhow!("Invalid boolean value: {}", value))?;
            config_input
                .objects
                .get_or_insert_with(Default::default)
                .extensions = Some(bool_val);
        }

        ["docker", "auto_cleanup"] => {
            let bool_val = value
                .parse::<bool>()
                .map_err(|_| anyhow!("Invalid boolean value: {}", value))?;
            config_input
                .docker
                .get_or_insert_with(Default::default)
                .auto_cleanup = Some(bool_val);
        }
        ["docker", "check_system_identifier"] => {
            let bool_val = value
                .parse::<bool>()
                .map_err(|_| anyhow!("Invalid boolean value: {}", value))?;
            config_input
                .docker
                .get_or_insert_with(Default::default)
                .check_system_identifier = Some(bool_val);
        }

        _ => return Err(anyhow!("Unknown or unsupported configuration key: {}", key)),
    }

    // Write updated config back to file
    let yaml_str = serde_yaml::to_string(&config_input)?;
    std::fs::write(config_path, yaml_str)?;

    Ok(())
}

/// List all configuration values
fn list_config_values(config: &Config, format: &OutputFormat) -> Result<()> {
    // Create a structured representation of the config
    let config_map = serde_json::json!({
        "databases": {
            "dev": config.databases.dev,
            "target": config.databases.target,
        },
        "directories": {
            "schema": config.directories.schema,
            "migrations": config.directories.migrations,
            "baselines": config.directories.baselines,
            "roles": config.directories.roles,
        },
        "migration": {
            "default_mode": config.migration.default_mode,
            "validate_baseline_consistency": config.migration.validate_baseline_consistency,
            "create_baselines_by_default": config.migration.create_baselines_by_default,
            "tracking_table": {
                "schema": config.migration.tracking_table.schema,
                "name": config.migration.tracking_table.name,
            }
        },
        "objects": {
            "comments": config.objects.comments,
            "grants": config.objects.grants,
            "triggers": config.objects.triggers,
            "extensions": config.objects.extensions,
            "include": {
                "schemas": config.objects.include.schemas,
                "tables": config.objects.include.tables,
            },
            "exclude": {
                "schemas": config.objects.exclude.schemas,
                "tables": config.objects.exclude.tables,
            }
        },
        "docker": {
            "auto_cleanup": config.docker.auto_cleanup,
            "check_system_identifier": config.docker.check_system_identifier,
        }
    });

    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&config_map)?);
        }
        OutputFormat::Yaml => {
            println!("{}", serde_yaml::to_string(&config_map)?);
        }
        OutputFormat::Text => {
            println!("Current Configuration:");
            println!();
            print_config_text(&config_map, 0);
        }
    }

    Ok(())
}

/// Print configuration in text format with indentation
fn print_config_text(value: &serde_json::Value, indent: usize) {
    let prefix = "  ".repeat(indent);

    match value {
        serde_json::Value::Object(map) => {
            for (key, val) in map {
                if val.is_object() {
                    println!("{}{}:", prefix, key);
                    print_config_text(val, indent + 1);
                } else if val.is_array() {
                    println!("{}{}: {:?}", prefix, key, val);
                } else {
                    println!("{}{}: {}", prefix, key, val);
                }
            }
        }
        _ => {
            println!("{}{}", prefix, value);
        }
    }
}

/// Validate configuration file
fn validate_config_file(config_file: &str) -> Result<()> {
    use crate::config::{ConfigBuilder, ConfigInput};

    let config_path = Path::new(config_file);
    if !config_path.exists() {
        return Err(anyhow!("Configuration file '{}' not found", config_file));
    }

    // Try to parse as YAML
    let config_str = std::fs::read_to_string(config_path)?;
    let config_input: ConfigInput =
        serde_yaml::from_str(&config_str).map_err(|e| anyhow!("Invalid YAML syntax: {}", e))?;

    // Try to resolve with defaults to ensure all fields are valid
    let _resolved = ConfigBuilder::new()
        .with_file(config_input)
        .resolve()
        .map_err(|e| anyhow!("Configuration validation failed: {}", e))?;

    Ok(())
}

/// Print a configuration value
fn print_value(value: &str, format: &OutputFormat) {
    match format {
        OutputFormat::Text => println!("{}", value),
        OutputFormat::Json => {
            let json_val = serde_json::json!(value);
            println!("{}", json_val);
        }
        OutputFormat::Yaml => {
            println!("{}", value);
        }
    }
}
