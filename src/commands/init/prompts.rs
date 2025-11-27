use anyhow::Result;
use dialoguer::{Confirm, Input, MultiSelect};
use std::path::PathBuf;

use super::import::ImportSource;
use super::{BaselineCreationConfig, DatabaseState, InitOptions, ObjectManagementConfig};
use crate::prompts::ShadowDatabaseInput;

/// Gather initialization options using CLI arguments when available
pub async fn gather_init_options_with_args(args: &super::InitArgs) -> Result<InitOptions> {
    // Current directory as default project directory
    let project_dir = std::env::current_dir()?;

    // Database URL - use CLI arg or prompt
    let (dev_database_url, detected_pg_version) = if let Some(url) = &args.dev_url {
        // CLI arg provided - test connection and detect version
        print!("🔄 Testing connection...");
        match sqlx::PgPool::connect(url).await {
            Ok(pool) => {
                let version: Result<(String,), _> =
                    sqlx::query_as("SHOW server_version").fetch_one(&pool).await;
                pool.close().await;
                match version {
                    Ok((v,)) => {
                        let pg_version = v.split_whitespace().next().unwrap_or(&v).to_string();
                        println!(" ✅ (PostgreSQL {})", pg_version);
                        (url.clone(), Some(pg_version))
                    }
                    Err(_) => {
                        println!(" ✅");
                        (url.clone(), None)
                    }
                }
            }
            Err(e) => {
                println!(" ❌");
                return Err(anyhow::anyhow!("Connection failed: {}", e));
            }
        }
    } else if args.defaults {
        ("postgres://localhost/pgmt_dev".to_string(), None)
    } else {
        let result = crate::prompts::prompt_database_url_with_guidance().await?;
        (result.url, result.pg_version)
    };

    // Shadow database configuration
    // - If --shadow-pg-version is specified, use auto mode with that version
    // - If --auto-shadow is specified, use auto mode
    // - If defaults mode, use auto mode
    // - Otherwise prompt (but we'll simplify this prompt too)
    let shadow_config = if args.auto_shadow || args.shadow_pg_version.is_some() || args.defaults {
        crate::prompts::ShadowDatabaseInput::Auto
    } else {
        crate::prompts::prompt_shadow_mode_with_explanation().await?
    };

    // Schema directory - use CLI arg
    let schema_dir = PathBuf::from(&args.schema_dir);

    // Import existing schema option - use CLI arg or prompt
    let import_source = if args.no_import || args.defaults {
        None
    } else {
        prompt_import_source().await?
    };

    // Object management configuration - use defaults for now
    // Will be prompted later with catalog context in the main flow
    let object_config = ObjectManagementConfig::default();

    // Baseline configuration - use CLI args or set up for prompting
    let baseline_config = if args.create_baseline {
        BaselineCreationConfig {
            create_baseline: Some(true),
            description: args.baseline_description.clone(),
        }
    } else if args.no_baseline || args.defaults {
        BaselineCreationConfig {
            create_baseline: Some(false),
            description: None,
        }
    } else {
        BaselineCreationConfig {
            create_baseline: None, // Will prompt user based on database state
            description: args.baseline_description.clone(),
        }
    };

    // Roles file handling:
    // 1. If --roles-file provided, use that path
    // 2. Otherwise, auto-detect roles.sql in project directory
    let roles_file = if let Some(path) = &args.roles_file {
        println!("   Using roles file: {}", path);
        Some(path.clone())
    } else if project_dir.join("roles.sql").exists() {
        println!("   Found roles.sql - will use for shadow database setup");
        Some("roles.sql".to_string())
    } else {
        None
    };

    Ok(InitOptions {
        project_dir,
        dev_database_url,
        shadow_config,
        shadow_pg_version: args.shadow_pg_version.clone(),
        detected_pg_version,
        schema_dir,
        import_source,
        object_config,
        baseline_config,
        tracking_table: crate::config::types::TrackingTable::default(),
        roles_file,
    })
}

/// Prompt for schema import options
pub async fn prompt_import_source() -> Result<Option<ImportSource>> {
    let options = vec![
        (0, "None (empty project)"),
        (1, "SQL dump file (e.g., from pg_dump)"),
        (2, "Directory with SQL files"),
        (3, "Live database connection"),
    ];

    let choice = crate::prompts::prompt_select("📥 Import from", options)?;

    let source = match choice {
        0 => return Ok(None),
        1 => {
            let file = prompt_import_sql_file()?;
            ImportSource::SqlFile(file)
        }
        2 => {
            let dir = prompt_import_directory()?;
            ImportSource::Directory(dir)
        }
        3 => {
            let url = prompt_import_database_url().await?;
            ImportSource::Database(url)
        }
        _ => return Err(anyhow::anyhow!("Invalid choice")),
    };

    Ok(Some(source))
}

/// Prompt for import directory with validation
fn prompt_import_directory() -> Result<PathBuf> {
    loop {
        let dir = crate::prompts::prompt_directory_with_validation(
            "📁 SQL files directory",
            Some("./db/migrate"),
        )?;

        // Validate directory contains SQL files
        match crate::db::sql_executor::discover_sql_files_ordered(&dir) {
            Ok(files) if files.is_empty() => {
                println!("⚠️  Directory '{}' contains no SQL files.", dir.display());
                let retry = Confirm::new()
                    .with_prompt("Try a different directory?")
                    .default(true)
                    .interact()?;

                if !retry {
                    return Err(anyhow::anyhow!("No SQL files found for import"));
                }
                continue;
            }
            Ok(files) => {
                println!("✅ Found {} SQL files in directory", files.len());
                return Ok(dir);
            }
            Err(e) => {
                println!("❌ Error reading directory: {}", e);
                let retry = Confirm::new()
                    .with_prompt("Try a different directory?")
                    .default(true)
                    .interact()?;

                if !retry {
                    return Err(anyhow::anyhow!("Cannot read directory for import"));
                }
                continue;
            }
        }
    }
}

/// Prompt for SQL file with validation
fn prompt_import_sql_file() -> Result<PathBuf> {
    loop {
        let file_path: String = Input::new()
            .with_prompt("📄 SQL file path")
            .default("./dump.sql".to_string())
            .interact_text()?;

        let file_path = PathBuf::from(file_path.trim());

        // Validate file exists and is readable
        match super::import::sql_file::validate_sql_file(&file_path) {
            Ok(_) => {
                println!("✅ SQL file validation passed");
                return Ok(file_path);
            }
            Err(e) => {
                println!("❌ SQL file validation failed: {}", e);
                let retry = Confirm::new()
                    .with_prompt("Try a different file?")
                    .default(true)
                    .interact()?;

                if !retry {
                    return Err(anyhow::anyhow!("Invalid SQL file for import"));
                }
                continue;
            }
        }
    }
}

/// Prompt for database URL for import
async fn prompt_import_database_url() -> Result<String> {
    crate::prompts::prompt_database_url_simple("🔗 Source database URL").await
}

/// Prompt for object management configuration (without catalog context)
pub fn prompt_object_management_config() -> Result<ObjectManagementConfig> {
    let advanced_explanation = "
⚙️  Advanced Options (Optional)
   pgmt can manage additional database objects like comments, user permissions,
   triggers, and extensions. The defaults work well for most projects.";

    println!("{}", advanced_explanation);

    let configure_advanced = Confirm::new()
        .with_prompt("Configure advanced object management?")
        .default(false)
        .interact()?;

    if !configure_advanced {
        return Ok(ObjectManagementConfig::default());
    }

    let items = vec![
        "Comments (table/column documentation)",
        "Grants (user permissions)",
        "Triggers (automated actions)",
        "Extensions (PostgreSQL extensions)",
    ];

    println!("\n🎯 Select object types to manage (use Space to toggle, Enter to confirm):");
    let selections = MultiSelect::new()
        .with_prompt("Which object types should pgmt manage?")
        .items(&items)
        .defaults(&[true, true, true, true]) // All enabled by default
        .interact()?;

    // Convert selections to individual booleans
    let comments = selections.contains(&0);
    let grants = selections.contains(&1);
    let triggers = selections.contains(&2);
    let extensions = selections.contains(&3);

    Ok(ObjectManagementConfig {
        comments,
        grants,
        triggers,
        extensions,
    })
}

/// Prompt for object management configuration WITH catalog context
/// Uses smart defaults, only prompts for grants if there are many (>100)
pub fn prompt_object_management_config_with_context(
    catalog: &crate::catalog::Catalog,
) -> Result<ObjectManagementConfig> {
    // Count objects
    let comment_count = count_commentable_objects(catalog);
    let grant_count = catalog.grants.len();
    let trigger_count = catalog.triggers.len();
    let extension_count = catalog.extensions.len();

    // Smart defaults: enable what exists in the database
    let mut config = ObjectManagementConfig {
        comments: comment_count > 0,
        grants: grant_count > 0,
        triggers: trigger_count > 0,
        extensions: extension_count > 0,
    };

    // Only prompt for grants if there are many (complex permission setup)
    // Simple projects with few/no grants don't need to think about this
    if grant_count > 100 {
        println!(
            "\n   Your schema has {} grant definitions across multiple roles.",
            grant_count
        );
        let manage_grants = Confirm::new()
            .with_prompt("🔑 Manage GRANTs/permissions?")
            .default(true)
            .interact()?;
        config.grants = manage_grants;
    }

    Ok(config)
}

/// Count objects that can have comments (tables, views, functions)
fn count_commentable_objects(catalog: &crate::catalog::Catalog) -> usize {
    catalog.tables.len() + catalog.views.len() + catalog.functions.len()
}

/// Prompt user for baseline creation based on database state with detailed explanations
pub fn prompt_baseline_creation(database_state: &DatabaseState) -> Result<bool> {
    match database_state {
        DatabaseState::Empty => {
            println!("\n📊 Database Analysis: Empty database detected");
            println!("   No baseline needed for empty databases.");
            println!("   You can create schema files and use 'pgmt apply' to build your schema.\n");
            Ok(false)
        }
        DatabaseState::Existing { object_count } => {
            println!("\n📊 Database Analysis: Existing database detected");
            println!(
                "   Found {} database objects that can be imported into schema files.",
                object_count
            );

            let explanation = "
❓ Should we create a baseline from your existing database?

💡 CREATE a baseline if:
   ✅ This database has EVER been deployed (production, staging, etc.)
   ✅ Other developers or environments use this schema
   ✅ You want clean migration history going forward

   A baseline captures your current state as 'migration zero' so future
   migrations only contain NEW changes, not recreating existing objects.

⚠️  SKIP baseline ONLY if:
   ❌ This database has NEVER been deployed anywhere
   ❌ You're the only developer and this is purely local
   ❌ You want the first migration to recreate everything from scratch

   Without a baseline, 'pgmt migrate new' will try to DROP and recreate
   ALL existing objects, which will fail on deployed databases.

🔍 What is a baseline?
   A baseline is a SQL snapshot of your current database schema.
   It gets marked as 'applied' so future migrations build on top of it.";

            println!("{}", explanation);

            let create_baseline = Confirm::new()
                .with_prompt("Create baseline? (Yes if this has EVER been deployed)")
                .default(true)
                .interact()?;

            if create_baseline {
                println!("✅ Baseline will be created and marked as applied");
                println!("   Future migrations will only contain new changes");
            } else {
                println!("⚠️  Baseline creation skipped");
                println!("   Next migration will attempt to recreate existing objects");
            }

            Ok(create_baseline)
        }
    }
}

/// Prompt for project creation confirmation with summary
pub fn prompt_project_confirmation(options: &InitOptions) -> Result<bool> {
    // Build compact summary line
    let shadow_version = options
        .shadow_pg_version
        .as_ref()
        .or(options
            .detected_pg_version
            .as_ref()
            .map(|v| {
                // Extract major version from detected (e.g., "15" from "15.4")
                crate::prompts::extract_major_version(v)
            })
            .as_ref())
        .cloned()
        .unwrap_or_else(|| "auto".to_string());

    let shadow_desc = match &options.shadow_config {
        ShadowDatabaseInput::Auto => {
            if options.detected_pg_version.is_some() {
                format!("Docker (PG {} - matches dev)", shadow_version)
            } else {
                format!("Docker (PG {})", shadow_version)
            }
        }
        ShadowDatabaseInput::Manual(url) => format!("Manual ({})", mask_sensitive_url(url)),
    };

    // Build roles info if present
    let roles_info = options
        .roles_file
        .as_ref()
        .map(|f| format!(" | Roles: {}", f))
        .unwrap_or_default();

    println!(
        "\n📋 Setup: {}",
        mask_sensitive_url(&options.dev_database_url)
    );
    println!("   💡 Shadow: {}{}", shadow_desc, roles_info);

    if let Some(ref import_source) = options.import_source {
        println!("   📥 Import: {}", import_source.description());
    }

    Confirm::new()
        .with_prompt("\n🚀 Proceed?")
        .default(true)
        .interact()
        .map_err(Into::into)
}

/// Mask sensitive parts of database URL for display
fn mask_sensitive_url(url: &str) -> String {
    // Handle case where URL doesn't contain ://
    if !url.contains("://") {
        return "Invalid URL".to_string();
    }

    // Split on :// to remove protocol
    let parts: Vec<&str> = url.splitn(2, "://").collect();
    if parts.len() != 2 {
        return "Invalid URL".to_string();
    }

    let authority_and_path = parts[1];

    // Check if there's user info (user:pass@host or user@host)
    if let Some(at_pos) = authority_and_path.find('@') {
        let user_info = &authority_and_path[..at_pos];
        let host_and_path = &authority_and_path[at_pos + 1..];

        // Extract just the username part (before any colon)
        let username = if let Some(colon_pos) = user_info.find(':') {
            &user_info[..colon_pos]
        } else {
            user_info
        };

        format!("{}@{}", username, host_and_path)
    } else {
        // No user info, just return host and path
        authority_and_path.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mask_sensitive_url() {
        assert_eq!(
            mask_sensitive_url("postgres://user:pass@localhost:5432/mydb"),
            "user@localhost:5432/mydb"
        );

        assert_eq!(
            mask_sensitive_url("postgres://localhost/mydb"),
            "localhost/mydb"
        );

        assert_eq!(mask_sensitive_url("invalid url"), "Invalid URL");
    }
}
