use anyhow::Result;
use dialoguer::{Confirm, Input, MultiSelect, Select};
use std::path::PathBuf;

use super::import::ImportSource;
use super::{BaselineCreationConfig, DatabaseState, InitOptions, ObjectManagementConfig};
use crate::config::types::{ConfigInput, Directories};
use crate::prompts::ShadowDatabaseInput;

/// Gather initialization options using CLI arguments when available
/// If `existing_config` is provided (re-init), its values are used as
/// defaults in prompts — same precedence as CLI args, just one layer lower.
pub async fn gather_init_options_with_args(
    args: &super::InitArgs,
    existing_config: Option<&ConfigInput>,
) -> Result<InitOptions> {
    // Current directory as default project directory
    let project_dir = std::env::current_dir()?;

    // Get directory defaults
    let dir_defaults = Directories::default();

    // Convenience views into the existing config's sections
    let existing_databases = existing_config.and_then(|c| c.databases.as_ref());
    let existing_directories = existing_config.and_then(|c| c.directories.as_ref());

    // Database URL - use CLI arg or prompt. The validation connection also
    // fetches the installed extensions, so the shadow guidance below doesn't
    // need to reconnect.
    let (dev_database_url, detected_pg_version, detected_extensions) = if let Some(url) =
        &args.dev_url
    {
        // CLI arg provided - test connection and detect version
        print!("🔄 Testing connection...");
        match sqlx::PgPool::connect(url).await {
            Ok(pool) => {
                let version: Result<(String,), _> =
                    sqlx::query_as("SHOW server_version").fetch_one(&pool).await;
                let extensions = crate::prompts::fetch_installed_extensions(&pool).await;
                pool.close().await;
                match version {
                    Ok((v,)) => {
                        let pg_version = v.split_whitespace().next().unwrap_or(&v).to_string();
                        println!(" ✅ (PostgreSQL {})", pg_version);
                        (url.clone(), Some(pg_version), Some(extensions))
                    }
                    Err(_) => {
                        println!(" ✅");
                        (url.clone(), None, Some(extensions))
                    }
                }
            }
            Err(e) => {
                println!(" ❌");
                return Err(anyhow::anyhow!("Connection failed: {}", e));
            }
        }
    } else if args.defaults {
        // In defaults mode, use existing value if available, otherwise use default
        let url = existing_databases
            .and_then(|d| d.dev_url.clone())
            .unwrap_or_else(|| "postgres://localhost/pgmt_dev".to_string());
        (url, None, None)
    } else {
        // Interactive prompt - pass existing value as default
        let existing_url = existing_databases.and_then(|d| d.dev_url.clone());
        let result =
            crate::prompts::prompt_database_url_with_guidance_and_default(existing_url).await?;
        (result.url, result.pg_version, Some(result.extensions))
    };

    // Shadow database configuration. Precedence (clap enforces the flags are
    // mutually consistent):
    // - --shadow-url            -> external URL (skips Docker)
    // - --shadow-image          -> Docker on that image (optionally --shadow-platform)
    // - --auto-shadow / --shadow-pg-version / --defaults -> auto (stock postgres)
    // - otherwise prompt, warning when the source DB uses extensions the stock
    //   postgres image lacks (PostGIS, …)
    let shadow_config = if let Some(url) = &args.shadow_url {
        crate::prompts::ShadowDatabaseInput::Manual(url.clone())
    } else if let Some(image) = &args.shadow_image {
        crate::prompts::ShadowDatabaseInput::Docker {
            image: image.clone(),
            platform: args.shadow_platform.clone(),
        }
    } else if args.auto_shadow || args.shadow_pg_version.is_some() || args.defaults {
        // Still warn in the non-interactive paths — an auto shadow on a source
        // DB with nonstandard extensions fails at the first migration.
        let nonstandard =
            nonstandard_extensions(detected_extensions.as_deref(), &dev_database_url).await;
        if !nonstandard.is_empty() {
            println!(
                "⚠️  Extensions not in the stock postgres image: {}",
                nonstandard.join(", ")
            );
            println!(
                "   The auto shadow database will likely fail; set databases.shadow.docker.image in pgmt.yaml."
            );
            println!("   See https://docs.pgmt.dev/docs/reference/configuration");
        }
        crate::prompts::ShadowDatabaseInput::Auto
    } else {
        let nonstandard =
            nonstandard_extensions(detected_extensions.as_deref(), &dev_database_url).await;
        crate::prompts::prompt_shadow_mode_with_explanation(&nonstandard).await?
    };

    // Schema directory - CLI arg > existing > default
    let schema_dir = if args.schema_dir != "schema" {
        // CLI arg was explicitly set (not default value)
        PathBuf::from(&args.schema_dir)
    } else {
        // Use existing or default
        existing_directories
            .and_then(|d| d.schema_dir.clone())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("schema"))
    };

    // Directory configuration - CLI args > existing > defaults
    let migrations_dir = args
        .migrations_dir
        .clone()
        .or_else(|| existing_directories.and_then(|d| d.migrations_dir.clone()))
        .unwrap_or_else(|| dir_defaults.migrations.clone());

    let baselines_dir = args
        .baselines_dir
        .clone()
        .or_else(|| existing_directories.and_then(|d| d.baselines_dir.clone()))
        .unwrap_or_else(|| dir_defaults.baselines.clone());

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

    // Managed-object scoping: honor an existing pgmt.yaml on re-init so the
    // shadow clean during import preserves non-managed (platform) schemas.
    let objects = crate::config::builder::resolve_objects_input(
        existing_config.and_then(|c| c.objects.as_ref()),
        &crate::config::types::Objects::default(),
    );

    Ok(InitOptions {
        project_dir,
        dev_database_url,
        shadow_config,
        shadow_pg_version: args.shadow_pg_version.clone(),
        detected_pg_version,
        schema_dir,
        migrations_dir,
        baselines_dir,
        import_source,
        object_config,
        baseline_config,
        tracking_table: crate::config::types::TrackingTable::default(),
        roles_file,
        objects,
    })
}

/// Extensions shipped in the official postgres Docker image (plpgsql + contrib).
/// Generated from `postgres:18-alpine` via
/// `ls /usr/local/share/postgresql/extension/*.control`.
const STOCK_IMAGE_EXTENSIONS: &[&str] = &[
    "amcheck",
    "autoinc",
    "bloom",
    "bool_plperl",
    "bool_plperlu",
    "btree_gin",
    "btree_gist",
    "citext",
    "cube",
    "dblink",
    "dict_int",
    "dict_xsyn",
    "earthdistance",
    "file_fdw",
    "fuzzystrmatch",
    "hstore",
    "hstore_plperl",
    "hstore_plperlu",
    "hstore_plpython3u",
    "insert_username",
    "intagg",
    "intarray",
    "isn",
    "jsonb_plperl",
    "jsonb_plperlu",
    "jsonb_plpython3u",
    "lo",
    "ltree",
    "ltree_plpython3u",
    "moddatetime",
    "pageinspect",
    "pg_buffercache",
    "pg_freespacemap",
    "pg_logicalinspect",
    "pg_prewarm",
    "pg_stat_statements",
    "pg_surgery",
    "pg_trgm",
    "pg_visibility",
    "pg_walinspect",
    "pgcrypto",
    "pgrowlocks",
    "pgstattuple",
    "plperl",
    "plperlu",
    "plpgsql",
    "plpython3u",
    "pltcl",
    "pltclu",
    "postgres_fdw",
    "refint",
    "seg",
    "sslinfo",
    "tablefunc",
    "tcn",
    "tsm_system_rows",
    "tsm_system_time",
    "unaccent",
    "uuid-ossp",
    "xml2",
];

/// Best-effort list of installed extensions the stock postgres image does not
/// ship, so init can warn before configuring a shadow that would fail.
///
/// Uses the extensions already fetched over the validation connection when
/// available; only the --defaults path (which never validated the URL)
/// connects here. Returns empty on any connection or query error — the
/// warning is a nicety, not a hard requirement.
async fn nonstandard_extensions(detected: Option<&[String]>, url: &str) -> Vec<String> {
    let installed = match detected {
        Some(extensions) => extensions.to_vec(),
        None => {
            let pool = match sqlx::postgres::PgPoolOptions::new()
                .acquire_timeout(std::time::Duration::from_secs(5))
                .connect(url)
                .await
            {
                Ok(pool) => pool,
                Err(_) => return Vec::new(),
            };
            let extensions = crate::prompts::fetch_installed_extensions(&pool).await;
            pool.close().await;
            extensions
        }
    };
    filter_nonstandard_extensions(installed)
}

/// Keep only extensions the stock postgres image does not provide.
fn filter_nonstandard_extensions(installed: Vec<String>) -> Vec<String> {
    installed
        .into_iter()
        .filter(|e| !STOCK_IMAGE_EXTENSIONS.contains(&e.as_str()))
        .collect()
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
/// Returns whether to proceed with initialization
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
        ShadowDatabaseInput::Docker { image, platform } => match platform {
            Some(p) => format!("Docker ({} on {})", image, p),
            None => format!("Docker ({})", image),
        },
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
    println!(
        "   📁 Directories: {} | {} | {}",
        options.schema_dir.display(),
        options.migrations_dir,
        options.baselines_dir
    );

    if let Some(ref import_source) = options.import_source {
        println!("   📥 Import: {}", import_source.description());
    }

    // Show confirmation prompt
    let choices = vec!["Yes, proceed", "No, cancel"];

    let selection = Select::new()
        .with_prompt("🚀 Proceed with these settings?")
        .items(&choices)
        .default(0)
        .interact()?;

    Ok(selection == 0)
}

/// Prompt to customize directories
/// Returns (schema_dir, migrations_dir, baselines_dir)
#[allow(dead_code)]
pub fn prompt_directory_customization(
    current_schema: &str,
    current_migrations: &str,
    current_baselines: &str,
) -> Result<(String, String, String)> {
    println!("\n📁 Directory Configuration:");

    let schema_dir: String = Input::new()
        .with_prompt("Schema directory")
        .default(current_schema.to_string())
        .interact_text()?;

    let migrations_dir: String = Input::new()
        .with_prompt("Migrations directory")
        .default(current_migrations.to_string())
        .interact_text()?;

    let baselines_dir: String = Input::new()
        .with_prompt("Baselines directory")
        .default(current_baselines.to_string())
        .interact_text()?;

    Ok((schema_dir, migrations_dir, baselines_dir))
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

    #[test]
    fn test_filter_nonstandard_extensions_stock_only() {
        // Contrib extensions ship with the official postgres image — no warning.
        let installed = vec![
            "plpgsql".to_string(),
            "pg_trgm".to_string(),
            "hstore".to_string(),
            "uuid-ossp".to_string(),
            "pgcrypto".to_string(),
        ];
        assert!(filter_nonstandard_extensions(installed).is_empty());
    }

    #[test]
    fn test_filter_nonstandard_extensions_keeps_third_party() {
        let installed = vec![
            "plpgsql".to_string(),
            "postgis".to_string(),
            "pg_trgm".to_string(),
            "timescaledb".to_string(),
            "vector".to_string(),
        ];
        assert_eq!(
            filter_nonstandard_extensions(installed),
            vec!["postgis", "timescaledb", "vector"]
        );
    }
}
