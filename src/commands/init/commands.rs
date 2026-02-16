use anyhow::Result;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use super::InitArgs;
use super::import::{ImportSource, import_schema};
use super::project::{create_project_structure, generate_config_file};
use super::prompts::{gather_init_options_with_args, prompt_baseline_creation};
use crate::baseline::operations::{
    BaselineCreationRequest, create_baseline, display_baseline_summary,
};
use crate::catalog::Catalog;
use crate::config::load_config;
use crate::constants::CONFIG_FILENAME;
use crate::db::connection::mask_url_password;
use crate::migration_tracking;
use crate::prompts::ShadowDatabaseInput;

/// Result of checking for existing configuration
#[derive(Debug)]
pub enum ExistingConfigResult {
    /// No existing config found - proceed with fresh init
    NotFound,
    /// User chose to update existing config - provides loaded config
    Update(Box<crate::config::types::ConfigInput>),
    /// User chose fresh init - overwrite existing config
    Fresh,
    /// User cancelled the operation
    Cancelled,
}

/// Values extracted from existing config for use as defaults
#[derive(Debug, Default)]
pub struct ExistingConfigDefaults {
    pub dev_url: Option<String>,
    #[allow(dead_code)] // Future: use for shadow config prompting
    pub shadow_url: Option<String>,
    pub shadow_pg_version: Option<String>,
    pub schema_dir: Option<String>,
    pub migrations_dir: Option<String>,
    pub baselines_dir: Option<String>,
    #[allow(dead_code)] // Future: use for roles file prompting
    pub roles_file: Option<String>,
}

impl From<&crate::config::types::ConfigInput> for ExistingConfigDefaults {
    fn from(config: &crate::config::types::ConfigInput) -> Self {
        let shadow_url = config
            .databases
            .as_ref()
            .and_then(|d| d.shadow.as_ref())
            .and_then(|s| s.url.clone());

        let shadow_pg_version = config
            .databases
            .as_ref()
            .and_then(|d| d.shadow.as_ref())
            .and_then(|s| s.docker.as_ref())
            .and_then(|d| d.version.clone());

        Self {
            dev_url: config.databases.as_ref().and_then(|d| d.dev_url.clone()),
            shadow_url,
            shadow_pg_version,
            schema_dir: config
                .directories
                .as_ref()
                .and_then(|d| d.schema_dir.clone()),
            migrations_dir: config
                .directories
                .as_ref()
                .and_then(|d| d.migrations_dir.clone()),
            baselines_dir: config
                .directories
                .as_ref()
                .and_then(|d| d.baselines_dir.clone()),
            roles_file: config
                .directories
                .as_ref()
                .and_then(|d| d.roles_file.clone()),
        }
    }
}

/// Check for existing config file and prompt user for how to proceed
pub fn check_existing_config(
    project_dir: &Path,
    force_fresh: bool,
) -> Result<ExistingConfigResult> {
    let config_path = project_dir.join(CONFIG_FILENAME);

    if !config_path.exists() {
        return Ok(ExistingConfigResult::NotFound);
    }

    // If --fresh flag was passed, skip prompting
    if force_fresh {
        println!(
            "⚠️  Existing {} will be overwritten (--fresh flag)\n",
            CONFIG_FILENAME
        );
        return Ok(ExistingConfigResult::Fresh);
    }

    // Load existing config
    let config_path_str = config_path.to_string_lossy();
    let (existing_config, _) = load_config(&config_path_str)?;
    let defaults = ExistingConfigDefaults::from(&existing_config);

    // Show current configuration
    println!("📋 Existing configuration found:\n");
    if let Some(ref url) = defaults.dev_url {
        println!("   Database: {}", mask_url_password(url));
    }
    if let Some(ref schema_dir) = defaults.schema_dir {
        println!("   Schema dir: {}", schema_dir);
    }
    if let Some(ref migrations_dir) = defaults.migrations_dir {
        println!("   Migrations: {}", migrations_dir);
    }
    if let Some(ref baselines_dir) = defaults.baselines_dir {
        println!("   Baselines: {}", baselines_dir);
    }
    if let Some(ref pg_version) = defaults.shadow_pg_version {
        println!("   Shadow PG: {}", pg_version);
    }
    println!();

    // Prompt user for action
    let choices = vec![
        "Update - modify existing configuration",
        "Fresh - start over with new configuration",
        "Cancel - keep current configuration",
    ];

    let selection = dialoguer::Select::new()
        .with_prompt("What would you like to do?")
        .items(&choices)
        .default(0)
        .interact()?;

    match selection {
        0 => {
            println!("\n✏️  Update mode: existing values will be shown as defaults\n");
            Ok(ExistingConfigResult::Update(Box::new(existing_config)))
        }
        1 => {
            println!("\n🔄 Fresh mode: creating new configuration\n");
            Ok(ExistingConfigResult::Fresh)
        }
        _ => {
            println!("\n❌ Keeping existing configuration");
            Ok(ExistingConfigResult::Cancelled)
        }
    }
}

/// Complete configuration for project initialization
#[derive(Debug)]
pub struct InitOptions {
    pub project_dir: std::path::PathBuf,
    pub dev_database_url: String,
    pub shadow_config: ShadowDatabaseInput,
    /// PostgreSQL version for auto shadow database (e.g., "14", "15", "16")
    /// If None, uses detected_pg_version from dev database
    pub shadow_pg_version: Option<String>,
    /// PostgreSQL version detected from dev database connection (e.g., "15.4")
    pub detected_pg_version: Option<String>,
    pub schema_dir: std::path::PathBuf,
    pub migrations_dir: String,
    pub baselines_dir: String,
    pub import_source: Option<ImportSource>,
    pub object_config: ObjectManagementConfig,
    pub baseline_config: BaselineCreationConfig,
    #[allow(dead_code)]
    pub tracking_table: crate::config::types::TrackingTable,
    /// Path to roles file (None means no roles file, Some("roles.sql") means auto-detected or explicit)
    pub roles_file: Option<String>,
}

/// Configuration options for what database objects to manage
#[derive(Debug, Clone)]
pub struct ObjectManagementConfig {
    pub comments: bool,
    pub grants: bool,
    pub triggers: bool,
    pub extensions: bool,
}

impl Default for ObjectManagementConfig {
    fn default() -> Self {
        Self {
            comments: true,
            grants: true,
            triggers: true,
            extensions: true,
        }
    }
}

/// Configuration for baseline creation during init
#[derive(Debug, Clone, Default)]
pub struct BaselineCreationConfig {
    /// Whether to create baseline: None = prompt user, Some(true/false) = explicit
    pub create_baseline: Option<bool>,
    /// Custom description for baseline
    pub description: Option<String>,
}

/// Command with CLI arguments for non-interactive mode
pub async fn cmd_init_with_args(args: &InitArgs) -> Result<()> {
    println!("🚀 Welcome to pgmt! Let's set up your PostgreSQL migration project.\n");

    // Check for existing config before proceeding
    let project_dir = std::env::current_dir()?;
    let existing_config = check_existing_config(&project_dir, args.fresh)?;

    // Handle the different init modes
    let existing_defaults = match existing_config {
        ExistingConfigResult::NotFound | ExistingConfigResult::Fresh => None,
        ExistingConfigResult::Update(config) => Some(ExistingConfigDefaults::from(config.as_ref())),
        ExistingConfigResult::Cancelled => {
            return Ok(());
        }
    };

    // Gather configuration through prompts or CLI args (WITHOUT object management yet)
    let mut options = gather_init_options_with_args(args, existing_defaults.as_ref()).await?;

    // Show confirmation summary and get user approval (unless using defaults)
    if !args.defaults {
        let confirmed = super::prompts::prompt_project_confirmation(&options)?;
        if !confirmed {
            println!("❌ Project initialization cancelled by user.");
            return Ok(());
        }
    }

    // Step 1: Create directories only (no config file yet)
    println!("🏗️  Creating project structure...");
    create_project_structure(&options)?;
    println!("✅ Project directories created");

    // Step 2: Import existing schema catalog (just fetch, don't process yet)
    let catalog = if let Some(ref import_source) = options.import_source {
        import_catalog_from_source(import_source, &options).await?
    } else {
        None
    };

    // Step 3: Show preview and ask object management WITH context (if interactive)
    if let Some(ref cat) = catalog {
        // Show import preview
        show_catalog_preview(cat);

        // Ask object management questions with catalog context (if interactive)
        if !args.defaults {
            options.object_config =
                super::prompts::prompt_object_management_config_with_context(cat)?;
        }
    } else if !args.defaults {
        // No catalog, ask without context
        options.object_config = super::prompts::prompt_object_management_config()?;
    }

    // Step 4: Process the catalog (baseline, generate, validate, create)
    let baseline_result = if let Some(ref cat) = catalog {
        process_imported_catalog(cat, &options).await?
    } else {
        BaselineResult::NotRequested
    };

    // Step 5: Write config file LAST (now we have all the information)
    println!("📝 Generating configuration file...");
    generate_config_file(&options, &options.project_dir)?;
    println!("✅ pgmt.yaml created");

    // Success summary
    print_success_summary(&options, &baseline_result);

    Ok(())
}

/// Convert prompts::ShadowDatabaseInput + version to config::types::ShadowDatabase
fn resolve_shadow_database(
    shadow_config: &ShadowDatabaseInput,
    shadow_pg_version: Option<&String>,
    detected_pg_version: Option<&String>,
) -> crate::config::types::ShadowDatabase {
    use crate::config::types::{ShadowDatabase, ShadowDockerConfig};

    match shadow_config {
        ShadowDatabaseInput::Auto => {
            // CLI takes precedence, then detected version, then default
            let version = shadow_pg_version.or(detected_pg_version);
            if let Some(v) = version {
                let major_version = crate::prompts::extract_major_version(v);
                ShadowDatabase::Docker(ShadowDockerConfig {
                    version: Some(major_version),
                    ..Default::default()
                })
            } else {
                ShadowDatabase::Auto
            }
        }
        ShadowDatabaseInput::Manual(url) => ShadowDatabase::Url(url.clone()),
    }
}

/// Import catalog from source without processing it yet
/// Returns the catalog for later processing
async fn import_catalog_from_source(
    import_source: &ImportSource,
    options: &InitOptions,
) -> Result<Option<Catalog>> {
    println!("📥 Importing existing schema...");
    println!("   Source: {}", import_source.description());

    // Convert ShadowDatabaseInput to ShadowDatabase for import
    let shadow_database = resolve_shadow_database(
        &options.shadow_config,
        options.shadow_pg_version.as_ref(),
        options.detected_pg_version.as_ref(),
    );

    // Resolve roles file path for import (roles must exist before schema GRANTs)
    let roles_path = options
        .roles_file
        .as_ref()
        .map(|f| options.project_dir.join(f));

    match import_schema(
        import_source.clone(),
        &shadow_database,
        roles_path.as_deref(),
    )
    .await
    {
        Ok(catalog) => {
            println!("✅ Schema import completed");
            Ok(Some(catalog))
        }
        Err(e) => {
            // Use {:#} to show the full error chain including the root cause
            eprintln!("\n⚠️  Schema import failed:\n{:#}", e);
            eprintln!("\n🔧 What would you like to do?");

            let recovery_options = vec![
                "Skip import and continue with empty project",
                "Exit setup (you can run 'pgmt init' again later)",
            ];

            let choice = dialoguer::Select::new()
                .with_prompt("Choose an option")
                .items(&recovery_options)
                .default(0)
                .interact()?;

            match choice {
                0 => {
                    println!(
                        "⚠️  Skipping schema import. You can add schema files manually later."
                    );
                    println!(
                        "   💡 Tip: You can also try importing again with 'pgmt apply' after setup."
                    );
                    eprintln!("   Continuing with empty project setup...");
                    Ok(None)
                }
                1 => {
                    println!("❌ Setup cancelled. Run 'pgmt init' again when ready.");
                    std::process::exit(1);
                }
                _ => Ok(None),
            }
        }
    }
}

/// Result of baseline creation during init
#[derive(Debug, Clone)]
pub enum BaselineResult {
    /// Baseline was not requested
    NotRequested,
    /// Baseline was successfully created and synced
    Created,
    /// Validation found issues that need manual resolution (e.g., circular deps)
    NeedsAttention { reason: String },
    /// Baseline creation was requested but failed
    Failed(String),
}

/// Process an imported catalog - generate files, validate, then ask about baseline
async fn process_imported_catalog(
    catalog: &Catalog,
    options: &InitOptions,
) -> Result<BaselineResult> {
    let total_objects = count_catalog_objects(catalog);

    if total_objects == 0 {
        println!("⚠️  No database objects found in the imported schema.");
        println!("   Continuing with empty schema directory...");
        return Ok(BaselineResult::NotRequested);
    }

    // Step 1: Generate schema files (no questions, just do it)
    println!("\n📝 Generating schema files from your database...");
    let file_count = match generate_schema_files(catalog, options).await {
        Ok(count) => count,
        Err(e) => {
            eprintln!("❌ Schema file generation failed: {}", e);
            return Ok(BaselineResult::Failed(e.to_string()));
        }
    };
    println!("✅ Generated {} schema files", file_count);

    // Step 2: Validate schema files
    println!("\n🔍 Validating schema files...");
    let schema_dir = options.project_dir.join(&options.schema_dir);
    let roles_path = options
        .roles_file
        .as_ref()
        .map(|f| options.project_dir.join(f));

    match validate_schema_files(
        &schema_dir,
        roles_path.as_deref(),
        &options.shadow_config,
        options.shadow_pg_version.as_ref(),
        options.detected_pg_version.as_ref(),
    )
    .await
    {
        Ok(_) => {
            println!("✅ Schema validation passed");
        }
        Err(e) => {
            let error_str = format!("{:#}", e);

            // Check if this is a circular dependency (expected for complex databases)
            if error_str.contains("Circular dependency") {
                println!("\n📌 Circular dependency detected in schema files");
                if let Some(cycle_info) = extract_circular_dep_info(&error_str) {
                    println!("   {}", cycle_info);
                }
                println!();
                println!("   This is common in complex databases with bidirectional foreign keys.");
                println!("   To fix: move one foreign key to a separate file (e.g., constraints/)");
                println!("   so the tables can be created before the constraint is added.");
                return Ok(BaselineResult::NeedsAttention {
                    reason: "Circular dependency detected".to_string(),
                });
            }

            // Other validation failures are actual errors
            println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            println!("⚠️  SCHEMA VALIDATION FAILED");
            println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
            println!("{}\n", error_str);
            println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
            println!("Next steps:");
            println!("  1. Fix dependencies in schema files (add '-- require:' statements)");
            println!("  2. Test with: pgmt apply --dry-run");
            println!("  3. Repeat until validation passes");
            println!("  4. Create baseline: pgmt baseline create\n");
            return Ok(BaselineResult::Failed(e.to_string()));
        }
    }

    // Step 3: Schema is valid! Now ask about baseline
    let database_state = analyze_database_state(catalog);
    let should_create_baseline = match &options.baseline_config.create_baseline {
        Some(true) => true,   // CLI --create-baseline
        Some(false) => false, // CLI --no-baseline
        None => {
            // Interactive prompting based on database state
            prompt_baseline_creation(&database_state)?
        }
    };

    // Step 4: Create baseline if requested
    if should_create_baseline {
        let version = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        match create_baseline_with_migration_sync(catalog, options, version).await {
            Ok((_baseline_path, _baseline_content)) => Ok(BaselineResult::Created),
            Err(e) => {
                handle_baseline_failure(&e);
                Ok(BaselineResult::Failed(e.to_string()))
            }
        }
    } else {
        Ok(BaselineResult::NotRequested)
    }
}

/// Count total objects in a catalog
fn count_catalog_objects(catalog: &Catalog) -> usize {
    catalog.tables.len()
        + catalog.views.len()
        + catalog.functions.len()
        + catalog.types.len()
        + catalog.sequences.len()
        + catalog.indexes.len()
        + catalog.constraints.len()
        + catalog.triggers.len()
        + catalog.extensions.len()
        + catalog.grants.len()
}

/// Represents the state of a database for baseline decision making
#[derive(Debug)]
pub enum DatabaseState {
    /// Database is empty or only has the migration tracking table
    Empty,
    /// Database has existing objects that should be captured in a baseline
    Existing { object_count: usize },
}

/// Analyze database state to determine if it's empty or contains existing objects
fn analyze_database_state(catalog: &Catalog) -> DatabaseState {
    let total_objects = count_catalog_objects(catalog);

    // Consider database empty if it has 1 or fewer objects
    // (the migration tracking table is excluded from object counts by filtering)
    if total_objects <= 1 {
        DatabaseState::Empty
    } else {
        DatabaseState::Existing {
            object_count: total_objects,
        }
    }
}

/// Show a preview of catalog contents
fn show_catalog_preview(catalog: &Catalog) {
    let total_objects = count_catalog_objects(catalog);

    println!("\n📊 Schema Import Preview:");
    println!("  📋 {} tables", catalog.tables.len());
    println!("  👁 {} views", catalog.views.len());
    println!("  ⚙️ {} functions", catalog.functions.len());
    println!("  🏷️ {} custom types", catalog.types.len());
    println!("  🔢 {} sequences", catalog.sequences.len());
    println!("  📇 {} indexes", catalog.indexes.len());
    println!("  🔗 {} constraints", catalog.constraints.len());
    println!("  ⚡  {} triggers", catalog.triggers.len());
    println!("  🧩 {} extensions", catalog.extensions.len());
    println!("  🔑 {} grants", catalog.grants.len());
    println!("  ═══════════════════");
    println!("  📦 {} total objects", total_objects);
}

/// Extract the circular dependency cycle info from an error message
/// Returns the cycle portion like "A.sql -> B.sql -> A.sql"
fn extract_circular_dep_info(error_str: &str) -> Option<String> {
    // Look for the cycle pattern in the error message
    // Format: "Circular dependency detected: A -> B -> A"
    if let Some(start) = error_str.find("Circular dependency detected:") {
        let after_prefix = &error_str[start + "Circular dependency detected:".len()..];
        // Take until end of line or end of string
        let cycle = after_prefix
            .lines()
            .next()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        return cycle;
    }
    None
}

/// Handle baseline creation failure with user-friendly error messages and guidance
fn handle_baseline_failure(error: &anyhow::Error) {
    println!("\n❌ Baseline creation failed: {}", error);

    if error.to_string().contains("relation") && error.to_string().contains("does not exist") {
        println!("\n🔍 This error often indicates missing function dependencies.");
        println!("   Some functions may reference tables that haven't been loaded yet.");
        println!("   This is a known limitation - see README for details.");
        println!("\n💡 Common fixes:");
        println!("   • Add '-- require: tables/table_name.sql' to function files");
        println!("   • Check function bodies for table references");
        println!("   • Ensure proper loading order in your schema files");
    } else {
        println!("\n🔍 Baseline creation encountered an error.");
        println!("   This might be due to:");
        println!("   • Missing dependencies between schema objects");
        println!("   • Permission issues");
        println!("   • Database connection problems");
    }

    println!("\n⚠️  Skipping baseline creation due to errors.");
    println!("💡 After fixing the dependency issues, run: pgmt baseline create");
}

/// Create baseline from imported catalog during init and sync migration state
async fn create_baseline_with_migration_sync(
    catalog: &Catalog,
    options: &InitOptions,
    version: u64,
) -> Result<(std::path::PathBuf, String)> {
    println!("💾 Creating baseline from current database state...");

    // Create baseline using shared logic
    let request = BaselineCreationRequest {
        catalog: catalog.clone(),
        version,
        description: options
            .baseline_config
            .description
            .clone()
            .unwrap_or_else(|| "baseline".to_string()),
        baselines_dir: options.project_dir.join(&options.baselines_dir),
        verbose: false, // Less verbose for init context
    };

    let result = create_baseline(request).await?;

    // Show custom success message for init context
    println!(
        "✅ Created baseline: {}",
        result.path.file_name().unwrap().to_str().unwrap()
    );

    // Show baseline summary using shared display function
    display_baseline_summary(&result);

    // Mark baseline as applied in migration tracking
    println!("🔄 Marking baseline as applied in migration tracking...");

    // Connect to development database for migration tracking
    use sqlx::PgPool;
    let dev_pool = PgPool::connect(&options.dev_database_url).await?;

    // Use default tracking table configuration (will be read from config later)
    let tracking_table = crate::config::types::TrackingTable {
        schema: "public".to_string(),
        name: "pgmt_migrations".to_string(),
    };

    // Calculate checksum for baseline content
    let checksum = migration_tracking::calculate_checksum(&result.baseline_sql);

    // Record baseline as applied
    migration_tracking::record_baseline_as_applied(
        &dev_pool,
        &tracking_table,
        version,
        &options
            .baseline_config
            .description
            .clone()
            .unwrap_or_else(|| "baseline".to_string()),
        &checksum,
    )
    .await?;

    println!("✅ Baseline marked as applied in migration tracking");
    println!("💡 Future migrations will only contain NEW changes");

    Ok((result.path, result.baseline_sql))
}

/// Print success summary at the end of initialization
pub fn print_success_summary(options: &InitOptions, baseline_result: &BaselineResult) {
    match baseline_result {
        BaselineResult::Created => {
            println!("\n🎉 Project initialized successfully!");
            println!("\n📝 Created:");
            println!("  ✅ pgmt.yaml (configuration)");
            println!(
                "  ✅ {} directory with modular files",
                options.schema_dir.display()
            );
            println!("  ✅ migrations/ directory");
            println!("  ✅ schema_baselines/ directory");
            println!("  ✅ Initial baseline from existing database");

            println!("\nNext steps:");
            println!("  🚀 Run 'pgmt migrate new \"description\"' to create new migrations");
            println!("  💡 Future migrations will only contain NEW changes");
        }
        BaselineResult::NeedsAttention { reason } => {
            println!("\n🎉 Project initialized successfully!");
            println!("\n📝 Created:");
            println!("  ✅ pgmt.yaml (configuration)");
            println!(
                "  ✅ {} directory with modular files",
                options.schema_dir.display()
            );
            println!("  ✅ migrations/ directory");
            println!("  ✅ schema_baselines/ directory");

            println!("\n📌 {}", reason);
            println!("\nNext steps:");
            println!(
                "  1. Move one foreign key from the cycle to a separate file (e.g., schema/constraints/)"
            );
            println!("  2. Test with: pgmt apply --dry-run");
            println!("  3. Create baseline: pgmt baseline create");
            println!("  💻 Run 'pgmt apply' to sync your dev database");
            println!("  🚀 Run 'pgmt migrate new \"description\"' to create migrations");
        }
        BaselineResult::Failed(error) => {
            // Validation failure - schema needs to be fixed
            if error.contains("relation") || error.contains("does not exist") {
                println!("\n⚠️ Project initialized - schema validation failed\n");
                println!("📝 Created:");
                println!("   ✅ pgmt.yaml");
                println!(
                    "   ✅ {} (needs dependency fixes)",
                    options.schema_dir.display()
                );
                println!("   ✅ migrations/");
                println!("\n🔧 Next steps:");
                println!("   1. Fix schema dependencies (see error above)");
                println!("   2. Test with: pgmt apply --dry-run");
                println!("   3. Repeat until validation passes");
                println!("   4. Create baseline: pgmt baseline create");
            } else {
                // Check if baseline was explicitly requested via CLI
                let was_explicit_request =
                    matches!(options.baseline_config.create_baseline, Some(true));

                if was_explicit_request {
                    println!("\n⚠️ Project partially initialized - baseline creation failed!");
                    println!("\n📝 Created:");
                    println!("  ✅ pgmt.yaml (configuration)");
                    println!(
                        "  ✅ {} directory with modular files",
                        options.schema_dir.display()
                    );
                    println!("  ✅ migrations/ directory");
                    println!("  ✅ schema_baselines/ directory");
                    println!("  ❌ Initial baseline creation failed: {}", error);

                    println!("\nNext steps:");
                    println!("  🔧 Fix the baseline creation issue:");
                    println!("     • Check database connectivity and permissions");
                    println!("     • Review schema file dependencies");
                    println!("     • Consider running 'pgmt baseline create' manually");
                    println!("  💻 Run 'pgmt apply' to sync your dev database");
                    println!("  🚀 Run 'pgmt migrate new \"description\"' to create migrations");
                } else {
                    // Interactive prompt case - user chose baseline but it failed (non-validation)
                    println!("\n🎉 Project initialized successfully!");
                    println!("\n📝 Created:");
                    println!("  ✅ pgmt.yaml (configuration)");
                    println!(
                        "  ✅ {} directory with modular files",
                        options.schema_dir.display()
                    );
                    println!("  ✅ migrations/ directory");
                    println!("  ✅ schema_baselines/ directory");
                    println!("  ⚠️ Baseline creation failed (see error above)");

                    println!("\nNext steps:");
                    println!("  💡 Fix the issue and create baseline: pgmt baseline create");
                    println!("  💻 Run 'pgmt apply' to sync your dev database");
                    println!("  🚀 Run 'pgmt migrate new \"description\"' to create migrations");
                }
            }
        }
        BaselineResult::NotRequested => {
            println!("\n🎉 Project initialized successfully!");
            println!("\n📝 Created:");
            println!("  ✅ pgmt.yaml (configuration)");
            println!(
                "  ✅ {} directory with modular files",
                options.schema_dir.display()
            );
            println!("  ✅ migrations/ directory");
            println!("  ✅ schema_baselines/ directory");

            println!("\nNext steps:");
            println!("  💻 Run 'pgmt apply' to sync your dev database");
            println!(
                "  📝 Add schema files to {} and customize as needed",
                options.schema_dir.display()
            );
            println!("  🚀 Run 'pgmt migrate new \"description\"' to create migrations");
        }
    }

    println!("  📚 Visit https://docs.pgmt.dev for more information");
}

/// Validate generated schema files by applying them to a shadow database
async fn validate_schema_files(
    schema_dir: &std::path::Path,
    roles_file: Option<&std::path::Path>,
    shadow_config: &ShadowDatabaseInput,
    shadow_pg_version: Option<&String>,
    detected_pg_version: Option<&String>,
) -> Result<()> {
    validate_schema_files_impl(
        schema_dir,
        roles_file,
        shadow_config,
        shadow_pg_version,
        detected_pg_version,
    )
    .await
}

/// Implementation of schema validation
async fn validate_schema_files_impl(
    schema_dir: &std::path::Path,
    roles_file: Option<&std::path::Path>,
    shadow_config: &ShadowDatabaseInput,
    shadow_pg_version: Option<&String>,
    detected_pg_version: Option<&String>,
) -> Result<()> {
    use crate::db::cleaner;
    use crate::db::connection::connect_with_retry;
    use crate::db::schema_processor::{SchemaProcessor, SchemaProcessorConfig};

    // Get shadow URL from in-memory config (no yaml file needed!)
    let shadow_database =
        resolve_shadow_database(shadow_config, shadow_pg_version, detected_pg_version);
    let shadow_url = shadow_database.get_connection_string().await?;

    // Connect to shadow database
    let pool = connect_with_retry(&shadow_url).await?;

    // Clean shadow database first
    cleaner::clean_shadow_db(&pool, &crate::config::types::Objects::default()).await?;

    // Apply roles file before schema files (if provided)
    if let Some(roles_path) = roles_file
        && roles_path.exists()
    {
        crate::schema_ops::apply_roles_file(&pool, roles_path).await?;
    }

    // Process schema directory (loads, orders, and applies all files)
    // Note: clean_before_apply is false since we already cleaned above
    let config = SchemaProcessorConfig {
        verbose: false,            // Silent validation
        clean_before_apply: false, // Already cleaned above
        ..Default::default()
    };
    let processor = SchemaProcessor::new(pool.clone(), config);
    processor.process_schema_directory(schema_dir).await?;

    pool.close().await;
    Ok(())
}

/// Generate modular schema files using the diffing-based schema generator
async fn generate_schema_files(catalog: &Catalog, options: &InitOptions) -> Result<usize> {
    use crate::schema_generator::{SchemaGenerator, SchemaGeneratorConfig};

    let schema_path = options.project_dir.join(&options.schema_dir);
    std::fs::create_dir_all(&schema_path)?;

    // Configure schema generation based on object management settings
    let config = SchemaGeneratorConfig {
        include_comments: options.object_config.comments,
        include_grants: options.object_config.grants,
        include_triggers: options.object_config.triggers,
        include_extensions: options.object_config.extensions,
    };

    // Create and run the diffing-based schema generator
    let generator = SchemaGenerator::new(catalog.clone(), schema_path.clone(), config);
    generator.generate_files()?;

    // Count generated files
    let file_count = count_generated_files(&schema_path)?;

    Ok(file_count)
}

/// Count the number of files generated in the schema directory
fn count_generated_files(schema_dir: &std::path::PathBuf) -> Result<usize> {
    let mut count = 0;

    if schema_dir.exists() {
        for entry in std::fs::read_dir(schema_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("sql") {
                count += 1;
            } else if path.is_dir() {
                // Recursively count files in subdirectories
                count += count_generated_files(&path)?;
            }
        }
    }

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_management_config_default() {
        let config = ObjectManagementConfig::default();
        assert!(config.comments);
        assert!(config.grants);
        assert!(config.triggers);
        assert!(config.extensions);
    }

    #[test]
    fn test_count_catalog_objects() {
        let catalog = Catalog::empty();
        assert_eq!(count_catalog_objects(&catalog), 0);
    }

    #[test]
    fn test_count_generated_files() {
        use std::env;

        let temp_dir = env::temp_dir().join("pgmt_test_count_files");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        // Create test files
        std::fs::write(temp_dir.join("test1.sql"), "SELECT 1;").unwrap();
        std::fs::write(temp_dir.join("test2.sql"), "SELECT 2;").unwrap();
        std::fs::write(temp_dir.join("readme.txt"), "Not SQL").unwrap();

        let count = count_generated_files(&temp_dir).unwrap();
        assert_eq!(count, 2); // Only .sql files are counted

        // Clean up
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_existing_config_defaults_from_config_input() {
        use crate::config::types::{
            ConfigInput, DatabasesInput, DirectoriesInput,
            ShadowDatabaseInput as ConfigShadowInput, ShadowDockerInput,
        };

        let config = ConfigInput {
            databases: Some(DatabasesInput {
                dev_url: Some("postgres://localhost/mydb".to_string()),
                shadow: Some(ConfigShadowInput {
                    docker: Some(ShadowDockerInput {
                        version: Some("15".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            directories: Some(DirectoriesInput {
                schema_dir: Some("custom_schema".to_string()),
                migrations_dir: Some("db/migrations".to_string()),
                baselines_dir: Some("db/baselines".to_string()),
                roles_file: Some("roles.sql".to_string()),
            }),
            ..Default::default()
        };

        let defaults = ExistingConfigDefaults::from(&config);

        assert_eq!(
            defaults.dev_url,
            Some("postgres://localhost/mydb".to_string())
        );
        assert_eq!(defaults.shadow_pg_version, Some("15".to_string()));
        assert_eq!(defaults.schema_dir, Some("custom_schema".to_string()));
        assert_eq!(defaults.migrations_dir, Some("db/migrations".to_string()));
        assert_eq!(defaults.baselines_dir, Some("db/baselines".to_string()));
        assert_eq!(defaults.roles_file, Some("roles.sql".to_string()));
    }

    #[test]
    fn test_existing_config_defaults_from_empty_config() {
        use crate::config::types::ConfigInput;

        let config = ConfigInput::default();
        let defaults = ExistingConfigDefaults::from(&config);

        assert_eq!(defaults.dev_url, None);
        assert_eq!(defaults.shadow_pg_version, None);
        assert_eq!(defaults.schema_dir, None);
        assert_eq!(defaults.migrations_dir, None);
        assert_eq!(defaults.baselines_dir, None);
        assert_eq!(defaults.roles_file, None);
    }

    #[test]
    fn test_check_existing_config_not_found() {
        use std::env;

        let temp_dir = env::temp_dir().join("pgmt_test_no_config");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        // No config file - should return NotFound
        let result = check_existing_config(&temp_dir, false).unwrap();
        assert!(matches!(result, ExistingConfigResult::NotFound));

        // Clean up
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_check_existing_config_fresh_flag() {
        use std::env;

        let temp_dir = env::temp_dir().join("pgmt_test_fresh_flag");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        // Create a config file
        let config_content = r#"
databases:
  dev_url: postgres://localhost/test
"#;
        std::fs::write(temp_dir.join("pgmt.yaml"), config_content).unwrap();

        // With --fresh flag, should return Fresh without prompting
        let result = check_existing_config(&temp_dir, true).unwrap();
        assert!(matches!(result, ExistingConfigResult::Fresh));

        // Clean up
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
