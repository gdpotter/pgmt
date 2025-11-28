use crate::config::{Config, ObjectFilter};
use crate::db::connection::connect_with_retry;
use crate::migrate::{MigrationGenerationInput, generate_migration};
use crate::migration::{
    BaselineConfig, ensure_baseline_for_migration, get_migration_starting_state,
    validate_baseline_against_catalog_with_suggestions,
};
use crate::prompts::prompt_required_string_with_validation;
use anyhow::Result;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::debug;

pub async fn cmd_migrate_new(
    config: &Config,
    root_dir: &Path,
    description: Option<&str>,
) -> Result<()> {
    let description = prompt_required_string_with_validation(
        description,
        "Enter migration description",
        |input| {
            if input.is_empty() {
                return Err("Description cannot be empty".to_string());
            }
            if input.contains('/') || input.contains('\\') {
                return Err("Description cannot contain path separators".to_string());
            }
            if input.len() > 100 {
                return Err("Description must be 100 characters or less".to_string());
            }
            Ok(())
        },
    )?;

    println!("Generating migration: {}", description);

    let migrations_dir = root_dir.join("migrations");
    let baselines_dir = root_dir.join("schema_baselines");
    std::fs::create_dir_all(&migrations_dir)?;
    std::fs::create_dir_all(&baselines_dir)?;

    let version = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| anyhow::anyhow!("System time is before Unix epoch: {}", e))?
        .as_secs();

    let shadow_url = config.databases.shadow.get_connection_string().await?;
    let shadow_pool = connect_with_retry(&shadow_url).await?;

    let baseline_config = BaselineConfig {
        validate_consistency: config.migration.validate_baseline_consistency,
        verbose: true,
    };
    let roles_file = root_dir.join(&config.directories.roles);
    let old_catalog = get_migration_starting_state(
        &shadow_pool,
        &baselines_dir,
        &migrations_dir,
        &roles_file,
        &baseline_config,
    )
    .await?;

    debug!("Applying current schema to shadow database");
    // Use apply_current_schema_to_shadow which respects file dependency settings
    // This function creates its own connection, so close the existing one first
    shadow_pool.close().await;
    let new_catalog = crate::schema_ops::apply_current_schema_to_shadow(config, root_dir).await?;

    // Reconnect for baseline validation later
    let shadow_pool = connect_with_retry(&shadow_url).await?;

    let filter = ObjectFilter::new(&config.objects, &config.migration.tracking_table);
    let filtered_old_catalog = filter.filter_catalog(old_catalog);
    let filtered_new_catalog = filter.filter_catalog(new_catalog);

    debug!("Generating migration steps");
    let migration_result = generate_migration(MigrationGenerationInput {
        old_catalog: filtered_old_catalog,
        new_catalog: filtered_new_catalog.clone(),
        description: description.clone(),
        version,
    })?;

    if !migration_result.has_changes {
        println!("No changes detected - no migration needed");
        return Ok(());
    }

    let migration_path = migrations_dir.join(&migration_result.migration_filename);
    std::fs::write(&migration_path, &migration_result.migration_sql)?;
    println!("Created migration: {}", migration_path.display());

    let should_create_baseline = config.migration.create_baselines_by_default;
    if should_create_baseline {
        let result = ensure_baseline_for_migration(
            &baselines_dir,
            version,
            &migration_result.migration_sql,
            &baseline_config,
        )
        .await?;
        println!("Created baseline: {}", result.path.display());

        if baseline_config.validate_consistency {
            let suggest_file_deps = config.schema.augment_dependencies_from_files;
            validate_baseline_against_catalog_with_suggestions(
                &shadow_pool,
                &result.path,
                &filtered_new_catalog,
                &baseline_config,
                suggest_file_deps,
                &roles_file,
            )
            .await?;
        }
    } else {
        println!("Skipping baseline creation (use --create-baseline to create one)");
    }

    println!("Migration generation complete!");
    Ok(())
}
