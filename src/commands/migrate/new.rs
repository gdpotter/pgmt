use crate::baseline::operations::BaselineCreationRequest;
use crate::config::Config;
use crate::migrate::{MigrationGenerationInput, generate_migration};
use crate::migration::{
    BaselineConfig, get_migration_starting_state, validate_baseline_against_catalog,
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
    create_baseline: bool,
    shadow: &crate::config::ShadowDatabase,
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

    let migrations_dir = root_dir.join(&config.directories.migrations);
    let baselines_dir = root_dir.join(&config.directories.baselines);
    std::fs::create_dir_all(&migrations_dir)?;
    std::fs::create_dir_all(&baselines_dir)?;

    let version = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| anyhow::anyhow!("System time is before Unix epoch: {}", e))?
        .as_secs();

    let baseline_config = BaselineConfig {
        validate_consistency: config.migration.validate_baseline_consistency,
        verbose: true,
    };
    let roles_file = root_dir.join(&config.directories.roles);

    // Each phase needs its own pristine shadow: the replay below leaves the
    // shadow populated, and `clean_shadow_db` is a no-op on branch shadows, so
    // reusing one branch would make the schema-file apply collide ("already
    // exists"). `drop_branch` reclaims the ephemeral branch right after.
    let starting_pool = shadow.connect_fresh().await?;
    let old_catalog = get_migration_starting_state(
        &starting_pool,
        &baselines_dir,
        &migrations_dir,
        &roles_file,
        &baseline_config,
        config,
    )
    .await?;
    crate::db::branch::drop_branch(starting_pool).await?;

    debug!("Applying current schema to shadow database");
    let new_catalog =
        crate::schema_ops::apply_current_schema_to_shadow(config, root_dir, shadow).await?;

    // Validate column ordering before generating migration
    crate::validation::apply_column_order_validation(
        &old_catalog,
        &new_catalog,
        config.migration.column_order,
    )?;

    debug!("Generating migration steps");
    let migration_result = generate_migration(MigrationGenerationInput {
        old_catalog,
        new_catalog: new_catalog.clone(),
        description: description.clone(),
        version,
        filename_prefix: config.migration.filename_prefix.clone(),
    })?;

    if !migration_result.has_changes {
        println!("No changes detected - no migration needed");
        return Ok(());
    }

    let migration_path = migrations_dir.join(&migration_result.migration_filename);
    std::fs::write(&migration_path, &migration_result.migration_sql)?;
    println!("Created migration: {}", migration_path.display());

    // --create-baseline opts in for this run; absence falls back to config
    // (the flag can only add, never suppress a configured default)
    let should_create_baseline = create_baseline || config.migration.create_baselines_by_default;
    if should_create_baseline {
        // Generate the baseline from the full desired catalog, not the migration
        // SQL — the migration is a delta against the prior state, so writing it
        // would produce a partial baseline for any non-initial migration.
        let result = crate::baseline::operations::create_baseline(BaselineCreationRequest {
            catalog: new_catalog.clone(),
            version,
            description: "baseline".to_string(),
            baselines_dir: baselines_dir.clone(),
            verbose: baseline_config.verbose,
        })
        .await?;
        println!("Created baseline: {}", result.path.display());

        if baseline_config.validate_consistency {
            let validate_pool = shadow.connect_fresh().await?;
            validate_baseline_against_catalog(
                &validate_pool,
                &result.path,
                &new_catalog,
                &baseline_config,
                &roles_file,
                config,
            )
            .await?;
            crate::db::branch::drop_branch(validate_pool).await?;
        }
    } else {
        println!("Skipping baseline creation (use --create-baseline to create one)");
    }

    println!("Migration generation complete!");
    Ok(())
}
