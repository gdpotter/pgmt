use crate::baseline::operations::BaselineCreationRequest;
use crate::catalog::Catalog;
use crate::config::Config;
use crate::migrate::{MigrationGenerationInput, generate_migration};
use crate::migration::{
    BaselineConfig, get_migration_starting_state, get_migration_starting_state_with_attribution,
    validate_baseline_against_catalog,
};
use crate::modules::{
    HistoricalAttribution, evaluate_module_generation, render_migration_with_acquisitions,
    write_sectioned_baseline,
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
    let modules_enabled = config.modules.is_enabled();

    // Each phase needs its own pristine shadow: the replay below leaves the
    // shadow populated, and `clean_shadow_db` is a no-op on branch shadows, so
    // reusing one branch would make the schema-file apply collide ("already
    // exists"). `drop_branch` reclaims the ephemeral branch right after.
    //
    // Module projects also collect per-section attribution during the replay
    // (which module's section created each object) — that's what lets DROP
    // steps and re-tags be attributed, since dropped objects have no current
    // file.
    let starting_pool = shadow.connect_fresh().await?;
    let (old_catalog, historical) = if modules_enabled {
        get_migration_starting_state_with_attribution(
            &starting_pool,
            &baselines_dir,
            &migrations_dir,
            &roles_file,
            &baseline_config,
            config,
        )
        .await?
    } else {
        let catalog = get_migration_starting_state(
            &starting_pool,
            &baselines_dir,
            &migrations_dir,
            &roles_file,
            &baseline_config,
            config,
        )
        .await?;
        (catalog, HistoricalAttribution::default())
    };
    crate::db::branch::drop_branch(starting_pool).await?;

    debug!("Applying current schema to shadow database");
    let (new_catalog, file_mapping) =
        crate::schema_ops::apply_current_schema_to_shadow_with_mapping(config, root_dir, shadow)
            .await?;

    // Validate column ordering before generating migration
    crate::validation::apply_column_order_validation(
        &old_catalog,
        &new_catalog,
        config.migration.column_order,
    )?;

    debug!("Generating migration steps");
    let migration_result = generate_migration(MigrationGenerationInput {
        old_catalog: old_catalog.clone(),
        new_catalog: new_catalog.clone(),
        description: description.clone(),
        version,
        filename_prefix: config.migration.filename_prefix.clone(),
    })?;

    // Module projects: validate cross-module references and check whether
    // this change diverges the partition from what history implies —
    // a re-tag or a drop that breaks another module's replayability. Any
    // divergence demands a re-anchoring baseline alongside the migration.
    let should_create_baseline = create_baseline || config.migration.create_baselines_by_default;
    let module_gen = evaluate_module_generation(
        config,
        &old_catalog,
        &new_catalog,
        &file_mapping,
        &historical,
        should_create_baseline,
        "re-run with --create-baseline to emit a re-anchoring baseline.",
    )?;
    let partition_diverged = module_gen.as_ref().is_some_and(|m| m.diverged);

    if !migration_result.has_changes && !partition_diverged {
        println!("No changes detected - no migration needed");
        return Ok(());
    }

    // Baseline first (--create-baseline opts in for this run; absence falls
    // back to config — the flag can only add, never suppress a configured
    // default): when the partition re-anchors, migration V's acquisition
    // sections (§11/§12) derive from the baseline's provenance cut, so the
    // baseline's sections must exist before the migration is rendered.
    if should_create_baseline {
        // Generate the baseline from the full desired catalog, not the migration
        // SQL — the migration is a delta against the prior state, so writing it
        // would produce a partial baseline for any non-initial migration.
        let result = crate::baseline::operations::create_baseline(BaselineCreationRequest {
            catalog: new_catalog.clone(),
            base_catalog: Catalog::empty(),
            version,
            description: "baseline".to_string(),
            baselines_dir: baselines_dir.clone(),
            verbose: baseline_config.verbose,
        })
        .await?;

        // Module projects rewrite the baseline into provenance-cut per-module
        // sections, with `remaps` recording prior ownership wherever it
        // changed — the re-anchor record crossings consume. (The baseline
        // renders the desired post-V state; the migration's acquisition
        // sections render the moved objects from the STARTING catalog instead
        // — §12 — so nothing here feeds the migration.)
        if let Some(module_gen) = &module_gen {
            write_sectioned_baseline(
                &result.path,
                &result.steps,
                &new_catalog,
                &module_gen.partition,
                &file_mapping,
                &historical,
            )?;
        }
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

    // The migration: ordinary diff sections plus, at a re-anchor, the
    // acquisition sections for module-sourced moves (§11/§12 — base-sourced
    // moves are satisfied everywhere by construction and stay baseline-only).
    let migration_sql: Option<String> = match &module_gen {
        Some(module_gen) => render_migration_with_acquisitions(
            if migration_result.has_changes {
                &migration_result.steps
            } else {
                &[]
            },
            module_gen.diverged,
            &old_catalog,
            &new_catalog,
            &module_gen.partition,
            &file_mapping,
            &historical,
        )?,
        None => migration_result
            .has_changes
            .then(|| migration_result.migration_sql.clone()),
    };
    match migration_sql {
        Some(sql) => {
            let migration_path = migrations_dir.join(&migration_result.migration_filename);
            std::fs::write(&migration_path, &sql)?;
            println!("Created migration: {}", migration_path.display());
        }
        None => {
            // Pure base-sourced re-tag (e.g. modularizing an existing
            // project): ownership moved but nothing needs acquiring anywhere.
            // The re-anchoring baseline above records the new partition.
            println!("No schema changes - emitting re-anchoring baseline only.");
        }
    }

    println!("Migration generation complete!");
    Ok(())
}
