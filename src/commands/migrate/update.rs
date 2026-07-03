use crate::baseline::operations::{BaselineCreationRequest, create_baseline};
use crate::catalog::Catalog;
use crate::config::Config;
use crate::migrate::{MigrationGenerationInput, generate_migration};
use crate::migration::{
    BaselineConfig, find_latest_migration, generate_baseline_filename,
    get_migration_update_starting_state, should_manage_baseline_for_migration,
    validate_baseline_against_catalog,
};
use crate::modules::{
    HistoricalAttribution, evaluate_module_generation, sectioned_migration_sql,
    write_sectioned_baseline,
};
use anyhow::{Result, anyhow};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::debug;

pub async fn cmd_migrate_update_with_options(
    config: &Config,
    root_dir: &Path,
    dry_run: bool,
    shadow: &crate::config::ShadowDatabase,
) -> Result<()> {
    if dry_run {
        println!("🔍 Dry-run mode: previewing changes without applying them");
    }

    println!("Updating latest migration with current changes");

    // Create necessary directories
    let migrations_dir = root_dir.join(&config.directories.migrations);
    let baselines_dir = root_dir.join(&config.directories.baselines);
    std::fs::create_dir_all(&migrations_dir)?;
    std::fs::create_dir_all(&baselines_dir)?;

    // Find the latest migration
    let latest_migration = find_latest_migration(&migrations_dir)?;
    if latest_migration.is_none() {
        return Err(anyhow::anyhow!(
            "No migrations found. Use 'pgmt migrate new <description>' to create the first migration."
        ));
    }

    let latest_migration = latest_migration.unwrap();
    println!("Updating migration: {}", latest_migration.path.display());

    // Step 1: Load the baseline that corresponds to the previous migration
    let baseline_config = BaselineConfig {
        validate_consistency: config.migration.validate_baseline_consistency,
        verbose: true,
    };
    let roles_file = root_dir.join(&config.directories.roles);

    // Each pristine-start phase gets its own fresh branch — the replay dirties
    // the shadow and `clean_shadow_db` is a no-op on branches, so a shared
    // branch would make the schema-file apply collide. See `migrate new`.
    let starting_pool = shadow.connect_fresh().await?;
    let mut historical = HistoricalAttribution::default();
    let attribution = config.modules.is_enabled().then_some(&mut historical);
    let old_catalog = get_migration_update_starting_state(
        &starting_pool,
        &baselines_dir,
        &migrations_dir,
        latest_migration.version,
        &roles_file,
        &baseline_config,
        config,
        attribution,
    )
    .await?;
    crate::db::branch::drop_branch(starting_pool).await?;

    // Step 2: Reset shadow database and apply current schema
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

    // Step 3: Generate migration using pure logic
    debug!("Generating updated migration steps");
    let migration_result = generate_migration(MigrationGenerationInput {
        old_catalog: old_catalog.clone(),
        new_catalog: new_catalog.clone(),
        description: latest_migration.description.clone(), // We keep the original description
        version: latest_migration.version,
        filename_prefix: config.migration.filename_prefix.clone(),
    })?;

    // Whether a paired baseline will be (re)generated below — that baseline is
    // also what a partition re-anchor requires.
    let baseline_filename = generate_baseline_filename(latest_migration.version);
    let baseline_path = baselines_dir.join(&baseline_filename);
    let should_update_baseline = should_manage_baseline_for_migration(
        config,
        &baseline_path,
        config.migration.create_baselines_by_default,
    );

    let module_gen = evaluate_module_generation(
        config,
        &old_catalog,
        &new_catalog,
        &file_mapping,
        &historical,
        should_update_baseline,
        // `migrate update` has no --create-baseline flag; re-anchoring is owned
        // by `migrate new`, so point the user there rather than at a flag this
        // subcommand would reject as unknown.
        "run 'pgmt migrate new <description> --create-baseline' to emit a re-anchoring baseline.",
    )?;
    let partition_diverged = module_gen.as_ref().is_some_and(|m| m.diverged);

    if !migration_result.has_changes {
        println!("No changes detected - updating migration to be empty");

        // Update the migration file to be empty since no changes are needed
        let empty_migration_sql = "-- No changes detected\n";
        std::fs::write(&latest_migration.path, empty_migration_sql)?;
        println!(
            "Updated migration: {} (now empty)",
            latest_migration.path.display()
        );

        if !partition_diverged {
            return Ok(());
        }
        // Pure re-tag: fall through so the re-anchoring baseline regenerates.
    } else {
        // Step 4: Update migration file (module projects write module-tagged
        // sections; everything else the plain form).
        let migration_sql = match &module_gen {
            Some(module_gen) => sectioned_migration_sql(
                &migration_result.steps,
                &old_catalog,
                &new_catalog,
                &module_gen.partition,
                &file_mapping,
                &historical,
            )?,
            None => migration_result.migration_sql.clone(),
        };
        std::fs::write(&latest_migration.path, &migration_sql)?;
        println!("Updated migration: {}", latest_migration.path.display());
    }

    // Step 5: Optionally update the corresponding baseline
    if should_update_baseline {
        let result = create_baseline(BaselineCreationRequest {
            catalog: new_catalog.clone(),
            base_catalog: Catalog::empty(),
            version: latest_migration.version,
            description: "baseline".to_string(),
            baselines_dir: baselines_dir.clone(),
            verbose: baseline_config.verbose,
        })
        .await?;
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
        println!("Updated baseline: {}", result.path.display());

        // Step 6: Validate that the baseline matches the intended schema using pure logic
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
        println!(
            "Skipping baseline update (baseline does not exist and create_baselines_by_default is false)"
        );
    }

    println!("Migration update complete!");
    Ok(())
}

/// Update a specific migration with current changes (renumbers if not latest)
pub async fn cmd_migrate_update_specific(
    config: &Config,
    root_dir: &Path,
    version_str: &str,
    backup: bool,
    dry_run: bool,
    shadow: &crate::config::ShadowDatabase,
) -> Result<()> {
    use crate::migration::parsing::find_migration_by_version;

    if dry_run {
        println!(
            "🔍 Dry-run mode: previewing migration update for: {}",
            version_str
        );
    } else {
        println!("Updating migration: {}", version_str);
    }

    // Create necessary directories
    let migrations_dir = root_dir.join(&config.directories.migrations);
    let baselines_dir = root_dir.join(&config.directories.baselines);
    std::fs::create_dir_all(&migrations_dir)?;
    std::fs::create_dir_all(&baselines_dir)?;

    // Find the target migration
    let target_migration = find_migration_by_version(&migrations_dir, version_str)?;
    let target_migration = match target_migration {
        Some(migration) => migration,
        None => {
            return Err(anyhow!(
                "Migration '{}' not found. Use 'pgmt migrate status' to see available migrations.",
                version_str
            ));
        }
    };

    println!(
        "Found migration: {} ({})",
        target_migration.path.display(),
        target_migration.description
    );

    // Create backup if requested
    if backup && !dry_run {
        let backup_path = target_migration.path.with_extension("sql.bak");
        std::fs::copy(&target_migration.path, &backup_path)?;
        println!("💾 Backup created: {}", backup_path.display());
    } else if backup && dry_run {
        let backup_path = target_migration.path.with_extension("sql.bak");
        println!("💾 Would create backup: {}", backup_path.display());
    }

    // Check if this is the latest migration
    let latest_migration = find_latest_migration(&migrations_dir)?;
    let is_latest = latest_migration
        .map(|latest| latest.version == target_migration.version)
        .unwrap_or(false);

    // Get the baseline state before this migration
    let baseline_config = BaselineConfig {
        validate_consistency: config.migration.validate_baseline_consistency,
        verbose: true,
    };
    let roles_file = root_dir.join(&config.directories.roles);

    // Fresh branch per pristine-start phase (see `migrate new`): the replay
    // dirties the shadow and branch cleans are no-ops, so reuse would collide.
    let starting_pool = shadow.connect_fresh().await?;
    let mut historical = HistoricalAttribution::default();
    let attribution = config.modules.is_enabled().then_some(&mut historical);
    let old_catalog = get_migration_update_starting_state(
        &starting_pool,
        &baselines_dir,
        &migrations_dir,
        target_migration.version,
        &roles_file,
        &baseline_config,
        config,
        attribution,
    )
    .await?;
    crate::db::branch::drop_branch(starting_pool).await?;

    // Apply current schema to shadow database
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

    // Determine version and description for the new migration
    let (new_version, new_description) = if is_latest {
        // For latest migration, keep same version and description
        (
            target_migration.version,
            target_migration.description.clone(),
        )
    } else {
        // For older migration, generate new timestamp
        let new_version = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| anyhow!("System time is before Unix epoch: {}", e))?
            .as_secs();
        (new_version, target_migration.description.clone())
    };

    // Generate migration content
    debug!("Generating updated migration steps");
    let migration_result = generate_migration(MigrationGenerationInput {
        old_catalog: old_catalog.clone(),
        new_catalog: new_catalog.clone(),
        description: new_description.clone(),
        version: new_version,
        filename_prefix: config.migration.filename_prefix.clone(),
    })?;

    // Whether a paired baseline will be (re)generated below — required for a
    // partition re-anchor.
    let baseline_filename = generate_baseline_filename(new_version);
    let baseline_path = baselines_dir.join(&baseline_filename);
    let should_update_baseline = should_manage_baseline_for_migration(
        config,
        &baseline_path,
        config.migration.create_baselines_by_default,
    );

    let module_gen = evaluate_module_generation(
        config,
        &old_catalog,
        &new_catalog,
        &file_mapping,
        &historical,
        should_update_baseline,
        // `migrate update` has no --create-baseline flag; re-anchoring is owned
        // by `migrate new`, so point the user there rather than at a flag this
        // subcommand would reject as unknown.
        "run 'pgmt migrate new <description> --create-baseline' to emit a re-anchoring baseline.",
    )?;
    let partition_diverged = module_gen.as_ref().is_some_and(|m| m.diverged);

    // Module projects render module-tagged sections.
    let migration_sql = match &module_gen {
        Some(module_gen) if migration_result.has_changes => sectioned_migration_sql(
            &migration_result.steps,
            &old_catalog,
            &new_catalog,
            &module_gen.partition,
            &file_mapping,
            &historical,
        )?,
        _ => migration_result.migration_sql.clone(),
    };

    if !migration_result.has_changes {
        if is_latest {
            println!("No changes detected - updating migration to be empty");
            let empty_migration_sql = "-- No changes detected\n";
            std::fs::write(&target_migration.path, empty_migration_sql)?;
            println!(
                "Updated migration: {} (now empty)",
                target_migration.path.display()
            );
        } else {
            println!("No changes detected - conflicts resolved by other migrations");
            // For older migrations with no changes, still create/update the file with a comment
            let empty_migration_content = format!(
                "-- Migration: {}\n-- Version: {}{}\n-- Generated by pgmt migrate update (renumbered from {}{})\n-- No changes needed - conflicts resolved by intervening migrations\n",
                new_description,
                config.migration.filename_prefix,
                new_version,
                config.migration.filename_prefix,
                target_migration.version
            );

            if is_latest {
                // Overwrite existing file
                std::fs::write(&target_migration.path, &empty_migration_content)?;
                println!(
                    "Updated migration: {} (no changes needed)",
                    target_migration.path.display()
                );
            } else {
                // Delete old file and create new one
                std::fs::remove_file(&target_migration.path)?;
                let new_filename = format!(
                    "{}{}_{}.sql",
                    config.migration.filename_prefix,
                    new_version,
                    new_description.replace(' ', "_")
                );
                let new_path = migrations_dir.join(&new_filename);
                std::fs::write(&new_path, &empty_migration_content)?;
                println!(
                    "Migration {} updated to {} (no changes needed)",
                    target_migration.version, new_version
                );
                println!("Created: {}", new_path.display());
            }
        }
        if !partition_diverged {
            return Ok(());
        }
        // Pure re-tag: fall through so the re-anchoring baseline regenerates.
    }

    // Write the migration file
    if migration_result.has_changes && dry_run {
        println!(
            "📝 Preview: Generated migration content ({} chars)",
            migration_result.migration_sql.len()
        );
        if is_latest {
            println!("🔄 Would update: {}", target_migration.path.display());
        } else {
            let new_filename = format!(
                "{}{}_{}.sql",
                config.migration.filename_prefix,
                new_version,
                new_description.replace(' ', "_")
            );
            let new_path = migrations_dir.join(&new_filename);
            println!(
                "🔄 Would rename {} → {}",
                target_migration.version, new_version
            );
            println!("   Delete: {}", target_migration.path.display());
            println!("   Create: {}", new_path.display());
        }
        println!("\n📋 Migration preview:\n{}", migration_sql);
    } else if migration_result.has_changes && is_latest {
        // For latest migration, overwrite the existing file (current behavior)
        std::fs::write(&target_migration.path, &migration_sql)?;
        println!("Updated migration: {}", target_migration.path.display());
    } else if migration_result.has_changes {
        // For older migration, delete old file and create new one with fresh timestamp
        std::fs::remove_file(&target_migration.path)?;
        let new_filename = format!(
            "{}{}_{}.sql",
            config.migration.filename_prefix,
            new_version,
            new_description.replace(' ', "_")
        );
        let new_path = migrations_dir.join(&new_filename);
        std::fs::write(&new_path, &migration_sql)?;
        println!(
            "Migration {} updated to {} (renumbered)",
            target_migration.version, new_version
        );
        println!("Deleted: {}", target_migration.path.display());
        println!("Created: {}", new_path.display());
    }

    // Handle baseline updates (similar to existing logic)
    if should_update_baseline {
        let result = create_baseline(BaselineCreationRequest {
            catalog: new_catalog.clone(),
            base_catalog: Catalog::empty(),
            version: new_version,
            description: "baseline".to_string(),
            baselines_dir: baselines_dir.clone(),
            verbose: baseline_config.verbose,
        })
        .await?;
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
        if is_latest {
            println!("Updated baseline: {}", result.path.display());
        } else {
            println!("Created baseline: {}", result.path.display());
        }

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
    } else if is_latest {
        println!(
            "Skipping baseline update (baseline does not exist and create_baselines_by_default is false)"
        );
    } else {
        println!("Skipping baseline creation (create_baselines_by_default is false)");
    }

    if dry_run {
        println!("🔍 Dry-run complete! No changes were made.");
    } else {
        println!("Migration update complete!");
    }
    Ok(())
}
