use crate::config::{Config, ObjectFilter};
use crate::db::connection::connect_with_retry;
use crate::migrate::{MigrationGenerationInput, generate_migration};
use crate::migration::{
    BaselineConfig, ensure_baseline_for_migration, find_latest_migration,
    generate_baseline_filename, get_migration_update_starting_state,
    should_manage_baseline_for_migration, validate_baseline_against_catalog_with_suggestions,
};
use anyhow::{Result, anyhow};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::debug;

pub async fn cmd_migrate_update_with_options(
    config: &Config,
    root_dir: &Path,
    dry_run: bool,
) -> Result<()> {
    if dry_run {
        println!("🔍 Dry-run mode: previewing changes without applying them");
    }

    println!("Updating latest migration with current changes");

    // Create necessary directories
    let migrations_dir = root_dir.join("migrations");
    let baselines_dir = root_dir.join("schema_baselines");
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

    // Connect to shadow database with retry logic
    let shadow_url = config.databases.shadow.get_connection_string().await?;
    let shadow_pool = connect_with_retry(&shadow_url).await?;

    // Step 1: Load the baseline that corresponds to the previous migration
    let baseline_config = BaselineConfig {
        validate_consistency: config.migration.validate_baseline_consistency,
        verbose: true,
    };
    let old_catalog = get_migration_update_starting_state(
        &shadow_pool,
        &baselines_dir,
        &migrations_dir,
        latest_migration.version,
        &baseline_config,
    )
    .await?;

    // Step 2: Reset shadow database and apply current schema
    debug!("Applying current schema to shadow database");
    // Use apply_current_schema_to_shadow which respects file dependency settings
    // This function creates its own connection, so close the existing one first
    shadow_pool.close().await;
    let new_catalog = crate::schema_ops::apply_current_schema_to_shadow(config, root_dir).await?;

    // Reconnect for baseline validation later
    let shadow_url = config.databases.shadow.get_connection_string().await?;
    let shadow_pool = connect_with_retry(&shadow_url).await?;

    // Apply object filtering
    let filter = ObjectFilter::new(&config.objects, &config.migration.tracking_table);
    let filtered_old_catalog = filter.filter_catalog(old_catalog);
    let filtered_new_catalog = filter.filter_catalog(new_catalog);

    // Step 3: Generate migration using pure logic
    debug!("Generating updated migration steps");
    let migration_result = generate_migration(MigrationGenerationInput {
        old_catalog: filtered_old_catalog,
        new_catalog: filtered_new_catalog.clone(),
        description: latest_migration.description.clone(), // We keep the original description
        version: latest_migration.version,
    })?;

    if !migration_result.has_changes {
        println!("No changes detected - updating migration to be empty");

        // Update the migration file to be empty since no changes are needed
        let empty_migration_sql = "-- No changes detected\n";
        std::fs::write(&latest_migration.path, empty_migration_sql)?;
        println!(
            "Updated migration: {} (now empty)",
            latest_migration.path.display()
        );

        return Ok(());
    }

    // Step 4: Update migration file
    std::fs::write(&latest_migration.path, &migration_result.migration_sql)?;
    println!("Updated migration: {}", latest_migration.path.display());

    // Step 5: Optionally update the corresponding baseline
    let baseline_filename = generate_baseline_filename(latest_migration.version);
    let baseline_path = baselines_dir.join(&baseline_filename);
    let should_update_baseline = should_manage_baseline_for_migration(
        config,
        &baseline_path,
        config.migration.create_baselines_by_default,
    );

    if should_update_baseline {
        let result = ensure_baseline_for_migration(
            &baselines_dir,
            latest_migration.version,
            &migration_result.migration_sql,
            &baseline_config,
        )
        .await?;
        println!("Updated baseline: {}", result.path.display());

        // Step 6: Validate that the baseline matches the intended schema using pure logic
        if baseline_config.validate_consistency {
            // Use enhanced validation with file dependency suggestions when enabled
            let suggest_file_deps = config.schema.augment_dependencies_from_files;
            validate_baseline_against_catalog_with_suggestions(
                &shadow_pool,
                &result.path,
                &filtered_new_catalog,
                &baseline_config,
                suggest_file_deps,
            )
            .await?;
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
    let migrations_dir = root_dir.join("migrations");
    let baselines_dir = root_dir.join("schema_baselines");
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

    // Connect to shadow database
    let shadow_url = config.databases.shadow.get_connection_string().await?;
    let shadow_pool = connect_with_retry(&shadow_url).await?;

    // Get the baseline state before this migration
    let baseline_config = BaselineConfig {
        validate_consistency: config.migration.validate_baseline_consistency,
        verbose: true,
    };
    let old_catalog = get_migration_update_starting_state(
        &shadow_pool,
        &baselines_dir,
        &migrations_dir,
        target_migration.version,
        &baseline_config,
    )
    .await?;

    // Apply current schema to shadow database
    debug!("Applying current schema to shadow database");
    // Use apply_current_schema_to_shadow which respects file dependency settings
    // This function creates its own connection, so close the existing one first
    shadow_pool.close().await;
    let new_catalog = crate::schema_ops::apply_current_schema_to_shadow(config, root_dir).await?;

    // Reconnect for baseline validation later
    let shadow_url = config.databases.shadow.get_connection_string().await?;
    let shadow_pool = connect_with_retry(&shadow_url).await?;

    // Apply object filtering
    let filter = ObjectFilter::new(&config.objects, &config.migration.tracking_table);
    let filtered_old_catalog = filter.filter_catalog(old_catalog);
    let filtered_new_catalog = filter.filter_catalog(new_catalog);

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
        old_catalog: filtered_old_catalog,
        new_catalog: filtered_new_catalog.clone(),
        description: new_description.clone(),
        version: new_version,
    })?;

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
                "-- Migration: {}\n-- Version: V{}\n-- Generated by pgmt migrate update (renumbered from V{})\n-- No changes needed - conflicts resolved by intervening migrations\n",
                new_description, new_version, target_migration.version
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
                let new_filename =
                    format!("V{}_{}.sql", new_version, new_description.replace(' ', "_"));
                let new_path = migrations_dir.join(&new_filename);
                std::fs::write(&new_path, &empty_migration_content)?;
                println!(
                    "Migration V{} updated to V{} (no changes needed)",
                    target_migration.version, new_version
                );
                println!("Created: {}", new_path.display());
            }
        }
        return Ok(());
    }

    // Write the migration file
    if dry_run {
        println!(
            "📝 Preview: Generated migration content ({} chars)",
            migration_result.migration_sql.len()
        );
        if is_latest {
            println!("🔄 Would update: {}", target_migration.path.display());
        } else {
            let new_filename =
                format!("V{}_{}.sql", new_version, new_description.replace(' ', "_"));
            let new_path = migrations_dir.join(&new_filename);
            println!(
                "🔄 Would rename V{} → V{}",
                target_migration.version, new_version
            );
            println!("   Delete: {}", target_migration.path.display());
            println!("   Create: {}", new_path.display());
        }
        println!(
            "\n📋 Migration preview:\n{}",
            migration_result.migration_sql
        );
    } else if is_latest {
        // For latest migration, overwrite the existing file (current behavior)
        std::fs::write(&target_migration.path, &migration_result.migration_sql)?;
        println!("Updated migration: {}", target_migration.path.display());
    } else {
        // For older migration, delete old file and create new one with fresh timestamp
        std::fs::remove_file(&target_migration.path)?;
        let new_filename = format!("V{}_{}.sql", new_version, new_description.replace(' ', "_"));
        let new_path = migrations_dir.join(&new_filename);
        std::fs::write(&new_path, &migration_result.migration_sql)?;
        println!(
            "Migration V{} updated to V{} (renumbered)",
            target_migration.version, new_version
        );
        println!("Deleted: {}", target_migration.path.display());
        println!("Created: {}", new_path.display());
    }

    // Handle baseline updates (similar to existing logic)
    let baseline_filename = generate_baseline_filename(new_version);
    let baseline_path = baselines_dir.join(&baseline_filename);
    let should_update_baseline = should_manage_baseline_for_migration(
        config,
        &baseline_path,
        config.migration.create_baselines_by_default,
    );

    if should_update_baseline {
        let result = ensure_baseline_for_migration(
            &baselines_dir,
            new_version,
            &migration_result.migration_sql,
            &baseline_config,
        )
        .await?;
        if is_latest {
            println!("Updated baseline: {}", result.path.display());
        } else {
            println!("Created baseline: {}", result.path.display());
        }

        if baseline_config.validate_consistency {
            // Use enhanced validation with file dependency suggestions when enabled
            let suggest_file_deps = config.schema.augment_dependencies_from_files;
            validate_baseline_against_catalog_with_suggestions(
                &shadow_pool,
                &result.path,
                &filtered_new_catalog,
                &baseline_config,
                suggest_file_deps,
            )
            .await?;
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
