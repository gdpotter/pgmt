use crate::baseline::operations::{
    BaselineCreationRequest, create_baseline, display_baseline_summary, display_baseline_usage_info,
};
use crate::config::Config;
use crate::constants::BASELINE_FILENAME_PREFIX;
use crate::db::connection::connect_with_retry;
use crate::diff::operations::SqlRenderer;
use crate::migration::discover_baselines;
use anyhow::{Result, anyhow};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::debug;

pub async fn cmd_baseline_create(
    config: &Config,
    root_dir: &std::path::Path,
    force: bool,
) -> Result<()> {
    debug!("Loading schema files into shadow database");
    let catalog = crate::schema_ops::apply_current_schema_to_shadow(config, root_dir).await?;

    let shadow_url = config.databases.shadow.get_connection_string().await?;
    let shadow_pool = connect_with_retry(&shadow_url).await?;

    let version = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| anyhow!("System time is before Unix epoch: {}", e))?
        .as_secs();

    let request = BaselineCreationRequest {
        catalog: catalog.clone(),
        version,
        description: "baseline".to_string(),
        baselines_dir: root_dir.join(&config.directories.baselines),
        verbose: true,
    };

    let result = create_baseline(request).await?;

    if tracing::enabled!(tracing::Level::DEBUG) {
        debug!(
            "Baseline generation completed with {} migration steps",
            result.steps.len()
        );

        if !result.steps.is_empty() {
            debug!("🔍 Migration step dependencies:");
            for (idx, step) in result.steps.iter().enumerate() {
                let step_id = step.db_object_id();
                let step_type = match step {
                    crate::diff::operations::MigrationStep::Schema(_) => "Schema",
                    crate::diff::operations::MigrationStep::Table(_) => "Table",
                    crate::diff::operations::MigrationStep::View(_) => "View",
                    crate::diff::operations::MigrationStep::Type(_) => "Type",
                    crate::diff::operations::MigrationStep::Domain(_) => "Domain",
                    crate::diff::operations::MigrationStep::Sequence(_) => "Sequence",
                    crate::diff::operations::MigrationStep::Function(_) => "Function",
                    crate::diff::operations::MigrationStep::Aggregate(_) => "Aggregate",
                    crate::diff::operations::MigrationStep::Index(_) => "Index",
                    crate::diff::operations::MigrationStep::Constraint(_) => "Constraint",
                    crate::diff::operations::MigrationStep::Trigger(_) => "Trigger",
                    crate::diff::operations::MigrationStep::Extension(_) => "Extension",
                    crate::diff::operations::MigrationStep::Grant(_) => "Grant",
                };

                let dependencies = catalog
                    .forward_deps
                    .get(&step_id)
                    .map(|deps| {
                        deps.iter()
                            .map(|d| format!("{:?}", d))
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .unwrap_or_else(|| "none".to_string());

                debug!(
                    "  Step {}: {} {:?} -> depends on: [{}]",
                    idx + 1,
                    step_type,
                    step_id,
                    dependencies
                );
            }
        }
    }

    // Only validate when explicitly requested or when --force is not used
    if config.migration.validate_baseline_consistency && !force {
        use crate::migration::baseline::{
            BaselineConfig, validate_baseline_against_catalog_with_suggestions,
        };

        let baseline_config = BaselineConfig {
            validate_consistency: true,
            verbose: false, // Keep validation quiet to avoid cluttering output
        };

        // This provides better error messages when validation fails
        let suggest_file_deps = config.schema.augment_dependencies_from_files;
        if let Err(validation_error) = validate_baseline_against_catalog_with_suggestions(
            &shadow_pool,
            &result.path,
            &catalog,
            &baseline_config,
            suggest_file_deps,
        )
        .await
        {
            eprintln!("⚠️  Baseline validation detected inconsistencies:");
            eprintln!("{}", validation_error);
            eprintln!();
            eprintln!(
                "💡 The baseline was created successfully but may not be perfectly consistent."
            );
            eprintln!(
                "   Use 'pgmt baseline create --force' to skip validation if this is expected."
            );
            return Err(validation_error);
        }

        if config.migration.validate_baseline_consistency {
            println!("✅ Baseline validation passed");
        }
    } else if force {
        println!("⚠️  Skipping baseline validation due to --force flag");
    }

    display_baseline_summary(&result);
    display_baseline_usage_info();

    Ok(())
}

pub async fn cmd_baseline_list(config: &Config, root_dir: &std::path::Path) -> Result<()> {
    println!("📋 Listing existing baselines...");

    let baselines_dir = root_dir.join(&config.directories.baselines);
    if !baselines_dir.exists() {
        println!(
            "No baselines directory found at: {}",
            baselines_dir.display()
        );
        return Ok(());
    }

    let mut baselines = Vec::new();
    for entry in fs::read_dir(&baselines_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "sql")
            && let Some(filename) = path.file_name().and_then(|n| n.to_str())
            && filename.starts_with(BASELINE_FILENAME_PREFIX)
            && let Some(version_str) = filename
                .strip_prefix(BASELINE_FILENAME_PREFIX)
                .and_then(|s| s.strip_suffix(".sql"))
            && let Ok(version) = version_str.parse::<u64>()
        {
            let metadata = fs::metadata(&path)?;
            baselines.push((version, filename.to_string(), path, metadata));
        }
    }

    if baselines.is_empty() {
        println!("No baseline files found in: {}", baselines_dir.display());
        return Ok(());
    }

    baselines.sort_by(|a, b| b.0.cmp(&a.0));

    println!("Found {} baseline(s):", baselines.len());
    println!();

    for (version, filename, path, metadata) in baselines {
        let created = metadata
            .created()
            .or_else(|_| metadata.modified())
            .map(|time| {
                let datetime: chrono::DateTime<chrono::Local> = time.into();
                datetime.format("%Y-%m-%d %H:%M:%S").to_string()
            })
            .unwrap_or_else(|_| "unknown".to_string());

        let size = format_size(metadata.len());

        println!("📄 V{} - {} ({})", version, filename, created);
        println!("   📂 Path: {}", path.display());
        println!("   📊 Size: {}", size);
        println!();
    }

    Ok(())
}

pub async fn cmd_baseline_clean(
    config: &Config,
    root_dir: &std::path::Path,
    keep: usize,
    older_than_days: Option<u64>,
    dry_run: bool,
) -> Result<()> {
    if dry_run {
        println!("🔍 Baseline cleanup (DRY RUN) - no files will be deleted");
    } else {
        println!("🧹 Cleaning up old baseline files...");
    }

    let baselines_dir = root_dir.join(&config.directories.baselines);
    if !baselines_dir.exists() {
        println!(
            "No baselines directory found at: {}",
            baselines_dir.display()
        );
        return Ok(());
    }

    let discovered_baselines = discover_baselines(&baselines_dir)?;

    if discovered_baselines.is_empty() {
        println!("No baseline files found to clean up");
        return Ok(());
    }

    let mut baselines = Vec::new();
    for baseline in discovered_baselines {
        let metadata = fs::metadata(&baseline.path)?;
        baselines.push((baseline.version, baseline.path, metadata));
    }

    baselines.sort_by(|a, b| b.0.cmp(&a.0));

    let mut files_to_delete = Vec::new();

    if keep > 0 && baselines.len() > keep {
        println!(
            "Keeping {} most recent baseline(s):",
            keep.min(baselines.len())
        );
        for (version, path, _) in baselines.iter().take(keep) {
            println!(
                "  ✅ V{} - {}",
                version,
                path.file_name().unwrap().to_str().unwrap()
            );
        }
        println!();

        baselines = baselines.into_iter().skip(keep).collect();
    } else if keep > 0 {
        println!(
            "Keeping all {} baseline(s) (keep={}):",
            baselines.len(),
            keep
        );
        for (version, path, _) in &baselines {
            println!(
                "  ✅ V{} - {}",
                version,
                path.file_name().unwrap().to_str().unwrap()
            );
        }
        println!();
        baselines.clear();
    }

    if let Some(days) = older_than_days {
        let cutoff_time = SystemTime::now() - std::time::Duration::from_secs(days * 24 * 60 * 60);

        for (version, path, metadata) in baselines {
            let file_time = metadata.created().or_else(|_| metadata.modified())?;
            if file_time < cutoff_time {
                files_to_delete.push((version, path));
            } else {
                println!(
                    "  ⏰ V{} - {} (not old enough, {} days)",
                    version,
                    path.file_name().unwrap().to_str().unwrap(),
                    (SystemTime::now().duration_since(file_time)?.as_secs()) / (24 * 60 * 60)
                );
            }
        }
    } else {
        for (version, path, _) in baselines {
            files_to_delete.push((version, path));
        }
    }

    if files_to_delete.is_empty() {
        println!("No baseline files to delete");
        return Ok(());
    }

    println!("Files to delete ({}):", files_to_delete.len());
    for (version, path) in &files_to_delete {
        let metadata = fs::metadata(path)?;
        let size = format_size(metadata.len());
        println!(
            "  🗑️  V{} - {} ({})",
            version,
            path.file_name().unwrap().to_str().unwrap(),
            size
        );
    }

    if dry_run {
        println!();
        println!("DRY RUN: No files were actually deleted");
        println!("Run without --dry-run to perform the deletion");
    } else {
        println!();
        let mut deleted_count = 0;
        let mut deleted_size = 0u64;

        for (version, path) in files_to_delete {
            match fs::metadata(&path) {
                Ok(metadata) => {
                    deleted_size += metadata.len();
                    match fs::remove_file(&path) {
                        Ok(()) => {
                            println!("  ✅ Deleted V{}", version);
                            deleted_count += 1;
                        }
                        Err(e) => {
                            eprintln!("  ❌ Failed to delete V{}: {}", version, e);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("  ❌ Failed to get metadata for V{}: {}", version, e);
                }
            }
        }

        println!();
        println!("✅ Cleanup complete!");
        println!("   📊 Deleted {} file(s)", deleted_count);
        println!("   💾 Freed {} of disk space", format_size(deleted_size));
    }

    Ok(())
}

fn format_size(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{:.1} {}", size, UNITS[unit_index])
    }
}
