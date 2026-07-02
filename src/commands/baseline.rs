use crate::baseline::operations::{
    BaselineCreationRequest, create_baseline, display_baseline_summary, display_baseline_usage_info,
};
use crate::catalog::file_dependencies::FileToObjectMapping;
use crate::config::Config;
use crate::config::filter::ObjectFilter;
use crate::diff::operations::SqlRenderer;
use crate::migration::{discover_baselines, discover_migrations, replay_history_for_checkpoint};
use crate::modules::{ModulePartition, render_sectioned_migration, sectionize_steps};
use anyhow::Result;
use std::fs;
use tracing::debug;

/// Checkpoint the migration log: collapse the existing baseline + migrations
/// into a single baseline at the latest version, deleting the collapsed
/// migrations by default.
///
/// A baseline asserts "replaying history through V produces exactly this", so
/// it is generated FROM a replay of history — never from the schema files,
/// which may have drifted ahead. Un-migrated schema drift stays out of the
/// checkpoint and surfaces in the next `migrate new`, where it belongs.
///
/// When `keep_migrations` is true, migrations are preserved (baseline-only mode).
/// When `dry_run` is true, shows what would happen without making changes.
pub async fn cmd_migrate_baseline(
    config: &Config,
    root_dir: &std::path::Path,
    force: bool,
    keep_migrations: bool,
    dry_run: bool,
    shadow: &crate::config::ShadowDatabase,
) -> Result<()> {
    let migrations_dir = root_dir.join(&config.directories.migrations);
    let baselines_dir = root_dir.join(&config.directories.baselines);
    let roles_file = root_dir.join(&config.directories.roles);

    // Discover existing files before creating the baseline
    let migrations = discover_migrations(&migrations_dir)?;
    let existing_baselines = discover_baselines(&baselines_dir)?;

    // There is no log to checkpoint without migrations. Creating a baseline
    // from schema files is a different operation with its own on-ramps.
    if migrations.is_empty() {
        anyhow::bail!(
            "no migrations to checkpoint: `migrate baseline` collapses the migration log \
             into a baseline. To create a baseline from your schema files, use \
             `pgmt migrate new <description> --create-baseline` (or `pgmt init` for a \
             new project)."
        );
    }
    let version = migrations.last().expect("checked non-empty").version;

    if dry_run && !keep_migrations {
        // Show what would happen
        println!("DRY RUN - no files will be changed\n");
        println!("Would create baseline at version {}", version);

        if !migrations.is_empty() {
            println!();
            println!("Migrations to delete ({}):", migrations.len());
            for m in &migrations {
                println!(
                    "  - {} ({})",
                    m.version,
                    m.path.file_name().unwrap().to_str().unwrap()
                );
            }
        }

        if !existing_baselines.is_empty() {
            println!();
            println!("Old baselines to delete ({}):", existing_baselines.len());
            for b in &existing_baselines {
                println!(
                    "  - {} ({})",
                    b.version,
                    b.path.file_name().unwrap().to_str().unwrap()
                );
            }
        }

        println!();
        println!("DRY RUN: No files were modified. Run without --dry-run to proceed.");
        return Ok(());
    }

    // Checkpoint: replay the full history onto a pristine shadow. The shadow's
    // pre-history base (image-provided substrate) is captured so the baseline
    // diff subtracts substrate structurally rather than via the `objects`
    // predicate; module projects also collect per-section attribution so the
    // checkpoint keeps every object's module tag.
    debug!("Replaying migration history into shadow database");
    let replay_pool = shadow.connect_fresh().await?;
    let (base_catalog, replayed, historical) = replay_history_for_checkpoint(
        &replay_pool,
        &baselines_dir,
        &migrations_dir,
        &roles_file,
        config,
    )
    .await?;
    crate::db::branch::drop_branch(replay_pool).await?;

    // The managed view of the replayed state — used for the dependency debug
    // output below and for baseline validation. The baseline itself is
    // replayed-minus-base; re-applied onto a substrate-bearing shadow it
    // reproduces this view, so validating against it holds whether or not the
    // user scoped `objects`.
    let catalog = ObjectFilter::from_config(config).filter_catalog(replayed.clone());

    let request = BaselineCreationRequest {
        catalog: replayed.clone(),
        base_catalog: base_catalog.clone(),
        version,
        description: "baseline".to_string(),
        baselines_dir: baselines_dir.clone(),
        verbose: true,
    };

    let result = create_baseline(request).await?;

    // Module projects: rewrite the checkpoint into module-tagged sections.
    // Attribution comes from the replayed history alone (a checkpoint has no
    // \"desired\" side), and ownership never changes across a checkpoint, so
    // these baselines carry no `remaps` — re-anchoring is exclusively
    // `migrate new --create-baseline`'s job.
    if config.modules.is_enabled() {
        let partition = ModulePartition::from_config(config)?;
        let sections = sectionize_steps(
            &result.steps,
            &base_catalog,
            &replayed,
            &partition,
            &FileToObjectMapping::new(),
            &historical,
        )?;
        std::fs::write(&result.path, render_sectioned_migration(&sections))?;
    }

    if tracing::enabled!(tracing::Level::DEBUG) {
        debug!(
            "Baseline generation completed with {} migration steps",
            result.steps.len()
        );

        if !result.steps.is_empty() {
            debug!("Migration step dependencies:");
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
                    crate::diff::operations::MigrationStep::Operator(_) => "Operator",
                    crate::diff::operations::MigrationStep::Cast(_) => "Cast",
                    crate::diff::operations::MigrationStep::Index(_) => "Index",
                    crate::diff::operations::MigrationStep::Constraint(_) => "Constraint",
                    crate::diff::operations::MigrationStep::Trigger(_) => "Trigger",
                    crate::diff::operations::MigrationStep::Policy(_) => "Policy",
                    crate::diff::operations::MigrationStep::Extension(_) => "Extension",
                    crate::diff::operations::MigrationStep::Grant(_) => "Grant",
                    crate::diff::operations::MigrationStep::Comment(_) => "Comment",
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

    // Validate baseline unless --force
    if config.migration.validate_baseline_consistency && !force {
        use crate::migration::baseline::{BaselineConfig, validate_baseline_against_catalog};

        let baseline_config = BaselineConfig {
            validate_consistency: true,
            verbose: false,
        };

        // Baseline validation replays into a pristine shadow, so it needs its
        // own fresh branch — the checkpoint replay above dirtied its own.
        let validate_pool = shadow.connect_fresh().await?;
        let validation_result = validate_baseline_against_catalog(
            &validate_pool,
            &result.path,
            &catalog,
            &baseline_config,
            &roles_file,
            config,
        )
        .await;
        crate::db::branch::drop_branch(validate_pool).await?;
        if let Err(validation_error) = validation_result {
            eprintln!("Baseline validation failed\n");
            eprintln!("{:#}", validation_error);
            eprintln!();
            eprintln!("To fix: Add `-- require:` headers to ensure correct file ordering.");
            eprintln!("   Use 'pgmt debug dependencies' to analyze dependency relationships.");
            eprintln!("   Use 'pgmt migrate baseline --force' to skip this validation.");
            std::process::exit(1);
        }

        println!("Baseline validation passed");
    } else if force {
        println!("Skipping baseline validation due to --force flag");
    }

    display_baseline_summary(&result);

    // Clean up old files unless --keep-migrations
    if !keep_migrations {
        let mut deleted_migrations = 0;
        for m in &migrations {
            match fs::remove_file(&m.path) {
                Ok(()) => deleted_migrations += 1,
                Err(e) => eprintln!("Failed to delete migration {}: {}", m.version, e),
            }
        }

        let mut deleted_baselines = 0;
        for b in &existing_baselines {
            match fs::remove_file(&b.path) {
                Ok(()) => deleted_baselines += 1,
                Err(e) => eprintln!("Failed to delete old baseline {}: {}", b.version, e),
            }
        }

        if deleted_migrations > 0 || deleted_baselines > 0 {
            println!();
            println!(
                "Cleaned up {} migration(s) and {} old baseline(s)",
                deleted_migrations, deleted_baselines
            );
        }
    } else {
        display_baseline_usage_info();
    }

    Ok(())
}

pub async fn cmd_baseline_list(config: &Config, root_dir: &std::path::Path) -> Result<()> {
    println!("Listing existing baselines...");

    let baselines_dir = root_dir.join(&config.directories.baselines);
    if !baselines_dir.exists() {
        println!(
            "No baselines directory found at: {}",
            baselines_dir.display()
        );
        return Ok(());
    }

    let discovered = discover_baselines(&baselines_dir)?;

    if discovered.is_empty() {
        println!("No baseline files found in: {}", baselines_dir.display());
        return Ok(());
    }

    println!("Found {} baseline(s):", discovered.len());
    println!();

    // Display newest first
    for baseline in discovered.iter().rev() {
        let metadata = fs::metadata(&baseline.path)?;

        let created = metadata
            .created()
            .or_else(|_| metadata.modified())
            .map(|time| {
                let datetime: chrono::DateTime<chrono::Local> = time.into();
                datetime.format("%Y-%m-%d %H:%M:%S").to_string()
            })
            .unwrap_or_else(|_| "unknown".to_string());

        let size = format_size(metadata.len());
        let filename = baseline.path.file_name().unwrap().to_str().unwrap();

        println!("  {} - {} ({})", baseline.version, filename, created);
        println!("    Path: {}", baseline.path.display());
        println!("    Size: {}", size);
        println!();
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
