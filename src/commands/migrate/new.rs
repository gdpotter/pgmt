use crate::commands::migrate::pipeline::{
    BaselinePolicy, GenerationRequest, StartingState, generate,
};
use crate::config::Config;
use crate::migrate::migration_filename;
use crate::prompts::prompt_required_string_with_validation;
use anyhow::Result;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

pub async fn cmd_migrate_new(
    config: &Config,
    root_dir: &Path,
    description: Option<&str>,
    create_baseline: bool,
    empty: bool,
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

    let should_create_baseline = create_baseline || config.migration.create_baselines_by_default;

    // A stub needs no diff, and with no baseline to build there is nothing left
    // to read the database for. Pending schema changes stay pending.
    if empty && !should_create_baseline {
        let path = write_empty_migration(&migrations_dir, config, version, &description)?;
        println!("Created empty migration: {}", path.display());
        return Ok(());
    }

    // `--create-baseline` opts in for this run; its absence falls back to
    // config, so the flag can only add, never suppress a configured default.
    let generated = generate(
        config,
        root_dir,
        shadow,
        GenerationRequest {
            version,
            description: description.clone(),
            starting_state: StartingState::FullHistory,
            baseline: if should_create_baseline {
                BaselinePolicy::Requested
            } else {
                BaselinePolicy::Never
            },
            reanchor_hint: "re-run with --create-baseline to emit a re-anchoring baseline.",
            dry_run: false,
            stub_migration: empty,
        },
    )
    .await?;

    if !empty && generated.is_empty() {
        println!("No changes detected - no migration needed");
        return Ok(());
    }

    match &generated.baseline {
        Some(baseline) => println!("Created baseline: {}", baseline.path.display()),
        None if !should_create_baseline => {
            println!("Skipping baseline creation (use --create-baseline to create one)")
        }
        None => {}
    }

    if empty {
        let path = write_empty_migration(&migrations_dir, config, version, &description)?;
        println!("Created empty migration: {}", path.display());
        println!("Migration generation complete!");
        return Ok(());
    }

    match generated.migration_sql {
        Some(sql) => {
            let migration_path = migrations_dir.join(&generated.filename);
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

/// Write the stub for a hand-written migration. Having no section header, it
/// applies as a single base (unmoduled) section.
fn write_empty_migration(
    migrations_dir: &Path,
    config: &Config,
    version: u64,
    description: &str,
) -> Result<std::path::PathBuf> {
    let filename = migration_filename(&config.migration.filename_prefix, version, description);
    let path = migrations_dir.join(filename);
    std::fs::write(&path, "")?;
    Ok(path)
}
