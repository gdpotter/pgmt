use crate::commands::migrate::pipeline::{
    BaselinePolicy, GenerationRequest, StartingState, generate,
};
use crate::config::Config;
use crate::migration::find_latest_migration;
use anyhow::{Result, anyhow};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// `migrate update` has no `--create-baseline` flag; re-anchoring is owned by
/// `migrate new`, so point the user there rather than at a flag this subcommand
/// would reject as unknown.
const REANCHOR_HINT: &str =
    "run 'pgmt migrate new <description> --create-baseline' to emit a re-anchoring baseline.";

const NO_CHANGES_SQL: &str = "-- No changes detected\n";

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

    let migrations_dir = root_dir.join(&config.directories.migrations);
    let baselines_dir = root_dir.join(&config.directories.baselines);
    std::fs::create_dir_all(&migrations_dir)?;
    std::fs::create_dir_all(&baselines_dir)?;

    let latest_migration = find_latest_migration(&migrations_dir)?.ok_or_else(|| {
        anyhow!(
            "No migrations found. Use 'pgmt migrate new <description>' to create the first migration."
        )
    })?;
    println!("Updating migration: {}", latest_migration.path.display());

    let generated = generate(
        config,
        root_dir,
        shadow,
        GenerationRequest {
            version: latest_migration.version,
            // The original description is kept; only the content is rewritten.
            description: latest_migration.description.clone(),
            starting_state: StartingState::Before(latest_migration.version),
            baseline: BaselinePolicy::IfManaged,
            reanchor_hint: REANCHOR_HINT,
            dry_run,
            stub_migration: false,
        },
    )
    .await?;

    if generated.is_empty() {
        println!("No changes detected - updating migration to be empty");

        if dry_run {
            println!(
                "🔄 Would update: {} (now empty)",
                latest_migration.path.display()
            );
            println!("🔍 Dry-run complete! No changes were made.");
        } else {
            std::fs::write(&latest_migration.path, NO_CHANGES_SQL)?;
            println!(
                "Updated migration: {} (now empty)",
                latest_migration.path.display()
            );
        }

        return Ok(());
    }

    match &generated.baseline {
        Some(baseline) if dry_run => {
            println!("🔄 Would update baseline: {}", baseline.path.display())
        }
        Some(baseline) => println!("Updated baseline: {}", baseline.path.display()),
        None => println!(
            "Skipping baseline update (baseline does not exist and create_baselines_by_default is false)"
        ),
    }

    let migration_sql = generated
        .migration_sql
        .unwrap_or_else(|| NO_CHANGES_SQL.to_string());
    if dry_run {
        println!(
            "📝 Preview: Generated migration content ({} chars)",
            migration_sql.len()
        );
        println!("🔄 Would update: {}", latest_migration.path.display());
        println!("\n📋 Migration preview:\n{}", migration_sql);
        println!("🔍 Dry-run complete! No changes were made.");
    } else {
        std::fs::write(&latest_migration.path, &migration_sql)?;
        println!("Updated migration: {}", latest_migration.path.display());
        println!("Migration update complete!");
    }
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

    let migrations_dir = root_dir.join(&config.directories.migrations);
    let baselines_dir = root_dir.join(&config.directories.baselines);
    std::fs::create_dir_all(&migrations_dir)?;
    std::fs::create_dir_all(&baselines_dir)?;

    let target_migration =
        find_migration_by_version(&migrations_dir, version_str)?.ok_or_else(|| {
            anyhow!(
                "Migration '{}' not found. Use 'pgmt migrate status' to see available migrations.",
                version_str
            )
        })?;

    println!(
        "Found migration: {} ({})",
        target_migration.path.display(),
        target_migration.description
    );

    if backup {
        let backup_path = target_migration.path.with_extension("sql.bak");
        if dry_run {
            println!("💾 Would create backup: {}", backup_path.display());
        } else {
            std::fs::copy(&target_migration.path, &backup_path)?;
            println!("💾 Backup created: {}", backup_path.display());
        }
    }

    let is_latest = find_latest_migration(&migrations_dir)?
        .map(|latest| latest.version == target_migration.version)
        .unwrap_or(false);

    // The latest migration keeps its version; an older one is renumbered to the
    // head of the log, since its replacement has to apply after everything that
    // already ran.
    let new_version = if is_latest {
        target_migration.version
    } else {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| anyhow!("System time is before Unix epoch: {}", e))?
            .as_secs()
    };
    let new_description = target_migration.description.clone();

    let generated = generate(
        config,
        root_dir,
        shadow,
        GenerationRequest {
            version: new_version,
            description: new_description.clone(),
            // Replayed up to the migration being rewritten, whatever version
            // its replacement takes.
            starting_state: StartingState::Before(target_migration.version),
            baseline: BaselinePolicy::IfManaged,
            reanchor_hint: REANCHOR_HINT,
            dry_run,
            stub_migration: false,
        },
    )
    .await?;

    let renumbered_filename = format!(
        "{}{}_{}.sql",
        config.migration.filename_prefix,
        new_version,
        new_description.replace(' ', "_")
    );
    let renumbered_path = migrations_dir.join(&renumbered_filename);

    if !generated.has_changes {
        if is_latest {
            println!("No changes detected - updating migration to be empty");
            if dry_run {
                println!(
                    "🔄 Would update: {} (now empty)",
                    target_migration.path.display()
                );
            } else {
                std::fs::write(&target_migration.path, NO_CHANGES_SQL)?;
                println!(
                    "Updated migration: {} (now empty)",
                    target_migration.path.display()
                );
            }
        } else {
            println!("No changes detected - conflicts resolved by other migrations");
            // An older migration is still renumbered when it has no changes.
            if dry_run {
                println!(
                    "🔄 Would rename {} → {}",
                    target_migration.version, new_version
                );
                println!("   Delete: {}", target_migration.path.display());
                println!("   Create: {}", renumbered_path.display());
            } else {
                let content = format!(
                    "-- Migration: {}\n-- Version: {}{}\n-- Generated by pgmt migrate update (renumbered from {}{})\n-- No changes needed - conflicts resolved by intervening migrations\n",
                    new_description,
                    config.migration.filename_prefix,
                    new_version,
                    config.migration.filename_prefix,
                    target_migration.version
                );
                std::fs::remove_file(&target_migration.path)?;
                std::fs::write(&renumbered_path, &content)?;
                println!(
                    "Migration {} updated to {} (no changes needed)",
                    target_migration.version, new_version
                );
                println!("Created: {}", renumbered_path.display());
            }
        }

        if !generated.partition_diverged {
            if dry_run {
                println!("🔍 Dry-run complete! No changes were made.");
            }
            return Ok(());
        }
        // Pure re-tag: fall through so the re-anchoring baseline is reported
        // and any acquisition sections are written.
    }

    match &generated.baseline {
        Some(baseline) if dry_run => {
            let verb = if is_latest { "update" } else { "create" };
            println!("🔄 Would {} baseline: {}", verb, baseline.path.display())
        }
        Some(baseline) if is_latest => {
            println!("Updated baseline: {}", baseline.path.display())
        }
        Some(baseline) => println!("Created baseline: {}", baseline.path.display()),
        None if is_latest => println!(
            "Skipping baseline update (baseline does not exist and create_baselines_by_default is false)"
        ),
        None => println!("Skipping baseline creation (create_baselines_by_default is false)"),
    }

    // `None` means there is nothing left to write: the no-changes handling
    // above already produced the file.
    if let Some(migration_sql) = &generated.migration_sql {
        if dry_run {
            println!(
                "📝 Preview: Generated migration content ({} chars)",
                migration_sql.len()
            );
            if is_latest {
                println!("🔄 Would update: {}", target_migration.path.display());
            } else {
                println!(
                    "🔄 Would rename {} → {}",
                    target_migration.version, new_version
                );
                println!("   Delete: {}", target_migration.path.display());
                println!("   Create: {}", renumbered_path.display());
            }
            println!("\n📋 Migration preview:\n{}", migration_sql);
        } else if is_latest {
            std::fs::write(&target_migration.path, migration_sql)?;
            println!("Updated migration: {}", target_migration.path.display());
        } else {
            // The no-changes fall-through (a pure re-tag with acquisitions)
            // already moved the file; only delete what still exists.
            if target_migration.path.exists() {
                std::fs::remove_file(&target_migration.path)?;
                println!("Deleted: {}", target_migration.path.display());
            }
            std::fs::write(&renumbered_path, migration_sql)?;
            println!(
                "Migration {} updated to {} (renumbered)",
                target_migration.version, new_version
            );
            println!("Created: {}", renumbered_path.display());
        }
    }

    if dry_run {
        println!("🔍 Dry-run complete! No changes were made.");
    } else {
        println!("Migration update complete!");
    }
    Ok(())
}
