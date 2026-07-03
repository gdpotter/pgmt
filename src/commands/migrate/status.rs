use crate::config::Config;
use crate::migration::{
    BaselineConfig, discover_migrations, find_latest_baseline, get_migration_starting_state,
};
use crate::migration_tracking::{
    ensure_section_tracking_table, ensure_tracking_table_exists, format_tracking_table_name,
};
use crate::validation::{ValidationConfig, validate_catalogs};
use crate::validation_output::{BaselineInfo, ValidationOutputOptions, format_validation_output};
use anyhow::{Result, anyhow};
use std::path::Path;

use crate::db::connection::connect_to_database;

pub async fn cmd_migrate_status(config: &Config, dev: &crate::config::DevUrl) -> Result<()> {
    println!("Checking migration status");

    let dev_pool = connect_to_database(dev.as_str(), "development database").await?;

    let tracking_table_name = format_tracking_table_name(&config.migration.tracking_table)?;

    // Ensure the tracking tables exist (and are migrated to the current shape)
    ensure_tracking_table_exists(&dev_pool, &config.migration.tracking_table).await?;
    ensure_section_tracking_table(&dev_pool, &config.migration.tracking_table).await?;

    // The version row is written when a migration STARTS (see
    // register_migration_start), so surface rows whose recorded sections
    // aren't all complete — they're in-progress or failed, not applied.
    let sections_table = format!(
        r#""{}"."{}_sections""#,
        config.migration.tracking_table.schema, config.migration.tracking_table.name
    );
    let applied_migrations: Vec<(i64, String, String, i64, bool)> = sqlx::query_as(&format!(
        "SELECT m.version, m.description, m.applied_at::TEXT,
                COUNT(s.section_name) FILTER (WHERE s.status <> 'completed') AS incomplete,
                m.is_baseline
         FROM {} m
         LEFT JOIN {} s
           ON s.migration_version = m.version AND s.is_baseline = m.is_baseline
         GROUP BY m.version, m.is_baseline, m.description, m.applied_at
         ORDER BY m.version",
        tracking_table_name, sections_table
    ))
    .fetch_all(&dev_pool)
    .await?;

    if applied_migrations.is_empty() {
        println!("No migrations have been applied");
    } else {
        println!("Applied migrations:");
        for (version, description, applied_at, incomplete, is_baseline) in applied_migrations {
            if incomplete > 0 {
                // Baselines can't be resumed by `migrate apply` (it never
                // re-runs baselines and skips versions <= a baseline's
                // version); a half-applied baseline resumes with `provision`.
                let resume_command = if is_baseline {
                    "pgmt migrate provision"
                } else {
                    "pgmt migrate apply"
                };
                println!(
                    "  {} - {} (INCOMPLETE: {} section(s) pending or failed — resume with `{}`)",
                    version, description, incomplete, resume_command
                );
            } else {
                println!("  {} - {} (applied: {})", version, description, applied_at);
            }
        }
    }

    dev_pool.close().await;
    Ok(())
}

pub async fn cmd_migrate_validate(
    config: &Config,
    root_dir: &Path,
    validation_options: &ValidationOutputOptions,
    shadow: &crate::config::ShadowDatabase,
) -> Result<()> {
    if !validation_options.quiet {
        eprintln!("🔍 Validating migration consistency...");
    }

    // Create necessary directories
    let migrations_dir = root_dir.join(&config.directories.migrations);
    let baselines_dir = root_dir.join(&config.directories.baselines);
    std::fs::create_dir_all(&migrations_dir)?;
    std::fs::create_dir_all(&baselines_dir)?;

    // Step 1: Reconstruct expected state from baseline + migration files,
    // through the same replay path migrate new/update use for their starting
    // state — both commands must see the same history.
    if !validation_options.quiet {
        eprintln!("📊 Reconstructing expected state from baseline + migration files...");
    }
    let roles_file = root_dir.join(&config.directories.roles);
    let baseline_config = BaselineConfig {
        validate_consistency: false,
        verbose: !validation_options.quiet,
    };
    // Reconstruction and desired-state each need their own pristine branch:
    // the replay dirties the shadow and branch cleans are no-ops, so sharing
    // one branch would make the schema-file apply collide. See `migrate new`.
    let starting_pool = shadow.connect_fresh().await?;
    let expected_catalog = get_migration_starting_state(
        &starting_pool,
        &baselines_dir,
        &migrations_dir,
        &roles_file,
        &baseline_config,
        config,
    )
    .await?;
    crate::db::branch::drop_branch(starting_pool).await?;

    // Step 2: Get desired state from current schema files
    if !validation_options.quiet {
        eprintln!("🔍 Loading desired state from current schema files...");
    }
    let desired_catalog =
        crate::schema_ops::apply_current_schema_to_shadow(config, root_dir, shadow).await?;

    // Step 3: Compare expected state (baseline + migrations) vs desired state (schema files)
    if !validation_options.quiet {
        eprintln!(
            "🔍 Comparing expected state (baseline + migrations) vs desired state (schema files)..."
        );
    }
    let validation_config = ValidationConfig {
        show_differences: validation_options.format == "human", // Only show differences in human format
        verbose: false,
    };

    let result = validate_catalogs(
        &expected_catalog,
        &desired_catalog,
        config,
        &validation_config,
    )?;

    // Step 4: Collect migration information for reporting
    let all_migrations = discover_migrations(&migrations_dir)?;
    let migration_versions: Vec<u64> = all_migrations.iter().map(|m| m.version).collect();

    let baseline_info = if let Some(latest_baseline) = find_latest_baseline(&baselines_dir)? {
        Some(BaselineInfo {
            version: latest_baseline.version,
            object_count: 0, // TODO: Could extract this from baseline file analysis
            description: format!("baseline_V{}", latest_baseline.version),
        })
    } else {
        None
    };

    // Step 5: Format and output results for CI/CD validation
    let output = format_validation_output(
        &result,
        validation_options,
        &migration_versions,
        &[], // No "unapplied" concept in CI/CD validation
        baseline_info.as_ref(),
    )?;

    println!("{}", output);

    // Return appropriate exit code for CI/CD
    if result.passed {
        if !validation_options.quiet {
            eprintln!("✅ Migration consistency validation passed");
        }
        Ok(())
    } else {
        Err(anyhow!(
            "Migration validation failed: Schema files don't match expected state from baseline + migrations (found {} differences)",
            result.differences.len()
        ))
    }
}
