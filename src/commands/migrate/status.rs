use crate::config::Config;
use crate::db::connection::connect_with_retry;
use crate::migration::{
    BaselineConfig, discover_migrations, find_latest_baseline, get_migration_starting_state,
};
use crate::migration_tracking::format_tracking_table_name;
use crate::validation::{ValidationConfig, validate_catalogs};
use crate::validation_output::{BaselineInfo, ValidationOutputOptions, format_validation_output};
use anyhow::{Result, anyhow};
use std::path::Path;

use crate::db::connection::connect_to_database;

pub async fn cmd_migrate_status(config: &Config, dev: &crate::config::DevUrl) -> Result<()> {
    println!("Checking migration status");

    let dev_pool = connect_to_database(dev.as_str(), "development database").await?;

    let tracking_table_name = format_tracking_table_name(&config.migration.tracking_table)?;

    sqlx::query(&format!(
        r#"
        CREATE TABLE IF NOT EXISTS {} (
            version BIGINT PRIMARY KEY,
            description TEXT NOT NULL,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            checksum TEXT NOT NULL
        )
        "#,
        tracking_table_name
    ))
    .execute(&dev_pool)
    .await?;

    // Get list of applied migrations
    let applied_migrations: Vec<(i64, String, String)> = sqlx::query_as(&format!(
        "SELECT version, description, applied_at::TEXT FROM {} ORDER BY version",
        tracking_table_name
    ))
    .fetch_all(&dev_pool)
    .await?;

    if applied_migrations.is_empty() {
        println!("No migrations have been applied");
    } else {
        println!("Applied migrations:");
        for (version, description, applied_at) in applied_migrations {
            println!("  {} - {} (applied: {})", version, description, applied_at);
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

    // Connect to shadow database for both reconstructions
    let shadow_url = shadow.get_connection_string().await?;
    let shadow_pool = connect_with_retry(&shadow_url).await?;

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
    let expected_catalog = get_migration_starting_state(
        &shadow_pool,
        &baselines_dir,
        &migrations_dir,
        &roles_file,
        &baseline_config,
        config,
    )
    .await?;

    // Step 2: Get desired state from current schema files
    if !validation_options.quiet {
        eprintln!("🔍 Loading desired state from current schema files...");
    }
    let desired_catalog =
        crate::schema_ops::build_desired_state(config, root_dir, &shadow_pool).await?;

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

