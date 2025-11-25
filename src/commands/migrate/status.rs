use crate::catalog::Catalog;
use crate::config::{Config, ObjectFilter};
use crate::db::connection::connect_with_retry;
use crate::migration::{discover_migrations, find_latest_baseline};
use crate::migration_tracking::format_tracking_table_name;
use crate::validation::{ValidationConfig, validate_catalogs};
use crate::validation_output::{BaselineInfo, ValidationOutputOptions, format_validation_output};
use anyhow::{Context, Result, anyhow};
use sqlx::PgPool;
use std::path::Path;

pub async fn cmd_migrate_status(config: &Config) -> Result<()> {
    println!("Checking migration status");

    let dev_pool = PgPool::connect(&config.databases.dev).await?;

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
            println!("  V{} - {} (applied: {})", version, description, applied_at);
        }
    }

    dev_pool.close().await;
    Ok(())
}

pub async fn cmd_migrate_validate(
    config: &Config,
    root_dir: &Path,
    validation_options: &ValidationOutputOptions,
) -> Result<()> {
    if !validation_options.quiet {
        eprintln!("🔍 Validating migration consistency...");
    }

    // Create necessary directories
    let migrations_dir = root_dir.join("migrations");
    let baselines_dir = root_dir.join("schema_baselines");
    std::fs::create_dir_all(&migrations_dir)?;
    std::fs::create_dir_all(&baselines_dir)?;

    // Connect to shadow database for both reconstructions
    let shadow_url = config.databases.shadow.get_connection_string().await?;
    let shadow_pool = connect_with_retry(&shadow_url).await?;

    // Step 1: Reconstruct expected state from baseline + migration files
    if !validation_options.quiet {
        eprintln!("📊 Reconstructing expected state from baseline + migration files...");
    }
    let expected_catalog =
        reconstruct_expected_state_from_schema_files(&shadow_pool, &baselines_dir, &migrations_dir)
            .await?;

    // Step 2: Get desired state from current schema files
    if !validation_options.quiet {
        eprintln!("🔍 Loading desired state from current schema files...");
    }
    let desired_catalog =
        crate::schema_ops::apply_current_schema_to_shadow(config, root_dir).await?;

    // Apply object filtering to both catalogs
    let filter = ObjectFilter::new(&config.objects, &config.migration.tracking_table);
    let filtered_expected = filter.filter_catalog(expected_catalog);
    let filtered_desired = filter.filter_catalog(desired_catalog);

    // Step 3: Compare expected state (baseline + migrations) vs desired state (schema files)
    if !validation_options.quiet {
        eprintln!(
            "🔍 Comparing expected state (baseline + migrations) vs desired state (schema files)..."
        );
    }
    let validation_config = ValidationConfig {
        show_differences: validation_options.format == "human", // Only show differences in human format
        apply_object_filter: false,                             // Already filtered above
        verbose: false,
    };

    let result = validate_catalogs(
        &filtered_expected,
        &filtered_desired,
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

/// Reconstruct expected database state from schema files (baseline + ALL migration files)
/// This validates against the source of truth, not what the database claims is applied
async fn reconstruct_expected_state_from_schema_files(
    shadow_pool: &PgPool,
    baselines_dir: &Path,
    migrations_dir: &Path,
) -> Result<Catalog> {
    use crate::commands::migrate::section_executor::{ExecutionMode, SectionExecutor};
    use crate::config::types::TrackingTable;
    use crate::db::cleaner;
    use crate::db::schema_executor::SchemaExecutor;
    use crate::migration::{parse_migration_sections, validate_sections};
    use crate::progress::SectionReporter;

    // Clean shadow database
    cleaner::clean_shadow_db(shadow_pool).await?;

    // Apply the latest baseline if it exists
    if let Some(baseline) = find_latest_baseline(baselines_dir)? {
        let baseline_sql = std::fs::read_to_string(&baseline.path)?;
        SchemaExecutor::execute_sql_with_enhanced_errors(
            shadow_pool,
            &baseline.path,
            &baseline_sql,
        )
        .await
        .context("Failed to apply baseline during schema file reconstruction")?;
    }

    // Get ALL migration files and apply them in chronological order
    let all_migrations = discover_migrations(migrations_dir)?;
    let mut sorted_migrations = all_migrations;
    sorted_migrations.sort_by_key(|m| m.version);

    // Apply migrations that come after the baseline (if any)
    let baseline_version = find_latest_baseline(baselines_dir)?
        .map(|b| b.version)
        .unwrap_or(0);

    for migration_file in sorted_migrations {
        if migration_file.version > baseline_version {
            let migration_sql = std::fs::read_to_string(&migration_file.path).context(format!(
                "Failed to read migration file V{}",
                migration_file.version
            ))?;

            // Parse sections (creates default section if no sections defined)
            let sections = parse_migration_sections(&migration_file.path, &migration_sql)?;
            validate_sections(&sections)?;

            // Execute using SectionExecutor in Validation mode
            let tracking_table = TrackingTable::default();
            let reporter = SectionReporter::new(sections.len(), false);
            let mut executor = SectionExecutor::new(
                shadow_pool.clone(),
                tracking_table,
                reporter,
                ExecutionMode::Validation,
            );

            for section in &sections {
                executor
                    .execute_section(migration_file.version, section)
                    .await
                    .context(format!(
                        "Failed to apply migration file V{} during reconstruction",
                        migration_file.version
                    ))?;
            }
        }
    }

    // Load and return the reconstructed catalog
    Catalog::load(shadow_pool).await
}
