//! pgmt migrate diff - Detect schema drift in target database
//!
//! This command compares the target (production) database against your
//! schema files to detect drift - changes made outside of migrations.
//!
//! Use this in CI/CD pipelines to ensure production matches the expected schema.

use crate::catalog::Catalog;
use crate::commands::diff_output::{DiffContext, DiffFormat, has_differences, output_diff};
use crate::config::{Config, ObjectFilter};
use crate::diff::{cascade, diff_all, diff_order};
use crate::schema_ops::apply_current_schema_to_shadow;
use anyhow::{Result, anyhow};
use sqlx::PgPool;
use std::path::Path;

/// Arguments for migrate diff command
#[derive(Debug)]
pub struct MigrateDiffArgs {
    pub format: DiffFormat,
    pub output_sql: Option<String>,
}

impl Default for MigrateDiffArgs {
    fn default() -> Self {
        Self {
            format: DiffFormat::Detailed,
            output_sql: None,
        }
    }
}

/// Detect drift between schema files and target database
///
/// Compares your schema files (source of truth) against the target database
/// to detect any drift - changes that were made outside of the migration process.
///
/// Exit codes:
/// - 0: No drift detected, target matches schema
/// - 1: Drift detected, target differs from schema
pub async fn cmd_migrate_diff(
    config: &Config,
    root_dir: &Path,
    args: MigrateDiffArgs,
) -> Result<()> {
    // Ensure target database is configured
    let target_url = config.databases.target.as_ref().ok_or_else(|| {
        anyhow!(
            "Target database URL not configured.\n\
            Set TARGET_DATABASE_URL environment variable or databases.target_url in pgmt.yaml"
        )
    })?;

    eprintln!("Checking target database for drift...\n");

    // Load schema into shadow database
    eprintln!("Loading schema files...");
    let schema_catalog = apply_current_schema_to_shadow(config, root_dir).await?;

    // Load target database catalog
    eprintln!("Loading target database...");
    let target_pool = PgPool::connect(target_url).await?;
    let target_catalog = Catalog::load(&target_pool).await?;

    // Apply object filtering (excludes tracking table and configured exclusions)
    let filter = ObjectFilter::new(&config.objects, &config.migration.tracking_table);
    let filtered_target_catalog = filter.filter_catalog(target_catalog);
    let filtered_schema_catalog = filter.filter_catalog(schema_catalog);

    // Compute differences (target -> schema, so SQL shows how to fix target)
    eprintln!("Computing differences...\n");
    let steps = diff_all(&filtered_target_catalog, &filtered_schema_catalog);
    let expanded_steps = cascade::expand(steps, &filtered_target_catalog, &filtered_schema_catalog);
    let ordered_steps = diff_order(
        expanded_steps,
        &filtered_target_catalog,
        &filtered_schema_catalog,
    )?;

    // Output results
    let context = DiffContext::new("target database", "schema files");
    output_diff(
        &ordered_steps,
        &args.format,
        &context,
        &filtered_target_catalog,
        &filtered_schema_catalog,
        args.output_sql.as_deref(),
    )?;

    // Exit with code 1 if drift detected
    if has_differences(&ordered_steps) {
        eprintln!("\nDrift detected! Target database differs from schema files.");
        std::process::exit(1);
    } else {
        eprintln!("No drift detected. Target database matches schema files.");
    }

    Ok(())
}
