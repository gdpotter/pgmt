//! pgmt migrate diff - Detect schema drift in target database
//!
//! This command compares the target (production) database against your
//! schema files to detect drift - changes made outside of migrations.
//!
//! Use this in CI/CD pipelines to ensure production matches the expected schema.

use crate::catalog::Catalog;
use crate::commands::diff_output::{DiffContext, DiffFormat, has_differences, output_diff};
use crate::config::{Config, ObjectFilter};
use crate::diff::plan;
use crate::schema_ops::desired_and_target;
use anyhow::Result;
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
    target: &crate::config::TargetUrl,
    shadow: &crate::config::ShadowDatabase,
) -> Result<()> {
    eprintln!("Checking target database for drift...\n");

    // The target catalog does not depend on the shadow work, so the two load
    // at the same time.
    eprintln!("Loading schema files and target database...");
    let filter = ObjectFilter::from_config(config);
    let comparison = desired_and_target(config, root_dir, shadow, async {
        let target_pool =
            crate::db::connection::connect_to_database(target.as_str(), "target database").await?;
        Catalog::load_managed(&target_pool, &filter).await
    })
    .await?;
    let schema_catalog = comparison.desired.catalog;
    let target_catalog = comparison.target;

    // Compute differences (target -> schema, so SQL shows how to fix target)
    eprintln!("Computing differences...\n");
    let ordered_steps = plan(&target_catalog, &schema_catalog)?;

    // Output results
    let context = DiffContext::new("target database", "schema files");
    output_diff(
        &ordered_steps,
        &args.format,
        &context,
        &target_catalog,
        &schema_catalog,
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
