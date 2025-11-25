//! pgmt diff - Compare schema files against development database
//!
//! This command shows what changes would be applied by `pgmt apply`.
//! It compares your schema files (source of truth) to the dev database.

use crate::catalog::Catalog;
use crate::config::{Config, ObjectFilter};
use crate::diff::{cascade, diff_all, diff_order};
use crate::schema_ops::apply_current_schema_to_shadow;
use anyhow::Result;
use sqlx::PgPool;
use std::path::Path;

use super::diff_output::{DiffContext, has_differences, output_diff};

// Re-export DiffFormat for use in main.rs
pub use super::diff_output::DiffFormat;

/// Arguments for the diff command
#[derive(Debug)]
pub struct DiffArgs {
    pub format: DiffFormat,
    pub output_sql: Option<String>,
}

impl Default for DiffArgs {
    fn default() -> Self {
        Self {
            format: DiffFormat::Detailed,
            output_sql: None,
        }
    }
}

/// Compare schema files against development database
///
/// This shows what `pgmt apply` would do - the differences between
/// your schema files and the current state of the dev database.
pub async fn cmd_diff(config: &Config, root_dir: &Path, args: DiffArgs) -> Result<()> {
    eprintln!("Comparing schema files with dev database...\n");

    // Load schema into shadow database
    eprintln!("Loading schema files...");
    let schema_catalog = apply_current_schema_to_shadow(config, root_dir).await?;

    // Load dev database catalog
    eprintln!("Loading dev database...");
    let dev_pool = PgPool::connect(&config.databases.dev).await?;
    let dev_catalog = Catalog::load(&dev_pool).await?;

    // Apply object filtering (excludes tracking table and configured exclusions)
    let filter = ObjectFilter::new(&config.objects, &config.migration.tracking_table);
    let filtered_dev_catalog = filter.filter_catalog(dev_catalog);
    let filtered_schema_catalog = filter.filter_catalog(schema_catalog);

    // Compute differences (dev -> schema, so SQL shows how to update dev)
    eprintln!("Computing differences...\n");
    let steps = diff_all(&filtered_dev_catalog, &filtered_schema_catalog);
    let expanded_steps = cascade::expand(steps, &filtered_dev_catalog, &filtered_schema_catalog);
    let ordered_steps = diff_order(
        expanded_steps,
        &filtered_dev_catalog,
        &filtered_schema_catalog,
    )?;

    // Output results
    let context = DiffContext::new("dev database", "schema files");
    output_diff(
        &ordered_steps,
        &args.format,
        &context,
        &filtered_dev_catalog,
        &filtered_schema_catalog,
        args.output_sql.as_deref(),
    )?;

    // Exit with code 1 if differences found
    if has_differences(&ordered_steps) {
        std::process::exit(1);
    }

    Ok(())
}
