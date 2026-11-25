//! pgmt diff - Compare schema files against development database
//!
//! This command shows what changes would be applied by `pgmt apply`.
//! It compares your schema files (source of truth) to the dev database.

use crate::catalog::Catalog;
use crate::config::{Config, ObjectFilter};
use crate::diff::plan;
use crate::schema_ops::desired_and_target;
use anyhow::Result;
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
pub async fn cmd_diff(
    config: &Config,
    root_dir: &Path,
    args: DiffArgs,
    dev: &crate::config::DevUrl,
    shadow: &crate::config::ShadowDatabase,
) -> Result<()> {
    eprintln!("Comparing schema files with dev database...\n");

    // The dev catalog does not depend on the shadow work, so the two load at
    // the same time.
    eprintln!("Loading schema files and dev database...");
    let filter = ObjectFilter::from_config(config);
    let comparison = desired_and_target(config, root_dir, shadow, async {
        let dev_pool =
            crate::db::connection::connect_to_database(dev.as_str(), "development database")
                .await?;
        Catalog::load_managed(&dev_pool, &filter).await
    })
    .await?;
    let schema_catalog = comparison.desired.catalog;
    let dev_catalog = comparison.target;

    // Compute differences (dev -> schema, so SQL shows how to update dev)
    eprintln!("Computing differences...\n");
    let ordered_steps = plan(&dev_catalog, &schema_catalog)?;

    // Output results
    let context = DiffContext::new("dev database", "schema files");
    output_diff(
        &ordered_steps,
        &args.format,
        &context,
        &dev_catalog,
        &schema_catalog,
        args.output_sql.as_deref(),
    )?;

    // Exit with code 1 if differences found
    if has_differences(&ordered_steps) {
        std::process::exit(1);
    }

    Ok(())
}
