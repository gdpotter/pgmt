use crate::catalog::Catalog;
use crate::config::Config;
use crate::schema_ops::desired_and_target;
use crate::validation::{ValidationConfig, validate_catalogs};
use anyhow::{Result, anyhow};
use std::path::Path;

pub async fn cmd_validate(
    config: &Config,
    root_dir: &Path,
    dev: &crate::config::DevUrl,
    shadow: &crate::config::ShadowDatabase,
) -> Result<()> {
    println!("🔍 Validating schema consistency...");

    let dev_pool =
        crate::db::connection::connect_to_database(dev.as_str(), "development database").await?;

    println!("📊 Loading current database schema...");
    let filter = crate::config::filter::ObjectFilter::from_config(config);
    let validation_config = ValidationConfig::default();

    println!("🔍 Validating database against schema files...");
    let comparison = desired_and_target(
        config,
        root_dir,
        shadow,
        Catalog::load_managed(&dev_pool, &filter),
    )
    .await?;

    let result = validate_catalogs(
        &comparison.target,
        &comparison.desired.catalog,
        config,
        &validation_config,
    )?;

    if result.passed {
        println!("✅ {}", result.message);
        Ok(())
    } else {
        println!("❌ {}", result.message);
        Err(anyhow!(
            "Schema validation failed: database does not match expected schema"
        ))
    }
}
