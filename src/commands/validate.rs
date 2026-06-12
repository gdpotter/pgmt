use crate::catalog::Catalog;
use crate::config::Config;
use crate::validation::{ValidationConfig, validate_database_against_schema_files};
use anyhow::{Result, anyhow};
use std::path::Path;

pub async fn cmd_validate(config: &Config, root_dir: &Path) -> Result<()> {
    println!("🔍 Validating schema consistency...");

    let dev_pool =
        crate::db::connection::connect_to_database(&config.databases.dev, "development database")
            .await?;

    println!("📊 Loading current database schema...");
    let filter = crate::config::filter::ObjectFilter::from_config(config);
    let db_catalog = Catalog::load_managed(&dev_pool, &filter).await?;

    let validation_config = ValidationConfig::default();

    let result =
        validate_database_against_schema_files(&db_catalog, config, root_dir, &validation_config)
            .await?;

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
