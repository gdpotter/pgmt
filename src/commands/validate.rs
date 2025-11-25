use crate::catalog::Catalog;
use crate::config::Config;
use crate::validation::{ValidationConfig, validate_database_against_schema_files};
use anyhow::{Result, anyhow};
use sqlx::PgPool;
use std::path::Path;

pub async fn cmd_validate(config: &Config) -> Result<()> {
    println!("🔍 Validating schema consistency...");

    let dev_pool = PgPool::connect(&config.databases.dev).await?;

    println!("📊 Loading current database schema...");
    let db_catalog = Catalog::load(&dev_pool).await?;

    let validation_config = ValidationConfig::default();
    let root_dir = Path::new(".");

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
