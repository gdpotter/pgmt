use crate::catalog::Catalog;
use crate::config::Config;
use crate::db::cleaner;
use crate::db::connection::connect_with_retry;
use crate::db::error_context::SqlErrorContext;
use crate::db::schema_processor::{SchemaProcessor, SchemaProcessorConfig};
use anyhow::{Context, Result};
use sqlx::PgPool;
use std::path::Path;
use tracing::{debug, info};

#[derive(Debug, Clone)]
pub struct SchemaOpsConfig {
    pub clean_before_apply: bool,
    pub verbose: bool,
    pub validate_after_apply: bool,
}

impl Default for SchemaOpsConfig {
    fn default() -> Self {
        Self {
            clean_before_apply: true,
            verbose: true,
            validate_after_apply: true,
        }
    }
}

/// Load and apply schema files to a database
pub async fn load_and_apply_schema(
    pool: &PgPool,
    schema_dir: &Path,
    config: &SchemaOpsConfig,
) -> Result<()> {
    if config.clean_before_apply {
        info!("🧹 Cleaning database before applying schema...");
        cleaner::clean_shadow_db(pool)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to clean database: {}", e))?;
    }

    // Use the schema processor for better error reporting and file dependency tracking
    let processor_config = SchemaProcessorConfig {
        verbose: config.verbose,
        clean_before_apply: false, // Already cleaned above if requested
    };
    let processor = SchemaProcessor::new(pool.clone(), processor_config);
    processor.process_schema_directory(schema_dir).await?;

    if config.validate_after_apply {
        info!("🔍 Validating applied schema...");
        validate_schema_applied(pool).await?;
        info!("✅ Schema validation completed");
    }

    Ok(())
}

/// Apply schema with file dependency tracking and augmentation
async fn apply_schema_with_file_dependency_tracking(
    shadow_pool: &PgPool,
    schema_dir: &Path,
    config: &SchemaOpsConfig,
    verbose: bool,
) -> Result<Catalog> {
    // Use SchemaProcessor which encapsulates all the file dependency tracking logic
    let processor_config = SchemaProcessorConfig {
        verbose,
        clean_before_apply: config.clean_before_apply,
    };
    let processor = SchemaProcessor::new(shadow_pool.clone(), processor_config);
    let processed_schema = processor
        .process_schema_directory(schema_dir)
        .await
        .with_context(|| {
            format!(
                "Failed to process schema files from directory: {}\n\n\
                Common causes:\n\
                • Schema directory doesn't exist or is empty\n\
                • Circular dependencies between files (A requires B, B requires A)\n\
                • Missing dependency files referenced in '-- require:' headers\n\
                • Invalid file paths in dependency declarations\n\
                • SQL syntax errors in schema files",
                schema_dir.display()
            )
        })?;

    let catalog = processed_schema.with_file_dependencies_applied();

    if config.validate_after_apply {
        info!("🔍 Validating applied schema...");
        validate_schema_applied(shadow_pool).await?;
        info!("✅ Schema validation completed");
    }

    Ok(catalog)
}

/// Apply roles file to a database if it exists
///
/// The roles file is applied before schema files to ensure that roles
/// referenced in GRANT statements exist. This is particularly important
/// for shadow databases which start with no custom roles.
///
/// The file should use idempotent statements like `CREATE ROLE IF NOT EXISTS`
/// to handle cases where roles may already exist.
pub async fn apply_roles_file(pool: &PgPool, roles_file: &Path) -> Result<()> {
    if !roles_file.exists() {
        debug!("Roles file not found at {}, skipping", roles_file.display());
        return Ok(());
    }

    info!("Applying roles from {}...", roles_file.display());

    let content = std::fs::read_to_string(roles_file)
        .with_context(|| format!("Failed to read roles file: {}", roles_file.display()))?;

    // Execute the roles file using raw_sql which supports multiple statements
    // This allows for complex scripts with CREATE ROLE, DO blocks, etc.
    debug!("Executing roles file: {}", truncate_for_log(&content));

    sqlx::raw_sql(&content).execute(pool).await.map_err(|e| {
        let ctx = SqlErrorContext::from_sqlx_error(&e, &content);
        anyhow::anyhow!(
            "{}",
            ctx.format(&roles_file.display().to_string(), &content)
        )
    })?;

    info!("Roles applied successfully");
    Ok(())
}

/// Truncate a string for logging purposes
fn truncate_for_log(s: &str) -> String {
    if s.len() > 100 {
        format!("{}...", &s[..100])
    } else {
        s.to_string()
    }
}

/// Apply current schema from config to shadow database
pub async fn apply_current_schema_to_shadow(config: &Config, root_dir: &Path) -> Result<Catalog> {
    let shadow_url = config.databases.shadow.get_connection_string().await?;
    let schema_dir = root_dir.join(&config.directories.schema);
    let roles_file = root_dir.join(&config.directories.roles);

    let ops_config = SchemaOpsConfig::default();

    apply_schema_to_shadow_with_roles(
        &shadow_url,
        &schema_dir,
        Some(&roles_file),
        &ops_config,
        config.schema.augment_dependencies_from_files,
        config.schema.verbose_file_processing,
    )
    .await
}

/// Apply schema to shadow database with roles file support and optional file dependency augmentation
pub async fn apply_schema_to_shadow_with_roles(
    shadow_url: &str,
    schema_dir: &Path,
    roles_file: Option<&Path>,
    config: &SchemaOpsConfig,
    enable_file_deps: bool,
    verbose_file_processing: bool,
) -> Result<Catalog> {
    let shadow_pool = connect_with_retry(shadow_url).await?;

    // Clean the database first if requested
    if config.clean_before_apply {
        info!("🧹 Cleaning database before applying schema...");
        cleaner::clean_shadow_db(&shadow_pool)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to clean database: {}", e))?;
    }

    // Apply roles file before schema files (if provided)
    if let Some(roles_path) = roles_file {
        apply_roles_file(&shadow_pool, roles_path).await?;
    }

    // Create a config that skips cleaning since we already did it
    let schema_config = SchemaOpsConfig {
        clean_before_apply: false,
        ..*config
    };

    // If file dependency augmentation is enabled, use the enhanced method
    let catalog = if enable_file_deps {
        apply_schema_with_file_dependency_tracking(
            &shadow_pool,
            schema_dir,
            &schema_config,
            verbose_file_processing,
        )
        .await?
    } else {
        // Use the traditional method
        load_and_apply_schema(&shadow_pool, schema_dir, &schema_config).await?;

        Catalog::load(&shadow_pool)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to load catalog from shadow database: {}", e))?
    };

    shadow_pool.close().await;
    Ok(catalog)
}

/// Validate that schema was applied correctly by checking basic connectivity and structure
async fn validate_schema_applied(pool: &PgPool) -> Result<()> {
    // Basic connectivity test
    sqlx::query("SELECT 1")
        .execute(pool)
        .await
        .map_err(|e| anyhow::anyhow!("Database connectivity test failed: {}", e))?;

    // Check that we can access basic PostgreSQL system tables
    sqlx::query("SELECT count(*) FROM pg_tables WHERE schemaname NOT IN ('information_schema', 'pg_catalog')")
        .execute(pool)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to query system tables: {}", e))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_ops_config_default() {
        let config = SchemaOpsConfig::default();
        assert!(config.clean_before_apply);
        assert!(config.verbose);
        assert!(config.validate_after_apply);
    }

    #[test]
    fn test_schema_ops_config_custom() {
        let config = SchemaOpsConfig {
            clean_before_apply: false,
            verbose: false,
            validate_after_apply: false,
        };
        assert!(!config.clean_before_apply);
        assert!(!config.verbose);
        assert!(!config.validate_after_apply);
    }
}
