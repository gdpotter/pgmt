use crate::catalog::Catalog;
use crate::catalog::file_dependencies::FileToObjectMapping;
use crate::config::Config;
use crate::config::filter::ObjectFilter;
use crate::db::cleaner;
use crate::db::error_context::SqlErrorContext;
use crate::db::schema_processor::{SchemaProcessor, SchemaProcessorConfig};
use anyhow::{Context, Result};
use sqlx::PgPool;
use std::path::Path;
use tracing::{debug, info};

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

    sqlx::raw_sql(sqlx::AssertSqlSafe(content.clone()))
        .execute(pool)
        .await
        .map_err(|e| {
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

/// Build the desired-state catalog on a shadow database: clean it, apply the
/// roles file and schema files, and return the **managed** catalog (the shadow
/// branch inherits image substrate, which is outside the managed universe).
///
/// This is THE desired-state source. Every command that needs "what the schema
/// files describe" — apply, diff, migrate new/update/validate/diff, baseline —
/// goes through here, so settings like
/// `schema.augment_dependencies_from_files` apply uniformly.
pub async fn build_desired_state(
    config: &Config,
    root_dir: &Path,
    shadow_pool: &PgPool,
) -> Result<Catalog> {
    clean_shadow_for_schema(config, root_dir, shadow_pool).await?;
    let (catalog, _) = apply_schema_files_to_shadow(config, root_dir, shadow_pool).await?;
    Ok(ObjectFilter::from_config(config).filter_catalog(catalog))
}

/// Clean the shadow database and apply the roles file, leaving it ready for
/// schema files. (For docker/branch shadows the clean is a no-op.)
async fn clean_shadow_for_schema(
    config: &Config,
    root_dir: &Path,
    shadow_pool: &PgPool,
) -> Result<()> {
    let roles_file = root_dir.join(&config.directories.roles);

    info!("🧹 Cleaning database before applying schema...");
    cleaner::clean_shadow_db(shadow_pool, &config.objects)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to clean database: {}", e))?;

    apply_roles_file(shadow_pool, &roles_file).await?;
    Ok(())
}

/// Apply the schema files to an already-cleaned shadow and return the
/// resulting **unfiltered** catalog plus the file→object mapping (callers
/// apply the managed-universe filter if needed).
async fn apply_schema_files_to_shadow(
    config: &Config,
    root_dir: &Path,
    shadow_pool: &PgPool,
) -> Result<(Catalog, FileToObjectMapping)> {
    let schema_dir = root_dir.join(&config.directories.schema);

    let processor_config = SchemaProcessorConfig {
        verbose: config.schema.verbose_file_processing,
        clean_before_apply: false, // Already cleaned by clean_shadow_for_schema
        objects: config.objects.clone(),
    };
    let processor = SchemaProcessor::new(shadow_pool.clone(), processor_config);
    let processed_schema = processor
        .process_schema_directory(&schema_dir)
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

    let file_mapping = processed_schema.file_mapping.clone();
    let catalog = if config.schema.augment_dependencies_from_files {
        processed_schema.with_file_dependencies_applied()
    } else {
        processed_schema.catalog
    };

    info!("🔍 Validating applied schema...");
    validate_schema_applied(shadow_pool).await?;
    info!("✅ Schema validation completed");

    Ok((catalog, file_mapping))
}

/// Connect to the given shadow database, build the desired state on it,
/// and return the managed catalog. Convenience wrapper around
/// [`build_desired_state`] for commands that don't hold a shadow pool.
pub async fn apply_current_schema_to_shadow(
    config: &Config,
    root_dir: &Path,
    shadow: &crate::config::ShadowDatabase,
) -> Result<Catalog> {
    let shadow_pool = shadow.connect_fresh().await?;

    let catalog = build_desired_state(config, root_dir, &shadow_pool).await?;

    // Reclaim the ephemeral branch now rather than leaking one per call (matters
    // for long-running callers like `apply --watch`); no-op for external URLs.
    crate::db::branch::drop_branch(shadow_pool).await?;
    Ok(catalog)
}

/// Like [`apply_current_schema_to_shadow`], but also returns the file→object
/// mapping so module-aware generation can attribute desired-state objects to
/// their owning modules. Identical shadow work — the mapping is computed by
/// the schema processor either way.
pub async fn apply_current_schema_to_shadow_with_mapping(
    config: &Config,
    root_dir: &Path,
    shadow: &crate::config::ShadowDatabase,
) -> Result<(Catalog, FileToObjectMapping)> {
    let shadow_pool = shadow.connect_fresh().await?;

    clean_shadow_for_schema(config, root_dir, &shadow_pool).await?;
    let (catalog, mapping) = apply_schema_files_to_shadow(config, root_dir, &shadow_pool).await?;
    let managed = ObjectFilter::from_config(config).filter_catalog(catalog);

    crate::db::branch::drop_branch(shadow_pool).await?;
    Ok((managed, mapping))
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
