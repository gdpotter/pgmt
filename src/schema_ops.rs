use crate::catalog::Catalog;
use crate::catalog::file_dependencies::{FileDependencyAugmentation, FileToObjectMapping};
use crate::config::Config;
use crate::config::filter::ObjectFilter;
use crate::db::cleaner;
use crate::db::error_context::SqlErrorContext;
use crate::db::schema_processor::{SchemaProcessor, SchemaProcessorConfig};
use crate::diff::operations::SqlRenderer;
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

/// The desired state, plus the two things a caller that writes a baseline
/// needs alongside it.
pub struct DesiredState {
    /// The shadow's state before any schema file ran: whatever the image
    /// provides (extensions, platform schemas), on top of a cleaned database
    /// with the roles file applied. A baseline is the diff FROM this, so
    /// substrate cancels structurally instead of relying on the `objects`
    /// predicate to name it.
    pub base: Catalog,
    /// What the schema files describe, as a target holds it once pgmt's plan
    /// for it is applied.
    pub catalog: Catalog,
    /// Which file produced which object, for module attribution.
    pub mapping: FileToObjectMapping,
}

/// Build the desired state on the shadow database, as two pristine phases:
///
/// 1. Clean the shadow, apply the roles file and the schema files, and load
///    the **managed** catalog (the shadow inherits image substrate, which is
///    outside the managed universe).
/// 2. [`settle`] that catalog on a fresh shadow: apply pgmt's plan for it and
///    load what the server made of the plan.
///
/// A target never receives the schema files, only pgmt's rendering of each
/// object, and Postgres does not always reproduce its own rendering when it
/// re-parses it: `col::text = ANY (ARRAY[...]::text[])` comes back with the
/// cast moved onto each element. Phase 1 alone is therefore not a state any
/// target can reach, and diffing against it would never converge.
///
/// This is THE desired-state source. Every command that needs "what the schema
/// files describe" — apply, diff, migrate new/update/validate/diff, baseline —
/// goes through here, so settings like
/// `schema.augment_dependencies_from_files` apply uniformly.
pub async fn build_desired_state(
    config: &Config,
    root_dir: &Path,
    shadow: &crate::config::ShadowDatabase,
) -> Result<DesiredState> {
    let filter = ObjectFilter::from_config(config);

    let (written, mapping, augmentation) = shadow
        .with_fresh(|pool| async move {
            clean_shadow_for_schema(config, root_dir, &pool).await?;
            apply_schema_files_to_shadow(config, root_dir, &pool).await
        })
        .await?;
    // File-declared edges order the plan that settles the state, and are laid
    // back over the reloaded catalog, which knows nothing of files.
    let written = with_augmentation(filter.filter_catalog(written), augmentation.as_ref());

    info!("🔁 Replaying generated SQL on a fresh shadow database...");
    let settled = shadow
        .with_fresh(|pool| async move {
            clean_shadow_for_schema(config, root_dir, &pool).await?;
            settle(&pool, &written, async |pool| {
                Catalog::load_managed(pool, &filter).await
            })
            .await
        })
        .await?;

    Ok(DesiredState {
        base: settled.base,
        catalog: with_augmentation(settled.catalog, augmentation.as_ref()),
        mapping,
    })
}

fn with_augmentation(
    catalog: Catalog,
    augmentation: Option<&FileDependencyAugmentation>,
) -> Catalog {
    match augmentation {
        Some(augmentation) => catalog.with_file_dependencies_augmented(augmentation.clone()),
        None => catalog,
    }
}

/// A desired state after [`settle`].
pub struct Settled {
    /// The pristine database the plan was applied to, as loaded before it.
    pub base: Catalog,
    /// The same database after the plan.
    pub catalog: Catalog,
}

/// Apply pgmt's plan for `written` to `pool`, a pristine database, and load
/// the result with `load` (which also loads the base the plan starts from).
///
/// The result is what every target holds after pgmt migrates it to `written`,
/// so it is the state to diff targets against. Settling must not change which
/// objects exist, only how the server renders them: an object the plan drops
/// or invents is a planner bug, and failing here keeps it from being absorbed
/// silently into the desired state.
pub async fn settle(
    pool: &PgPool,
    written: &Catalog,
    load: impl AsyncFn(&PgPool) -> Result<Catalog>,
) -> Result<Settled> {
    let base = load(pool).await?;

    for step in crate::diff::plan(&base, written)? {
        for rendered in step.to_sql() {
            sqlx::raw_sql(sqlx::AssertSqlSafe(rendered.sql.clone()))
                .execute(pool)
                .await
                .with_context(|| {
                    format!(
                        "pgmt could not apply its own plan for the schema files to a \
                        fresh shadow database. This is likely a pgmt bug; please report \
                        it with the schema that triggers it.\n\nFailing statement:\n{}",
                        rendered.sql
                    )
                })?;
        }
    }

    let catalog = load(pool).await?;

    let written_ids = written.object_ids();
    let settled_ids = catalog.object_ids();
    if written_ids != settled_ids {
        let mut missing: Vec<_> = written_ids.difference(&settled_ids).collect();
        let mut extra: Vec<_> = settled_ids.difference(&written_ids).collect();
        missing.sort();
        extra.sort();
        anyhow::bail!(
            "pgmt's own plan for the schema files does not reproduce them on a fresh \
            shadow database. This is likely a pgmt bug; please report it with the \
            schema that triggers it.\n\nMissing after the plan: {:?}\nNot in the \
            schema files: {:?}",
            missing,
            extra
        );
    }

    Ok(Settled { base, catalog })
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
/// resulting **unfiltered**, unaugmented catalog, the file→object mapping, and
/// the file-declared dependency edges when
/// `schema.augment_dependencies_from_files` is on.
async fn apply_schema_files_to_shadow(
    config: &Config,
    root_dir: &Path,
    shadow_pool: &PgPool,
) -> Result<(
    Catalog,
    FileToObjectMapping,
    Option<FileDependencyAugmentation>,
)> {
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

    let augmentation = config
        .schema
        .augment_dependencies_from_files
        .then_some(processed_schema.augmentation);

    info!("🔍 Validating applied schema...");
    validate_schema_applied(shadow_pool).await?;
    info!("✅ Schema validation completed");

    Ok((
        processed_schema.catalog,
        processed_schema.file_mapping,
        augmentation,
    ))
}

/// The desired-state catalog alone, for commands that need neither the base
/// nor the file mapping. See [`build_desired_state`].
pub async fn apply_current_schema_to_shadow(
    config: &Config,
    root_dir: &Path,
    shadow: &crate::config::ShadowDatabase,
) -> Result<Catalog> {
    Ok(build_desired_state(config, root_dir, shadow).await?.catalog)
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
