use crate::catalog::Catalog;
use crate::catalog::file_dependencies::{FileDependencyAugmentation, FileToObjectMapping};
use crate::catalog::id::{DbObjectId, DependsOn};
use crate::config::Config;
use crate::config::filter::ObjectFilter;
use crate::db::cleaner;
use crate::db::error_context::SqlErrorContext;
use crate::db::plan_executor::{execute_sections, render_sections};
use crate::db::schema_processor::{SchemaProcessor, SchemaProcessorConfig};
use crate::diff::grants::is_owner_grant;
use anyhow::{Context, Result};
use sqlx::PgPool;
use std::collections::HashSet;
use std::path::Path;
use tracing::{debug, info, warn};

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
    /// The file-declared dependency edges laid over `catalog`, when
    /// `schema.augment_dependencies_from_files` is on.
    pub augmentation: Option<FileDependencyAugmentation>,
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
        augmentation,
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

/// A catalog after [`settle`].
pub struct Settled {
    /// The pristine database the plan was applied to, as loaded before it.
    pub base: Catalog,
    /// The same database after the plan.
    pub catalog: Catalog,
}

/// Apply pgmt's plan for `written` to `pool`, a pristine database, and load
/// the result with `load` (which also loads the base the plan starts from).
///
/// The result is what every target holds after pgmt migrates it to `written`.
/// Settling must not change which objects exist, only how the server renders
/// them: an object the plan drops or invents is a planner bug, so settling
/// fails rather than hand back a different set of objects.
///
/// Owner grants are left out of that check. The plan runs as the shadow's
/// user and pgmt does not manage ownership, so the replay's owner grants name
/// that user whoever owned `written`; the diff ignores owner grants for the
/// same reason.
pub async fn settle(
    pool: &PgPool,
    written: &Catalog,
    load: impl AsyncFn(&PgPool) -> Result<Catalog>,
) -> Result<Settled> {
    const PLAN_FAILED: &str = "pgmt could not apply its own plan to a fresh shadow \
        database. This is likely a pgmt bug; please report it with the schema that \
        triggers it.";

    let base = load(pool).await?;

    let sections = render_sections(&crate::modules::sectionize_unmoduled(&base, written)?);
    execute_sections(pool, &sections, |_| {})
        .await
        .context(PLAN_FAILED)?;

    let catalog = load(pool).await?;

    let written_ids = ids_without_owner_grants(written);
    let settled_ids = ids_without_owner_grants(&catalog);
    if written_ids != settled_ids {
        let mut missing: Vec<_> = written_ids.difference(&settled_ids).collect();
        let mut extra: Vec<_> = settled_ids.difference(&written_ids).collect();
        missing.sort();
        extra.sort();
        anyhow::bail!(
            "pgmt's own plan does not reproduce the objects it was made for on a fresh \
            shadow database. This is likely a pgmt bug; please report it with the schema \
            that triggers it.\n\nMissing after the plan: {:?}\nAdded by the plan: {:?}",
            missing,
            extra
        );
    }

    Ok(Settled { base, catalog })
}

fn ids_without_owner_grants(catalog: &Catalog) -> HashSet<DbObjectId> {
    let mut ids = catalog.object_ids();
    for grant in catalog.grants.iter().filter(|g| is_owner_grant(g)) {
        ids.remove(&DependsOn::id(grant));
    }
    ids
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
        clean_before_apply: false, // Already cleaned by clean_shadow_for_schema
        objects: config.objects.clone(),
    };
    let processor = SchemaProcessor::new(shadow_pool.clone(), processor_config);
    let processed_schema = processor.process_schema_directory(&schema_dir).await?;

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

/// The two sides of a comparison against the schema files.
pub struct Comparison {
    /// What the schema files describe. See [`build_desired_state`].
    pub desired: DesiredState,
    /// The state being compared with it: a live database, or the replay of
    /// baseline + migrations.
    pub target: Catalog,
}

/// Build the desired state and load the target it is compared with.
///
/// This is THE comparison source. Every command that diffs some state against
/// the schema files — apply, diff, validate, migrate new/update/validate/diff
/// — gets both sides here, so a rule about how the two are made comparable
/// applies to all of them.
///
/// `load_target` runs concurrently with the shadow work. A loader that itself
/// needs a fresh shadow (the migration replay) is awaited by its caller first
/// and passed in ready.
///
/// The target that comes back is settled whenever it differs from the
/// desired state; see [`settle_target`].
pub async fn desired_and_target(
    config: &Config,
    root_dir: &Path,
    shadow: &crate::config::ShadowDatabase,
    load_target: impl Future<Output = Result<Catalog>>,
) -> Result<Comparison> {
    let (desired, target) =
        tokio::try_join!(build_desired_state(config, root_dir, shadow), load_target)?;
    let target = settle_target(config, root_dir, shadow, target, &desired).await?;
    Ok(Comparison { desired, target })
}

/// The target as it compares with the settled desired state.
///
/// The desired state is in the form Postgres stores for pgmt's rendering. A
/// target built from other SQL — hand-written migrations, or a database pgmt
/// did not build — can hold the form Postgres stored for that SQL instead,
/// and differ from the desired state only in how a definition renders: a
/// varchar `IN (...)` list without the cast on each element, a later UNION arm
/// without its column alias. Passing the target through the same rendering
/// removes exactly those differences. The settled target has the target's
/// objects, so a plan made from it applies to the target itself.
///
/// The replay is ordered by the same file-declared edges as the desired
/// state's, where both ends exist in the target: a SQL function body is
/// checked when it is created, and a call to another function is not a
/// catalog dependency.
///
/// A target that already matches needs no settling. One the shadow cannot
/// rebuild, such as a grant to a role the roles file does not create,
/// compares as loaded.
async fn settle_target(
    config: &Config,
    root_dir: &Path,
    shadow: &crate::config::ShadowDatabase,
    target: Catalog,
    desired: &DesiredState,
) -> Result<Catalog> {
    if crate::diff::plan(&target, &desired.catalog)?.is_empty() {
        return Ok(target);
    }

    let written = match &desired.augmentation {
        Some(augmentation) => {
            let present = target.object_ids();
            let edges = FileDependencyAugmentation {
                additional_dependencies: augmentation
                    .additional_dependencies
                    .iter()
                    .filter(|(id, _)| Catalog::id_present_in(&present, id))
                    .map(|(id, deps)| {
                        let deps = deps
                            .iter()
                            .filter(|dep| Catalog::id_present_in(&present, dep))
                            .cloned()
                            .collect();
                        (id.clone(), deps)
                    })
                    .collect(),
            };
            target.clone().with_file_dependencies_augmented(edges)
        }
        None => target.clone(),
    };

    info!("🔁 Replaying the target on a fresh shadow database to compare it...");
    let filter = ObjectFilter::from_config(config);
    let written = &written;
    let settled = shadow
        .with_fresh(|pool| async move {
            clean_shadow_for_schema(config, root_dir, &pool).await?;
            settle(&pool, written, async |pool| {
                Catalog::load_managed(pool, &filter).await
            })
            .await
        })
        .await;

    match settled {
        Ok(settled) => Ok(settled.catalog),
        Err(e) => {
            warn!(
                "Could not rebuild the target on a shadow database to compare it, so \
                differences only in how Postgres renders a definition may be reported \
                as changes: {}",
                e.root_cause()
            );
            debug!("Settling the target failed: {e:#}");
            Ok(target)
        }
    }
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
