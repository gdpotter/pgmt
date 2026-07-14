use anyhow::{Context, Result};
use sqlx::PgPool;
use std::path::Path;
use tracing::info;

use crate::catalog::Catalog;
use crate::catalog::identity::{CatalogIdentity, find_new_objects};
use crate::commands::migrate::section_executor::{ExecutionMode, SectionExecutor};
use crate::config::Config;
use crate::config::filter::ObjectFilter;
use crate::config::types::TrackingTable;
use crate::db::cleaner;
use crate::migration::section_parser::MigrationSection;
use crate::migration::{
    ParsedMigration, discover_migrations, find_baseline_for_version, find_latest_baseline,
    parse_migration_sections, validate_sections,
};
use crate::migration_tracking::{ensure_section_tracking_table, initialize_sections};
use crate::modules::HistoricalAttribution;
use crate::progress::SectionReporter;
use crate::validation::validate_baseline_consistency;

/// Configuration for baseline operations
#[derive(Debug, Clone)]
pub struct BaselineConfig {
    pub validate_consistency: bool,
    pub verbose: bool,
}

impl Default for BaselineConfig {
    fn default() -> Self {
        Self {
            validate_consistency: true,
            verbose: true,
        }
    }
}

/// Shadow catalogs feed baseline rendering and validation: scope them to the
/// managed universe. Shadow branches legitimately contain image-provided
/// substrate (excluded schemas, their extensions), which must never appear in
/// baselines or count as a validation difference.
async fn load_managed_catalog(shadow_pool: &PgPool, config: &Config) -> Result<Catalog> {
    let filter = ObjectFilter::from_config(config);
    Catalog::load_managed(shadow_pool, &filter).await
}

/// Parse baseline SQL into its sections.
///
/// Baselines use the same `-- pgmt:section` header syntax as migrations; a
/// header-less baseline parses as one "default" transactional section, which
/// preserves the historical all-or-nothing apply for existing baselines.
fn parse_baseline_sections(baseline_sql: &str, source: &str) -> Result<Vec<MigrationSection>> {
    let sections = parse_migration_sections(Path::new(source), baseline_sql)
        .with_context(|| format!("Failed to parse baseline sections ({})", source))?;
    validate_sections(&sections)
        .with_context(|| format!("Invalid section configuration in baseline ({})", source))?;
    Ok(sections)
}

/// Execute sections against the shadow in validation mode, optionally
/// collecting per-section historical attribution: snapshot object identities
/// before and after each section (one cheap UNION-ALL query each) and tag
/// whatever appeared with the section's `module`. Only pays the snapshot cost
/// when a collector is passed — i.e. when the project declares modules.
async fn execute_validation_sections(
    shadow_pool: &PgPool,
    sections: &[MigrationSection],
    config: &Config,
    mut attribution: Option<&mut HistoricalAttribution>,
) -> Result<()> {
    let reporter = SectionReporter::new(sections.len(), false);
    let mut executor = SectionExecutor::new(
        shadow_pool.clone(),
        config.migration.tracking_table.clone(),
        reporter,
        ExecutionMode::Validation,
        false,
    );

    let mut previous = match attribution {
        Some(_) => Some(CatalogIdentity::load(shadow_pool).await?),
        None => None,
    };

    for section in sections {
        executor
            .execute_section(0, section)
            .await
            .with_context(|| format!("Failed to apply section '{}'", section.name))?;

        if let Some(attr) = attribution.as_deref_mut() {
            let current = CatalogIdentity::load(shadow_pool).await?;
            let created = find_new_objects(
                previous.as_ref().expect("snapshot taken when collecting"),
                &current,
            );
            attr.record(created, section.module.as_deref());
            previous = Some(current);
        }
    }

    Ok(())
}

/// Load a baseline SQL file into a shadow database and return the resulting catalog
pub async fn load_baseline_into_shadow(
    shadow_pool: &PgPool,
    baseline_path: &Path,
    roles_file: &Path,
    config: &Config,
) -> Result<Catalog> {
    load_baseline_into_shadow_inner(shadow_pool, baseline_path, roles_file, config, None).await
}

/// Clean the shadow and apply the roles file: the pristine pre-history state
/// every replay starts from. (For docker/branch shadows the clean is a no-op
/// and this state is the image substrate.)
async fn prepare_shadow_for_replay(
    shadow_pool: &PgPool,
    roles_file: &Path,
    config: &Config,
) -> Result<()> {
    cleaner::clean_shadow_db(shadow_pool, &config.objects).await?;
    // Apply roles before any history (handles non-existent files gracefully)
    crate::schema_ops::apply_roles_file(shadow_pool, roles_file).await?;
    Ok(())
}

/// Parse and execute a baseline file's sections against an already-prepared
/// shadow (validation mode), optionally collecting per-section attribution.
async fn apply_baseline_file_sections(
    shadow_pool: &PgPool,
    baseline_path: &Path,
    config: &Config,
    attribution: Option<&mut HistoricalAttribution>,
) -> Result<()> {
    let baseline_sql = std::fs::read_to_string(baseline_path)
        .with_context(|| format!("Failed to read baseline file: {}", baseline_path.display()))?;
    let source = baseline_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("unknown_baseline.sql");

    let sections = parse_baseline_sections(&baseline_sql, source)?;
    execute_validation_sections(shadow_pool, &sections, config, attribution)
        .await
        .with_context(|| format!("Failed to apply baseline SQL: {}", baseline_path.display()))
}

/// Section-by-section baseline replay (validation mode), optionally
/// collecting object→module attribution from the baseline's section tags.
async fn load_baseline_into_shadow_inner(
    shadow_pool: &PgPool,
    baseline_path: &Path,
    roles_file: &Path,
    config: &Config,
    attribution: Option<&mut HistoricalAttribution>,
) -> Result<Catalog> {
    prepare_shadow_for_replay(shadow_pool, roles_file, config).await?;
    apply_baseline_file_sections(shadow_pool, baseline_path, config, attribution).await?;
    load_managed_catalog(shadow_pool, config).await
}

/// Checkpoint the migration log: replay the full history (latest baseline +
/// subsequent migrations) onto a pristine shadow and return the shadow's
/// pre-history **base** (image substrate — baseline generation diffs against
/// it), the **unfiltered** replayed catalog, and per-section attribution
/// (populated only when the project declares modules).
///
/// This is `migrate baseline`'s source: a baseline asserts "replaying history
/// through V produces exactly this", so it is generated FROM that replay —
/// never from the schema files, which may have drifted ahead. Drift belongs
/// in the next `migrate new`, not smuggled into history.
pub async fn replay_history_for_checkpoint(
    shadow_pool: &PgPool,
    baselines_dir: &Path,
    migrations_dir: &Path,
    roles_file: &Path,
    config: &Config,
) -> Result<(Catalog, Catalog, HistoricalAttribution)> {
    let all_migrations = discover_migrations(migrations_dir)?;
    let mut attribution = HistoricalAttribution::default();
    let mut collector: Option<&mut HistoricalAttribution> =
        config.modules.is_enabled().then_some(&mut attribution);

    prepare_shadow_for_replay(shadow_pool, roles_file, config).await?;
    let base = Catalog::load_unfiltered(shadow_pool).await?;

    let migrations_to_replay = if let Some(baseline) = find_latest_baseline(baselines_dir)? {
        apply_baseline_file_sections(
            shadow_pool,
            &baseline.path,
            config,
            collector.as_deref_mut(),
        )
        .await?;
        warn_pre_baseline_migrations(&all_migrations, baseline.version);
        all_migrations
            .into_iter()
            .filter(|m| m.version > baseline.version)
            .collect()
    } else {
        all_migrations
    };

    replay_migrations(shadow_pool, &migrations_to_replay, config, false, collector).await?;

    let catalog = Catalog::load_unfiltered(shadow_pool).await?;
    Ok((base, catalog, attribution))
}

/// Apply baseline SQL to a real target database (used by `migrate provision`).
///
/// Unlike [`load_baseline_into_shadow`], this does NOT clean the database or
/// apply the roles file: the target is a real, long-lived database whose roles
/// are managed externally — the same contract as `migrate apply`'s grants.
///
/// The baseline executes section-by-section through the same machinery as
/// migrations, with section rows recorded under `is_baseline = TRUE`. A
/// header-less baseline is a single transactional "default" section — the
/// whole file applies atomically, exactly as before sections existed. A
/// multi-section baseline gets per-section atomicity with resume: a re-run
/// skips sections already recorded Completed.
pub async fn apply_baseline_to_target(
    pool: &PgPool,
    tracking_table: &TrackingTable,
    version: u64,
    baseline_sql: &str,
    source: &str,
    select_section: impl Fn(&MigrationSection) -> bool,
) -> Result<()> {
    // Pair each selected section with its index in the FULL baseline file
    // (enumerate before filtering) so section_order stays stable per version —
    // a module subset provision registers only a subset in each call.
    let sections: Vec<(i32, MigrationSection)> = parse_baseline_sections(baseline_sql, source)?
        .into_iter()
        .enumerate()
        .map(|(i, s)| (i as i32, s))
        .filter(|(_, s)| select_section(s))
        .collect();

    ensure_section_tracking_table(pool, tracking_table).await?;
    initialize_sections(pool, tracking_table, version, true, &sections).await?;

    let reporter = SectionReporter::new(sections.len(), false);
    let mut executor = SectionExecutor::new(
        pool.clone(),
        tracking_table.clone(),
        reporter,
        ExecutionMode::Production,
        true,
    );
    for (_, section) in &sections {
        executor
            .execute_section(version, section)
            .await
            .with_context(|| {
                format!(
                    "Baseline {} failed at section '{}' ({})",
                    version, section.name, source
                )
            })?;
    }

    Ok(())
}

/// Replay migration files onto a shadow database, in order.
///
/// This is THE replay path — every reconstruction (migrate new/update starting
/// state, migrate validate expected state) goes through here. It is
/// section-aware: a migration is parsed into its sections and executed with
/// each section's transaction mode, so non-transactional sections (e.g.
/// CREATE INDEX CONCURRENTLY) replay the same way they run in production.
async fn replay_migrations(
    shadow_pool: &PgPool,
    migrations: &[ParsedMigration],
    config: &Config,
    verbose: bool,
    mut attribution: Option<&mut HistoricalAttribution>,
) -> Result<()> {
    for migration in migrations {
        if verbose {
            println!(
                "  Applying {} - {}",
                migration.version, migration.description
            );
        }

        let migration_sql = std::fs::read_to_string(&migration.path).with_context(|| {
            format!(
                "Failed to read migration file: {}",
                migration.path.display()
            )
        })?;

        let sections = parse_migration_sections(&migration.path, &migration_sql)?;
        validate_sections(&sections)?;

        // Full-replay pin (modules.md §12): shadow replay builds history with
        // every module present, so a migration REMAP section's source always
        // exists during replay — the acquired objects are already there
        // (created by the source's history or the covering baseline), and
        // executing the section would double-create. The uniform execution
        // rule therefore satisfied-skips migration remap sections in replay,
        // always. Baseline remap sections are different (a fresh shadow holds
        // nothing) and replay through `apply_baseline_file_sections`.
        let sections: Vec<MigrationSection> = sections
            .into_iter()
            .filter(|s| s.remaps.is_empty())
            .collect();

        execute_validation_sections(shadow_pool, &sections, config, attribution.as_deref_mut())
            .await
            .with_context(|| {
                format!(
                    "Failed to apply migration {}: {}",
                    migration.version,
                    migration.path.display()
                )
            })?;
    }

    Ok(())
}

/// Warn about migrations that predate the baseline and will never replay.
/// Strictly-less: a migration at the baseline's own version is its PAIRED
/// migration (`migrate new --create-baseline`) — covered by design, not stale.
fn warn_pre_baseline_migrations(migrations: &[ParsedMigration], baseline_version: u64) {
    for m in migrations.iter().filter(|m| m.version < baseline_version) {
        eprintln!(
            "Warning: Migration {} predates baseline {} and will be skipped. \
             Run 'pgmt migrate update {}' to renumber it.",
            m.version, baseline_version, m.version
        );
    }
}

/// Validate that a baseline file reproduces the expected catalog
pub async fn validate_baseline_against_catalog(
    shadow_pool: &PgPool,
    baseline_path: &Path,
    expected_catalog: &Catalog,
    baseline_config: &BaselineConfig,
    roles_file: &Path,
    config: &Config,
) -> Result<()> {
    if !baseline_config.validate_consistency {
        return Ok(());
    }

    if baseline_config.verbose {
        println!("Validating baseline matches intended schema...");
    }

    let baseline_catalog =
        load_baseline_into_shadow(shadow_pool, baseline_path, roles_file, config).await?;
    validate_baseline_consistency(&baseline_catalog, expected_catalog, config)?;

    if baseline_config.verbose {
        println!("✓ Baseline validation passed");
    }

    Ok(())
}

/// Get the starting catalog state for migration generation: the latest
/// baseline (or empty schema) plus every migration after it.
///
/// This is also the "expected state" for `migrate validate` — both commands
/// reconstruct history through the same baseline + replay path.
pub async fn get_migration_starting_state(
    shadow_pool: &PgPool,
    baselines_dir: &Path,
    migrations_dir: &Path,
    roles_file: &Path,
    baseline_config: &BaselineConfig,
    config: &Config,
) -> Result<Catalog> {
    get_migration_starting_state_inner(
        shadow_pool,
        baselines_dir,
        migrations_dir,
        roles_file,
        baseline_config,
        config,
        None,
    )
    .await
}

/// Like [`get_migration_starting_state`], but also collects object→module
/// attribution from the replayed history's section tags (per-section identity
/// snapshots). Used by module-aware generation to attribute DROP steps —
/// their objects have no current file, so ownership can only come from the
/// checksummed history that created them.
pub async fn get_migration_starting_state_with_attribution(
    shadow_pool: &PgPool,
    baselines_dir: &Path,
    migrations_dir: &Path,
    roles_file: &Path,
    baseline_config: &BaselineConfig,
    config: &Config,
) -> Result<(Catalog, HistoricalAttribution)> {
    let mut attribution = HistoricalAttribution::default();
    let catalog = get_migration_starting_state_inner(
        shadow_pool,
        baselines_dir,
        migrations_dir,
        roles_file,
        baseline_config,
        config,
        Some(&mut attribution),
    )
    .await?;
    Ok((catalog, attribution))
}

#[allow(clippy::too_many_arguments)]
async fn get_migration_starting_state_inner(
    shadow_pool: &PgPool,
    baselines_dir: &Path,
    migrations_dir: &Path,
    roles_file: &Path,
    baseline_config: &BaselineConfig,
    config: &Config,
    mut attribution: Option<&mut HistoricalAttribution>,
) -> Result<Catalog> {
    let all_migrations = discover_migrations(migrations_dir)?;

    let migrations_to_replay = if let Some(baseline) = find_latest_baseline(baselines_dir)? {
        if baseline_config.verbose {
            info!("Loading baseline: {}", baseline.path.display());
        }
        load_baseline_into_shadow_inner(
            shadow_pool,
            &baseline.path,
            roles_file,
            config,
            attribution.as_deref_mut(),
        )
        .await?;
        warn_pre_baseline_migrations(&all_migrations, baseline.version);

        let after_baseline: Vec<_> = all_migrations
            .into_iter()
            .filter(|m| m.version > baseline.version)
            .collect();
        if baseline_config.verbose && !after_baseline.is_empty() {
            println!(
                "Applying {} migration(s) after baseline",
                after_baseline.len()
            );
        }
        after_baseline
    } else {
        if baseline_config.verbose {
            info!("No existing baseline found, reconstructing from existing migrations");
        }
        prepare_shadow_for_replay(shadow_pool, roles_file, config).await?;

        if all_migrations.is_empty() {
            println!("No existing migrations found, starting from empty schema");
        } else {
            println!(
                "Applying {} existing migration(s) to reconstruct state",
                all_migrations.len()
            );
        }
        all_migrations
    };

    replay_migrations(
        shadow_pool,
        &migrations_to_replay,
        config,
        baseline_config.verbose,
        attribution,
    )
    .await?;
    load_managed_catalog(shadow_pool, config).await
}

/// Get the starting catalog state for updating a specific migration version:
/// the baseline before the target version (or empty schema) plus the
/// migrations between them.
#[allow(clippy::too_many_arguments)]
pub async fn get_migration_update_starting_state(
    shadow_pool: &PgPool,
    baselines_dir: &Path,
    migrations_dir: &Path,
    target_version: u64,
    roles_file: &Path,
    baseline_config: &BaselineConfig,
    config: &Config,
    mut attribution: Option<&mut HistoricalAttribution>,
) -> Result<Catalog> {
    let all_migrations = discover_migrations(migrations_dir)?;

    let migrations_to_replay =
        if let Some(baseline) = find_baseline_for_version(baselines_dir, target_version)? {
            if baseline_config.verbose {
                info!("Loading previous baseline: {}", baseline.path.display());
            }
            load_baseline_into_shadow_inner(
                shadow_pool,
                &baseline.path,
                roles_file,
                config,
                attribution.as_deref_mut(),
            )
            .await?;

            let in_range: Vec<_> = all_migrations
                .into_iter()
                .filter(|m| m.version > baseline.version && m.version < target_version)
                .collect();
            if baseline_config.verbose && !in_range.is_empty() {
                println!(
                    "Applying {} migration(s) between baseline and target",
                    in_range.len()
                );
            }
            in_range
        } else {
            if baseline_config.verbose {
                info!(
                    "No previous baseline found, reconstructing from migrations before {}",
                    target_version
                );
            }
            prepare_shadow_for_replay(shadow_pool, roles_file, config).await?;

            let before_target: Vec<_> = all_migrations
                .into_iter()
                .filter(|m| m.version < target_version)
                .collect();
            if before_target.is_empty() {
                println!(
                    "No existing migrations found before {}, starting from empty schema",
                    target_version
                );
            } else {
                println!(
                    "Applying {} existing migration(s) before {}",
                    before_target.len(),
                    target_version
                );
            }
            before_target
        };

    replay_migrations(
        shadow_pool,
        &migrations_to_replay,
        config,
        baseline_config.verbose,
        attribution,
    )
    .await?;
    load_managed_catalog(shadow_pool, config).await
}

/// Helper to determine if a baseline should be created or updated for a migration
pub fn should_manage_baseline_for_migration(
    _config: &Config,
    baseline_path: &Path,
    create_baselines_by_default: bool,
) -> bool {
    create_baselines_by_default || baseline_path.exists()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_baseline_config_default() {
        let config = BaselineConfig::default();
        assert!(config.validate_consistency);
        assert!(config.verbose);
    }

    #[test]
    fn test_should_manage_baseline_for_migration() {
        let temp_dir = env::temp_dir().join("pgmt_test_baseline_management");
        let baseline_path = temp_dir.join("baseline_V123.sql");

        assert!(should_manage_baseline_for_migration(
            &Config::default(),
            &baseline_path,
            true
        ));

        assert!(!should_manage_baseline_for_migration(
            &Config::default(),
            &baseline_path,
            false
        ));

        std::fs::create_dir_all(&temp_dir).unwrap();
        std::fs::write(&baseline_path, "test").unwrap();
        assert!(should_manage_baseline_for_migration(
            &Config::default(),
            &baseline_path,
            false
        ));

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
