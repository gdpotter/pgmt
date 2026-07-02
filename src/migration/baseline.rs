use anyhow::{Context, Result};
use sqlx::PgPool;
use std::path::Path;
use tracing::info;

use crate::catalog::Catalog;
use crate::commands::migrate::section_executor::{ExecutionMode, SectionExecutor};
use crate::config::Config;
use crate::config::filter::ObjectFilter;
use crate::config::types::TrackingTable;
use crate::db::cleaner;
use crate::migration::{
    ParsedMigration, discover_migrations, find_baseline_for_version, find_latest_baseline,
    parse_migration_sections, validate_sections,
};
use crate::migration_tracking::{ensure_section_tracking_table, initialize_sections};
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
fn parse_baseline_sections(
    baseline_sql: &str,
    source: &str,
) -> Result<Vec<crate::migration::section_parser::MigrationSection>> {
    let sections = parse_migration_sections(Path::new(source), baseline_sql)
        .with_context(|| format!("Failed to parse baseline sections ({})", source))?;
    validate_sections(&sections)
        .with_context(|| format!("Invalid section configuration in baseline ({})", source))?;
    Ok(sections)
}

/// Load a baseline SQL file into a shadow database and return the resulting catalog
pub async fn load_baseline_into_shadow(
    shadow_pool: &PgPool,
    baseline_path: &Path,
    roles_file: &Path,
    config: &Config,
) -> Result<Catalog> {
    cleaner::clean_shadow_db(shadow_pool, &config.objects).await?;

    // Apply roles before baseline (handles non-existent files gracefully)
    crate::schema_ops::apply_roles_file(shadow_pool, roles_file).await?;

    let baseline_sql = std::fs::read_to_string(baseline_path)
        .with_context(|| format!("Failed to read baseline file: {}", baseline_path.display()))?;
    let source = baseline_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("unknown_baseline.sql");

    // Replay section-by-section so each section runs with its declared
    // transaction mode (validation mode: no tracking, no retries). The version
    // is only used for tracking, which validation mode never touches.
    let sections = parse_baseline_sections(&baseline_sql, source)?;
    let reporter = SectionReporter::new(sections.len(), false);
    let mut executor = SectionExecutor::new(
        shadow_pool.clone(),
        config.migration.tracking_table.clone(),
        reporter,
        ExecutionMode::Validation,
        true,
    );
    for section in &sections {
        executor
            .execute_section(0, section)
            .await
            .with_context(|| {
                format!("Failed to apply baseline SQL: {}", baseline_path.display())
            })?;
    }

    load_managed_catalog(shadow_pool, config).await
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
) -> Result<()> {
    let sections = parse_baseline_sections(baseline_sql, source)?;

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
    for section in &sections {
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

        let reporter = SectionReporter::new(sections.len(), false);
        let mut executor = SectionExecutor::new(
            shadow_pool.clone(),
            config.migration.tracking_table.clone(),
            reporter,
            ExecutionMode::Validation,
            false,
        );

        for section in &sections {
            executor
                .execute_section(migration.version, section)
                .await
                .with_context(|| {
                    format!(
                        "Failed to apply migration {}: {}",
                        migration.version,
                        migration.path.display()
                    )
                })?;
        }
    }

    Ok(())
}

/// Warn about migrations that predate the baseline and will never replay
fn warn_pre_baseline_migrations(migrations: &[ParsedMigration], baseline_version: u64) {
    for m in migrations.iter().filter(|m| m.version <= baseline_version) {
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
    let all_migrations = discover_migrations(migrations_dir)?;

    let migrations_to_replay = if let Some(baseline) = find_latest_baseline(baselines_dir)? {
        if baseline_config.verbose {
            info!("Loading baseline: {}", baseline.path.display());
        }
        load_baseline_into_shadow(shadow_pool, &baseline.path, roles_file, config).await?;
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
        cleaner::clean_shadow_db(shadow_pool, &config.objects).await?;
        crate::schema_ops::apply_roles_file(shadow_pool, roles_file).await?;

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
    )
    .await?;
    load_managed_catalog(shadow_pool, config).await
}

/// Get the starting catalog state for updating a specific migration version:
/// the baseline before the target version (or empty schema) plus the
/// migrations between them.
pub async fn get_migration_update_starting_state(
    shadow_pool: &PgPool,
    baselines_dir: &Path,
    migrations_dir: &Path,
    target_version: u64,
    roles_file: &Path,
    baseline_config: &BaselineConfig,
    config: &Config,
) -> Result<Catalog> {
    let all_migrations = discover_migrations(migrations_dir)?;

    let migrations_to_replay =
        if let Some(baseline) = find_baseline_for_version(baselines_dir, target_version)? {
            if baseline_config.verbose {
                info!("Loading previous baseline: {}", baseline.path.display());
            }
            load_baseline_into_shadow(shadow_pool, &baseline.path, roles_file, config).await?;

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
            cleaner::clean_shadow_db(shadow_pool, &config.objects).await?;
            crate::schema_ops::apply_roles_file(shadow_pool, roles_file).await?;

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
