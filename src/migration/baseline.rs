use anyhow::{Context, Result};
use sqlx::PgPool;
use std::path::{Path, PathBuf};
use tracing::info;

use crate::catalog::Catalog;
use crate::config::Config;
use crate::config::types::Objects;
use crate::db::cleaner;
use crate::db::schema_executor::BaselineExecutor;
use crate::migration::{
    discover_migrations, find_baseline_for_version, find_latest_baseline,
    generate_baseline_filename,
};
use crate::validation::validate_baseline_consistency_with_suggestions;

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

/// Result of baseline operations
#[derive(Debug)]
pub struct BaselineOperationResult {
    pub path: PathBuf,
}

/// Load a baseline SQL file into a shadow database and return the resulting catalog
pub async fn load_baseline_into_shadow(
    shadow_pool: &PgPool,
    baseline_path: &Path,
    roles_file: &Path,
    objects: &Objects,
) -> Result<Catalog> {
    cleaner::clean_shadow_db(shadow_pool, objects).await?;

    // Apply roles before baseline (handles non-existent files gracefully)
    crate::schema_ops::apply_roles_file(shadow_pool, roles_file).await?;

    let baseline_sql = std::fs::read_to_string(baseline_path)
        .with_context(|| format!("Failed to read baseline file: {}", baseline_path.display()))?;

    let executor = BaselineExecutor::new(shadow_pool.clone(), false, false);
    let source = baseline_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("unknown_baseline.sql");

    executor
        .execute_baseline(&baseline_sql, source)
        .await
        .with_context(|| format!("Failed to apply baseline SQL: {}", baseline_path.display()))?;

    Catalog::load(shadow_pool).await
}

/// Update or create a baseline file for a migration
pub async fn ensure_baseline_for_migration(
    baselines_dir: &Path,
    version: u64,
    baseline_sql: &str,
    config: &BaselineConfig,
) -> Result<BaselineOperationResult> {
    let baseline_filename = generate_baseline_filename(version);
    let baseline_path = baselines_dir.join(&baseline_filename);

    std::fs::create_dir_all(baselines_dir).with_context(|| {
        format!(
            "Failed to create baselines directory: {}",
            baselines_dir.display()
        )
    })?;

    if config.verbose {
        println!("💾 Writing baseline: {}", baseline_path.display());
    }
    std::fs::write(&baseline_path, baseline_sql)
        .with_context(|| format!("Failed to write baseline file: {}", baseline_path.display()))?;

    Ok(BaselineOperationResult {
        path: baseline_path,
    })
}

/// Enhanced validation with optional file dependency suggestions
pub async fn validate_baseline_against_catalog_with_suggestions(
    shadow_pool: &PgPool,
    baseline_path: &Path,
    expected_catalog: &Catalog,
    config: &BaselineConfig,
    suggest_file_dependencies: bool,
    roles_file: &Path,
    objects: &Objects,
) -> Result<()> {
    if !config.validate_consistency {
        return Ok(());
    }

    if config.verbose {
        println!("Validating baseline matches intended schema...");
    }

    let baseline_catalog =
        load_baseline_into_shadow(shadow_pool, baseline_path, roles_file, objects).await?;
    validate_baseline_consistency_with_suggestions(
        &baseline_catalog,
        expected_catalog,
        suggest_file_dependencies,
    )?;

    if config.verbose {
        println!("✓ Baseline validation passed");
    }

    Ok(())
}

/// Get the starting catalog state for migration generation
/// Returns either the latest baseline or reconstructs from migration chain,
/// then applies any migrations that come after the baseline
pub async fn get_migration_starting_state(
    shadow_pool: &PgPool,
    baselines_dir: &Path,
    migrations_dir: &Path,
    roles_file: &Path,
    config: &BaselineConfig,
    objects: &Objects,
) -> Result<Catalog> {
    let latest_baseline = find_latest_baseline(baselines_dir)?;

    if let Some(baseline) = latest_baseline {
        if config.verbose {
            info!("Loading baseline: {}", baseline.path.display());
        }
        load_baseline_into_shadow(shadow_pool, &baseline.path, roles_file, objects).await?;

        // Apply any migrations that come after the baseline
        apply_migrations_after_version(shadow_pool, migrations_dir, baseline.version, config).await
    } else {
        if config.verbose {
            info!("No existing baseline found, reconstructing from existing migrations");
        }
        reconstruct_from_migration_chain(shadow_pool, migrations_dir, roles_file, objects).await
    }
}

/// Get the starting catalog state for updating a specific migration version
/// Loads the baseline before the target version, then applies any migrations
/// between the baseline and the target version
pub async fn get_migration_update_starting_state(
    shadow_pool: &PgPool,
    baselines_dir: &Path,
    migrations_dir: &Path,
    target_version: u64,
    roles_file: &Path,
    config: &BaselineConfig,
    objects: &Objects,
) -> Result<Catalog> {
    let previous_baseline = find_baseline_for_version(baselines_dir, target_version)?;

    if let Some(baseline) = previous_baseline {
        if config.verbose {
            info!("Loading previous baseline: {}", baseline.path.display());
        }
        load_baseline_into_shadow(shadow_pool, &baseline.path, roles_file, objects).await?;

        // Apply migrations between baseline and target version
        apply_migrations_in_range(
            shadow_pool,
            migrations_dir,
            baseline.version,
            target_version,
            config,
        )
        .await
    } else {
        if config.verbose {
            info!(
                "No previous baseline found, reconstructing from migrations before V{}",
                target_version
            );
        }
        reconstruct_from_migration_chain_before_version(
            shadow_pool,
            migrations_dir,
            target_version,
            roles_file,
            objects,
        )
        .await
    }
}

/// Apply migrations that come after a specific version
/// Used after loading a baseline to apply subsequent migrations
async fn apply_migrations_after_version(
    shadow_pool: &PgPool,
    migrations_dir: &Path,
    after_version: u64,
    config: &BaselineConfig,
) -> Result<Catalog> {
    let all_migrations = discover_migrations(migrations_dir)?;
    let migrations_to_apply: Vec<_> = all_migrations
        .into_iter()
        .filter(|m| m.version > after_version)
        .collect();

    if !migrations_to_apply.is_empty() {
        if config.verbose {
            println!(
                "Applying {} migration(s) after baseline",
                migrations_to_apply.len()
            );
        }

        for migration in migrations_to_apply {
            if config.verbose {
                println!(
                    "  Applying V{} - {}",
                    migration.version, migration.description
                );
            }

            let migration_sql = std::fs::read_to_string(&migration.path).with_context(|| {
                format!(
                    "Failed to read migration file: {}",
                    migration.path.display()
                )
            })?;

            let executor = BaselineExecutor::new(shadow_pool.clone(), false, false);
            let source = format!("V{} - {}", migration.version, migration.description);
            executor
                .execute_baseline(&migration_sql, &source)
                .await
                .with_context(|| {
                    format!(
                        "Failed to apply migration V{}: {}",
                        migration.version,
                        migration.path.display()
                    )
                })?;
        }
    }

    Catalog::load(shadow_pool).await
}

/// Apply migrations in a version range (after_version, before_version)
/// Used after loading a baseline when updating a specific migration
async fn apply_migrations_in_range(
    shadow_pool: &PgPool,
    migrations_dir: &Path,
    after_version: u64,
    before_version: u64,
    config: &BaselineConfig,
) -> Result<Catalog> {
    let all_migrations = discover_migrations(migrations_dir)?;
    let migrations_to_apply: Vec<_> = all_migrations
        .into_iter()
        .filter(|m| m.version > after_version && m.version < before_version)
        .collect();

    if !migrations_to_apply.is_empty() {
        if config.verbose {
            println!(
                "Applying {} migration(s) between baseline and target",
                migrations_to_apply.len()
            );
        }

        for migration in migrations_to_apply {
            if config.verbose {
                println!(
                    "  Applying V{} - {}",
                    migration.version, migration.description
                );
            }

            let migration_sql = std::fs::read_to_string(&migration.path).with_context(|| {
                format!(
                    "Failed to read migration file: {}",
                    migration.path.display()
                )
            })?;

            let executor = BaselineExecutor::new(shadow_pool.clone(), false, false);
            let source = format!("V{} - {}", migration.version, migration.description);
            executor
                .execute_baseline(&migration_sql, &source)
                .await
                .with_context(|| {
                    format!(
                        "Failed to apply migration V{}: {}",
                        migration.version,
                        migration.path.display()
                    )
                })?;
        }
    }

    Catalog::load(shadow_pool).await
}

/// Reconstruct catalog by applying all migrations in chronological order
async fn reconstruct_from_migration_chain(
    shadow_pool: &PgPool,
    migrations_dir: &Path,
    roles_file: &Path,
    objects: &Objects,
) -> Result<Catalog> {
    cleaner::clean_shadow_db(shadow_pool, objects).await?;

    // Apply roles before migrations (handles non-existent files gracefully)
    crate::schema_ops::apply_roles_file(shadow_pool, roles_file).await?;

    let migrations = discover_migrations(migrations_dir)?;

    if migrations.is_empty() {
        println!("No existing migrations found, starting from empty schema");
        return Catalog::load(shadow_pool).await;
    }

    println!(
        "Applying {} existing migration(s) to reconstruct state",
        migrations.len()
    );

    for migration in migrations {
        println!(
            "  Applying V{} - {}",
            migration.version, migration.description
        );

        let migration_sql = std::fs::read_to_string(&migration.path).with_context(|| {
            format!(
                "Failed to read migration file: {}",
                migration.path.display()
            )
        })?;

        let executor = BaselineExecutor::new(shadow_pool.clone(), false, false);
        let source = format!("V{} - {}", migration.version, migration.description);
        executor
            .execute_baseline(&migration_sql, &source)
            .await
            .with_context(|| {
                format!(
                    "Failed to apply migration V{}: {}",
                    migration.version,
                    migration.path.display()
                )
            })?;
    }

    Catalog::load(shadow_pool).await
}

/// Reconstruct catalog by applying migrations before a specific version
async fn reconstruct_from_migration_chain_before_version(
    shadow_pool: &PgPool,
    migrations_dir: &Path,
    target_version: u64,
    roles_file: &Path,
    objects: &Objects,
) -> Result<Catalog> {
    use crate::migration::find_migrations_before_version;

    cleaner::clean_shadow_db(shadow_pool, objects).await?;

    // Apply roles before migrations (handles non-existent files gracefully)
    crate::schema_ops::apply_roles_file(shadow_pool, roles_file).await?;

    let migrations = find_migrations_before_version(migrations_dir, target_version)?;

    if migrations.is_empty() {
        println!(
            "No existing migrations found before V{}, starting from empty schema",
            target_version
        );
        return Catalog::load(shadow_pool).await;
    }

    println!(
        "Applying {} existing migration(s) before V{}",
        migrations.len(),
        target_version
    );

    for migration in migrations {
        println!(
            "  Applying V{} - {}",
            migration.version, migration.description
        );

        let migration_sql = std::fs::read_to_string(&migration.path).with_context(|| {
            format!(
                "Failed to read migration file: {}",
                migration.path.display()
            )
        })?;

        let executor = BaselineExecutor::new(shadow_pool.clone(), false, false);
        let source = format!("V{} - {}", migration.version, migration.description);
        executor
            .execute_baseline(&migration_sql, &source)
            .await
            .with_context(|| {
                format!(
                    "Failed to apply migration V{}: {}",
                    migration.version,
                    migration.path.display()
                )
            })?;
    }

    Catalog::load(shadow_pool).await
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
