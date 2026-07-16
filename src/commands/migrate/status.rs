use crate::config::Config;
use crate::migration::{
    BaselineConfig, discover_migrations, find_latest_baseline, get_migration_starting_state,
};
use crate::migration_tracking::TrackingStore;
use crate::modules::{UNMODULED_DISPLAY, display_module};
use crate::validation::{ValidationConfig, validate_catalogs};
use crate::validation_output::{BaselineInfo, ValidationOutputOptions, format_validation_output};
use anyhow::{Result, anyhow};
use std::collections::BTreeMap;
use std::path::Path;

use crate::db::connection::connect_to_database;

/// Report migration status for a target or dev database.
///
/// Status is the triage tool for production incidents, so it is strictly
/// READ-ONLY: it takes no advisory lock (a stuck deploy must stay diagnosable)
/// and never creates or evolves the tracking tables against the reported
/// database — it probes for their existence with `to_regclass` and degrades
/// gracefully when they are absent. `database_label` ("target"/"dev") and
/// `database_url` are resolved by the caller through the connection args
/// structs (`--target-url` flag > `PGMT_TARGET_URL` > yaml target > dev
/// fallback), so this function just reports on whatever it is handed.
pub async fn cmd_migrate_status(
    config: &Config,
    _root_dir: &Path,
    database_label: &str,
    database_url: &str,
) -> Result<()> {
    println!("Migration status for {} database", database_label);

    let pool = connect_to_database(database_url, &format!("{} database", database_label)).await?;

    let result = report_status(config, &pool).await;
    pool.close().await;
    result
}

async fn report_status(config: &Config, pool: &sqlx::PgPool) -> Result<()> {
    let store = TrackingStore::new(pool, &config.migration.tracking_table)?;

    // Read-only probe: never CREATE or evolve the tracking tables on the
    // reported database. If pgmt has never run here, there is nothing to show.
    if !relation_exists(pool, store.main_table()).await? {
        println!("No migrations have been applied");
        return Ok(());
    }
    let sections_exist = relation_exists(pool, store.sections_table()).await?;

    if sections_exist {
        // The version row is written when a migration STARTS (see
        // register_migration_start), so surface rows whose recorded sections
        // aren't all complete — they're in-progress or failed, not applied.
        let applied_migrations = store.migration_listing().await?;
        print_migration_listing(&applied_migrations);
    } else {
        // Legacy target: a main tracking table with no per-section table. We do
        // not evolve it here (read-only), so treat every recorded version as
        // fully applied.
        let listing = store.migration_listing_legacy().await?;
        print_migration_listing(&listing);
    }

    // Per-module rollup for module projects, derived from the stored section
    // rows' module column. Needs the section table; a legacy target without it
    // can't be rolled up by module.
    if config.modules.is_enabled() && sections_exist {
        print_module_rollup(config, &store).await?;
    }

    Ok(())
}

/// Whether a relation exists, without touching it. `qualified` is a
/// double-quoted `"schema"."name"` reference.
async fn relation_exists(pool: &sqlx::PgPool, qualified: &str) -> Result<bool> {
    let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(qualified)
        .fetch_one(pool)
        .await?;
    Ok(exists)
}

fn print_migration_listing(applied_migrations: &[(i64, String, String, i64, bool)]) {
    if applied_migrations.is_empty() {
        println!("No migrations have been applied");
        return;
    }
    println!("Applied migrations:");
    for (version, description, applied_at, incomplete, is_baseline) in applied_migrations {
        if *incomplete > 0 {
            // Baselines can't be resumed by `migrate apply` (it never re-runs
            // baselines and skips versions <= a baseline's version); a
            // half-applied baseline resumes with `provision`.
            let resume_command = if *is_baseline {
                "pgmt migrate provision"
            } else {
                "pgmt migrate apply"
            };
            println!(
                "  {} - {} (INCOMPLETE: {} section(s) pending or failed — resume with `{}`)",
                version, description, incomplete, resume_command
            );
        } else {
            println!("  {} - {} (applied: {})", version, description, applied_at);
        }
    }
}

/// One module's tally of recorded section rows on the reported database.
#[derive(Default)]
struct ModuleTally {
    completed: usize,
    incomplete: usize,
    /// Any incomplete section belongs to a baseline row — resume needs
    /// baseline content (`provision`), not a migration replay (`apply`).
    incomplete_baseline: bool,
}

/// Print the "Modules" summary: one compact line per declared module plus the
/// base. Establishment comes from the STORED subscription (§13) when the
/// subscription tables exist; the *literal* established set — what covered
/// section rows imply via their stored `module` column — is the audit-side
/// cross-check, and the read-only fallback on a pre-subscription target
/// (status never evolves the tracking schema). Per-module counts and resume
/// hints are tallied from the same section rows' `module` column.
async fn print_module_rollup(config: &Config, store: &TrackingStore) -> Result<()> {
    let literal = store.established_module_literals().await?;
    let stored = if store.subscription_tables_exist().await? {
        Some(store.load_subscription().await?)
    } else {
        None
    };
    let established = match &stored {
        Some(sub) => sub.modules.clone(),
        None => {
            println!(
                "Note: pre-subscription target — module subscription tables not present \
                 (they arrive with the next apply/provision). Establishment below is \
                 derived from recorded section rows."
            );
            literal.clone()
        }
    };

    // Audit cross-check: section rows naming a module outside the stored
    // subscription. (The reverse — subscribed with no rows yet, e.g. right
    // after a re-tag crossing — is normal and not flagged.)
    if let Some(sub) = &stored {
        let divergent: Vec<&str> = literal
            .difference(&sub.modules)
            .map(String::as_str)
            .collect();
        if !divergent.is_empty() {
            println!(
                "Warning: recorded section rows name module(s) not in this target's \
                 subscription: {}. Rows are historical literals; expected after a re-tag \
                 that renamed/absorbed these modules, otherwise investigate.",
                divergent.join(", ")
            );
        }
    }

    // Tally every recorded section row by owning module (None = base), read
    // straight off the stored `module` column (§9: authoritative).
    let rows = store.section_module_statuses().await?;

    let mut tallies: BTreeMap<Option<String>, ModuleTally> = BTreeMap::new();
    for (module, is_baseline, status) in rows {
        let tally = tallies.entry(module).or_default();
        if status.is_covered() {
            tally.completed += 1;
        } else {
            tally.incomplete += 1;
            if is_baseline {
                tally.incomplete_baseline = true;
            }
        }
    }

    // Row order: the base first, then declared modules alphabetically.
    let mut keys: Vec<Option<String>> = vec![None];
    keys.extend(config.modules.modules.keys().cloned().map(Some));

    // Align the module-name column for compact, scannable output.
    let label_width = keys
        .iter()
        .map(|k| display_module(k.as_deref()).len())
        .max()
        .unwrap_or(UNMODULED_DISPLAY.len());

    println!("Modules:");
    for key in keys {
        let label = display_module(key.as_deref());
        let tally = tallies.get(&key);
        let is_established = match &key {
            None => tally.is_some_and(|t| t.completed > 0),
            Some(m) => established.contains(m),
        };

        let line = match tally {
            // No section rows at all. The base having no rows is only possible
            // on an otherwise-empty tracking table; a declared module with no
            // rows is simply not deployed here (expected on subset targets).
            None => match &key {
                None => "no sections recorded".to_string(),
                Some(_) => "not established (expected on subset targets)".to_string(),
            },
            Some(t) if t.incomplete == 0 => {
                format!("established — {} section(s) applied", t.completed)
            }
            Some(t) => {
                let base_cmd = if t.incomplete_baseline {
                    "pgmt migrate provision"
                } else {
                    "pgmt migrate apply"
                };
                let resume = match &key {
                    None => base_cmd.to_string(),
                    Some(m) => format!("{} --modules {}", base_cmd, m),
                };
                let state = if is_established {
                    "established"
                } else {
                    "incomplete"
                };
                format!(
                    "{} — {} applied, {} pending/failed (resume with `{}`)",
                    state, t.completed, t.incomplete, resume
                )
            }
        };
        println!("  {:width$}  {}", label, line, width = label_width);
    }

    Ok(())
}

pub async fn cmd_migrate_validate(
    config: &Config,
    root_dir: &Path,
    validation_options: &ValidationOutputOptions,
    shadow: &crate::config::ShadowDatabase,
) -> Result<()> {
    if !validation_options.quiet {
        eprintln!("🔍 Validating migration consistency...");
    }

    // Create necessary directories
    let migrations_dir = root_dir.join(&config.directories.migrations);
    let baselines_dir = root_dir.join(&config.directories.baselines);
    std::fs::create_dir_all(&migrations_dir)?;
    std::fs::create_dir_all(&baselines_dir)?;

    // Step 1: Reconstruct expected state from baseline + migration files,
    // through the same replay path migrate new/update use for their starting
    // state — both commands must see the same history.
    if !validation_options.quiet {
        eprintln!("📊 Reconstructing expected state from baseline + migration files...");
    }
    let roles_file = root_dir.join(&config.directories.roles);
    let baseline_config = BaselineConfig {
        validate_consistency: false,
        verbose: !validation_options.quiet,
    };
    // Reconstruction and desired-state each need their own pristine branch:
    // the replay dirties the shadow and branch cleans are no-ops, so sharing
    // one branch would make the schema-file apply collide. See `migrate new`.
    let starting_pool = shadow.connect_fresh().await?;
    let expected_catalog = get_migration_starting_state(
        &starting_pool,
        &baselines_dir,
        &migrations_dir,
        &roles_file,
        &baseline_config,
        config,
    )
    .await?;
    crate::db::branch::drop_branch(starting_pool).await?;

    // Step 2: Get desired state from current schema files
    if !validation_options.quiet {
        eprintln!("🔍 Loading desired state from current schema files...");
    }
    let desired_catalog =
        crate::schema_ops::apply_current_schema_to_shadow(config, root_dir, shadow).await?;

    // Step 3: Compare expected state (baseline + migrations) vs desired state (schema files)
    if !validation_options.quiet {
        eprintln!(
            "🔍 Comparing expected state (baseline + migrations) vs desired state (schema files)..."
        );
    }
    let validation_config = ValidationConfig {
        show_differences: validation_options.format == "human", // Only show differences in human format
        verbose: false,
    };

    let result = validate_catalogs(
        &expected_catalog,
        &desired_catalog,
        config,
        &validation_config,
    )?;

    // Step 4: Collect migration information for reporting
    let all_migrations = discover_migrations(&migrations_dir)?;
    let migration_versions: Vec<u64> = all_migrations.iter().map(|m| m.version).collect();

    let baseline_info = if let Some(latest_baseline) = find_latest_baseline(&baselines_dir)? {
        Some(BaselineInfo {
            version: latest_baseline.version,
            object_count: 0,
            description: format!("baseline_V{}", latest_baseline.version),
        })
    } else {
        None
    };

    // Step 5: Format and output results for CI/CD validation
    let output = format_validation_output(
        &result,
        validation_options,
        &migration_versions,
        &[], // No "unapplied" concept in CI/CD validation
        baseline_info.as_ref(),
    )?;

    println!("{}", output);

    // Return appropriate exit code for CI/CD
    if result.passed {
        if !validation_options.quiet {
            eprintln!("✅ Migration consistency validation passed");
        }
        Ok(())
    } else {
        Err(anyhow!(
            "Migration validation failed: Schema files don't match expected state from baseline + migrations (found {} differences)",
            result.differences.len()
        ))
    }
}
