use crate::commands::migrate::section_executor::{ExecutionMode, SectionExecutor};
use crate::config::Config;
use crate::migration::{
    ParsedMigration, discover_migrations, parse_migration_sections, validate_sections,
};
use crate::migration_tracking::section_tracking::{
    SectionStatus, section_statuses, validate_and_sync_section_checksums,
};
use crate::migration_tracking::{
    MigrationLock, calculate_checksum, ensure_section_tracking_table, ensure_tracking_table_exists,
    format_tracking_table_name, initialize_sections, register_migration_start, version_from_db,
};
use crate::modules::{
    ModuleSelection, literal_established_modules, modules_needing_baseline_content,
    parse_section_files,
};
use crate::progress::SectionReporter;
use anyhow::{Context, Result};
use sqlx::PgPool;
use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::time::Instant;
use tracing::debug;

pub async fn cmd_migrate_apply(
    config: &Config,
    root_dir: &Path,
    target: &crate::config::TargetUrl,
    selection: ModuleSelection,
) -> Result<()> {
    println!("Applying migrations to target database");

    let migrations_dir = root_dir.join(&config.directories.migrations);
    if !migrations_dir.exists() {
        println!("No migrations directory found - nothing to apply");
        return Ok(());
    }

    let pool =
        crate::db::connection::connect_to_database(target.as_str(), "target database").await?;

    // Serialize concurrent apply/provision runs against the same tracking table
    // BEFORE reading the tracking table or applying anything. Held on a dedicated
    // connection for the whole run; released explicitly on success (and on drop
    // otherwise).
    let lock = MigrationLock::acquire(target.as_str(), &config.migration.tracking_table).await?;

    let migrations = discover_migrations(&migrations_dir)?;

    let result = apply_with_module_guard(config, root_dir, &pool, &migrations, &selection).await;
    lock.release().await?;
    result
}

/// The module-aware body of `migrate apply`, split out so the advisory lock
/// wraps every path (including the guards' refusals) with a single release.
async fn apply_with_module_guard(
    config: &Config,
    root_dir: &Path,
    pool: &PgPool,
    migrations: &[ParsedMigration],
    selection: &ModuleSelection,
) -> Result<()> {
    // A baseline row whose registered sections aren't all completed is a
    // crashed/incomplete `provision`. Its version must NOT be blindly trusted as
    // "covers everything ≤ V" — doing so would skip migrations onto a
    // half-built schema. But the untrustworthiness is scoped to what actually
    // failed to land, so scope the refusal by module (apply never resumes a
    // baseline; provision itself resumes such baselines, so this guard lives on
    // the apply command, not the shared apply loop).
    //
    // - An incomplete BASE section (module IS NULL) means the baseline's base
    //   content is half-built: the version is untrustworthy as a base watermark,
    //   so refuse EVERY apply. This only arises from a crashed FRESH provision
    //   that failed on a base section — adoption never runs base sections.
    // - Incomplete MODULE sections mean only those modules are half-built. The
    //   base content of that baseline version DID land: either the base sections
    //   completed in this same provision run before the module section failed,
    //   or (adoption) the base was already caught up to the baseline version via
    //   migrations before adoption began — adoption is gated on exactly that
    //   (`established_pending_through`) and inserts the baseline main row
    //   idempotently (ON CONFLICT DO NOTHING), so a crashed adoption never moves
    //   the watermark past content the base already has. So a base-only (or
    //   any-unaffected-module) apply is safe to proceed; only refuse when the
    //   selection actually names one of the half-built modules.
    ensure_tracking_table_exists(pool, &config.migration.tracking_table).await?;
    ensure_section_tracking_table(pool, &config.migration.tracking_table).await?;
    let incomplete = crate::migration_tracking::section_tracking::incomplete_baseline_sections(
        pool,
        &config.migration.tracking_table,
    )
    .await?;
    if !incomplete.is_empty() {
        // Any incomplete BASE section → the watermark itself is untrustworthy;
        // refuse all applies with the original, version-scoped guidance.
        let base_incomplete_version = incomplete
            .iter()
            .filter(|(_, _, module)| module.is_none())
            .map(|(version, _, _)| *version)
            .max();
        if let Some(version) = base_incomplete_version {
            anyhow::bail!(
                "baseline {version} is only partially applied — a `migrate provision` did not \
                 finish. Complete it with `pgmt migrate provision` before applying migrations."
            );
        }

        // All incomplete sections belong to specific modules. Collect the
        // affected modules and the baseline version they belong to.
        let affected_version = incomplete
            .iter()
            .map(|(version, _, _)| *version)
            .max()
            .expect("incomplete is non-empty and all rows carry a module");
        let affected_modules: BTreeSet<String> = incomplete
            .iter()
            .filter_map(|(_, _, module)| module.clone())
            .collect();
        let selection_hits: BTreeSet<&String> = affected_modules
            .iter()
            .filter(|m| selection.selects(Some(m)))
            .collect();

        let module_list = affected_modules
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        let module_args = affected_modules
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join(",");

        if !selection_hits.is_empty() {
            // The selection names a half-built module: applying its migrations
            // onto an unfinished baseline would build on a partial schema.
            anyhow::bail!(
                "module(s) {module_list} have partially applied baseline content (baseline \
                 {affected_version}) — a `migrate provision --modules {module_args}` did not \
                 finish. Complete it with `pgmt migrate provision --modules {module_args}` before \
                 applying its migrations."
            );
        }

        // The half-built modules are unaffected by this selection (e.g. a
        // base-only deploy while billing's adoption is half-done). The base
        // watermark is honest (see above), so proceed — but warn loudly so the
        // stuck adoption doesn't go unnoticed.
        eprintln!(
            "Warning: module(s) {module_list} have partially applied baseline content (baseline \
             {affected_version}) from an unfinished `migrate provision --modules {module_args}`. \
             This apply does not touch them and proceeds; complete their adoption with \
             `pgmt migrate provision --modules {module_args}`."
        );
    }

    // Module projects: derive what's established here and refuse adoptions
    // that need baseline content — `apply` only ever replays sections; a
    // module whose pre-baseline state lives in a committed baseline must be
    // adopted via `provision --modules`.
    let established = if selection.named().is_some() {
        ensure_tracking_table_exists(pool, &config.migration.tracking_table).await?;
        ensure_section_tracking_table(pool, &config.migration.tracking_table).await?;
        let files =
            parse_section_files(migrations, &root_dir.join(&config.directories.baselines))?;
        let established =
            literal_established_modules(pool, &config.migration.tracking_table, &files).await?;
        let needs_baseline = modules_needing_baseline_content(selection, &established, &files);
        if !needs_baseline.is_empty() {
            anyhow::bail!(
                "adopting module(s) {} here requires baseline content — their pre-baseline \
                 state lives in the committed baseline, not the migrations.\n\
                 Adopt via: pgmt migrate provision --modules {}",
                needs_baseline.join(", "),
                needs_baseline.join(",")
            );
        }
        established
    } else {
        BTreeSet::new()
    };

    apply_pending_migrations(pool, config, migrations, selection, &established).await
}

/// Apply migration files to a database, skipping any already recorded in the
/// tracking table (and validating their checksums haven't drifted).
///
/// This is THE production apply path: section execution, checksum validation,
/// and tracking-table recording all live here, so `migrate apply` and
/// `migrate provision` can't diverge. The caller selects which migrations to
/// consider (e.g. provision passes only those after the baseline it just
/// applied) and is responsible for connecting to the target.
pub(crate) async fn apply_pending_migrations(
    pool: &PgPool,
    config: &Config,
    migrations: &[ParsedMigration],
    selection: &ModuleSelection,
    established: &BTreeSet<String>,
) -> Result<()> {
    let tracking_table_name = format_tracking_table_name(&config.migration.tracking_table)?;

    // Ensure the tracking tables exist (and are migrated to the current shape)
    ensure_tracking_table_exists(pool, &config.migration.tracking_table).await?;
    ensure_section_tracking_table(pool, &config.migration.tracking_table).await?;

    // All tracking rows: version + checksum + is_baseline.
    let rows: Vec<(i64, String, bool)> = sqlx::query_as(&format!(
        "SELECT version, checksum, is_baseline FROM {}",
        tracking_table_name
    ))
    .fetch_all(pool)
    .await?;

    // A recorded baseline covers every migration up to its version. Those
    // migration files (if still present alongside the baseline) must be skipped
    // rather than re-applied or checksum-compared against the baseline.
    let baseline_version = rows
        .iter()
        .filter(|(_, _, is_baseline)| *is_baseline)
        .map(|(version, _, _)| version_from_db(*version))
        .max();

    // Migration rows only: a version can also host a baseline row (paired
    // `--create-baseline`), whose checksum is the baseline file's, not the
    // migration's — it must never enter the migration checksum comparison.
    let applied_migrations: HashMap<u64, String> = rows
        .into_iter()
        .filter(|(_, _, is_baseline)| !is_baseline)
        .map(|(v, checksum, _)| (version_from_db(v), checksum))
        .collect();

    // Apply unapplied migrations
    for migration in migrations {
        // Skip migrations covered by a recorded baseline.
        if baseline_version.is_some_and(|bv| migration.version <= bv) {
            // Distinguish the benign case (covered by, or at, the baseline) from
            // a silent hazard: a migration merged late with a version STRICTLY
            // below an already-recorded baseline watermark and NO tracking row.
            // Such a migration was never applied here, and never will be — it is
            // skipped on established targets and excluded from fresh-provision
            // replay (which only replays versions after the baseline). It is a
            // consistent-but-wrong state, so tell the user loudly. Any recorded
            // row for the version (baseline OR migration) means it is accounted
            // for; only a total absence below the watermark is the hazard. The
            // at-baseline-version case (paired `--create-baseline` migration) is
            // excluded by the strict `<` comparison.
            let below_watermark = baseline_version.is_some_and(|bv| migration.version < bv);
            if below_watermark && !applied_migrations.contains_key(&migration.version) {
                eprintln!(
                    "WARNING: migration {} ({}) is below the baseline watermark ({}) \
                     and was never applied to this database. It will never run here or on \
                     any other target (fresh provisions replay only migrations after the \
                     baseline). If its changes are needed, run 'pgmt migrate update {}' to \
                     regenerate it above the baseline.",
                    migration.version,
                    migration.description,
                    baseline_version.expect("below_watermark implies a baseline exists"),
                    migration.version,
                );
            } else {
                debug!(
                    "Migration {} is covered by the baseline, skipping",
                    migration.version
                );
            }
            continue;
        }

        // Read migration SQL first so we can validate checksum
        let migration_sql = std::fs::read_to_string(&migration.path).with_context(|| {
            format!(
                "Failed to read migration file: {}",
                migration.path.display()
            )
        })?;

        // Calculate checksum
        let checksum = calculate_checksum(&migration_sql);

        // Parse migration into sections
        let sections = parse_migration_sections(&migration.path, &migration_sql)
            .with_context(|| format!("Failed to parse migration {}", migration.version))?;

        // Validate sections
        validate_sections(&sections).with_context(|| {
            format!(
                "Invalid section configuration in migration {}",
                migration.version
            )
        })?;

        // Immutability is enforced at SECTION granularity (the unit of resume):
        // an already-applied section may never change, but an unapplied one may
        // be fixed in the repo and re-run. Validate the current file's sections
        // against the registered rows; the whole-file checksum is only a
        // fallback guard for legacy rows with no per-section checksums.
        let registered = applied_migrations.get(&migration.version);
        if let Some(stored_file_checksum) = registered {
            let had_checksummed = validate_and_sync_section_checksums(
                pool,
                &config.migration.tracking_table,
                migration.version,
                false,
                &sections,
            )
            .await?;
            if stored_file_checksum != &checksum {
                if !had_checksummed {
                    // Legacy safety: no per-section checksums exist to validate,
                    // so keep the original whole-file immutability bail.
                    anyhow::bail!(
                        "Migration {} has been modified after being applied!\n\
                         Expected checksum: {}\n\
                         Actual checksum:   {}\n\n\
                         Migrations must be immutable once applied. If you need to make changes:\n\
                         • Create a new migration with the changes\n\
                         • Or roll back and recreate this migration (dangerous in production)",
                        migration.version,
                        stored_file_checksum,
                        checksum
                    );
                }
                // Sections validated; keep the main-row fingerprint current.
                crate::migration_tracking::update_stored_file_checksum(
                    pool,
                    &config.migration.tracking_table,
                    migration.version,
                    false,
                    &checksum,
                )
                .await?;
            }
        }

        // Module selection: run the selected modules' (+ base) sections; the
        // rest are skipped and leave NO rows (derived skipped-ness,
        // no trace of unrequested work). Skips are signalled two-tier: an
        // established module being left behind is schema drift (warning); a
        // never-established one is expected on subset targets (info).
        let selected: Vec<&crate::migration::section_parser::MigrationSection> = sections
            .iter()
            .filter(|section| selection.selects(section.module.as_deref()))
            .collect();
        let statuses = if registered.is_some() {
            section_statuses(
                pool,
                &config.migration.tracking_table,
                migration.version,
                false,
            )
            .await?
        } else {
            Default::default()
        };

        // Only sections that have NOT already completed here are actually
        // being skipped — an unselected module whose sections ran in an
        // earlier deploy is up to date, not drifting.
        let mut skipped_modules: BTreeSet<&str> = BTreeSet::new();
        for section in &sections {
            if let Some(module) = section.module.as_deref()
                && !selection.selects(Some(module))
                && statuses.get(&section.name) != Some(&SectionStatus::Completed)
            {
                skipped_modules.insert(module);
            }
        }
        for module in &skipped_modules {
            if established.contains(*module) {
                eprintln!(
                    "Warning: module '{}' is established on this target but not in the \
                     requested set — its sections in migration {} were skipped. This is \
                     schema drift until a deploy names it (--modules ...,{}).",
                    module, migration.version, module
                );
            } else {
                println!(
                    "Skipping module '{}' sections in migration {} (not established here)",
                    module, migration.version
                );
            }
        }

        // Conservative intra-migration coupling check: section order encodes
        // potential dependency, so a selected section must not run while an
        // EARLIER unselected section of an established module is still
        // pending — its objects may be prerequisites. (Never-established
        // modules' objects don't exist here; real cross-module needs are
        // covered by the dependency-closure guard.)
        for (idx, section) in sections.iter().enumerate() {
            if !selection.selects(section.module.as_deref()) {
                continue;
            }
            for earlier in &sections[..idx] {
                if let Some(module) = earlier.module.as_deref()
                    && !selection.selects(Some(module))
                    && established.contains(module)
                    && statuses.get(&earlier.name) != Some(&SectionStatus::Completed)
                {
                    anyhow::bail!(
                        "migration {} couples module '{}' (section '{}') ahead of selected \
                         section '{}'; deploy them together (--modules ...,{})",
                        migration.version,
                        module,
                        earlier.name,
                        section.name,
                        module
                    );
                }
            }
        }

        if registered.is_some() {
            // The version row is written at start (register_migration_start),
            // so its existence doesn't mean "done" — completeness is derived.
            // The schema migration backfills a synthetic 'default' completed
            // section for every legacy main row, so a registered version with
            // ZERO section rows can no longer occur; if it does, tracking state
            // is corrupt and we must not guess.
            if statuses.is_empty() {
                anyhow::bail!(
                    "tracking state corrupt: version {} has a tracking row but no section \
                     rows; this should be impossible after schema migration — investigate \
                     before re-running",
                    migration.version
                );
            }
            // Under module selection, "done" means every SELECTED section is
            // Completed — unselected modules' sections stay unrecorded until
            // adopted.
            let fully_applied = selected
                .iter()
                .all(|s| statuses.get(&s.name) == Some(&SectionStatus::Completed));
            if fully_applied {
                debug!("Migration {} already applied, skipping", migration.version);
                continue;
            }
            let done = selected
                .iter()
                .filter(|s| statuses.get(&s.name) == Some(&SectionStatus::Completed))
                .count();
            println!(
                "\nResuming migration {} - {} ({}/{} sections already complete)",
                migration.version,
                migration.description,
                done,
                selected.len()
            );
            // Register any selected sections this target hasn't seen yet
            // (adopting a module completes past versions' sections). Each
            // carries its index in the FULL file so section_order stays stable
            // per version regardless of which subset this call registers.
            let missing: Vec<(i32, crate::migration::section_parser::MigrationSection)> = sections
                .iter()
                .enumerate()
                .filter(|(_, s)| {
                    selection.selects(s.module.as_deref()) && !statuses.contains_key(&s.name)
                })
                .map(|(i, s)| (i as i32, s.clone()))
                .collect();
            if !missing.is_empty() {
                initialize_sections(
                    pool,
                    &config.migration.tracking_table,
                    migration.version,
                    false,
                    &missing,
                )
                .await?;
            }
        } else {
            // Nothing selected and nothing recorded: leave zero trace.
            if selected.is_empty() {
                debug!(
                    "Migration {} has no selected sections, skipping",
                    migration.version
                );
                continue;
            }
            println!(
                "\nApplying migration {} - {}",
                migration.version, migration.description
            );
            // Register the version row + the SELECTED Pending section rows
            // atomically, before anything executes. Each selected section
            // carries its index in the FULL file so section_order stays stable
            // per version even when a later call registers another subset.
            let selected_ordered: Vec<(i32, crate::migration::section_parser::MigrationSection)> =
                sections
                    .iter()
                    .enumerate()
                    .filter(|(_, s)| selection.selects(s.module.as_deref()))
                    .map(|(i, s)| (i as i32, s.clone()))
                    .collect();
            register_migration_start(
                pool,
                &config.migration.tracking_table,
                migration.version,
                &migration.description,
                &checksum,
                &selected_ordered,
            )
            .await?;
        }

        let start = Instant::now();

        // Create section executor
        let reporter = SectionReporter::new(selected.len(), false); // TODO: Add verbose flag to config
        let mut executor = SectionExecutor::new(
            pool.clone(),
            config.migration.tracking_table.clone(),
            reporter,
            ExecutionMode::Production,
            false,
        );

        // Execute each selected section (already-completed ones skip inside)
        for section in &selected {
            executor
                .execute_section(migration.version, section)
                .await
                .with_context(|| {
                    format!(
                        "Migration {} failed at section '{}'",
                        migration.version, section.name
                    )
                })?;
        }

        let duration = start.elapsed();

        // Nothing to record here: the version row was registered at start and
        // per-section completion rows drive the derived applied-state.

        // Report completion
        let reporter = SectionReporter::new(selected.len(), false);
        reporter.migration_summary(duration, selected.len());
    }

    Ok(())
}
