use crate::commands::migrate::section_executor::{ExecutionMode, SectionExecutor};
use crate::config::Config;
use crate::migration::{
    ParsedMigration, discover_migrations, parse_migration_sections, validate_sections,
};
use crate::migration_tracking::section_tracking::{
    section_statuses, validate_and_sync_section_checksums,
};
use crate::migration_tracking::{
    MigrationLock, calculate_checksum, ensure_section_tracking_table, ensure_tracking_table_exists,
    format_tracking_table_name, initialize_sections, register_migration_start, version_from_db,
};
use crate::modules::{ModuleRuntime, ModuleSelection, SectionClassification, SkipNotice};
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
    let baselines_dir = root_dir.join(&config.directories.baselines);

    // The crossing sweep and the module guards key off committed baselines,
    // NOT the migrations dir — a repo whose only pending change is a re-anchor
    // baseline must still cross it. So only early-return (before connecting or
    // locking) when the migrations dir is absent AND no baseline exists; an
    // existing-but-empty migrations dir still connects, as before.
    // `discover_migrations` / `discover_baselines` return empty for an absent
    // directory.
    let migrations = discover_migrations(&migrations_dir)?;
    let has_baselines = !crate::migration::discover_baselines(&baselines_dir)?.is_empty();
    if !migrations_dir.exists() && !has_baselines {
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
    let store =
        crate::migration_tracking::TrackingStore::new(pool, &config.migration.tracking_table)?;

    // First-contact guard: in a repo that ships committed baselines, a fresh
    // target must be provisioned, not applied to. A bare `apply` against a
    // target pgmt has never written to (empty tracking → no baseline row) would
    // otherwise run migrations against a schema whose baseline was never laid
    // down, failing with raw `relation does not exist` errors. Scoped to a
    // genuinely-empty target so it can't fire on a mid-flight one, and skipped
    // entirely when the repo ships no baselines — that is the legitimate
    // apply-from-empty flow (fresh replay), which must keep working.
    let baselines_dir = root_dir.join(&config.directories.baselines);
    let has_committed_baseline = !crate::migration::discover_baselines(&baselines_dir)?.is_empty();
    if has_committed_baseline && store.main_table_is_empty().await? {
        anyhow::bail!(
            "this target has no baseline established; provision it first:\n  \
             pgmt migrate provision\n\
             `migrate apply` maintains an already-provisioned database — it does not lay \
             down baseline content, so applying migrations here would fail against a schema \
             the baseline was never applied to."
        );
    }

    let incomplete = store.incomplete_baseline_sections().await?;
    if !incomplete.is_empty() {
        // Any incomplete BASE section → the watermark itself is untrustworthy;
        // refuse all applies, naming the version to finish.
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

    // The stored subscription is THE establishment source: guards, skip
    // notices and the crossing loop all read it through the runtime.
    let mut runtime = ModuleRuntime::load(
        pool,
        &config.migration.tracking_table,
        &root_dir.join(&config.directories.baselines),
    )
    .await?;

    if let Some(named) = selection.named() {
        // The adoption guard, crossing-aware. `apply` only ever replays
        // sections, so a never-established module whose pre-baseline state
        // lives in a committed baseline normally must be adopted via
        // `provision --modules`. BUT a brand-new module introduced by a
        // committed re-anchor whose sources this target holds whole is
        // subscribed by THIS apply's own crossing sweep — it needs nothing
        // from the baseline, because the crossing relabels objects the target
        // already holds. Forecast the sweep (read-only) and waive the refusal
        // for exactly those modules: whether a crossing subscribes a module
        // cannot depend on whether the deploy command happened to name it
        // (`--modules all` names every declared module, including one born at
        // a split this target is about to cross).
        // A module that would BLOCK the crossing is never in this set, and
        // (a needed-modules block implies the module carries baseline content)
        // still gets the clean baseline-content refusal below when named; when
        // NOT named, the real apply loop's membrane surfaces unpreempted.
        let would_subscribe = runtime.forecast_crossings(migrations).await?;
        let still_needs: Vec<String> = runtime
            .needing_baseline_content(named.iter())
            .into_iter()
            .filter(|m| !would_subscribe.contains(m))
            .collect();
        if !still_needs.is_empty() {
            anyhow::bail!(
                "adopting module(s) {} here requires baseline content — their pre-baseline \
                 state lives in the committed baseline, not the migrations.\n\
                 Adopt via: pgmt migrate provision --modules {}",
                still_needs.join(", "),
                still_needs.join(",")
            );
        }
        // Explicitly requesting a module IS its adoption — but a module the
        // crossing sweep will subscribe is subscribed by the crossing loop
        // itself (with crossing provenance and the "Crossed re-anchor"
        // message), so subscribe only the pure-replay adoptions here.
        let to_adopt: BTreeSet<String> = named
            .iter()
            .filter(|m| !would_subscribe.contains(*m))
            .cloned()
            .collect();
        runtime.record_adopted(&to_adopt).await?;
    }

    let applied_any =
        apply_pending_migrations(pool, config, migrations, selection, &mut runtime).await?;
    if !applied_any {
        println!("Nothing to apply — up to date.");
    }
    Ok(())
}

/// Apply migration files to a database, skipping any already recorded in the
/// tracking table (and validating their checksums haven't drifted).
///
/// This is THE production apply path: section execution, checksum validation,
/// and tracking-table recording all live here, so `migrate apply` and
/// `migrate provision` can't diverge. The caller selects which migrations to
/// consider (e.g. provision passes only those after the baseline it just
/// applied) and is responsible for connecting to the target.
///
/// **Precondition:** the tracking tables already exist. Both command entry
/// points (`cmd_migrate_apply` → `apply_with_module_guard`, and
/// `cmd_migrate_provision` → `provision_inner`) ensure them once up front, so
/// this shared loop does not.
///
/// The `runtime` carries the target's stored subscription and the committed
/// re-anchors; this loop interleaves the **crossing loop** with section
/// execution: each re-anchor V is crossed after every version < V settles and
/// before version V's own sections run, and a final sweep after the last
/// migration consumes trailing re-anchors (a pure re-tag lands on a
/// fully-up-to-date target). A wholeness failure at V errors out here, which
/// is the strong membrane: nothing at or beyond V — base sections included —
/// executes.
///
/// Returns `true` if at least one migration actually ran or resumed here, so
/// the caller can emit a "nothing to apply" closing line when the target was
/// already up to date.
pub(crate) async fn apply_pending_migrations(
    pool: &PgPool,
    config: &Config,
    migrations: &[ParsedMigration],
    selection: &ModuleSelection,
    runtime: &mut ModuleRuntime,
) -> Result<bool> {
    let tracking_table_name = format_tracking_table_name(&config.migration.tracking_table)?;
    let mut applied_any = false;

    // All tracking rows: version + checksum + is_baseline.
    let rows: Vec<(i64, String, bool)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT version, checksum, is_baseline FROM {}",
        tracking_table_name
    )))
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
        // Crossing loop, phase order: consume every re-anchor STRICTLY
        // below this version single-shot (their versions are settled). A
        // re-anchor AT this version is two-phase — gate-checked now (before
        // V's sections; the gate's post-crossing vocabulary steers V's own
        // selection and warnings), committed after V's sections complete
        // (acquisition deltas live in migration V itself). A wholeness
        // failure bails at either point, refusing this and every later
        // version — the strong membrane.
        runtime
            .cross_re_anchors_through(Some(migration.version.saturating_sub(1)))
            .await?;

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
                    "Warning: migration {} ({}) is below the baseline watermark ({}) \
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
            // Nothing of this version runs here, so a re-anchor at it is a
            // single-shot gate+commit.
            runtime
                .cross_re_anchors_through(Some(migration.version))
                .await?;
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

        let registered = applied_migrations.get(&migration.version);

        // Two-phase gate: a re-anchor AT this version is gate-checked
        // now — before V's sections — and committed only after they complete
        // (acquisition deltas live in migration V itself). The gate sees
        // which (module, source) acquisition sections this migration carries:
        // a remap section is satisfiable when it WILL run in this apply.
        //
        // The gate runs BEFORE the immutability/checksum-sync step below: a
        // membrane refusal must abort before `update_stored_file_checksum`
        // mutates the stored fingerprint, and both need only the parsed
        // sections — no work is lost by deciding the refusal first.
        let acquirable = crate::modules::acquisition_pairs(&sections);
        let mut pending_crossing = runtime
            .gate_re_anchor_at(migration.version, &acquirable)
            .await?;
        // Commit helper for the early-exit paths: nothing of V remains to
        // run, so the gated crossing commits immediately.
        macro_rules! commit_pending {
            () => {
                if let Some(pending) = pending_crossing.take() {
                    runtime.commit_crossing(pending).await?;
                }
            };
        }

        // Immutability is enforced at SECTION granularity (the unit of resume):
        // an already-applied section may never change, but an unapplied one may
        // be fixed in the repo and re-run. Validate the current file's sections
        // against the registered rows; the whole-file checksum is only a
        // fallback guard for legacy rows with no per-section checksums.
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

        // Version V's own selection and warnings see the POST-crossing
        // vocabulary the gate computed — a split at V tags V's sections
        // with the new names. Source-held checks deliberately keep the
        // PRE-crossing subscription: remap sources are pre-V vocabulary, and
        // the commit (which drops absorbed sources) only lands after V's
        // sections run.
        let vocabulary: BTreeSet<String> = pending_crossing
            .as_ref()
            .map(|p| p.rewritten().clone())
            .unwrap_or_else(|| runtime.established().clone());

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

        // THE section classifier: one pure verdict for what runs, what is
        // recorded satisfied, what is skipped (and at what notice level), and
        // whether coupling is violated. `to_run` already excludes covered
        // sections, so SectionExecutor's per-section is_covered re-query is
        // defense-in-depth and the reporter count is accurate.
        let SectionClassification {
            to_run,
            to_satisfy,
            selected,
            skipped,
            coupling_violation,
        } = crate::modules::classify_sections(
            &sections,
            &statuses,
            &vocabulary,
            selection,
            runtime.established(),
        );

        for skip in &skipped {
            match skip.notice {
                SkipNotice::Drift => eprintln!(
                    "Warning: module '{}' is established on this target but not in the \
                     requested set — its sections in migration {} were skipped. This is \
                     schema drift until a deploy names it (--modules ...,{}).",
                    skip.module, migration.version, skip.module
                ),
                SkipNotice::NotEstablished => println!(
                    "Skipping module '{}' sections in migration {} (not established here)",
                    skip.module, migration.version
                ),
            }
        }

        if let Some(v) = &coupling_violation {
            anyhow::bail!(
                "migration {} couples module '{}' (section '{}') ahead of selected \
                 section '{}'; deploy them together (--modules ...,{})",
                migration.version,
                v.module,
                v.earlier_section,
                v.section,
                v.module
            );
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
            // covered (completed, or satisfied for source-held remap sections)
            // — unselected modules' sections stay unrecorded until adopted.
            let fully_applied = selected.iter().all(|(_, s)| {
                statuses.get(&s.name).is_some_and(|st| st.is_covered())
                    || crate::modules::remap_source_held(s, runtime.established())
            });
            if fully_applied && to_satisfy.is_empty() {
                debug!("Migration {} already applied, skipping", migration.version);
                commit_pending!();
                continue;
            }
            let done = selected
                .iter()
                .filter(|(_, s)| statuses.get(&s.name).is_some_and(|st| st.is_covered()))
                .count();
            applied_any = true;
            println!(
                "\nResuming migration {} - {} ({}/{} selected sections already complete)",
                migration.version,
                migration.description,
                done,
                selected.len()
            );
            // Register any TO-RUN sections this target hasn't seen yet
            // (adopting a module completes past versions' sections). Each
            // carries its index in the FULL file so section_order stays stable
            // per version regardless of which subset this call registers.
            // Source-satisfied remap sections are recorded below instead; the
            // classifier already excluded them from `to_run`.
            let missing: Vec<(i32, crate::migration::section_parser::MigrationSection)> = to_run
                .iter()
                .filter(|(_, s)| !statuses.contains_key(&s.name))
                .cloned()
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
            // Resume path: a main row already exists here (statuses is
            // non-empty — the corrupt zero-section case bailed above), so
            // recording the source-satisfied remap rows in their own
            // transaction cannot create the main-row-without-sections state.
            // Only first registration had that crash window; it folds the
            // satisfied rows into the registration transaction instead.
            if !to_satisfy.is_empty() {
                crate::migration_tracking::section_tracking::record_sections_satisfied(
                    pool,
                    &config.migration.tracking_table,
                    migration.version,
                    false,
                    &to_satisfy,
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
                commit_pending!();
                continue;
            }
            applied_any = true;
            println!(
                "\nApplying migration {} - {}",
                migration.version, migration.description
            );
            // Register the version row + the TO-RUN Pending section rows + the
            // source-satisfied remap rows, all in ONE transaction, before
            // anything executes. Folding `to_satisfy` into registration keeps
            // first registration crash-atomic: an acquisition migration whose
            // to-run set is empty (all sources already held) must never commit a
            // main row with zero section rows. Each section carries its index in
            // the FULL file so section_order stays stable per version even when a
            // later call registers another subset.
            register_migration_start(
                pool,
                &config.migration.tracking_table,
                migration.version,
                &migration.description,
                &checksum,
                &to_run,
                &to_satisfy,
            )
            .await?;
        }

        // Announce the source-satisfied remap sections (recorded above, folded
        // into first registration or in their own transaction on resume):
        // nothing runs for them — their objects are already present under the
        // source's name — but the rows account for them, so crossings and the
        // guards see the section as covered.
        for (_, section) in &to_satisfy {
            println!(
                "Section '{}' of migration {}: source '{}' is established here — \
                 recorded satisfied (objects already present; nothing to run)",
                section.name,
                migration.version,
                section.remaps.as_deref().unwrap_or("?"),
            );
        }

        let start = Instant::now();

        // Create section executor
        let reporter = SectionReporter::new(to_run.len(), false);
        let mut executor = SectionExecutor::new(
            pool.clone(),
            config.migration.tracking_table.clone(),
            reporter,
            ExecutionMode::Production,
            false,
        );

        // Execute each to-run section (any already-covered ones would skip
        // inside — defense-in-depth; the classifier already excluded them).
        for (_, section) in &to_run {
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
        let reporter = SectionReporter::new(to_run.len(), false);
        reporter.migration_summary(duration, to_run.len());

        // Commit phase: version V's sections are done, so the gated
        // crossing finalizes — subscription rewrite plus the ledger
        // consumption row (baseline row + satisfied section rows).
        commit_pending!();
    }

    // Final sweep: every migration is settled, so consume any re-anchors
    // beyond the last migration. This is what makes a pure re-tag (a
    // re-anchor with no accompanying DDL) land on the next bare apply of a
    // fully-up-to-date target — the loop keys off the derived cursor, not off
    // pending migrations.
    runtime.cross_re_anchors_through(None).await?;

    Ok(applied_any)
}
