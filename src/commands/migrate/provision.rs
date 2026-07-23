use crate::commands::migrate::apply::apply_pending_migrations;
use crate::config::Config;
use crate::migration::baseline::apply_baseline_to_target;
use crate::migration::{discover_migrations, find_latest_baseline};
use crate::migration_tracking::{
    MigrationLock, calculate_checksum, ensure_section_tracking_table, ensure_tracking_table_exists,
    register_baseline_start,
};
use crate::modules::{ModuleRuntime, ModuleSelection, parse_section_files};
use anyhow::{Context, Result};
use sqlx::PgPool;
use std::collections::BTreeSet;
use std::path::Path;

/// Provision a database from a baseline + post-baseline migrations.
///
/// Unlike `migrate apply` (which only maintains an already-established
/// database), provision is willing to lay down baseline content on a target:
/// the whole baseline on a fresh database, or — on a module project — a
/// single module's baseline sections when adopting it onto an established
/// database (`--modules`). It never diffs or generates SQL against the
/// target: everything it runs is committed (the baseline file and the
/// migration files). If baseline content collides with objects already
/// present, the section apply fails cleanly and is resumable.
pub async fn cmd_migrate_provision(
    config: &Config,
    root_dir: &Path,
    target: &crate::config::TargetUrl,
    dry_run: bool,
    selection: ModuleSelection,
) -> Result<()> {
    let pool =
        crate::db::connection::connect_to_database(target.as_str(), "target database").await?;

    // Serialize concurrent apply/provision runs against the same tracking table
    // BEFORE reading the tracking table or applying anything. Shares its key with
    // `migrate apply` (both derive it from the tracking table name), so the two
    // commands exclude each other. Held on a dedicated connection for the whole
    // run; released explicitly on every exit path (and on drop otherwise).
    let lock = MigrationLock::acquire(target.as_str(), &config.migration.tracking_table).await?;

    let result = provision_inner(config, root_dir, &pool, dry_run, selection).await;
    lock.release().await?;
    result
}

async fn provision_inner(
    config: &Config,
    root_dir: &Path,
    pool: &PgPool,
    dry_run: bool,
    selection: ModuleSelection,
) -> Result<()> {
    let migrations_dir = root_dir.join(&config.directories.migrations);
    let baselines_dir = root_dir.join(&config.directories.baselines);

    // Ensure the tracking tables exist so we can read the target's state.
    ensure_tracking_table_exists(pool, &config.migration.tracking_table).await?;
    ensure_section_tracking_table(pool, &config.migration.tracking_table).await?;

    let store =
        crate::migration_tracking::TrackingStore::new(pool, &config.migration.tracking_table)?;
    let established = store.target_is_established().await?;
    let migrations = discover_migrations(&migrations_dir)?;
    let latest_baseline = find_latest_baseline(&baselines_dir)?;

    if let Some(named) = selection.named()
        && named.is_empty()
    {
        println!(
            "No --modules given: provisioning the unmoduled base only ({} declared module(s) \
             not included; use --modules all for everything).",
            config.modules.modules.len()
        );
    }

    // The stored subscription is THE establishment source; the runtime
    // also carries the committed re-anchors so the shared apply loop can run
    // the crossing loop for any re-anchor above the derived cursor.
    let mut runtime =
        ModuleRuntime::load(pool, &config.migration.tracking_table, &baselines_dir).await?;

    let files = if selection.named().is_some() {
        parse_section_files(&migrations, &baselines_dir)?
    } else {
        Default::default()
    };

    // Already managed by pgmt: adopt any requested modules that need baseline
    // content, then behave like apply (catch up pending sections).
    if established {
        let needs_baseline = match selection.named() {
            Some(named) => runtime.needing_baseline_content(named.iter()),
            None => Vec::new(),
        };

        if !needs_baseline.is_empty() {
            // Adopt from the latest committed baseline. Provenance-cut
            // sections make an unconsumed re-anchor safe to adopt from:
            // its remap sections whose source the target holds are recorded
            // `satisfied` (objects already present under the source's name),
            // and only the plain sections + remap sections whose source the
            // target lacks actually run.
            let adoption_version = runtime
                .adoption_baseline()
                .map(|(version, _)| version)
                .expect("modules need baseline content, so an adoption baseline exists");
            let baseline = crate::migration::discover_baselines(&baselines_dir)?
                .into_iter()
                .find(|b| b.version == adoption_version)
                .expect("the adoption baseline was discovered from this directory");

            // Adoption constraint: a module's baseline sections may
            // reference its dependencies' objects, so those must already be
            // established here (or be adopted in the same run).
            for module in &needs_baseline {
                for dep in &config.modules.modules[module].depends_on {
                    if !runtime.established().contains(dep) && !needs_baseline.contains(dep) {
                        anyhow::bail!(
                            "adopting module '{}' requires its dependency '{}' on this target; \
                             adopt them together: pgmt migrate provision --modules {},{}",
                            module,
                            dep,
                            module,
                            dep
                        );
                    }
                }
            }

            // The adopted module's baseline sections assume the rest of the
            // target is at the baseline version. Refuse to write a baseline
            // row that would claim coverage the target doesn't have — require
            // the operator to roll its established modules forward first, via
            // an explicit `apply` (so any destructive established-module
            // migrations are surfaced), then re-run provision.
            let pending = crate::modules::established_pending_through(
                pool,
                &config.migration.tracking_table,
                &files,
                runtime.established(),
                baseline.version,
            )
            .await?;
            if !pending.is_empty() {
                let catch_up = if runtime.established().is_empty() {
                    "pgmt migrate apply".to_string()
                } else {
                    format!(
                        "pgmt migrate apply --modules {}",
                        runtime
                            .established()
                            .iter()
                            .cloned()
                            .collect::<Vec<_>>()
                            .join(",")
                    )
                };
                anyhow::bail!(
                    "cannot adopt module(s) {} from baseline {}: this target's established \
                     modules are not caught up to that version (migration(s) {} still pending). \
                     Roll them forward first with `{}`, then re-run provision.",
                    needs_baseline.join(", "),
                    baseline.version,
                    pending
                        .iter()
                        .map(u64::to_string)
                        .collect::<Vec<_>>()
                        .join(", "),
                    catch_up,
                );
            }

            if dry_run {
                println!(
                    "Would adopt module(s) {} from baseline {} (then apply pending sections).",
                    needs_baseline.join(", "),
                    baseline.version
                );
                return Ok(());
            }

            println!(
                "Adopting module(s) {} from baseline {}...",
                needs_baseline.join(", "),
                baseline.version
            );
            let baseline_sql = std::fs::read_to_string(&baseline.path).with_context(|| {
                format!("Failed to read baseline file: {}", baseline.path.display())
            })?;
            let source = baseline
                .path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("baseline.sql");

            // Only the adopted modules' sections: the target's base (and any
            // previously established modules) already exist here — re-running
            // their baseline sections would collide. Per-section rule:
            // a remap section whose source the target already holds is recorded
            // `satisfied` (its objects are present under the source's name);
            // everything else (plain sections, remap sections whose source the
            // target lacks) runs.
            let adopted: BTreeSet<&str> = needs_baseline.iter().map(String::as_str).collect();
            let is_adopted = |section: &crate::migration::section_parser::MigrationSection| {
                section
                    .module
                    .as_deref()
                    .is_some_and(|m| adopted.contains(m))
            };
            register_and_apply_baseline(
                pool,
                config,
                baseline.version,
                &baseline_sql,
                source,
                |section| {
                    is_adopted(section)
                        && !crate::modules::remap_source_held(section, runtime.established())
                },
            )
            .await?;

            // Record the source-covered remap sections as `satisfied`:
            // nothing ran for them, but they are accounted for so the crossing
            // and the incomplete-baseline guard see them as covered.
            let satisfied: Vec<(i32, crate::migration::section_parser::MigrationSection)> =
                crate::migration::parse_migration_sections(
                    std::path::Path::new(source),
                    &baseline_sql,
                )?
                .into_iter()
                .enumerate()
                .map(|(i, s)| (i as i32, s))
                .filter(|(_, s)| {
                    is_adopted(s) && crate::modules::remap_source_held(s, runtime.established())
                })
                .collect();
            crate::migration_tracking::section_tracking::record_sections_satisfied(
                pool,
                &config.migration.tracking_table,
                baseline.version,
                true,
                &satisfied,
            )
            .await?;
            println!("✅ Adopted module(s) {}.", needs_baseline.join(", "));
        } else if dry_run {
            println!(
                "Database is already provisioned. Pending migrations would be applied (use `migrate apply`)."
            );
            return Ok(());
        } else {
            println!("Database is already provisioned; applying any pending migrations.");
        }

        // Explicitly requesting a module IS its adoption: subscribe
        // any newly requested modules — both the baseline-adopted ones above
        // and pure-replay ones the apply loop below will catch up.
        if let Some(named) = selection.named() {
            runtime.record_adopted(named).await?;
        }
        let applied_any =
            apply_pending_migrations(pool, config, &migrations, &selection, &mut runtime).await?;
        if !applied_any {
            println!("Nothing to apply — up to date.");
        }
        return Ok(());
    }

    match latest_baseline {
        Some(baseline) => {
            let baseline_sql = std::fs::read_to_string(&baseline.path).with_context(|| {
                format!("Failed to read baseline file: {}", baseline.path.display())
            })?;
            let source = baseline
                .path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("baseline.sql");
            let post_baseline: Vec<_> = migrations
                .iter()
                .filter(|m| m.version > baseline.version)
                .cloned()
                .collect();

            if dry_run {
                println!(
                    "Would provision: apply baseline {} then {} migration(s).",
                    baseline.version,
                    post_baseline.len()
                );
                return Ok(());
            }

            println!("Applying baseline {}...", baseline.version);
            // Parity with apply's per-module skip line: a subset provision drops
            // the baseline sections of modules it wasn't asked for. Name them so
            // the run isn't silent about what it left out — those modules are
            // genuinely not established on this fresh target.
            report_unselected_baseline_modules(
                &baseline_sql,
                &baseline.path,
                &selection,
                baseline.version,
            )?;
            register_and_apply_baseline(
                pool,
                config,
                baseline.version,
                &baseline_sql,
                source,
                |section| selection.selects(section.module.as_deref()),
            )
            .await?;

            // Provision NEVER crosses: record the subscription from what was
            // provisioned. The baseline row just laid down seeds the derived
            // cursor to baseline.version, so every re-anchor ≤ it is moot;
            // later re-anchors among the post-baseline migrations are ordinary
            // crossings for the shared apply loop below.
            runtime
                .record_provisioned(&selection.named().cloned().unwrap_or_default())
                .await?;

            apply_pending_migrations(pool, config, &post_baseline, &selection, &mut runtime)
                .await?;
            println!("✅ Provisioned from baseline {}.", baseline.version);
        }
        None => {
            // No baseline: replay all migrations from scratch (valid on a fresh DB).
            if dry_run {
                println!(
                    "Would provision: apply {} migration(s) (no baseline).",
                    migrations.len()
                );
                return Ok(());
            }
            if migrations.is_empty() {
                println!("Nothing to provision: no baseline and no migrations found.");
                return Ok(());
            }
            println!("No baseline found; applying all migrations.");
            // Subscribe the requested modules; no baseline exists, so the
            // derived cursor stays None (and there are no re-anchors either —
            // re-anchors are baselines).
            runtime
                .record_provisioned(&selection.named().cloned().unwrap_or_default())
                .await?;
            apply_pending_migrations(pool, config, &migrations, &selection, &mut runtime).await?;
            println!("✅ Provisioned from migrations.");
        }
    }

    Ok(())
}

/// Announce the baseline sections a subset provision is skipping, one line per
/// module, in the same shape as `apply`'s "not established here" notice. Only
/// module sections the selection excludes are reported; the unmoduled base
/// always provisions and is never named.
fn report_unselected_baseline_modules(
    baseline_sql: &str,
    baseline_path: &Path,
    selection: &ModuleSelection,
    version: u64,
) -> Result<()> {
    let sections = crate::migration::parse_migration_sections(baseline_path, baseline_sql)?;
    let mut skipped: BTreeSet<String> = BTreeSet::new();
    for section in &sections {
        if let Some(module) = section.module.as_deref()
            && !selection.selects(Some(module))
        {
            skipped.insert(module.to_string());
        }
    }
    for module in skipped {
        println!(
            "Skipping module '{}' sections in baseline {} (not established here)",
            module, version
        );
    }
    Ok(())
}

/// Register the baseline row + selected Pending section rows (first-touch,
/// idempotent), then execute the selected sections. The registration-first
/// order gives baselines the same resume semantics as migrations.
async fn register_and_apply_baseline(
    pool: &PgPool,
    config: &Config,
    version: u64,
    baseline_sql: &str,
    source: &str,
    select_section: impl Fn(&crate::migration::section_parser::MigrationSection) -> bool + Copy,
) -> Result<()> {
    let new_checksum = calculate_checksum(baseline_sql);

    // Immutability at SECTION granularity (parity with the migration path):
    // an already-applied baseline section is pinned, but an unapplied one may
    // be fixed in the repo and re-run. Validate the current file's sections
    // against the registered rows and sync any unapplied ones.
    let full_sections =
        crate::migration::parse_migration_sections(Path::new(source), baseline_sql)?;
    crate::migration::validate_sections(&full_sections)
        .with_context(|| format!("Invalid section configuration in baseline ({})", source))?;
    let had_checksummed =
        crate::migration_tracking::section_tracking::validate_and_sync_section_checksums(
            pool,
            &config.migration.tracking_table,
            version,
            true,
            &full_sections,
        )
        .await?;

    // The whole-file checksum is a fallback guard only for legacy baseline rows
    // with no per-section checksums.
    let store =
        crate::migration_tracking::TrackingStore::new(pool, &config.migration.tracking_table)?;
    let existing_checksum = store.baseline_stored_checksum(version).await?;
    if let Some(stored) = existing_checksum
        && stored != new_checksum
    {
        if !had_checksummed {
            anyhow::bail!(
                "Baseline {} was modified after being partially applied!\n\
                 Expected checksum: {}\n\
                 Actual checksum:   {}\n\n\
                 A previous provision recorded some of this baseline's sections from \
                 different content; resuming from the edited file would leave the target \
                 matching neither version. Restore the baseline to the applied content to \
                 resume, or reset the target and re-provision from the current baseline.",
                version,
                stored,
                new_checksum,
            );
        }
        // Sections validated; keep the main-row fingerprint current.
        crate::migration_tracking::update_stored_file_checksum(
            pool,
            &config.migration.tracking_table,
            version,
            true,
            &new_checksum,
        )
        .await?;
    }

    // Pair each selected section with its index in the FULL baseline file
    // (enumerate before filtering) so section_order stays stable per version
    // across the separate registration calls a subset provision makes. Reuse
    // the sections already parsed above — no second (or third) parse.
    let selected: Vec<(i32, crate::migration::section_parser::MigrationSection)> = full_sections
        .iter()
        .enumerate()
        .filter(|(_, s)| select_section(s))
        .map(|(i, s)| (i as i32, s.clone()))
        .collect();
    register_baseline_start(
        pool,
        &config.migration.tracking_table,
        version,
        "baseline",
        &new_checksum,
        &selected,
    )
    .await?;
    apply_baseline_to_target(
        pool,
        &config.migration.tracking_table,
        version,
        &selected,
        source,
    )
    .await
}
