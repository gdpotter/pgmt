use crate::commands::migrate::apply::apply_pending_migrations;
use crate::config::Config;
use crate::migration::baseline::apply_baseline_to_target;
use crate::migration::{discover_migrations, find_latest_baseline};
use crate::migration_tracking::{
    MigrationLock, calculate_checksum, ensure_section_tracking_table,
    ensure_tracking_table_exists, format_tracking_table_name, register_baseline_start,
    version_to_db,
};
use crate::modules::{
    ModuleSelection, literal_established_modules, modules_needing_baseline_content,
    parse_section_files,
};
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

    let established = target_is_established(pool, config).await?;
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

    let (files, established_modules) = if selection.named().is_some() {
        let files = parse_section_files(&migrations, &baselines_dir)?;
        let established_modules =
            literal_established_modules(pool, &config.migration.tracking_table, &files).await?;
        (files, established_modules)
    } else {
        (Default::default(), BTreeSet::new())
    };

    // Already managed by pgmt: adopt any requested modules that need baseline
    // content, then behave like apply (catch up pending sections).
    if established {
        let needs_baseline =
            modules_needing_baseline_content(&selection, &established_modules, &files);

        if !needs_baseline.is_empty() {
            let baseline = latest_baseline
                .as_ref()
                .expect("modules need baseline content, so a baseline exists");

            // Adoption constraint: a module's baseline sections may
            // reference its dependencies' objects, so those must already be
            // established here (or be adopted in the same run).
            for module in &needs_baseline {
                for dep in &config.modules.modules[module].depends_on {
                    if !established_modules.contains(dep) && !needs_baseline.contains(dep) {
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
            // their baseline sections would collide.
            let adopted: BTreeSet<&str> = needs_baseline.iter().map(String::as_str).collect();
            register_and_apply_baseline(
                pool,
                config,
                baseline.version,
                &baseline_sql,
                source,
                |section| {
                    section
                        .module
                        .as_deref()
                        .is_some_and(|m| adopted.contains(m))
                },
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

        let established_after: BTreeSet<String> = if selection.named().is_some() {
            literal_established_modules(pool, &config.migration.tracking_table, &files).await?
        } else {
            BTreeSet::new()
        };
        return apply_pending_migrations(
            pool,
            config,
            &migrations,
            &selection,
            &established_after,
        )
        .await;
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
            register_and_apply_baseline(
                pool,
                config,
                baseline.version,
                &baseline_sql,
                source,
                |section| selection.selects(section.module.as_deref()),
            )
            .await?;

            apply_pending_migrations(
                pool,
                config,
                &post_baseline,
                &selection,
                &established_modules,
            )
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
            apply_pending_migrations(pool, config, &migrations, &selection, &established_modules)
                .await?;
            println!("✅ Provisioned from migrations.");
        }
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

    // Resume guard (parity with `apply_pending_migrations`' migration guard): a
    // baseline row already present at this version whose stored checksum differs
    // from the current file means the baseline was edited/regenerated after a
    // partial provision. Sections recorded Completed came from the OLD content,
    // and the section executor skips them by name — so resuming from the NEW file
    // would leave the target matching neither version. Refuse instead. Only fires
    // when a row already exists, so a normal first provision is unaffected.
    let tracking_table_name = format_tracking_table_name(&config.migration.tracking_table)?;
    let existing_checksum: Option<String> = sqlx::query_scalar(&format!(
        "SELECT checksum FROM {} WHERE version = $1 AND is_baseline = TRUE",
        tracking_table_name
    ))
    .bind(version_to_db(version)?)
    .fetch_optional(pool)
    .await?;
    if let Some(stored) = existing_checksum
        && stored != new_checksum
    {
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

    // Pair each selected section with its index in the FULL baseline file
    // (enumerate before filtering) so section_order stays stable per version
    // across the separate registration calls a subset provision makes.
    let selected: Vec<(i32, crate::migration::section_parser::MigrationSection)> =
        crate::migration::parse_migration_sections(Path::new(source), baseline_sql)?
            .into_iter()
            .enumerate()
            .map(|(i, s)| (i as i32, s))
            .filter(|(_, s)| select_section(s))
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
        baseline_sql,
        source,
        select_section,
    )
    .await
}

/// Whether pgmt already manages this database. The version row alone is not
/// decisive — it's written at start (first-touch), so a crashed provision
/// leaves one behind. Established means any of:
/// - a completed *migration* section (apply ran here),
/// - a baseline whose registered sections ALL completed (a provision
///   finished — a half-applied baseline must NOT count, or a failed provision
///   could never resume through the fresh path),
/// - a legacy version row with no section rows at all (recorded on completion
///   by older pgmt, fully applied by construction).
async fn target_is_established(pool: &PgPool, config: &Config) -> Result<bool> {
    let tracking_table_name = format_tracking_table_name(&config.migration.tracking_table)?;
    let sections_table = format!(
        r#""{}"."{}_sections""#,
        config.migration.tracking_table.schema, config.migration.tracking_table.name
    );
    let established: bool = sqlx::query_scalar(&format!(
        "SELECT EXISTS(SELECT 1 FROM {sections} WHERE status = 'completed' AND NOT is_baseline)
             OR EXISTS(SELECT 1 FROM {sections} s1
                 WHERE s1.is_baseline AND s1.status = 'completed'
                   AND NOT EXISTS(SELECT 1 FROM {sections} s2
                       WHERE s2.is_baseline
                         AND s2.migration_version = s1.migration_version
                         AND s2.status <> 'completed'))
             OR EXISTS(SELECT 1 FROM {main} m WHERE NOT EXISTS
                 (SELECT 1 FROM {sections} s
                  WHERE s.migration_version = m.version AND s.is_baseline = m.is_baseline))",
        sections = sections_table,
        main = tracking_table_name,
    ))
    .fetch_one(pool)
    .await?;
    Ok(established)
}
