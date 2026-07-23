//! `migrate resolve` — explicit break-glass repair of section tracking state.
//!
//! pgmt's normal recovery model is fix-in-repo: per-section checksums let you
//! edit an unapplied section and re-run `migrate apply`, which resumes. That
//! covers the common cases and needs no out-of-band tool.
//!
//! `resolve` is the escape hatch for the states fix-in-repo can't reach, one
//! coordinate at a time (never bulk), always printing the before/after state:
//!
//! - `--mark-completed <version>/<section>`: a DBA manually hot-fixed the
//!   database and a pending/failed/running section should be recorded as done
//!   without executing it again.
//! - `--restamp <version>[/<section>]`: a consciously edited COMPLETED section
//!   needs its stored checksum re-stamped to the current file content.
//!
//! (A failed or stale section needs no verb: the next `apply` re-runs it.)
//!
//! Every mutation runs under the same advisory lock `apply`/`provision` take,
//! so resolve can't race a concurrent deploy.

use crate::config::Config;
use crate::migration::section_parser::MigrationSection;
use crate::migration::{discover_baselines, discover_migrations, parse_migration_sections};
use crate::migration_tracking::section_tracking::{SectionStatus, format_sections_table_name};
use crate::migration_tracking::{
    MigrationLock, calculate_checksum, ensure_section_tracking_table, ensure_tracking_table_exists,
    update_stored_file_checksum, version_to_db,
};
use anyhow::{Context, Result, anyhow, bail};
use sqlx::PgPool;
use std::path::Path;

/// The single mutation the operator requested. Exactly one is built by the
/// caller (clap enforces the mutual exclusion + required-one at parse time).
pub enum ResolveVerb {
    /// `<version>/<section>` — mark the section completed without running it.
    MarkCompleted(String),
    /// `<version>[/<section>]` — re-stamp completed section checksum(s).
    Restamp(String),
}

pub async fn cmd_migrate_resolve(
    config: &Config,
    root_dir: &Path,
    target: &crate::config::TargetUrl,
    verb: ResolveVerb,
    is_baseline: bool,
) -> Result<()> {
    let pool =
        crate::db::connection::connect_to_database(target.as_str(), "target database").await?;

    // Resolve mutates tracking state and must exclude concurrent apply/provision
    // runs — take the same advisory lock they do, on a dedicated connection, and
    // release it on every exit path (drop releases it otherwise).
    let lock = MigrationLock::acquire(target.as_str(), &config.migration.tracking_table).await?;

    let result = resolve_inner(config, root_dir, &pool, verb, is_baseline).await;
    lock.release().await?;
    result
}

async fn resolve_inner(
    config: &Config,
    root_dir: &Path,
    pool: &PgPool,
    verb: ResolveVerb,
    is_baseline: bool,
) -> Result<()> {
    // Bring the tracking tables to the current shape before reading/writing them
    // (the target may predate per-section tracking).
    ensure_tracking_table_exists(pool, &config.migration.tracking_table).await?;
    ensure_section_tracking_table(pool, &config.migration.tracking_table).await?;

    match verb {
        ResolveVerb::MarkCompleted(coord) => {
            let (version, section) = parse_coord_required_section(&coord)?;
            mark_completed(config, root_dir, pool, version, &section, is_baseline).await
        }
        ResolveVerb::Restamp(coord) => {
            let (version, section) = parse_coord_optional_section(&coord)?;
            restamp(
                config,
                root_dir,
                pool,
                version,
                section.as_deref(),
                is_baseline,
            )
            .await
        }
    }
}

/// The human label for which row space we operate on, for messages.
fn kind(is_baseline: bool) -> &'static str {
    if is_baseline { "baseline" } else { "migration" }
}

// ---------------------------------------------------------------------------
// --mark-completed
// ---------------------------------------------------------------------------

async fn mark_completed(
    config: &Config,
    root_dir: &Path,
    pool: &PgPool,
    version: u64,
    section: &str,
    is_baseline: bool,
) -> Result<()> {
    let sections_table = format_sections_table_name(&config.migration.tracking_table);

    let status = fetch_section_status(pool, &sections_table, version, section, is_baseline)
        .await?
        .ok_or_else(|| {
            anyhow!(
                "no section row exists for {} {}/{}; `resolve` never creates rows — apply the \
                 migration (or provision the baseline) so the section is registered first",
                kind(is_baseline),
                version,
                section
            )
        })?;

    // Covered means completed OR satisfied. A satisfied row is a
    // source-covered adoption record — its objects are already present and it
    // is already accounted for, so overwriting it with `completed` would be a
    // downgrade of an intentional state. Refuse both.
    if status.parse::<SectionStatus>()?.is_covered() {
        bail!(
            "section {}/{} is already {} (covered); nothing to mark — it is already accounted for",
            version,
            section,
            status
        );
    }

    // Re-stamp stored metadata from the current file when it (and the section)
    // still exist, so a manual fix followed by --mark-completed leaves the row
    // self-consistent. If the file or section is pruned, leave metadata as-is.
    let file_section = load_file_sections(config, root_dir, version, is_baseline)?
        .and_then(|(sections, _checksum)| sections.into_iter().find(|s| s.name == section));

    if let Some(fs) = &file_section {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {} SET status = 'completed', completed_at = NOW(), last_error = NULL, \
                 checksum = $1, mode = $2, module = $3 \
             WHERE migration_version = $4 AND is_baseline = $5 AND section_name = $6",
            sections_table
        )))
        .bind(calculate_checksum(&fs.checksum_content()))
        .bind(fs.mode.as_str())
        .bind(fs.module.as_deref())
        .bind(version_to_db(version)?)
        .bind(is_baseline)
        .bind(section)
        .execute(pool)
        .await?;
    } else {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {} SET status = 'completed', completed_at = NOW(), last_error = NULL \
             WHERE migration_version = $1 AND is_baseline = $2 AND section_name = $3",
            sections_table
        )))
        .bind(version_to_db(version)?)
        .bind(is_baseline)
        .bind(section)
        .execute(pool)
        .await?;
    }

    println!(
        "Marked {} section {}/{} completed:",
        kind(is_baseline),
        version,
        section
    );
    println!("  status: {status} -> completed");
    if file_section.is_some() {
        println!("  re-stamped checksum/mode/module from the current file");
    } else {
        println!(
            "  note: {} file (or section) not found — stored checksum/mode/module left untouched",
            kind(is_baseline)
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// --restamp
// ---------------------------------------------------------------------------

async fn restamp(
    config: &Config,
    root_dir: &Path,
    pool: &PgPool,
    version: u64,
    section: Option<&str>,
    is_baseline: bool,
) -> Result<()> {
    let sections_table = format_sections_table_name(&config.migration.tracking_table);

    // The file must be present — re-stamping recomputes from its content.
    let (file_sections, file_checksum) =
        load_file_sections(config, root_dir, version, is_baseline)?.ok_or_else(|| {
            anyhow!(
                "no {} file found for version {}; --restamp recomputes checksums from the file, \
                 which must be present",
                kind(is_baseline),
                version
            )
        })?;

    // All recorded section rows for this version:
    // (name, status, stored checksum, stored section_order).
    let rows: Vec<(String, String, Option<String>, i32)> =
        sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT section_name, status, checksum, section_order FROM {} \
         WHERE migration_version = $1 AND is_baseline = $2 ORDER BY section_order",
            sections_table
        )))
        .bind(version_to_db(version)?)
        .bind(is_baseline)
        .fetch_all(pool)
        .await?;

    // Which sections to re-stamp: the named one (must exist + be covered), or
    // every covered section of the version. A `satisfied` row is covered and
    // immutable exactly like `completed`, so it is restampable too — a
    // consciously edited source-covered section re-stamps like any other.
    let is_covered = |s: &str| {
        s.parse::<SectionStatus>()
            .map(|st| st.is_covered())
            .unwrap_or(false)
    };
    let targets: Vec<&(String, String, Option<String>, i32)> = match section {
        Some(name) => {
            let row = rows.iter().find(|(n, _, _, _)| n == name).ok_or_else(|| {
                anyhow!(
                    "no section row exists for {} {}/{}",
                    kind(is_baseline),
                    version,
                    name
                )
            })?;
            if !is_covered(&row.1) {
                bail!(
                    "section {}/{} is {} (not covered); --restamp only re-stamps covered \
                     (completed/satisfied) sections. To re-run an unapplied section, fix it in \
                     the repo and apply (the next apply re-runs failed/stale sections).",
                    version,
                    name,
                    row.1
                );
            }
            vec![row]
        }
        None => rows.iter().filter(|(_, s, _, _)| is_covered(s)).collect(),
    };

    if targets.is_empty() {
        println!(
            "No covered {} sections to re-stamp for version {}.",
            kind(is_baseline),
            version
        );
        return Ok(());
    }

    println!(
        "Re-stamping {} covered {} section(s) for version {}:",
        targets.len(),
        kind(is_baseline),
        version
    );

    for (name, _status, stored_checksum, stored_order) in targets {
        // The section's index in the CURRENT file is its new section_order —
        // restamping a consciously reordered section syncs the stored order too,
        // otherwise the immutability reorder bail would keep blocking it forever.
        let (new_order, fs) = file_sections
            .iter()
            .enumerate()
            .find(|(_, s)| &s.name == name)
            .ok_or_else(|| {
                anyhow!(
                    "section '{}' is recorded as completed for {} {} but no longer exists in the \
                 file; --restamp cannot recompute a removed section",
                    name,
                    kind(is_baseline),
                    version
                )
            })?;
        let new_order = new_order as i32;

        let new_checksum = calculate_checksum(&fs.checksum_content());
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {} SET checksum = $1, mode = $2, module = $3, section_order = $4 \
             WHERE migration_version = $5 AND is_baseline = $6 AND section_name = $7",
            sections_table
        )))
        .bind(&new_checksum)
        .bind(fs.mode.as_str())
        .bind(fs.module.as_deref())
        .bind(new_order)
        .bind(version_to_db(version)?)
        .bind(is_baseline)
        .bind(name)
        .execute(pool)
        .await?;

        println!(
            "  {}: {} -> {}",
            name,
            stored_checksum.as_deref().unwrap_or("(none)"),
            new_checksum
        );
        if *stored_order != new_order {
            println!("    section_order: {stored_order} -> {new_order}");
        }
    }

    // Keep the main-row whole-file fingerprint current too.
    update_stored_file_checksum(
        pool,
        &config.migration.tracking_table,
        version,
        is_baseline,
        &file_checksum,
    )
    .await?;
    println!("  refreshed the main row's file checksum");

    Ok(())
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Fetch a single section row's status, or `None` if the row doesn't exist.
async fn fetch_section_status(
    pool: &PgPool,
    sections_table: &str,
    version: u64,
    section: &str,
    is_baseline: bool,
) -> Result<Option<String>> {
    let row: Option<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT status FROM {} \
         WHERE migration_version = $1 AND is_baseline = $2 AND section_name = $3",
        sections_table
    )))
    .bind(version_to_db(version)?)
    .bind(is_baseline)
    .bind(section)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(status,)| status))
}

/// Load and parse the sections of the migration (or baseline) file for a
/// version, along with its whole-file checksum. `None` when the file is absent
/// (pruned/consolidated away).
fn load_file_sections(
    config: &Config,
    root_dir: &Path,
    version: u64,
    is_baseline: bool,
) -> Result<Option<(Vec<MigrationSection>, String)>> {
    let path = if is_baseline {
        let dir = root_dir.join(&config.directories.baselines);
        discover_baselines(&dir)?
            .into_iter()
            .find(|b| b.version == version)
            .map(|b| b.path)
    } else {
        let dir = root_dir.join(&config.directories.migrations);
        discover_migrations(&dir)?
            .into_iter()
            .find(|m| m.version == version)
            .map(|m| m.path)
    };

    let Some(path) = path else {
        return Ok(None);
    };

    let sql = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read {}", path.display()))?;
    let sections = parse_migration_sections(&path, &sql)?;
    Ok(Some((sections, calculate_checksum(&sql))))
}

/// Parse a `<version>/<section>` coordinate where the section part is required.
fn parse_coord_required_section(coord: &str) -> Result<(u64, String)> {
    let (version_str, section) = coord.split_once('/').ok_or_else(|| {
        anyhow!(
            "expected '<version>/<section>', got '{}' — the section name is required for this \
             verb (e.g. 1700000000/create_users)",
            coord
        )
    })?;
    let version = parse_version(version_str)?;
    if section.is_empty() {
        bail!(
            "the section name is required in '<version>/<section>' (got '{}')",
            coord
        );
    }
    Ok((version, section.to_string()))
}

/// Parse a `<version>[/<section>]` coordinate where the section part is optional.
fn parse_coord_optional_section(coord: &str) -> Result<(u64, Option<String>)> {
    match coord.split_once('/') {
        Some((version_str, section)) => {
            let version = parse_version(version_str)?;
            let section = if section.is_empty() {
                None
            } else {
                Some(section.to_string())
            };
            Ok((version, section))
        }
        None => Ok((parse_version(coord)?, None)),
    }
}

fn parse_version(version_str: &str) -> Result<u64> {
    version_str
        .parse::<u64>()
        .with_context(|| format!("invalid version '{}': must be a whole number", version_str))
}
