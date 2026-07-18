//! The migration-tracking query surface.
//!
//! Every SQL statement that reads or writes pgmt's tracking tables is a
//! method on [`TrackingStore`]. Three tables:
//!
//! - the main table (`pgmt_migrations` by default) — version anchors: one row
//!   per applied migration/baseline with its whole-file checksum. A baseline
//!   row with zero section rows is a version-level event that processed no
//!   local work — either a crossing that consumed a re-anchor whose remaps
//!   relabeled nothing this target holds, or a plain checkpoint that applied
//!   no content here.
//! - `{name}_sections` — the authoritative record of what actually ran: one
//!   row per section with its per-section lifecycle status.
//! - `{name}_modules` — the module subscription: which modules this target
//!   has established.
//!
//! The re-anchor consumption cursor is NOT a stored value: it is derived as
//! the highest baseline version in the main table (provision-applied and
//! crossing-consumed rows alike). Distinct from the applied-baseline coverage
//! watermark, which is computed from section rows.
//!
//! The store owns a pool handle and every table identity, formatted ONCE from
//! the [`TrackingTable`] config at construction. Consumers never hand-format
//! a tracking-table name or re-encode a status predicate.
//!
//! Locking stays with callers: the store performs queries only. Methods that
//! must participate in a caller's transaction take an `impl PgExecutor` so the
//! caller can pass its own `&mut *tx`; the store still supplies the table name.
//!
//! The "covered" status predicate — a section whose objects are present,
//! whether it executed here (`completed`) or was covered by an established
//! source (`satisfied`) — is defined in exactly one place
//! ([`TrackingStore::covered_predicate`]) and reused by every method. Encoding
//! it per-call is what let `target_is_established` miss `satisfied`-only
//! baseline coverage and drive provision down the fresh path against a
//! populated database.

use crate::config::types::TrackingTable;
use crate::migration::section_parser::MigrationSection;
use crate::migration_tracking::section_tracking::SectionStatus;
use crate::migration_tracking::{format_tracking_table_name, version_from_db, version_to_db};
use crate::modules::{Subscription, SubscriptionSource};
use anyhow::{Context, Result};
use sqlx::PgPool;
use std::str::FromStr;

/// Concrete query surface over the migration-tracking tables. Construct once
/// per command (after the advisory lock is held and the tables ensured); it is
/// cheap to clone the pool handle it wraps.
#[derive(Clone)]
pub struct TrackingStore {
    pool: PgPool,
    /// `"schema"."name"` — the main tracking table.
    main: String,
    /// `"schema"."name_sections"`.
    sections: String,
    /// `"schema"."name_modules"` — the stored module subscription.
    modules: String,
}

impl TrackingStore {
    /// Build the store, formatting the table identities once. Fails only if the
    /// configured schema/table name is not a valid SQL identifier (the same
    /// validation `format_tracking_table_name` performs).
    pub fn new(pool: &PgPool, tracking_table: &TrackingTable) -> Result<Self> {
        let main = format_tracking_table_name(tracking_table)?;
        let suffixed = |suffix: &str| {
            format!(
                r#""{}"."{}_{}""#,
                tracking_table.schema, tracking_table.name, suffix
            )
        };
        Ok(Self {
            pool: pool.clone(),
            main,
            sections: suffixed("sections"),
            modules: suffixed("modules"),
        })
    }

    /// `"schema"."name"` — the main tracking table.
    pub fn main_table(&self) -> &str {
        &self.main
    }

    /// The pool handle this store wraps — for callers that must open their own
    /// transaction (e.g. the crossing/provision recorders) and then run the
    /// store's executor-taking writers inside it.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// `"schema"."name_sections"`.
    pub fn sections_table(&self) -> &str {
        &self.sections
    }

    /// THE covered-status predicate: a section whose objects are present here,
    /// whether it executed (`completed`) or was covered by an established
    /// source (`satisfied`). `col` is the (optionally alias-qualified) status
    /// column, e.g. `"status"` or `"s.status"`.
    fn covered_predicate(col: &str) -> String {
        format!("{col} IN ('completed', 'satisfied')")
    }

    /// Whether pgmt already manages this database. The main version row alone
    /// is not decisive — it's written at start (first-touch), so a crashed
    /// provision leaves one behind. Established means any of:
    /// - a covered *migration* section (apply ran here), or
    /// - a baseline whose registered sections are ALL covered (a provision
    ///   finished — a half-applied baseline must NOT count, or a failed
    ///   provision could never resume through the fresh path).
    ///
    /// "Covered" is `completed` OR `satisfied`: a baseline adopted through a
    /// re-anchor records its source-held remap sections `satisfied` — the
    /// objects already exist under the source's name — and those still
    /// establish the target. Counting only `completed` here was the
    /// bug that sent provision down the fresh path against a populated database.
    pub async fn target_is_established(&self) -> Result<bool> {
        let covered = |alias: &str| Self::covered_predicate(alias);
        let established: bool = sqlx::query_scalar(&format!(
            "SELECT EXISTS(SELECT 1 FROM {sections} WHERE {cov} AND NOT is_baseline)
                 OR EXISTS(SELECT 1 FROM {sections} s1
                     WHERE s1.is_baseline AND {cov_s1}
                       AND NOT EXISTS(SELECT 1 FROM {sections} s2
                           WHERE s2.is_baseline
                             AND s2.migration_version = s1.migration_version
                             AND NOT ({cov_s2})))",
            sections = self.sections,
            cov = covered("status"),
            cov_s1 = covered("s1.status"),
            cov_s2 = covered("s2.status"),
        ))
        .fetch_one(&self.pool)
        .await?;
        Ok(established)
    }

    /// Whether the main tracking table has no rows at all — a target pgmt has
    /// never written to (no migration row and no baseline row). Distinct from
    /// [`Self::target_is_established`], which asks about covered content: a
    /// crashed first provision leaves a baseline row here yet establishes
    /// nothing. Used by the apply first-contact guard.
    pub async fn main_table_is_empty(&self) -> Result<bool> {
        let has_row: bool =
            sqlx::query_scalar(&format!("SELECT EXISTS(SELECT 1 FROM {})", self.main))
                .fetch_one(&self.pool)
                .await
                .context("Failed to check whether the tracking table is empty")?;
        Ok(!has_row)
    }

    /// The stored whole-file checksum of the baseline row at `version`, if any.
    /// The fallback immutability guard for legacy baseline rows with no
    /// per-section checksums.
    pub async fn baseline_stored_checksum(&self, version: u64) -> Result<Option<String>> {
        let checksum: Option<String> = sqlx::query_scalar(&format!(
            "SELECT checksum FROM {} WHERE version = $1 AND is_baseline = TRUE",
            self.main
        ))
        .bind(version_to_db(version)?)
        .fetch_optional(&self.pool)
        .await?;
        Ok(checksum)
    }

    /// The target's honest applied-baseline watermark: the highest baseline
    /// version all of whose registered sections are covered, or `None`. A
    /// crashed baseline (a non-covered section) does not count.
    ///
    /// This is content coverage, so it is strictly section-based: a baseline
    /// counts only if it has at least one section row and all of them are
    /// covered. A zero-section baseline row (a consumed re-anchor that relabeled
    /// nothing this target holds) applied no content and must NOT count here —
    /// otherwise its zero non-covered sections would vacuously satisfy the
    /// "all covered" test and inflate the coverage floor. The consumed-through
    /// cursor is the separate concept that treats that row as load-bearing
    /// ([`Self::consumed_through_cursor`]).
    ///
    /// Used as the coverage floor adoption consults to decide whether the
    /// established modules are caught up.
    pub async fn applied_baseline_watermark(&self) -> Result<Option<u64>> {
        let watermark: Option<i64> = sqlx::query_scalar(&format!(
            "SELECT MAX(m.version) FROM {main} m
             WHERE m.is_baseline
               AND EXISTS (
                 SELECT 1 FROM {sections} s
                 WHERE s.is_baseline AND s.migration_version = m.version)
               AND NOT EXISTS (
                 SELECT 1 FROM {sections} s
                 WHERE s.is_baseline AND s.migration_version = m.version
                   AND NOT ({cov}))",
            main = self.main,
            sections = self.sections,
            cov = Self::covered_predicate("s.status"),
        ))
        .fetch_one(&self.pool)
        .await?;
        Ok(watermark.map(version_from_db))
    }

    /// Distinct module literals on covered section rows — the *literal*
    /// established set, read straight off the stored `module` column (the
    /// column is authoritative; no file lookup, no name-convention fallback).
    /// `None`-module (base) rows are excluded.
    ///
    /// **Audit-side cross-check only**: the module literals are
    /// epoch-stamped historical facts, never rewritten on re-tag, so after a
    /// crossing they legitimately diverge from the subscription. Never feed
    /// this into enforcement decisions.
    pub async fn established_module_literals(&self) -> Result<std::collections::BTreeSet<String>> {
        let modules: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT DISTINCT module FROM {} WHERE {} AND module IS NOT NULL",
            self.sections,
            Self::covered_predicate("status"),
        ))
        .fetch_all(&self.pool)
        .await?;
        Ok(modules.into_iter().collect())
    }

    /// Section names of the baseline at `version` the target has a covered
    /// (completed|satisfied) row for — the per-section adoption record the
    /// extended wholeness predicate consults.
    pub async fn covered_baseline_section_names(
        &self,
        version: u64,
    ) -> Result<std::collections::BTreeSet<String>> {
        let names: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT section_name FROM {sections}
             WHERE is_baseline AND migration_version = $1 AND {cov}",
            sections = self.sections,
            cov = Self::covered_predicate("status"),
        ))
        .bind(version_to_db(version)?)
        .fetch_all(&self.pool)
        .await?;
        Ok(names.into_iter().collect())
    }

    /// The `(version, section_name)` set of every COMPLETED migration (non-
    /// baseline) section — the "already done" record adoption diffs against.
    pub async fn completed_migration_sections(
        &self,
    ) -> Result<std::collections::BTreeSet<(u64, String)>> {
        let rows: Vec<(i64, String)> = sqlx::query_as(&format!(
            "SELECT migration_version, section_name FROM {} \
             WHERE NOT is_baseline AND status = 'completed'",
            self.sections,
        ))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(v, n)| (v as u64, n)).collect())
    }

    /// The "applied migrations" listing on a target that HAS the section
    /// table: `(version, description, applied_at, incomplete_count,
    /// is_baseline, consumed_re_anchor)`, version-ordered. `incomplete_count`
    /// is the number of the version's section rows that are not covered
    /// (in-progress or failed). `consumed_re_anchor` is true for a baseline row
    /// with zero section rows — a version-level event that applied no content
    /// here (a crossing consumed a re-anchor whose remaps relabeled nothing this
    /// target holds). Status renders it as consumed, not applied, so a
    /// zero-trace re-tag crossing doesn't masquerade as provisioned content.
    pub async fn migration_listing(&self) -> Result<Vec<(i64, String, String, i64, bool, bool)>> {
        let rows = sqlx::query_as(&format!(
            "SELECT m.version, m.description, m.applied_at::TEXT,
                    COUNT(s.section_name) FILTER (WHERE NOT ({cov})) AS incomplete,
                    m.is_baseline,
                    (m.is_baseline AND COUNT(s.section_name) = 0) AS consumed_re_anchor
             FROM {main} m
             LEFT JOIN {sections} s
               ON s.migration_version = m.version AND s.is_baseline = m.is_baseline
             GROUP BY m.version, m.is_baseline, m.description, m.applied_at
             ORDER BY m.version",
            main = self.main,
            sections = self.sections,
            cov = Self::covered_predicate("s.status"),
        ))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// The "applied migrations" listing on a LEGACY target (main table, no
    /// section table): every recorded version is treated as fully applied
    /// (`incomplete_count` = 0). Read-only status never evolves the schema. No
    /// section table means no crossing has ever run here, so no row is a
    /// consumed re-anchor.
    pub async fn migration_listing_legacy(
        &self,
    ) -> Result<Vec<(i64, String, String, i64, bool, bool)>> {
        let rows: Vec<(i64, String, String, bool)> = sqlx::query_as(&format!(
            "SELECT version, description, applied_at::TEXT, is_baseline FROM {} ORDER BY version",
            self.main
        ))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(v, d, a, b)| (v, d, a, 0, b, false))
            .collect())
    }

    /// Every recorded section row as `(owning module literal, is_baseline,
    /// status)` — the input to the per-module status rollup. Reads the stored
    /// `module` column (authoritative), so `None` is a base section.
    pub async fn section_module_statuses(
        &self,
    ) -> Result<Vec<(Option<String>, bool, SectionStatus)>> {
        let rows: Vec<(Option<String>, bool, String)> = sqlx::query_as(&format!(
            "SELECT module, is_baseline, status FROM {}",
            self.sections
        ))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|(module, is_baseline, status)| {
                Ok((module, is_baseline, SectionStatus::from_str(&status)?))
            })
            .collect()
    }

    /// The individual non-covered baseline SECTION rows — a crashed or
    /// otherwise incomplete provision, resolved to `(version, section_name,
    /// module)` so the caller can scope its response by module.
    pub async fn incomplete_baseline_sections(&self) -> Result<Vec<(u64, String, Option<String>)>> {
        let rows: Vec<(i64, String, Option<String>)> = sqlx::query_as(&format!(
            "SELECT migration_version, section_name, module FROM {} \
             WHERE is_baseline AND NOT ({})",
            self.sections,
            Self::covered_predicate("status"),
        ))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(v, name, module)| (v as u64, name, module))
            .collect())
    }

    // --- Stored module subscription ------------------------------------------
    //
    // "Which modules does this target have?" is target state — the same
    // species of fact as "which migrations ran" — so establishment is stored
    // in the `_modules` table beside the tracking table, never derived. The
    // re-anchor consumption cursor is NOT stored: it is derived from the
    // baseline main rows ([`Self::consumed_through_cursor`]). Read methods run
    // on the pool; writers take an `impl PgExecutor` so a caller can run them
    // inside its own crossing/provision transaction.

    /// Create the subscription table if absent (idempotent). Part of the
    /// "migrate the migrator" evolve step; safe to call repeatedly. Born in its
    /// current shape, so there is no column back-fill (yet) — future shape
    /// changes add guarded `ALTER`s here, exactly like the other tracking
    /// tables.
    ///
    /// `{name}_modules` is the current module set: one row per subscribed
    /// module. Empty = base only (correct for both a legacy target and a
    /// fresh-minimal one; no legacy heuristic). The base is never listed — it
    /// is established on every target.
    pub async fn ensure_subscription_tables(&self) -> Result<()> {
        sqlx::query(&format!(
            r#"CREATE TABLE IF NOT EXISTS {modules} (
                module TEXT PRIMARY KEY,
                adopted_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                adopted_by TEXT NOT NULL DEFAULT CURRENT_USER,
                source TEXT NOT NULL
            )"#,
            modules = self.modules
        ))
        .execute(&self.pool)
        .await
        .with_context(|| format!("Failed to create subscription table {}", self.modules))?;

        Ok(())
    }

    /// The re-anchor consumed-through cursor: the highest baseline version
    /// recorded on this target, whether a provision applied it or a crossing
    /// consumed it (both write a baseline main row). Re-anchors at or below
    /// this are consumed or moot; the crossing loop keys off re-anchors
    /// strictly above it. `None` = no baseline row (a legacy target that has
    /// never provisioned or crossed). This is a DERIVED value — the cursor is
    /// not stored, so provision seeding it is just another baseline row, not a
    /// special rule.
    pub async fn consumed_through_cursor(&self) -> Result<Option<u64>> {
        let cursor: Option<i64> = sqlx::query_scalar(&format!(
            "SELECT MAX(version) FROM {} WHERE is_baseline = TRUE",
            self.main
        ))
        .fetch_one(&self.pool)
        .await
        .context("Failed to read the consumed-through cursor")?;
        Ok(cursor.map(version_from_db))
    }

    /// Record that this target consumed the re-anchor at `version`: upsert its
    /// baseline main row with the file `checksum`. Consumption is a
    /// what-happened fact, recorded where they live — the baseline row itself is
    /// the record; no marker column distinguishes it, and its zero (or
    /// `satisfied`) section rows carry the trace of what the crossing relabeled.
    /// On a row a provision or adoption already wrote, the upsert is a no-op that
    /// preserves the existing row. Either way the stored checksum MUST equal
    /// `checksum` — per-artifact immutability extends to crossings, so an edited
    /// re-anchor is detectable against every target that consumed it. Runs on
    /// the caller's executor (the crossing transaction); a mismatch bails and
    /// the transaction rolls back.
    pub async fn record_crossing_consumption<'e>(
        &self,
        executor: impl sqlx::PgExecutor<'e>,
        version: u64,
        checksum: &str,
    ) -> Result<()> {
        // The upsert returns the row's resulting checksum: our own on a fresh
        // insert, the pre-existing one on conflict (the self-assignment leaves
        // it untouched but still returns the row).
        let stored: String = sqlx::query_scalar(&format!(
            "INSERT INTO {main} (version, description, checksum, is_baseline)
             VALUES ($1, 'baseline', $2, TRUE)
             ON CONFLICT (version, is_baseline)
             DO UPDATE SET checksum = {main}.checksum
             RETURNING checksum",
            main = self.main
        ))
        .bind(version_to_db(version)?)
        .bind(checksum)
        .fetch_one(executor)
        .await
        .with_context(|| format!("Failed to record consumption of re-anchor {version}"))?;

        if stored != checksum {
            anyhow::bail!(
                "re-anchor {version} was edited after it was consumed here.\n\
                 Recorded checksum: {stored}\n\
                 Current file:      {checksum}\n\n\
                 A consumed re-anchor is immutable — its remap instructions are pinned so \
                 every target that crossed it agrees on what moved. Restore \
                 baseline_{version}.sql to the consumed content, or reset this target and \
                 re-provision from the current baseline."
            );
        }
        Ok(())
    }

    /// Record `sections` of the baseline at `version` as `satisfied` on the
    /// caller's connection (the crossing transaction): a remap section whose
    /// source the target already holds, so nothing ran — the objects are
    /// present under the source's name and the crossing relabeled them.
    /// Insert-then-mark; an existing row keeps its status (a completed section
    /// is never demoted). Takes `&mut PgConnection` because each section needs
    /// two statements sharing the transaction.
    pub async fn record_sections_satisfied(
        &self,
        conn: &mut sqlx::PgConnection,
        version: u64,
        is_baseline: bool,
        sections: &[(i32, MigrationSection)],
    ) -> Result<()> {
        for (order, section) in sections {
            crate::migration_tracking::section_tracking::insert_pending_section(
                &mut *conn,
                &self.sections,
                version,
                is_baseline,
                *order,
                section,
            )
            .await?;
            sqlx::query(&format!(
                "UPDATE {} SET status = $1, completed_at = NOW(), applied_by = CURRENT_USER
                 WHERE migration_version = $2 AND is_baseline = $3 AND section_name = $4
                   AND status = 'pending'",
                self.sections
            ))
            .bind(SectionStatus::Satisfied.as_str())
            .bind(version_to_db(version)?)
            .bind(is_baseline)
            .bind(&section.name)
            .execute(&mut *conn)
            .await?;
        }
        Ok(())
    }

    /// Whether the subscription tables exist, without creating them (the
    /// read-only `migrate status` path must not evolve the schema).
    pub async fn subscription_tables_exist(&self) -> Result<bool> {
        let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(&self.modules)
            .fetch_one(&self.pool)
            .await?;
        Ok(exists)
    }

    /// Load the stored subscription (the module set). Assumes the table
    /// exists; read-only callers guard with [`Self::subscription_tables_exist`]
    /// first. The consumed-through cursor is not part of this — it is derived
    /// via [`Self::consumed_through_cursor`].
    pub async fn load_subscription(&self) -> Result<Subscription> {
        let modules: Vec<String> =
            sqlx::query_scalar(&format!("SELECT module FROM {}", self.modules))
                .fetch_all(&self.pool)
                .await
                .context("Failed to read the module subscription")?;

        Ok(Subscription {
            modules: modules.into_iter().collect(),
        })
    }

    /// Subscribe `module` (idempotent — an existing row keeps its original
    /// `source`/`adopted_at`). Runs on the caller's executor so it can share
    /// the crossing/provision transaction.
    pub async fn add_module<'e>(
        &self,
        executor: impl sqlx::PgExecutor<'e>,
        module: &str,
        source: &SubscriptionSource,
    ) -> Result<()> {
        sqlx::query(&format!(
            "INSERT INTO {} (module, source) VALUES ($1, $2)
             ON CONFLICT (module) DO NOTHING",
            self.modules
        ))
        .bind(module)
        .bind(source.as_db_string())
        .execute(executor)
        .await
        .with_context(|| format!("Failed to subscribe module '{module}'"))?;
        Ok(())
    }

    /// Unsubscribe `module` (idempotent). Runs on the caller's executor.
    pub async fn remove_module<'e>(
        &self,
        executor: impl sqlx::PgExecutor<'e>,
        module: &str,
    ) -> Result<()> {
        sqlx::query(&format!("DELETE FROM {} WHERE module = $1", self.modules))
            .bind(module)
            .execute(executor)
            .await
            .with_context(|| format!("Failed to unsubscribe module '{module}'"))?;
        Ok(())
    }
}
