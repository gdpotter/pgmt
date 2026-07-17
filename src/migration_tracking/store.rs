//! The migration-tracking query surface.
//!
//! Every SQL statement that reads or writes pgmt's tracking tables — the main
//! `pgmt_migrations` table, its `_sections` companion, and the subscription
//! tables (`_modules`, `_watermark`) — is a method on [`TrackingStore`].
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
    /// `"schema"."name_watermark"` — the crossing watermark (single row).
    watermark: String,
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
            watermark: suffixed("watermark"),
        })
    }

    /// `"schema"."name"` — the main tracking table.
    pub fn main_table(&self) -> &str {
        &self.main
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
    /// The crossing-watermark fallback for targets provisioned before the
    /// subscription tables existed, and the coverage floor adoption uses to
    /// decide whether the established modules are caught up.
    pub async fn applied_baseline_watermark(&self) -> Result<Option<u64>> {
        let watermark: Option<i64> = sqlx::query_scalar(&format!(
            "SELECT MAX(m.version) FROM {main} m
             WHERE m.is_baseline AND NOT EXISTS (
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
    /// is_baseline)`, version-ordered. `incomplete_count` is the number of the
    /// version's section rows that are not covered (in-progress or failed).
    pub async fn migration_listing(&self) -> Result<Vec<(i64, String, String, i64, bool)>> {
        let rows = sqlx::query_as(&format!(
            "SELECT m.version, m.description, m.applied_at::TEXT,
                    COUNT(s.section_name) FILTER (WHERE NOT ({cov})) AS incomplete,
                    m.is_baseline
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
    /// (`incomplete_count` = 0). Read-only status never evolves the schema.
    pub async fn migration_listing_legacy(&self) -> Result<Vec<(i64, String, String, i64, bool)>> {
        let rows: Vec<(i64, String, String, bool)> = sqlx::query_as(&format!(
            "SELECT version, description, applied_at::TEXT, is_baseline FROM {} ORDER BY version",
            self.main
        ))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(v, d, a, b)| (v, d, a, 0, b))
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
    // in two tables beside the tracking table (`_modules`, `_watermark`),
    // never derived. Read methods run on the pool; writers take an
    // `impl PgExecutor` so a caller can run them inside its own
    // crossing/provision transaction.

    /// Create the subscription tables if absent (idempotent). Part of the
    /// "migrate the migrator" evolve step; safe to call repeatedly. These
    /// tables are born in their current shape, so there is no column back-fill
    /// (yet) — future shape changes add guarded `ALTER`s here, exactly like
    /// the other tracking tables.
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

        // Single-row crossing watermark: an explicit stored value, never
        // derived. The CHECK pins it to one row; a missing row means "no
        // crossing consumed yet" (a pre-subscription / legacy target).
        sqlx::query(&format!(
            r#"CREATE TABLE IF NOT EXISTS {watermark} (
                singleton BOOLEAN PRIMARY KEY DEFAULT TRUE,
                crossing_watermark BIGINT NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CHECK (singleton)
            )"#,
            watermark = self.watermark
        ))
        .execute(&self.pool)
        .await
        .with_context(|| format!("Failed to create watermark table {}", self.watermark))?;

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

    /// Load the stored subscription (module set + explicit watermark). Assumes
    /// the tables exist; read-only callers guard with
    /// [`Self::subscription_tables_exist`] first.
    pub async fn load_subscription(&self) -> Result<Subscription> {
        let modules: Vec<String> =
            sqlx::query_scalar(&format!("SELECT module FROM {}", self.modules))
                .fetch_all(&self.pool)
                .await
                .context("Failed to read the module subscription")?;

        let watermark: Option<i64> = sqlx::query_scalar(&format!(
            "SELECT crossing_watermark FROM {} LIMIT 1",
            self.watermark
        ))
        .fetch_optional(&self.pool)
        .await
        .context("Failed to read the crossing watermark")?;

        Ok(Subscription {
            modules: modules.into_iter().collect(),
            watermark: watermark.map(version_from_db),
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

    /// Set the crossing watermark to `version` (upsert the single row). Runs
    /// on the caller's executor.
    pub async fn set_watermark<'e>(
        &self,
        executor: impl sqlx::PgExecutor<'e>,
        version: u64,
    ) -> Result<()> {
        sqlx::query(&format!(
            "INSERT INTO {} (singleton, crossing_watermark, updated_at)
             VALUES (TRUE, $1, NOW())
             ON CONFLICT (singleton)
             DO UPDATE SET crossing_watermark = EXCLUDED.crossing_watermark, updated_at = NOW()",
            self.watermark
        ))
        .bind(version_to_db(version)?)
        .execute(executor)
        .await
        .context("Failed to set the crossing watermark")?;
        Ok(())
    }
}
