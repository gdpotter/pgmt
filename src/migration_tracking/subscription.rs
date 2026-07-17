//! The stored module **subscription**.
//!
//! "Which modules does this target have?" is target state — the same species
//! of fact as "which migrations ran". Establishment is therefore **stored**,
//! not derived, in three tables beside the tracking table:
//!
//! - `{name}_modules` — the current module set: one row per subscribed module
//!   (`module PK, adopted_at, adopted_by, source`). Empty = **base only**
//!   (correct for both a legacy target and a fresh-minimal one; no legacy
//!   heuristic). The base is never listed — it is established on every target.
//! - `{name}_watermark` — a single-row table holding the **re-anchor crossing
//!   watermark** as its own explicit value. NEVER derived as `max()` over the
//!   event stream (that would make audit load-bearing). A missing row means
//!   "no crossing consumed yet" — a pre-subscription / legacy target.
//! - `{name}_events` — an append-only audit stream. Crossings are recorded as
//!   `(event='crossing', version, subscription_before, subscription_after,
//!   occurred_at, performed_by)`; future de-provision events reuse the shape.
//!
//! Every writer runs under the migration advisory lock (see
//! [`crate::migration_tracking::MigrationLock`]); the tables are created by the
//! same idempotent "migrate the migrator" evolve step as the rest of the
//! tracking schema (via [`ensure_subscription_tables`], hooked into
//! `ensure_section_tracking_table`). `migrate status --target-url` is
//! read-only and never evolves — it probes with [`subscription_tables_exist`]
//! and degrades gracefully on a pre-subscription target.

use crate::config::types::TrackingTable;
use crate::migration_tracking::version_to_db;
use anyhow::{Context, Result};
use sqlx::PgPool;
use std::collections::BTreeSet;

/// How a module came to be in the subscription — recorded on the
/// `{name}_modules` row's `source` column for audit.
#[derive(Debug, Clone)]
pub enum SubscriptionSource {
    /// Written by `provision --modules` on a fresh target.
    Provision,
    /// Written by adoption (`apply`/`provision --modules X`).
    Adopt,
    /// Written by the crossing loop when a re-anchor at `version` subscribed
    /// the module.
    Crossing(u64),
}

impl SubscriptionSource {
    pub fn as_db_string(&self) -> String {
        match self {
            Self::Provision => "provision".to_string(),
            Self::Adopt => "adopt".to_string(),
            Self::Crossing(version) => format!("crossing:{version}"),
        }
    }
}

/// The target's stored module subscription.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Subscription {
    /// Subscribed modules (the base is always established and never listed).
    pub modules: BTreeSet<String>,
    /// The re-anchor crossing watermark. `None` = no crossing consumed yet
    /// (a legacy / pre-subscription target).
    pub watermark: Option<u64>,
}

fn modules_table(tracking_table: &TrackingTable) -> String {
    format!(
        r#""{}"."{}_modules""#,
        tracking_table.schema, tracking_table.name
    )
}

fn watermark_table(tracking_table: &TrackingTable) -> String {
    format!(
        r#""{}"."{}_watermark""#,
        tracking_table.schema, tracking_table.name
    )
}

fn events_table(tracking_table: &TrackingTable) -> String {
    format!(
        r#""{}"."{}_events""#,
        tracking_table.schema, tracking_table.name
    )
}

/// Serialize a module set as the audit `subscription_before/after` value: a
/// comma-joined sorted list, or the literal `(base only)` when empty.
pub fn render_subscription_set(modules: &BTreeSet<String>) -> String {
    if modules.is_empty() {
        "(base only)".to_string()
    } else {
        modules.iter().cloned().collect::<Vec<_>>().join(",")
    }
}

/// Create the subscription tables if absent (idempotent). Part of the
/// "migrate the migrator" evolve step; safe to call repeatedly. These tables
/// are born in their current shape, so there is no column back-fill (yet) —
/// future shape changes add guarded `ALTER`s here, exactly like the other
/// tracking tables.
pub async fn ensure_subscription_tables(
    pool: &PgPool,
    tracking_table: &TrackingTable,
) -> Result<()> {
    let modules = modules_table(tracking_table);
    let watermark = watermark_table(tracking_table);
    let events = events_table(tracking_table);

    sqlx::query(&format!(
        r#"CREATE TABLE IF NOT EXISTS {modules} (
            module TEXT PRIMARY KEY,
            adopted_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            adopted_by TEXT NOT NULL DEFAULT CURRENT_USER,
            source TEXT NOT NULL
        )"#,
        modules = modules
    ))
    .execute(pool)
    .await
    .with_context(|| format!("Failed to create subscription table {modules}"))?;

    // Single-row watermark: an explicit stored value, never derived from the
    // event stream. The CHECK pins it to one row.
    sqlx::query(&format!(
        r#"CREATE TABLE IF NOT EXISTS {watermark} (
            singleton BOOLEAN PRIMARY KEY DEFAULT TRUE,
            crossing_watermark BIGINT NOT NULL,
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CHECK (singleton)
        )"#,
        watermark = watermark
    ))
    .execute(pool)
    .await
    .with_context(|| format!("Failed to create watermark table {watermark}"))?;

    sqlx::query(&format!(
        r#"CREATE TABLE IF NOT EXISTS {events} (
            id BIGSERIAL PRIMARY KEY,
            event TEXT NOT NULL,
            version BIGINT,
            subscription_before TEXT,
            subscription_after TEXT,
            occurred_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            performed_by TEXT NOT NULL DEFAULT CURRENT_USER
        )"#,
        events = events
    ))
    .execute(pool)
    .await
    .with_context(|| format!("Failed to create events table {events}"))?;

    Ok(())
}

/// Whether the subscription tables exist, without creating them. Used by the
/// read-only `migrate status --target-url` path, which must not evolve.
pub async fn subscription_tables_exist(
    pool: &PgPool,
    tracking_table: &TrackingTable,
) -> Result<bool> {
    let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(modules_table(tracking_table))
        .fetch_one(pool)
        .await?;
    Ok(exists)
}

/// Load the stored subscription (module set + watermark). Assumes the tables
/// exist; callers on the read-only path must guard with
/// [`subscription_tables_exist`] first.
pub async fn load_subscription(
    pool: &PgPool,
    tracking_table: &TrackingTable,
) -> Result<Subscription> {
    let modules: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT module FROM {}",
        modules_table(tracking_table)
    ))
    .fetch_all(pool)
    .await
    .context("Failed to read the module subscription")?;

    let watermark: Option<i64> = sqlx::query_scalar(&format!(
        "SELECT crossing_watermark FROM {} LIMIT 1",
        watermark_table(tracking_table)
    ))
    .fetch_optional(pool)
    .await
    .context("Failed to read the crossing watermark")?;

    Ok(Subscription {
        modules: modules.into_iter().collect(),
        watermark: watermark.map(crate::migration_tracking::version_from_db),
    })
}

/// Set the crossing watermark to `version` (upsert the single row). Runs on
/// the caller's executor so it can share the crossing/provision transaction.
pub async fn set_watermark<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    tracking_table: &TrackingTable,
    version: u64,
) -> Result<()> {
    sqlx::query(&format!(
        "INSERT INTO {} (singleton, crossing_watermark, updated_at)
         VALUES (TRUE, $1, NOW())
         ON CONFLICT (singleton)
         DO UPDATE SET crossing_watermark = EXCLUDED.crossing_watermark, updated_at = NOW()",
        watermark_table(tracking_table)
    ))
    .bind(version_to_db(version)?)
    .execute(executor)
    .await
    .context("Failed to set the crossing watermark")?;
    Ok(())
}

/// Subscribe `module` (idempotent — an existing row keeps its original
/// `source`/`adopted_at`).
pub async fn add_module<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    tracking_table: &TrackingTable,
    module: &str,
    source: &SubscriptionSource,
) -> Result<()> {
    sqlx::query(&format!(
        "INSERT INTO {} (module, source) VALUES ($1, $2)
         ON CONFLICT (module) DO NOTHING",
        modules_table(tracking_table)
    ))
    .bind(module)
    .bind(source.as_db_string())
    .execute(executor)
    .await
    .with_context(|| format!("Failed to subscribe module '{module}'"))?;
    Ok(())
}

/// Unsubscribe `module` (idempotent).
pub async fn remove_module<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    tracking_table: &TrackingTable,
    module: &str,
) -> Result<()> {
    sqlx::query(&format!(
        "DELETE FROM {} WHERE module = $1",
        modules_table(tracking_table)
    ))
    .bind(module)
    .execute(executor)
    .await
    .with_context(|| format!("Failed to unsubscribe module '{module}'"))?;
    Ok(())
}

/// Append one row to the audit event stream.
pub async fn record_event<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    tracking_table: &TrackingTable,
    event: &str,
    version: Option<u64>,
    before: &BTreeSet<String>,
    after: &BTreeSet<String>,
) -> Result<()> {
    sqlx::query(&format!(
        "INSERT INTO {} (event, version, subscription_before, subscription_after)
         VALUES ($1, $2, $3, $4)",
        events_table(tracking_table)
    ))
    .bind(event)
    .bind(version.map(version_to_db).transpose()?)
    .bind(render_subscription_set(before))
    .bind(render_subscription_set(after))
    .execute(executor)
    .await
    .context("Failed to append a subscription event")?;
    Ok(())
}
