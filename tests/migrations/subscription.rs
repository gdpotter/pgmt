//! Storage-layer tests for the stored module subscription:
//! the evolve step is idempotent, the module set round-trips, and the
//! consumed-through cursor is derived from the baseline main rows (never
//! stored).

use crate::helpers::harness::with_test_db;
use pgmt::config::types::TrackingTable;
use pgmt::migration_tracking::TrackingStore;
use pgmt::migration_tracking::ensure_section_tracking_table;
use pgmt::modules::{Subscription, SubscriptionSource};
use std::collections::BTreeSet;

fn tracking() -> TrackingTable {
    TrackingTable {
        schema: "public".to_string(),
        name: "pgmt_migrations".to_string(),
    }
}

fn set(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|s| s.to_string()).collect()
}

#[tokio::test]
async fn test_evolve_step_creates_subscription_tables_idempotently() {
    with_test_db(async |db| {
        let tt = tracking();
        let store = TrackingStore::new(db.pool(), &tt).unwrap();
        // No tables yet: the read-only probe reports absence.
        assert!(!store.subscription_tables_exist().await.unwrap());

        // The section-tracking evolve step creates the subscription tables.
        ensure_section_tracking_table(db.pool(), &tt).await.unwrap();
        assert!(store.subscription_tables_exist().await.unwrap());

        // Idempotent: running the evolve step (and the dedicated helper) again
        // is a no-op that preserves data.
        store
            .add_module(db.pool(), "app", &SubscriptionSource::Provision)
            .await
            .unwrap();
        ensure_section_tracking_table(db.pool(), &tt).await.unwrap();
        store.ensure_subscription_tables().await.unwrap();

        let sub = store.load_subscription().await.unwrap();
        assert_eq!(sub.modules, set(&["app"]));
    })
    .await;
}

/// The incomplete-baseline guard's covered set: a `satisfied` baseline
/// section is covered exactly like `completed` (its objects are present under
/// the source's name — a per-section adoption record), so it is NOT reported
/// as incomplete. A genuinely crashed provision — a `failed` or `pending` row
/// — still is, so the guard keeps blocking it.
#[tokio::test]
async fn test_satisfied_baseline_section_is_covered_crashed_still_blocks() {
    with_test_db(async |db| {
        let tt = tracking();
        ensure_section_tracking_table(db.pool(), &tt).await.unwrap();

        // A baseline at version 1400 with one section per terminal/covered
        // state plus one genuinely crashed (failed) module section.
        for (name, status, module) in [
            ("app", "completed", Some("app")),
            ("billing", "satisfied", Some("billing")),
            ("analytics", "failed", Some("analytics")),
        ] {
            sqlx::query(
                "INSERT INTO public.pgmt_migrations_sections
                     (migration_version, is_baseline, section_name, section_order, status, module)
                 VALUES (1400, TRUE, $1, 0, $2, $3)",
            )
            .bind(name)
            .bind(status)
            .bind(module)
            .execute(db.pool())
            .await
            .unwrap();
        }

        let store = TrackingStore::new(db.pool(), &tt).unwrap();
        let incomplete = store.incomplete_baseline_sections().await.unwrap();
        // Only the crashed analytics section is incomplete — completed and
        // satisfied are both covered.
        assert_eq!(incomplete.len(), 1, "{incomplete:?}");
        assert_eq!(
            incomplete[0],
            (1400, "analytics".to_string(), Some("analytics".to_string()))
        );
    })
    .await;
}

#[tokio::test]
async fn test_empty_subscription_is_base_only() {
    with_test_db(async |db| {
        let tt = tracking();
        let store = TrackingStore::new(db.pool(), &tt).unwrap();
        store.ensure_subscription_tables().await.unwrap();
        let sub = store.load_subscription().await.unwrap();
        assert_eq!(
            sub,
            Subscription {
                modules: BTreeSet::new(),
            },
            "empty modules = base only, no legacy heuristic"
        );
    })
    .await;
}

#[tokio::test]
async fn test_module_set_round_trips() {
    with_test_db(async |db| {
        let tt = tracking();
        let store = TrackingStore::new(db.pool(), &tt).unwrap();
        store.ensure_subscription_tables().await.unwrap();

        store
            .add_module(db.pool(), "billing", &SubscriptionSource::Provision)
            .await
            .unwrap();
        store
            .add_module(db.pool(), "analytics", &SubscriptionSource::Crossing(1200))
            .await
            .unwrap();
        // Idempotent: re-adding keeps the original source.
        store
            .add_module(db.pool(), "billing", &SubscriptionSource::Adopt)
            .await
            .unwrap();

        let sub = store.load_subscription().await.unwrap();
        assert_eq!(sub.modules, set(&["analytics", "billing"]));

        // The audit source is preserved from the first insert.
        let source: String = sqlx::query_scalar(
            r#"SELECT source FROM "public"."pgmt_migrations_modules" WHERE module = 'billing'"#,
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(source, "provision");
        let source: String = sqlx::query_scalar(
            r#"SELECT source FROM "public"."pgmt_migrations_modules" WHERE module = 'analytics'"#,
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(source, "crossing:1200");

        store.remove_module(db.pool(), "billing").await.unwrap();
        let sub = store.load_subscription().await.unwrap();
        assert_eq!(sub.modules, set(&["analytics"]));
    })
    .await;
}

/// A crossing-only baseline row — zero section rows (a zero-trace re-tag that
/// relabeled nothing this target holds) — is NOT applied content: it must not
/// make the target look established, and its absence of section rows is not a
/// crashed provision, so the incomplete-baseline guard must not trip on it.
/// Zero registered sections ≠ crashed provision.
#[tokio::test]
async fn test_crossing_only_row_is_not_established_and_does_not_trip_guard() {
    with_test_db(async |db| {
        let tt = tracking();
        pgmt::migration_tracking::ensure_tracking_table_exists(db.pool(), &tt)
            .await
            .unwrap();
        ensure_section_tracking_table(db.pool(), &tt).await.unwrap();
        let store = TrackingStore::new(db.pool(), &tt).unwrap();

        // A crossing consumed re-anchor 1200 but relabeled nothing: the main
        // row is written with zero section rows.
        let mut conn = db.pool().acquire().await.unwrap();
        store
            .record_crossing_consumption(&mut *conn, 1200, "checksum-1200")
            .await
            .unwrap();

        // A subsequent evolve must NOT forge a `default` completed section row
        // for this zero-section row: the backfill is a one-time action of the
        // evolve that introduces section tracking, and the table already exists.
        ensure_section_tracking_table(db.pool(), &tt).await.unwrap();
        let section_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM public.pgmt_migrations_sections WHERE migration_version = 1200",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(
            section_count, 0,
            "zero-trace crossing row keeps zero sections"
        );

        // Not established: a crossing-only row applied no content.
        assert!(
            !store.target_is_established().await.unwrap(),
            "a consumed-re-anchor row is not applied content"
        );
        // Not a crashed provision: zero section rows ≠ incomplete baseline.
        assert!(
            store
                .incomplete_baseline_sections()
                .await
                .unwrap()
                .is_empty(),
            "a crossing-only row does not trip the incomplete-baseline guard"
        );
    })
    .await;
}

/// The consumed-through cursor is derived, never stored: the highest baseline
/// version in the main table, whether provision applied it or a crossing
/// consumed it. No baseline rows → `None`.
#[tokio::test]
async fn test_consumed_cursor_is_derived_from_baseline_rows() {
    with_test_db(async |db| {
        let tt = tracking();
        pgmt::migration_tracking::ensure_tracking_table_exists(db.pool(), &tt)
            .await
            .unwrap();
        ensure_section_tracking_table(db.pool(), &tt).await.unwrap();
        let store = TrackingStore::new(db.pool(), &tt).unwrap();

        // No baseline rows yet.
        assert_eq!(store.consumed_through_cursor().await.unwrap(), None);

        // A crossing consumes re-anchor 1200: it upserts the baseline main row
        // with its checksum. The cursor picks it up.
        let mut conn = db.pool().acquire().await.unwrap();
        store
            .record_crossing_consumption(&mut *conn, 1200, "checksum-1200")
            .await
            .unwrap();
        assert_eq!(store.consumed_through_cursor().await.unwrap(), Some(1200));

        // A provision-applied baseline row at 1500 is a load-bearing cursor row
        // exactly the same way — the cursor is MAX, regardless of how the row
        // arrived.
        sqlx::query(
            "INSERT INTO public.pgmt_migrations (version, description, checksum, is_baseline) \
             VALUES (1500, 'baseline', 'checksum-1500', TRUE)",
        )
        .execute(db.pool())
        .await
        .unwrap();
        assert_eq!(store.consumed_through_cursor().await.unwrap(), Some(1500));

        // Re-consuming 1200 with the SAME checksum is idempotent (keeps the
        // row); a DIFFERENT checksum is an edited re-anchor and is rejected.
        store
            .record_crossing_consumption(&mut *conn, 1200, "checksum-1200")
            .await
            .unwrap();
        let err = store
            .record_crossing_consumption(&mut *conn, 1200, "edited-checksum")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("edited after it was consumed"), "{err}");
    })
    .await;
}

/// The synthetic-legacy backfill is a ONE-TIME action of the evolve that
/// introduces section tracking — never a standing per-row rule. A baseline row
/// with zero section rows created AFTER the section table already exists is a
/// legitimate ledger state (a version-level event that processed no local work)
/// and must survive every subsequent evolve untouched: no `default` completed
/// section is ever forged onto it.
#[tokio::test]
async fn test_zero_section_row_after_introduction_is_never_backfilled() {
    with_test_db(async |db| {
        let tt = tracking();
        pgmt::migration_tracking::ensure_tracking_table_exists(db.pool(), &tt)
            .await
            .unwrap();
        // Introduce section tracking first (this evolve is allowed to backfill,
        // but the main table is empty so there is nothing to backfill).
        ensure_section_tracking_table(db.pool(), &tt).await.unwrap();
        let store = TrackingStore::new(db.pool(), &tt).unwrap();

        // Now — with the section table already in existence — write a
        // zero-section baseline row (an irrelevant crossing consumption).
        let mut conn = db.pool().acquire().await.unwrap();
        store
            .record_crossing_consumption(&mut *conn, 2000, "checksum-2000")
            .await
            .unwrap();

        // Any number of later evolve passes must leave it at zero sections.
        for _ in 0..3 {
            ensure_section_tracking_table(db.pool(), &tt).await.unwrap();
        }
        let section_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM public.pgmt_migrations_sections WHERE migration_version = 2000",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(
            section_count, 0,
            "a post-introduction zero-section row is never backfilled"
        );
    })
    .await;
}

/// The legacy upgrade path: a target with a populated main table but NO section
/// table (pre-per-section-tracking history). The evolve that introduces the
/// section table backfills exactly one `default` completed section per existing
/// main row — once — so applied-ness becomes a derived function of section
/// rows. Re-running the evolve does not duplicate them.
#[tokio::test]
async fn test_legacy_upgrade_backfills_synthetic_sections_once() {
    with_test_db(async |db| {
        let tt = tracking();
        // A pre-per-section-tracking target: the main table exists and carries
        // history, but the section table does not yet exist.
        pgmt::migration_tracking::ensure_tracking_table_exists(db.pool(), &tt)
            .await
            .unwrap();
        for (version, is_baseline) in [(1000i64, false), (1100, false), (1200, true)] {
            sqlx::query(
                "INSERT INTO public.pgmt_migrations (version, description, checksum, is_baseline) \
                 VALUES ($1, 'legacy', 'legacy-checksum', $2)",
            )
            .bind(version)
            .bind(is_baseline)
            .execute(db.pool())
            .await
            .unwrap();
        }

        // The introducing evolve backfills one synthetic default section per row.
        ensure_section_tracking_table(db.pool(), &tt).await.unwrap();
        let rows: Vec<(i64, String, String)> = sqlx::query_as(
            "SELECT migration_version, section_name, status \
             FROM public.pgmt_migrations_sections ORDER BY migration_version",
        )
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(
            rows,
            vec![
                (1000, "default".to_string(), "completed".to_string()),
                (1100, "default".to_string(), "completed".to_string()),
                (1200, "default".to_string(), "completed".to_string()),
            ],
            "every legacy main row gets one synthetic default completed section"
        );

        // Idempotent: a second evolve does not duplicate the synthetic rows.
        ensure_section_tracking_table(db.pool(), &tt).await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM public.pgmt_migrations_sections")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(count, 3, "backfill is one-time, not repeated per evolve");
    })
    .await;
}
