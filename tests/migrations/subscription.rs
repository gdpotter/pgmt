//! Storage-layer tests for the stored module subscription:
//! the evolve step is idempotent, the two tables round-trip, and the
//! watermark is an explicit stored value (never derived).

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
                watermark: None,
            },
            "empty modules + no watermark = base only, no legacy heuristic"
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

/// The crossing watermark is a single explicit stored row: absent until first
/// set, then advanced in place by the upsert.
#[tokio::test]
async fn test_watermark_is_explicit_single_row() {
    with_test_db(async |db| {
        let tt = tracking();
        let store = TrackingStore::new(db.pool(), &tt).unwrap();
        store.ensure_subscription_tables().await.unwrap();

        // No watermark row yet.
        assert_eq!(store.load_subscription().await.unwrap().watermark, None);

        store.set_watermark(db.pool(), 1200).await.unwrap();
        assert_eq!(
            store.load_subscription().await.unwrap().watermark,
            Some(1200)
        );

        // The upsert advances it in place (single row).
        store.set_watermark(db.pool(), 1500).await.unwrap();
        assert_eq!(
            store.load_subscription().await.unwrap().watermark,
            Some(1500)
        );
        let count: i64 =
            sqlx::query_scalar(r#"SELECT COUNT(*) FROM "public"."pgmt_migrations_watermark""#)
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(count, 1, "watermark table stays single-row");
    })
    .await;
}
