//! Storage-layer tests for the stored module subscription (§9, §13, §18.3):
//! the evolve step is idempotent, the three tables round-trip, and the
//! watermark is an explicit stored value (never derived from the event
//! stream).

use crate::helpers::harness::with_test_db;
use pgmt::config::types::TrackingTable;
use pgmt::migration_tracking::ensure_section_tracking_table;
use pgmt::migration_tracking::subscription::{
    Subscription, SubscriptionSource, add_module, ensure_subscription_tables, load_subscription,
    record_event, remove_module, set_watermark, subscription_tables_exist,
};
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
        // No tables yet: the read-only probe reports absence.
        assert!(!subscription_tables_exist(db.pool(), &tt).await.unwrap());

        // The section-tracking evolve step creates the subscription tables.
        ensure_section_tracking_table(db.pool(), &tt).await.unwrap();
        assert!(subscription_tables_exist(db.pool(), &tt).await.unwrap());

        // Idempotent: running the evolve step (and the dedicated helper) again
        // is a no-op that preserves data.
        add_module(db.pool(), &tt, "app", &SubscriptionSource::Provision)
            .await
            .unwrap();
        ensure_section_tracking_table(db.pool(), &tt).await.unwrap();
        ensure_subscription_tables(db.pool(), &tt).await.unwrap();

        let sub = load_subscription(db.pool(), &tt).await.unwrap();
        assert_eq!(sub.modules, set(&["app"]));
    })
    .await;
}

#[tokio::test]
async fn test_empty_subscription_is_base_only() {
    with_test_db(async |db| {
        let tt = tracking();
        ensure_subscription_tables(db.pool(), &tt).await.unwrap();
        let sub = load_subscription(db.pool(), &tt).await.unwrap();
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
        ensure_subscription_tables(db.pool(), &tt).await.unwrap();

        add_module(db.pool(), &tt, "billing", &SubscriptionSource::Provision)
            .await
            .unwrap();
        add_module(
            db.pool(),
            &tt,
            "analytics",
            &SubscriptionSource::Crossing(1200),
        )
        .await
        .unwrap();
        // Idempotent: re-adding keeps the original source.
        add_module(db.pool(), &tt, "billing", &SubscriptionSource::Adopt)
            .await
            .unwrap();

        let sub = load_subscription(db.pool(), &tt).await.unwrap();
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

        remove_module(db.pool(), &tt, "billing").await.unwrap();
        let sub = load_subscription(db.pool(), &tt).await.unwrap();
        assert_eq!(sub.modules, set(&["analytics"]));
    })
    .await;
}

#[tokio::test]
async fn test_watermark_is_explicit_not_derived() {
    with_test_db(async |db| {
        let tt = tracking();
        ensure_subscription_tables(db.pool(), &tt).await.unwrap();

        // No watermark row yet.
        assert_eq!(
            load_subscription(db.pool(), &tt).await.unwrap().watermark,
            None
        );

        set_watermark(db.pool(), &tt, 1200).await.unwrap();
        assert_eq!(
            load_subscription(db.pool(), &tt).await.unwrap().watermark,
            Some(1200)
        );

        // Recording an event at a LATER version does NOT move the watermark:
        // the watermark is its own explicit value, never max() over the stream.
        record_event(
            db.pool(),
            &tt,
            "crossing",
            Some(1500),
            &set(&["app"]),
            &set(&["app", "analytics"]),
        )
        .await
        .unwrap();
        assert_eq!(
            load_subscription(db.pool(), &tt).await.unwrap().watermark,
            Some(1200),
            "watermark must not be derived from the event stream"
        );

        // The upsert advances it in place (single row).
        set_watermark(db.pool(), &tt, 1500).await.unwrap();
        assert_eq!(
            load_subscription(db.pool(), &tt).await.unwrap().watermark,
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

#[tokio::test]
async fn test_events_are_append_only_audit() {
    with_test_db(async |db| {
        let tt = tracking();
        ensure_subscription_tables(db.pool(), &tt).await.unwrap();

        record_event(
            db.pool(),
            &tt,
            "crossing",
            Some(1200),
            &BTreeSet::new(),
            &set(&["app", "analytics"]),
        )
        .await
        .unwrap();
        record_event(
            db.pool(),
            &tt,
            "crossing",
            Some(1600),
            &set(&["app", "analytics"]),
            &set(&["app"]),
        )
        .await
        .unwrap();

        let rows: Vec<(String, Option<i64>, String, String)> = sqlx::query_as(
            r#"SELECT event, version, subscription_before, subscription_after
               FROM "public"."pgmt_migrations_events" ORDER BY id"#,
        )
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0],
            (
                "crossing".to_string(),
                Some(1200),
                "(base only)".to_string(),
                "analytics,app".to_string()
            )
        );
        assert_eq!(
            rows[1],
            (
                "crossing".to_string(),
                Some(1600),
                "analytics,app".to_string(),
                "app".to_string()
            )
        );
    })
    .await;
}
