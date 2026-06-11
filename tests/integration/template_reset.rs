//! Integration tests for `shadow.url` + `reset: template` — an external
//! database pgmt is allowed to treat as its own (e.g. a CI service container).

use crate::helpers::harness::with_test_db;
use anyhow::Result;
use pgmt::config::types::{ShadowDatabase, ShadowResetMode};

#[tokio::test]
async fn test_url_shadow_template_reset() -> Result<()> {
    with_test_db(async |db| {
        // A dedicated database plays the externally-provisioned shadow (the
        // harness database itself holds an open pool, which would block the
        // template snapshot).
        let work_db = format!("pgmt_url_shadow_{}", uuid::Uuid::new_v4().simple());
        db.execute(&format!("CREATE DATABASE \"{}\"", work_db)).await;
        let base = db.url();
        let shadow_url = format!("{}/{}", &base[..base.rfind('/').unwrap()], work_db);

        // Baseline state (stands in for image init scripts / platform schemas).
        let pool = sqlx::PgPool::connect(&shadow_url).await.unwrap();
        sqlx::query("CREATE SCHEMA platform")
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;

        let shadow = ShadowDatabase::Url {
            url: shadow_url.clone(),
            reset: ShadowResetMode::Template,
        };

        // First contact snapshots the current state as the baseline.
        let url = shadow.get_connection_string().await.unwrap();
        assert_eq!(url, shadow_url, "url passes through unchanged");

        // Dirty the shadow after the snapshot.
        let pool = sqlx::PgPool::connect(&shadow_url).await.unwrap();
        sqlx::query("CREATE TABLE junk (id int)")
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;

        // Second contact resets from the template.
        shadow.get_connection_string().await.unwrap();

        let pool = sqlx::PgPool::connect(&shadow_url).await.unwrap();
        let junk: Option<String> = sqlx::query_scalar("SELECT to_regclass('public.junk')::text")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(junk.is_none(), "post-baseline state must be discarded");
        let baseline_survives: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'platform')",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(baseline_survives, "first-contact baseline must be preserved");

        // The scoped clean must skip template-provisioned shadows.
        sqlx::query("CREATE SCHEMA post_reset")
            .execute(&pool)
            .await
            .unwrap();
        pgmt::db::cleaner::clean_shadow_db(&pool, &pgmt::config::types::Objects::default())
            .await
            .unwrap();
        let survives: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'post_reset')",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(survives, "clean_shadow_db must no-op after a template reset");
        pool.close().await;

        // Cleanup: the work database and its template.
        db.execute(&format!(
            "DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)",
            work_db
        ))
        .await;
        db.execute(&format!(
            "DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)",
            pgmt::db::template::template_db_name(&work_db)
        ))
        .await;
    })
    .await;

    Ok(())
}

#[tokio::test]
async fn test_url_shadow_clean_mode_does_not_touch_databases() -> Result<()> {
    with_test_db(async |db| {
        // Default reset: clean must never create a template database.
        let shadow = ShadowDatabase::Url {
            url: db.url(),
            reset: ShadowResetMode::Clean,
        };
        shadow.get_connection_string().await.unwrap();

        let template_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname LIKE 'pgmt_template%')",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert!(
            !template_exists,
            "reset: clean must not create template databases"
        );
    })
    .await;

    Ok(())
}
