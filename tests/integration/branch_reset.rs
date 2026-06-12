//! Integration tests for `shadow.url` + `reset: branch` — an external source
//! database pgmt branches from but never writes to.

use crate::helpers::harness::with_test_db;
use anyhow::Result;
use pgmt::config::types::{ShadowDatabase, ShadowResetMode};

#[tokio::test]
async fn test_url_shadow_branch_mode() -> Result<()> {
    with_test_db(async |db| {
        // A dedicated database plays the externally-provisioned source (the
        // harness database itself holds an open pool; the branch source must
        // be connection-free).
        let source_db = format!("pgmt_src_{}", uuid::Uuid::new_v4().simple());
        db.execute(&format!("CREATE DATABASE \"{}\"", source_db))
            .await;
        let base = db.url();
        let source_url = format!("{}/{}", &base[..base.rfind('/').unwrap()], source_db);

        // Baseline state (stands in for image init scripts / platform schemas).
        let pool = sqlx::PgPool::connect(&source_url).await.unwrap();
        sqlx::query("CREATE SCHEMA platform")
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;

        let shadow = ShadowDatabase::Url {
            url: source_url.clone(),
            reset: ShadowResetMode::Branch,
        };

        // Provisioning returns a branch, not the source.
        let branch_url = shadow.get_connection_string().await.unwrap();
        assert_ne!(branch_url, source_url);
        assert!(
            branch_url.contains("pgmt_branch_"),
            "work database should be an ephemeral branch: {}",
            branch_url
        );

        let pool = sqlx::PgPool::connect(&branch_url).await.unwrap();
        let baseline: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'platform')",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(baseline, "branch inherits the source's baseline state");

        // Dirty the branch; the scoped clean must skip it (it's already fresh).
        sqlx::query("CREATE TABLE junk (id int)")
            .execute(&pool)
            .await
            .unwrap();
        pgmt::db::cleaner::clean_shadow_db(&pool, &pgmt::config::types::Objects::default())
            .await
            .unwrap();
        let junk_survives: Option<String> =
            sqlx::query_scalar("SELECT to_regclass('public.junk')::text")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(
            junk_survives.is_some(),
            "clean_shadow_db must no-op on a fresh branch"
        );
        pool.close().await;

        // The source was never written to.
        let pool = sqlx::PgPool::connect(&source_url).await.unwrap();
        let source_junk: Option<String> =
            sqlx::query_scalar("SELECT to_regclass('public.junk')::text")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(source_junk.is_none(), "source must stay pristine");
        pool.close().await;

        // A second provisioning gets a fresh branch with the dirt gone.
        let branch_url_2 = shadow.get_connection_string().await.unwrap();
        assert_ne!(
            branch_url_2, branch_url,
            "each provisioning gets its own branch"
        );
        let pool = sqlx::PgPool::connect(&branch_url_2).await.unwrap();
        let junk: Option<String> = sqlx::query_scalar("SELECT to_regclass('public.junk')::text")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(junk.is_none(), "new branch starts from the pristine source");
        pool.close().await;

        // Exit cleanup drops every branch — the server is left as found.
        pgmt::db::branch::cleanup_all_branches().await.unwrap();
        let branch_name = |url: &str| url.rsplit('/').next().unwrap().to_string();
        let leftover: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = ANY($1))")
                .bind(vec![branch_name(&branch_url), branch_name(&branch_url_2)])
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert!(!leftover, "cleanup must drop this process's branches");

        db.execute(&format!(
            "DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)",
            source_db
        ))
        .await;
    })
    .await;

    Ok(())
}

#[tokio::test]
async fn test_url_shadow_clean_mode_does_not_touch_databases() -> Result<()> {
    with_test_db(async |db| {
        // Default reset: clean must pass the url through and never create
        // branch databases.
        let shadow = ShadowDatabase::Url {
            url: db.url(),
            reset: ShadowResetMode::Clean,
        };
        let url = shadow.get_connection_string().await.unwrap();
        assert_eq!(url, db.url(), "clean mode uses the database as given");
    })
    .await;

    Ok(())
}
