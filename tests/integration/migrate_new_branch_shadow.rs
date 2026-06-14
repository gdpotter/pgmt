//! Regression test: `migrate new` on a branch-provisioned shadow must give
//! every pristine-start phase its own fresh branch.
//!
//! `clean_shadow_db` is a no-op on branch shadows (they are fresh copies of an
//! untouched source). `migrate new` runs up to three pristine-start phases
//! against the shadow — reconstruct history (replay), build desired state, and
//! validate the baseline. A shared branch carries the replay's objects into the
//! schema-file apply, so the first overlapping `CREATE` fails with "already
//! exists". The fix provisions and reclaims a fresh branch per phase; this test
//! is the guard. Repros without Docker via `reset: branch`, which marks its
//! branches provisioned exactly like the Docker auto shadow.

use crate::helpers::harness::with_test_db;
use anyhow::Result;
use pgmt::commands::cmd_migrate_new;
use pgmt::config::Config;
use pgmt::config::types::{ShadowDatabase, ShadowResetMode};
use std::fs;
use tempfile::TempDir;

#[tokio::test]
async fn test_migrate_new_branch_shadow_does_not_reuse_dirty_branch() -> Result<()> {
    let _branch_guard = crate::helpers::BRANCH_TEST_LOCK.lock().await;
    with_test_db(async |db| {
        // A connection-free source database for `reset: branch` to copy from
        // (the harness keeps an open pool on its own database, so it can't be a
        // branch source).
        let source_db = format!("pgmt_src_{}", uuid::Uuid::new_v4().simple());
        db.execute(&format!("CREATE DATABASE \"{}\"", source_db))
            .await;
        let base = db.url();
        let source_url = format!("{}/{}", &base[..base.rfind('/').unwrap()], source_db);

        let shadow = ShadowDatabase::Url {
            url: source_url.clone(),
            reset: ShadowResetMode::Branch,
        };

        // On-disk project; `schema/` holds the desired state.
        let project = TempDir::new().unwrap();
        let root = project.path();
        let schema_dir = root.join("schema");
        fs::create_dir_all(&schema_dir).unwrap();
        fs::write(
            schema_dir.join("users.sql"),
            "CREATE TABLE users (id serial PRIMARY KEY, name text NOT NULL);",
        )
        .unwrap();

        let config = Config::default();

        // First run: no prior migrations, so the replay phase is empty and this
        // succeeds even under the old shared-branch behavior. `--create-baseline`
        // writes a full-schema baseline and exercises the third pristine-start
        // phase — baseline validation — on its own fresh branch.
        cmd_migrate_new(&config, root, Some("create_users"), true, &shadow)
            .await
            .expect("first migrate new should succeed");

        // Distinct timestamp for the second migration/baseline (versions are
        // whole seconds).
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

        // Add a second object so there is a real diff to generate.
        fs::write(
            schema_dir.join("posts.sql"),
            "CREATE TABLE posts (id serial PRIMARY KEY, body text NOT NULL);",
        )
        .unwrap();

        // Second run: the replay phase now reconstructs `users` (from the
        // baseline) into its branch. The desired-state phase must get its OWN
        // fresh branch — otherwise re-applying `users` fails with "already
        // exists". This is the core regression guard. (No `--create-baseline`
        // here: a baseline built from a non-first migration's delta is an
        // unrelated concern.)
        cmd_migrate_new(&config, root, Some("create_posts"), false, &shadow)
            .await
            .expect("second migrate new must not collide on a reused dirty branch");

        // One migration per run.
        let mut migrations: Vec<_> = fs::read_dir(root.join("migrations"))
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|name| name.ends_with(".sql"))
            .collect();
        migrations.sort();
        assert_eq!(
            migrations.len(),
            2,
            "each run should generate one migration: {:?}",
            migrations
        );
        assert!(migrations[1].contains("create_posts"));

        // No `cleanup_all_branches` needed: each phase reclaims its own branch
        // via `drop_branch` inside `cmd_migrate_new`.
        db.execute(&format!(
            "DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)",
            source_db
        ))
        .await;
    })
    .await;

    Ok(())
}
