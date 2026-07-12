//! Tests for the "unrecorded migration below the baseline watermark" warning.
//!
//! `migrate apply` skips every migration whose version is at or below a recorded
//! baseline. That is correct for migrations already applied (they have a tracking
//! row) or genuinely covered by the baseline. But a migration merged LATE with a
//! version strictly below an already-recorded baseline watermark — and no tracking
//! row — was never applied anywhere and never will be (established targets skip it;
//! fresh provisions replay only versions after the baseline). apply must warn
//! loudly about that silent-skip hazard while still succeeding.

use crate::helpers::cli::with_cli_helper;
use anyhow::Result;
use predicates::prelude::*;

/// Insert a baseline tracking row directly, mirroring what `migrate provision`
/// records (`is_baseline = TRUE`) without running any baseline SQL.
async fn seed_baseline_row(pool: &sqlx::PgPool, version: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO public.pgmt_migrations (version, description, checksum, is_baseline) \
         VALUES ($1, $2, $3, TRUE)",
    )
    .bind(version)
    .bind("seeded_baseline")
    .bind("seeded-baseline-checksum")
    .execute(pool)
    .await?;
    Ok(())
}

/// A migration merged with a version strictly below a recorded baseline
/// watermark, with no tracking row, is skipped WITH a loud warning — not
/// silently — and apply still succeeds without applying it.
#[tokio::test]
async fn test_apply_warns_on_unrecorded_below_watermark_migration() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        // A normally-applied migration below the eventual watermark: it gets a
        // migration tracking row, so it must stay quiet on later applies.
        helper.write_migration_file(
            "1000000010_add_users.sql",
            "CREATE TABLE users (id SERIAL);\n",
        )?;
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();
        assert!(helper.table_exists_in_dev("public", "users").await?);

        // Record a baseline at version 1000000030 (as provision would).
        let pool = helper.connect_to_dev_db().await?;
        seed_baseline_row(&pool, 1_000_000_030).await?;
        pool.close().await;

        // A migration merged LATE with a version strictly below the watermark and
        // NO tracking row. It was never applied and never will be.
        helper.write_migration_file(
            "1000000020_late_feature.sql",
            "CREATE TABLE late_feature (id INT);\n",
        )?;

        // apply succeeds, warns about the below-watermark migration, and does NOT
        // apply it.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success()
            .stderr(predicate::str::contains("1000000020"))
            .stderr(predicate::str::contains("below the baseline watermark"))
            .stderr(predicate::str::contains("1000000030"))
            .stderr(predicate::str::contains("pgmt migrate update 1000000020"));

        // The late migration was skipped, not applied.
        assert!(
            !helper.table_exists_in_dev("public", "late_feature").await?,
            "below-watermark migration must not be applied"
        );
        // The legitimately-recorded migration below the watermark stayed quiet
        // (its table is still there and no warning was about it).
        assert!(helper.table_exists_in_dev("public", "users").await?);

        Ok(())
    })
    .await
}

/// Migrations legitimately covered by the baseline produce NO watermark warning:
/// one applied normally BEFORE the baseline (has a migration tracking row) and
/// the paired migration AT the baseline version (covered by the baseline row).
#[tokio::test]
async fn test_apply_stays_quiet_for_recorded_covered_migrations() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        // Applied normally before any baseline: gets a migration tracking row.
        helper.write_migration_file(
            "1000000010_add_users.sql",
            "CREATE TABLE users (id SERIAL);\n",
        )?;
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();

        // A baseline recorded AT version 1000000020, paired with a migration file
        // at the same version (the `--create-baseline` case). The migration at the
        // baseline version has no separate migration row — it is covered by the
        // baseline row — and must NOT warn (excluded by the strict `<` boundary).
        let pool = helper.connect_to_dev_db().await?;
        seed_baseline_row(&pool, 1_000_000_020).await?;
        pool.close().await;
        helper.write_migration_file(
            "1000000020_add_posts.sql",
            "CREATE TABLE posts (id SERIAL);\n",
        )?;

        // apply is a clean no-op with no watermark warning: 1000000010 is below
        // the watermark but has a tracking row; 1000000020 is AT the watermark.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success()
            .stderr(predicate::str::contains("below the baseline watermark").not());

        Ok(())
    })
    .await
}
