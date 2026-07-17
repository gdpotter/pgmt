//! Acceptance tests for the stored subscription + crossing loop + wholeness
//! membranes: re-anchors are consumed exactly once per target, at the
//! crossing, rewriting the stored subscription; wholeness failures are strong
//! membranes with adopt-first guidance.
//!
//! Together with `modules_deploy.rs`/`modules_generation.rs` (ordinary
//! migrations, new modules, cross-module drops, coupling, adoption paths,
//! the non-module characterization pin) these cover the module-dynamics grid;
//! this file owns the re-anchor rows (modularization, split, merge, demotion,
//! irrelevant crossings) across LEGACY / SUBSET / BEHIND targets.

use crate::helpers::cli::{CliTestHelper, next_version_tick, with_cli_helper};
use anyhow::Result;
use predicates::prelude::*;

/// Rewrite pgmt.yaml as `base_config` + the given `modules:` block (replacing
/// any previously set block — re-tags change the partition in place).
fn set_modules(helper: &CliTestHelper, base_config: &str, modules_yaml: &str) -> Result<()> {
    let config_path = helper.project_root.join("pgmt.yaml");
    std::fs::write(config_path, format!("{base_config}{modules_yaml}"))?;
    Ok(())
}

fn base_config(helper: &CliTestHelper) -> Result<String> {
    Ok(std::fs::read_to_string(
        helper.project_root.join("pgmt.yaml"),
    )?)
}

/// The single baseline's version (most tests emit exactly one re-anchor).
fn latest_baseline_version(helper: &CliTestHelper) -> Result<u64> {
    let mut versions: Vec<u64> = helper
        .list_baseline_files()?
        .iter()
        .filter_map(|name| {
            name.strip_prefix("baseline_")
                .and_then(|s| s.strip_suffix(".sql"))
                .and_then(|s| s.parse().ok())
        })
        .collect();
    versions.sort_unstable();
    versions
        .last()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("no baseline emitted"))
}

async fn subscription_modules(pool: &sqlx::PgPool) -> Result<Vec<String>> {
    Ok(
        sqlx::query_scalar("SELECT module FROM public.pgmt_migrations_modules ORDER BY module")
            .fetch_all(pool)
            .await?,
    )
}

async fn crossing_watermark(pool: &sqlx::PgPool) -> Result<Option<i64>> {
    Ok(
        sqlx::query_scalar("SELECT crossing_watermark FROM public.pgmt_migrations_watermark")
            .fetch_optional(pool)
            .await?,
    )
}

async fn table_exists(pool: &sqlx::PgPool, table: &str) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables
         WHERE table_schema = 'public' AND table_name = $1)",
    )
    .bind(table)
    .fetch_one(pool)
    .await?)
}

/// End-to-end LEGACY modularization: a LEGACY target (all-default pre-module
/// history) meets a modularization re-anchor. The next bare `apply` consumes
/// the crossing —
/// subscription becomes {app, analytics}, the watermark advances — and later
/// module sections warn as subscribed-but-unrequested until a deploy names
/// them (`--modules all` then runs them).
#[tokio::test]
async fn test_legacy_modularization_crossing_end_to_end() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        let base = base_config(helper)?;

        // Pre-module history: two unmoduled files, applied bare (LEGACY).
        helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL PRIMARY KEY);")?;
        helper.write_schema_file("events.sql", "CREATE TABLE events (id SERIAL PRIMARY KEY);")?;
        helper
            .command()
            .args(["migrate", "new", "initial"])
            .assert()
            .success();
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();

        let pool = helper.connect_to_dev_db().await?;
        assert_eq!(
            subscription_modules(&pool).await?,
            Vec::<String>::new(),
            "legacy target: empty subscription = base only"
        );
        assert_eq!(crossing_watermark(&pool).await?, None);

        // Modularize the existing files: a pure re-tag, no DDL.
        set_modules(
            helper,
            &base,
            "\nmodules:\n  app:\n    paths: [\"schema/users.sql\"]\n  analytics:\n    paths: [\"schema/events.sql\"]\n",
        )?;
        next_version_tick();
        helper
            .command()
            .args(["migrate", "new", "modularize", "--create-baseline"])
            .assert()
            .success();
        let re_anchor_version = latest_baseline_version(helper)?;

        // A later ordinary migration with module-tagged sections.
        helper.write_schema_file(
            "users.sql",
            "CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT);",
        )?;
        helper.write_schema_file(
            "events.sql",
            "CREATE TABLE events (id SERIAL PRIMARY KEY, kind TEXT);",
        )?;
        next_version_tick();
        helper
            .command()
            .args(["migrate", "new", "second"])
            .assert()
            .success();

        // Bare apply: the crossing is consumed (source = the base, always
        // whole on a fully-applied target) BEFORE migration `second` is
        // considered, so its app/analytics sections warn as
        // subscribed-but-unrequested and are skipped.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success()
            .stderr(predicate::str::contains(
                "module 'app' is established on this target but not in the requested set",
            ))
            .stderr(predicate::str::contains("schema drift"));

        assert_eq!(
            subscription_modules(&pool).await?,
            vec!["analytics".to_string(), "app".to_string()],
            "the crossing rewrote the subscription through the remaps"
        );
        assert_eq!(
            crossing_watermark(&pool).await?,
            Some(re_anchor_version as i64),
            "the watermark advanced to the consumed re-anchor"
        );
        // Skipped means skipped: the module columns don't exist yet.
        let email_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_name = 'users' AND column_name = 'email')",
        )
        .fetch_one(&pool)
        .await?;
        assert!(!email_exists, "bare apply never grows the target's surface");

        // Consumed once: a second bare apply leaves the subscription and
        // watermark untouched.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();
        assert_eq!(
            subscription_modules(&pool).await?,
            vec!["analytics".to_string(), "app".to_string()]
        );
        assert_eq!(
            crossing_watermark(&pool).await?,
            Some(re_anchor_version as i64)
        );

        // The operator updates the deploy command once: --modules all runs
        // the skipped sections.
        helper
            .command()
            .args([
                "migrate",
                "apply",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "all",
            ])
            .assert()
            .success();
        let email_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_name = 'users' AND column_name = 'email')",
        )
        .fetch_one(&pool)
        .await?;
        assert!(email_exists);

        // Status reads the stored subscription (and raises no divergence
        // warning — legacy default rows imply no module).
        helper
            .command()
            .args([
                "migrate",
                "status",
                "--target-url",
                &helper.dev_database_url,
            ])
            .assert()
            .success()
            .stdout(predicate::str::contains("app"))
            .stdout(predicate::str::contains("established"));

        pool.close().await;
        Ok(())
    })
    .await
}

/// Split into a brand-new module: objects move `app → metrics` where
/// `metrics` never existed. The stamp is `remaps="app"` alone (no
/// self-inclusion — metrics held nothing before), so the crossing
/// auto-subscribes it: the target already holds every object metrics owns;
/// declining would orphan them. Also pins provision-from-baseline W
/// initializing the watermark directly to W (provision never crosses).
#[tokio::test]
async fn test_move_into_brand_new_module_auto_subscribes() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        let base = base_config(helper)?;

        set_modules(
            helper,
            &base,
            "\nmodules:\n  app:\n    paths: [\"schema/app/**\"]\n",
        )?;
        helper.write_schema_file("app/users.sql", "CREATE TABLE users (id SERIAL PRIMARY KEY);")?;
        helper.write_schema_file(
            "app/events.sql",
            "CREATE TABLE events (id SERIAL PRIMARY KEY);",
        )?;
        helper
            .command()
            .args(["migrate", "new", "initial", "--create-baseline"])
            .assert()
            .success();
        let provision_baseline = latest_baseline_version(helper)?;

        // Fresh provision from baseline W: provision never crosses — it
        // initializes the watermark to W and writes the subscription from
        // what was provisioned.
        helper
            .command()
            .args([
                "migrate",
                "provision",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "app",
            ])
            .assert()
            .success();
        let pool = helper.connect_to_dev_db().await?;
        assert_eq!(subscription_modules(&pool).await?, vec!["app".to_string()]);
        assert_eq!(
            crossing_watermark(&pool).await?,
            Some(provision_baseline as i64),
            "provision never crosses — it initializes the watermark to its baseline's version"
        );

        // Move events into a brand-new module.
        std::fs::remove_file(helper.project_root.join("schema/app/events.sql"))?;
        helper.write_schema_file(
            "metrics/events.sql",
            "CREATE TABLE events (id SERIAL PRIMARY KEY);",
        )?;
        set_modules(
            helper,
            &base,
            "\nmodules:\n  app:\n    paths: [\"schema/app/**\"]\n  metrics:\n    paths: [\"schema/metrics/**\"]\n",
        )?;
        next_version_tick();
        helper
            .command()
            .args(["migrate", "new", "split", "--create-baseline"])
            .assert()
            .success();
        let re_anchor_version = latest_baseline_version(helper)?;
        assert!(re_anchor_version > provision_baseline);

        // The stamp: brand-new module lists only the source, no self.
        let baseline_sql =
            helper.read_baseline_file(&format!("baseline_{re_anchor_version}.sql"))?;
        assert!(
            baseline_sql.contains(r#"module="metrics" remaps="app""#),
            "brand-new module stamps the source alone:\n{baseline_sql}"
        );

        // Bare apply consumes the crossing: metrics auto-subscribes (app
        // survives — it still owns users in the post-split partition).
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();
        assert_eq!(
            subscription_modules(&pool).await?,
            vec!["app".to_string(), "metrics".to_string()]
        );
        assert_eq!(
            crossing_watermark(&pool).await?,
            Some(re_anchor_version as i64)
        );

        pool.close().await;
        Ok(())
    })
    .await
}

/// Move into a PRE-EXISTING module via per-section adoption. Provenance-
/// cut stamps a plain `b` section (retained `z`) + a remap `b_2 remaps="a"`
/// (acquired `y`). On a target subscribed to `a` but not `b`, bare apply hits
/// the needed-modules gate — the crossing would relabel `y` into the
/// unsubscribed pre-existing `b`, orphaning it — and BLOCKS with adopt-b
/// guidance. Adopting `b` THROUGH the unconsumed re-anchor
/// (`provision --modules b`) runs the plain section (creates `z`), records the
/// remap section `satisfied` (`y` already present under `a`'s name — no
/// collision), and the same run's crossing then relabels ownership. `a`
/// survives (it still owns `x`).
#[tokio::test]
async fn test_move_into_pre_existing_module_per_section_adoption() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        let base = base_config(helper)?;

        set_modules(
            helper,
            &base,
            "\nmodules:\n  a:\n    paths: [\"schema/a/**\"]\n  b:\n    paths: [\"schema/b/**\"]\n",
        )?;
        helper.write_schema_file("a/x.sql", "CREATE TABLE x (id SERIAL PRIMARY KEY);")?;
        helper.write_schema_file("a/y.sql", "CREATE TABLE y (id SERIAL PRIMARY KEY);")?;
        helper.write_schema_file("b/z.sql", "CREATE TABLE z (id SERIAL PRIMARY KEY);")?;
        helper
            .command()
            .args(["migrate", "new", "initial"])
            .assert()
            .success();

        // SUBSET target: a only (b never deployed here).
        helper
            .command()
            .args([
                "migrate",
                "provision",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "a",
            ])
            .assert()
            .success();
        let pool = helper.connect_to_dev_db().await?;
        assert!(table_exists(&pool, "y").await?);
        assert!(!table_exists(&pool, "z").await?);

        // Re-tag: y moves a → b (b pre-existing). Provenance-cut: plain `b`
        // (z) + a remap `b_2 remaps="a"` (y). Never `remaps="a,b"`.
        std::fs::remove_file(helper.project_root.join("schema/a/y.sql"))?;
        helper.write_schema_file("b/y.sql", "CREATE TABLE y (id SERIAL PRIMARY KEY);")?;
        next_version_tick();
        helper
            .command()
            .args(["migrate", "new", "move_y", "--create-baseline"])
            .assert()
            .success();
        let re_anchor_version = latest_baseline_version(helper)?;
        let baseline_sql =
            helper.read_baseline_file(&format!("baseline_{re_anchor_version}.sql"))?;
        assert!(
            baseline_sql.contains(r#"remaps="a""#) && !baseline_sql.contains("remaps=\"a,b\""),
            "pre-existing module: single-source remap section, no self-inclusion:\n{baseline_sql}"
        );

        // The membrane: bare apply blocks — the crossing would relabel y into
        // the unsubscribed pre-existing b (needed-modules gate). The
        // guidance names adopting b through the re-anchor, never the source a.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .failure()
            .stderr(predicate::str::contains(format!(
                "Adopt b before applying past {re_anchor_version}"
            )))
            .stderr(predicate::str::contains("provision --modules b"));
        assert_eq!(
            subscription_modules(&pool).await?,
            vec!["a".to_string()],
            "a blocked crossing mutates nothing"
        );
        assert_ne!(
            crossing_watermark(&pool).await?,
            Some(re_anchor_version as i64),
            "a blocked crossing does not advance the watermark"
        );

        // Adopt b through the unconsumed re-anchor: baseline content is needed
        // (b's plain section), so `apply` would refuse — provision is the path.
        helper
            .command()
            .args([
                "migrate",
                "provision",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "b",
            ])
            .assert()
            .success();

        // Plain section ran (z created); the remap section did not (y untouched,
        // no collision) but is recorded satisfied; the crossing relabelled.
        assert!(table_exists(&pool, "z").await?);
        assert_eq!(
            subscription_modules(&pool).await?,
            vec!["a".to_string(), "b".to_string()],
            "a survives (still owns x); b adopted then carried through the crossing"
        );
        assert_eq!(
            crossing_watermark(&pool).await?,
            Some(re_anchor_version as i64)
        );

        // b's remap sections (source `a` held) recorded `satisfied` — nothing
        // ran for them; its plain sections ran and recorded `completed`. Every
        // b baseline row is terminal-covered (no pending/running/failed).
        let statuses: Vec<String> = sqlx::query_scalar(
            "SELECT status FROM public.pgmt_migrations_sections
             WHERE is_baseline AND module = 'b' ORDER BY section_name",
        )
        .fetch_all(&pool)
        .await?;
        assert!(
            statuses.iter().any(|s| s == "satisfied"),
            "b has a source-satisfied remap section: {statuses:?}"
        );
        assert!(
            statuses.iter().any(|s| s == "completed"),
            "b has a run plain section: {statuses:?}"
        );
        assert!(
            statuses
                .iter()
                .all(|s| s == "satisfied" || s == "completed"),
            "no b baseline section is left pending/failed: {statuses:?}"
        );

        pool.close().await;
        Ok(())
    })
    .await
}

/// Demotion `a → (unmoduled)` runs BOTH ways on plain apply: with
/// the source subscribed, the unmoduled acquisition section in migration V
/// records `satisfied` (objects already present) and the crossing removes `a`
/// (its objects are base now); without it, the acquisition section flows with
/// the base and creates the demoted content — no membrane, nothing to adopt.
#[tokio::test]
async fn test_demotion_both_ways_via_plain_apply() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        let base = base_config(helper)?;

        set_modules(
            helper,
            &base,
            "\nmodules:\n  a:\n    paths: [\"schema/a/**\"]\n  k:\n    paths: [\"schema/k/**\"]\n",
        )?;
        helper.write_schema_file("meta.sql", "CREATE TABLE meta (id SERIAL PRIMARY KEY);")?;
        helper.write_schema_file("a/x.sql", "CREATE TABLE x (id SERIAL PRIMARY KEY);")?;
        helper.write_schema_file("k/w.sql", "CREATE TABLE w (id SERIAL PRIMARY KEY);")?;
        helper
            .command()
            .args(["migrate", "new", "initial"])
            .assert()
            .success();

        // Target 1: has a (and k). Target 2: k only — lacks a.
        helper
            .command()
            .args([
                "migrate",
                "provision",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "all",
            ])
            .assert()
            .success();
        let target2_url = helper.create_extra_database().await?;
        helper
            .command()
            .args([
                "migrate",
                "provision",
                "--target-url",
                &target2_url,
                "--modules",
                "k",
            ])
            .assert()
            .success();

        // Demote a: its file becomes unmoduled (k stays a module).
        std::fs::remove_file(helper.project_root.join("schema/a/x.sql"))?;
        helper.write_schema_file("x.sql", "CREATE TABLE x (id SERIAL PRIMARY KEY);")?;
        set_modules(
            helper,
            &base,
            "\nmodules:\n  k:\n    paths: [\"schema/k/**\"]\n",
        )?;
        next_version_tick();
        helper
            .command()
            .args(["migrate", "new", "demote", "--create-baseline"])
            .assert()
            .success();
        let re_anchor_version = latest_baseline_version(helper)?;
        let baseline_sql =
            helper.read_baseline_file(&format!("baseline_{re_anchor_version}.sql"))?;
        // Provenance-cut: the retained base object (meta) stays in a plain
        // `default` section; the demoted module's object goes into its own
        // unmoduled remap section `remaps="a"` (no self-inclusion; the
        // comma-list form is asserted-against once, file-wide, in the re-tag
        // test above).
        assert!(
            baseline_sql.contains(r#"remaps="a""#),
            "demotion: an unmoduled remap section records the demoted module as source:\n{baseline_sql}"
        );
        // The same-version MIGRATION carries the acquisition delta: an
        // unmoduled remap section with x's DDL and the reviewer comment.
        let migration_files = helper.list_migration_files()?;
        let demote_file = migration_files
            .iter()
            .find(|f| f.contains("demote"))
            .expect("the demote migration exists");
        let migration_sql = helper.read_migration_file(demote_file)?;
        assert!(
            migration_sql.contains(r#"name="default" remaps="a""#),
            "migration V carries the unmoduled acquisition section:\n{migration_sql}"
        );
        assert!(
            migration_sql.contains("objects moved from module 'a'"),
            "reviewer-facing comment above the acquisition section:\n{migration_sql}"
        );

        // A LATER base-only migration — flows everywhere after the crossing.
        helper.write_schema_file(
            "meta.sql",
            "CREATE TABLE meta (id SERIAL PRIMARY KEY, note TEXT);",
        )?;
        next_version_tick();
        helper
            .command()
            .args(["migrate", "new", "base_change"])
            .assert()
            .success();

        // Target 1 (a subscribed): demotion removes a; the base migration runs.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();
        let pool1 = helper.connect_to_dev_db().await?;
        assert_eq!(
            subscription_modules(&pool1).await?,
            vec!["k".to_string()],
            "demoted module drops out; its objects are base now"
        );
        assert_eq!(
            crossing_watermark(&pool1).await?,
            Some(re_anchor_version as i64)
        );
        let note_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_name = 'meta' AND column_name = 'note')",
        )
        .fetch_one(&pool1)
        .await?;
        assert!(note_exists);

        // Target 1's acquisition section recorded `satisfied` (it held x
        // under a's name — nothing ran).
        let satisfied1: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM public.pgmt_migrations_sections
             WHERE NOT is_baseline AND status = 'satisfied'",
        )
        .fetch_one(&pool1)
        .await?;
        assert!(
            satisfied1 >= 1,
            "source-held acquisition records satisfied rows (got {satisfied1})"
        );
        pool1.close().await;

        // Target 2 (a never subscribed, no x): plain apply RUNS the unmoduled
        // acquisition section — demoted content is base content and flows to
        // everyone — then the crossing commits and the
        // later base migration runs. No membrane, nothing to adopt.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &target2_url])
            .assert()
            .success();
        let pool2 = sqlx::PgPool::connect(&target2_url).await?;
        let x_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables
             WHERE table_schema = 'public' AND table_name = 'x')",
        )
        .fetch_one(&pool2)
        .await?;
        assert!(x_exists, "the acquisition section created the demoted table");
        let note_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_name = 'meta' AND column_name = 'note')",
        )
        .fetch_one(&pool2)
        .await?;
        assert!(note_exists, "the later base migration flowed through");
        assert_eq!(
            crossing_watermark(&pool2).await?,
            Some(re_anchor_version as i64)
        );
        assert_eq!(
            subscription_modules(&pool2).await?,
            vec!["k".to_string()],
            "the base is never listed; nothing else subscribed"
        );
        // The acquisition executed here: a completed row, not satisfied.
        let satisfied2: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM public.pgmt_migrations_sections
             WHERE NOT is_baseline AND status = 'satisfied'",
        )
        .fetch_one(&pool2)
        .await?;
        assert_eq!(satisfied2, 0, "source-absent acquisition actually ran");
        pool2.close().await;

        Ok(())
    })
    .await
}

/// `migrate status --target-url` is READ-ONLY: on a target whose tracking
/// tables predate the subscription tables it reports gracefully (a
/// pre-subscription note, establishment derived from section rows) and runs
/// no DDL — the subscription tables must NOT appear as a side effect.
#[tokio::test]
async fn test_status_on_pre_subscription_target_is_read_only() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        let base = base_config(helper)?;
        set_modules(
            helper,
            &base,
            "\nmodules:\n  core:\n    paths: [\"schema/core/**\"]\n",
        )?;
        helper.write_schema_file("core/c.sql", "CREATE TABLE c (id SERIAL PRIMARY KEY);")?;
        helper
            .command()
            .args(["migrate", "new", "initial"])
            .assert()
            .success();

        // Hand-build a pre-subscription target: main + sections tables in the
        // old shape, with a completed core row — no subscription tables.
        let pool = helper.connect_to_dev_db().await?;
        sqlx::query(
            "CREATE TABLE public.pgmt_migrations (
                 version BIGINT NOT NULL, description TEXT NOT NULL,
                 applied_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                 checksum TEXT NOT NULL, applied_by TEXT DEFAULT CURRENT_USER,
                 is_baseline BOOLEAN NOT NULL, PRIMARY KEY (version, is_baseline))",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "CREATE TABLE public.pgmt_migrations_sections (
                 migration_version BIGINT NOT NULL, is_baseline BOOLEAN NOT NULL,
                 section_name TEXT NOT NULL, section_order INT NOT NULL,
                 status TEXT NOT NULL, started_at TIMESTAMPTZ, completed_at TIMESTAMPTZ,
                 attempts INT DEFAULT 0, last_error TEXT, rows_affected BIGINT,
                 duration_ms BIGINT, checksum TEXT, mode TEXT, module TEXT,
                 applied_by TEXT, pgmt_version TEXT,
                 PRIMARY KEY (migration_version, is_baseline, section_name))",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO public.pgmt_migrations (version, description, checksum, is_baseline)
             VALUES (1, 'initial', 'x', FALSE)",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO public.pgmt_migrations_sections
                 (migration_version, is_baseline, section_name, section_order, status, module)
             VALUES (1, FALSE, 'core', 0, 'completed', 'core')",
        )
        .execute(&pool)
        .await?;

        helper
            .command()
            .args([
                "migrate",
                "status",
                "--target-url",
                &helper.dev_database_url,
            ])
            .assert()
            .success()
            .stdout(predicate::str::contains("pre-subscription target"));

        // Read-only: status must not have created the subscription tables.
        let modules_table_exists: bool =
            sqlx::query_scalar("SELECT to_regclass('public.pgmt_migrations_modules') IS NOT NULL")
                .fetch_one(&pool)
                .await?;
        assert!(
            !modules_table_exists,
            "status is read-only: no subscription tables may appear"
        );

        pool.close().await;
        Ok(())
    })
    .await
}

/// An irrelevant re-anchor (its sources entirely outside the target's
/// subscription) is still CROSSED on a subset target: the watermark advances
/// and a crossing event is recorded, but the subscription is untouched —
/// crossing means evaluated-and-consumed, not mutated.
#[tokio::test]
async fn test_irrelevant_re_anchor_advances_watermark() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        let base = base_config(helper)?;

        set_modules(
            helper,
            &base,
            "\nmodules:\n  core:\n    paths: [\"schema/core/**\"]\n  analytics:\n    paths: [\"schema/an/**\"]\n",
        )?;
        helper.write_schema_file("core/c.sql", "CREATE TABLE c (id SERIAL PRIMARY KEY);")?;
        helper.write_schema_file("an/e.sql", "CREATE TABLE e (id SERIAL PRIMARY KEY);")?;
        helper
            .command()
            .args(["migrate", "new", "initial"])
            .assert()
            .success();

        // SUBSET target: core only.
        helper
            .command()
            .args([
                "migrate",
                "provision",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "core",
            ])
            .assert()
            .success();

        // Rename analytics → reports: a re-tag whose source (analytics) this
        // target never had.
        std::fs::remove_file(helper.project_root.join("schema/an/e.sql"))?;
        helper.write_schema_file("rp/e.sql", "CREATE TABLE e (id SERIAL PRIMARY KEY);")?;
        set_modules(
            helper,
            &base,
            "\nmodules:\n  core:\n    paths: [\"schema/core/**\"]\n  reports:\n    paths: [\"schema/rp/**\"]\n",
        )?;
        next_version_tick();
        helper
            .command()
            .args(["migrate", "new", "rename_analytics", "--create-baseline"])
            .assert()
            .success();
        let re_anchor_version = latest_baseline_version(helper)?;

        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();
        let pool = helper.connect_to_dev_db().await?;
        assert_eq!(
            subscription_modules(&pool).await?,
            vec!["core".to_string()],
            "irrelevant remap: subscription unchanged"
        );
        assert_eq!(
            crossing_watermark(&pool).await?,
            Some(re_anchor_version as i64),
            "…but the crossing is still consumed: the watermark advances"
        );

        pool.close().await;
        Ok(())
    })
    .await
}

/// Full merge `a, b → c` with `b` REMOVED from
/// pgmt.yaml in the same change, on a target holding only `a`-side content —
/// PLAIN APPLY crosses. Migration V carries the acquisition delta: the
/// `remaps="b"` section executes (creates the b-side objects), the
/// `remaps="a"` section records `satisfied`, and the two-phase commit then
/// collapses the subscription to `{c}`. No membrane, no provision detour, and
/// nothing ever names the deleted source `b`.
#[tokio::test]
async fn test_full_merge_with_deleted_source_acquires_on_plain_apply() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        let base = base_config(helper)?;

        set_modules(
            helper,
            &base,
            "\nmodules:\n  a:\n    paths: [\"schema/a/**\"]\n  b:\n    paths: [\"schema/b/**\"]\n",
        )?;
        helper.write_schema_file("a/x.sql", "CREATE TABLE x (id SERIAL PRIMARY KEY);")?;
        helper.write_schema_file("b/y.sql", "CREATE TABLE y (id SERIAL PRIMARY KEY);")?;
        helper
            .command()
            .args(["migrate", "new", "initial"])
            .assert()
            .success();

        // Target 1 — SUBSET: a only (b never deployed — no y here).
        helper
            .command()
            .args([
                "migrate",
                "provision",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "a",
            ])
            .assert()
            .success();
        let pool = helper.connect_to_dev_db().await?;
        assert!(table_exists(&pool, "x").await?);
        assert!(!table_exists(&pool, "y").await?);

        // Target 2 — holds BOTH sources (a and b): the crossing has nothing to
        // create here, so every acquisition section records `satisfied` and the
        // subscription simply collapses to {c}.
        let both_url = helper.create_extra_database().await?;
        helper
            .command()
            .args([
                "migrate",
                "provision",
                "--target-url",
                &both_url,
                "--modules",
                "all",
            ])
            .assert()
            .success();

        // Merge a,b → c and DELETE b (and a) from config in the same change.
        std::fs::remove_file(helper.project_root.join("schema/a/x.sql"))?;
        std::fs::remove_file(helper.project_root.join("schema/b/y.sql"))?;
        helper.write_schema_file("c/x.sql", "CREATE TABLE x (id SERIAL PRIMARY KEY);")?;
        helper.write_schema_file("c/y.sql", "CREATE TABLE y (id SERIAL PRIMARY KEY);")?;
        set_modules(
            helper,
            &base,
            "\nmodules:\n  c:\n    paths: [\"schema/c/**\"]\n",
        )?;
        next_version_tick();
        helper
            .command()
            .args(["migrate", "new", "merge", "--create-baseline"])
            .assert()
            .success();
        let re_anchor_version = latest_baseline_version(helper)?;

        // The same-version migration carries one acquisition section per
        // source, each with the reviewer-facing audience comment.
        let migration_files = helper.list_migration_files()?;
        let merge_file = migration_files
            .iter()
            .find(|f| f.contains("merge"))
            .expect("the merge migration exists");
        let migration_sql = helper.read_migration_file(merge_file)?;
        assert!(
            migration_sql.contains(r#"module="c" remaps="a""#)
                && migration_sql.contains(r#"module="c" remaps="b""#),
            "migration V carries one acquisition section per source:\n{migration_sql}"
        );
        assert!(
            migration_sql.contains("objects moved from module 'b'"),
            "reviewer comment states the audience:\n{migration_sql}"
        );

        // PLAIN APPLY crosses: the remaps="b" section
        // executes, the remaps="a" section records satisfied, the commit
        // subscribes c and drops the absorbed sources.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();
        assert!(table_exists(&pool, "y").await?, "b's acquisition ran");
        assert!(table_exists(&pool, "x").await?, "a's objects stayed put");
        assert_eq!(
            subscription_modules(&pool).await?,
            vec!["c".to_string()],
            "commit: c subscribed, absorbed a and b removed"
        );
        assert_eq!(
            crossing_watermark(&pool).await?,
            Some(re_anchor_version as i64)
        );

        // Uniform rule on the rows: the a-sourced section satisfied (objects
        // were already here), the b-sourced one completed (it ran).
        let statuses: Vec<(String, String)> = sqlx::query_as(
            "SELECT section_name, status FROM public.pgmt_migrations_sections
             WHERE NOT is_baseline AND migration_version = $1 ORDER BY section_order",
        )
        .bind(re_anchor_version as i64)
        .fetch_all(&pool)
        .await?;
        assert!(
            statuses.iter().any(|(_, st)| st == "satisfied"),
            "a-sourced acquisition satisfied: {statuses:?}"
        );
        assert!(
            statuses.iter().any(|(_, st)| st == "completed"),
            "b-sourced acquisition executed: {statuses:?}"
        );

        // Consumed once: another bare apply changes nothing.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();
        assert_eq!(subscription_modules(&pool).await?, vec!["c".to_string()]);
        assert_eq!(
            crossing_watermark(&pool).await?,
            Some(re_anchor_version as i64)
        );

        // Target 2 (held BOTH sources): plain apply collapses the subscription
        // to {c}; every acquisition section recorded `satisfied` (nothing ran —
        // the objects were already present under a's and b's names).
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &both_url])
            .assert()
            .success();
        let both_pool = sqlx::PgPool::connect(&both_url).await?;
        assert_eq!(
            subscription_modules(&both_pool).await?,
            vec!["c".to_string()],
            "both sources whole -> c subscribed, absorbed a and b removed"
        );
        assert_eq!(
            crossing_watermark(&both_pool).await?,
            Some(re_anchor_version as i64)
        );
        let both_statuses: Vec<String> = sqlx::query_scalar(
            "SELECT status FROM public.pgmt_migrations_sections
             WHERE NOT is_baseline AND migration_version = $1",
        )
        .bind(re_anchor_version as i64)
        .fetch_all(&both_pool)
        .await?;
        assert!(
            !both_statuses.is_empty() && both_statuses.iter().all(|st| st == "satisfied"),
            "both sources held -> every acquisition section satisfied: {both_statuses:?}"
        );

        both_pool.close().await;
        pool.close().await;
        Ok(())
    })
    .await
}

/// Fresh provision from a re-anchor applies BOTH its plain and its remap
/// sections (the source is not established on a fresh target, so remap-section
/// objects are created, not relabelled) and pins every section row completed.
/// Provision never crosses: the watermark initializes directly to the
/// baseline's version.
#[tokio::test]
async fn test_fresh_provision_from_re_anchor_runs_all_sections() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        let base = base_config(helper)?;

        set_modules(
            helper,
            &base,
            "\nmodules:\n  app:\n    paths: [\"schema/app/**\"]\n",
        )?;
        helper.write_schema_file("app/users.sql", "CREATE TABLE users (id SERIAL PRIMARY KEY);")?;
        helper.write_schema_file(
            "app/events.sql",
            "CREATE TABLE events (id SERIAL PRIMARY KEY);",
        )?;
        helper
            .command()
            .args(["migrate", "new", "initial"])
            .assert()
            .success();

        // Split events into a brand-new module → a re-anchor (events becomes a
        // remap section sourced from app).
        std::fs::remove_file(helper.project_root.join("schema/app/events.sql"))?;
        helper.write_schema_file(
            "metrics/events.sql",
            "CREATE TABLE events (id SERIAL PRIMARY KEY);",
        )?;
        set_modules(
            helper,
            &base,
            "\nmodules:\n  app:\n    paths: [\"schema/app/**\"]\n  metrics:\n    paths: [\"schema/metrics/**\"]\n",
        )?;
        next_version_tick();
        helper
            .command()
            .args(["migrate", "new", "split", "--create-baseline"])
            .assert()
            .success();
        let re_anchor_version = latest_baseline_version(helper)?;

        // FRESH target, provision --modules all directly from the re-anchor.
        let fresh_url = helper.create_extra_database().await?;
        helper
            .command()
            .args([
                "migrate",
                "provision",
                "--target-url",
                &fresh_url,
                "--modules",
                "all",
            ])
            .assert()
            .success();

        let pool = sqlx::PgPool::connect(&fresh_url).await?;
        // Plain (app) and remap (metrics) section objects both exist.
        assert!(table_exists(&pool, "users").await?);
        assert!(table_exists(&pool, "events").await?);

        // Every baseline section row is completed — the remap section RAN on a
        // fresh target (its source is not established), it was not satisfied.
        let statuses: Vec<String> = sqlx::query_scalar(
            "SELECT status FROM public.pgmt_migrations_sections WHERE is_baseline",
        )
        .fetch_all(&pool)
        .await?;
        assert!(!statuses.is_empty(), "baseline section rows are pinned");
        assert!(
            statuses.iter().all(|s| s == "completed"),
            "fresh provision runs every section (no satisfied): {statuses:?}"
        );

        // Provision never crosses: it initializes the watermark directly.
        assert_eq!(
            crossing_watermark(&pool).await?,
            Some(re_anchor_version as i64),
            "provision initializes the watermark to the baseline's version"
        );
        assert_eq!(
            subscription_modules(&pool).await?,
            vec!["app".to_string(), "metrics".to_string()]
        );

        pool.close().await;
        Ok(())
    })
    .await
}

/// Load a target's MANAGED catalog through pgmt's own machinery (the same
/// path every diff/render uses): resolve the project config, build the managed
/// filter, and `load_managed`. This is the oracle for the convergence law.
async fn managed_catalog(
    url: &str,
    project_root: &std::path::Path,
) -> Result<pgmt::catalog::Catalog> {
    let (input, _) = pgmt::config::load_config(project_root.join("pgmt.yaml").to_str().unwrap())?;
    let config = pgmt::config::ConfigBuilder::new()
        .with_file(input)
        .resolve()?;
    let filter = pgmt::config::ObjectFilter::from_config(&config);
    let pool = sqlx::PgPool::connect(url).await?;
    let catalog = pgmt::catalog::Catalog::load_managed(&pool, &filter).await?;
    pool.close().await;
    Ok(catalog)
}

async fn column_exists(pool: &sqlx::PgPool, table: &str, column: &str) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns
         WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2)",
    )
    .bind(table)
    .bind(column)
    .fetch_one(pool)
    .await?)
}

/// Move + ALTER in one migration V. One change moves `widget` from module
/// `b` into module `c` AND adds a column. The acquisition section must render
/// `widget` from the STARTING catalog (its V−1 state — WITHOUT the new column),
/// and the ordinary section carries the ALTER. Applied to a source-holding
/// target (acquisition satisfied, only the ALTER runs) and a source-less target
/// (acquisition CREATEs the V−1 widget, then the ALTER adds the column), both
/// converge. Cloning the desired-state baseline instead would bake the column
/// into the acquisition CREATE and the ALTER would then double-apply it on the
/// source-less target.
#[tokio::test]
async fn test_move_plus_alter_converges_both_ways() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        let base = base_config(helper)?;

        set_modules(
            helper,
            &base,
            "\nmodules:\n  b:\n    paths: [\"schema/b/**\"]\n  c:\n    paths: [\"schema/c/**\"]\n",
        )?;
        helper.write_schema_file("b/widget.sql", "CREATE TABLE widget (id INT PRIMARY KEY);")?;
        helper.write_schema_file("c/other.sql", "CREATE TABLE other (id INT PRIMARY KEY);")?;
        helper
            .command()
            .args(["migrate", "new", "initial"])
            .assert()
            .success();

        // Target 1 (source-holding): b + c. Target 2 (source-less): c only.
        helper
            .command()
            .args([
                "migrate",
                "provision",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "b,c",
            ])
            .assert()
            .success();
        let target2_url = helper.create_extra_database().await?;
        helper
            .command()
            .args([
                "migrate",
                "provision",
                "--target-url",
                &target2_url,
                "--modules",
                "c",
            ])
            .assert()
            .success();

        // V: widget moves b → c AND gains a `name` column. `b` empties out and
        // is removed from config (full move).
        std::fs::remove_file(helper.project_root.join("schema/b/widget.sql"))?;
        helper.write_schema_file(
            "c/widget.sql",
            "CREATE TABLE widget (id INT PRIMARY KEY, name TEXT);",
        )?;
        set_modules(
            helper,
            &base,
            "\nmodules:\n  c:\n    paths: [\"schema/c/**\"]\n",
        )?;
        next_version_tick();
        helper
            .command()
            .args(["migrate", "new", "move_alter", "--create-baseline"])
            .assert()
            .success();
        let re_anchor_version = latest_baseline_version(helper)?;

        // The migration content: acquisition CREATE renders the V−1 widget
        // (no `name`); the ordinary ALTER section carries `name`.
        let move_file = helper
            .list_migration_files()?
            .into_iter()
            .find(|f| f.contains("move_alter"))
            .expect("the move migration exists");
        let migration_sql = helper.read_migration_file(&move_file)?;
        let acquisition_chunk = migration_sql
            .split("-- pgmt:section")
            .find(|s| s.contains(r#"remaps="b""#))
            .expect("migration carries a remaps=\"b\" acquisition section");
        // The quoted column identifier (`"name"`) distinguishes the column
        // from the section header's `name="c"` attribute.
        assert!(
            !acquisition_chunk.contains("\"name\""),
            "acquisition CREATE must be the STARTING (V−1) widget, without the new column:\n{acquisition_chunk}"
        );
        assert!(
            migration_sql.contains("ADD COLUMN") && migration_sql.contains("\"name\""),
            "the ordinary section carries the ALTER that adds the column:\n{migration_sql}"
        );

        // Apply V both ways (c requested so its ordinary ALTER runs).
        helper
            .command()
            .args([
                "migrate",
                "apply",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "c",
            ])
            .assert()
            .success();
        helper
            .command()
            .args([
                "migrate",
                "apply",
                "--target-url",
                &target2_url,
                "--modules",
                "c",
            ])
            .assert()
            .success();

        // Both targets end with widget(id, name).
        let pool1 = helper.connect_to_dev_db().await?;
        let pool2 = sqlx::PgPool::connect(&target2_url).await?;
        assert!(
            column_exists(&pool1, "widget", "name").await?,
            "source-holding target: the ALTER ran"
        );
        assert!(
            column_exists(&pool2, "widget", "name").await?,
            "source-less target: acquisition CREATE + ALTER ran"
        );
        assert_eq!(
            crossing_watermark(&pool1).await?,
            Some(re_anchor_version as i64)
        );
        assert_eq!(
            crossing_watermark(&pool2).await?,
            Some(re_anchor_version as i64)
        );
        pool1.close().await;
        pool2.close().await;

        // The convergence law: diff the two targets' managed catalogs through
        // pgmt's own engine — empty diff or the acquisition content was impure.
        let cat1 = managed_catalog(&helper.dev_database_url, &helper.project_root).await?;
        let cat2 = managed_catalog(&target2_url, &helper.project_root).await?;
        let diff = pgmt::diff::plan(&cat1, &cat2)?;
        assert!(
            diff.is_empty(),
            "convergence law: applying V both ways must yield identical catalogs, got {} step(s):\n{diff:#?}",
            diff.len()
        );

        Ok(())
    })
    .await
}

/// Cross-module-drop × SUBSET: on a target that established `core` but not
/// `billing`, a cross-module drop's enforced re-anchor runs core's drop while
/// billing's sections are irrelevant — billing was never deployed here, so its
/// objects don't exist and its sections leave no rows. The intra-migration
/// coupling guard stays silent because it is target-aware (billing, the
/// depended-on module, is not established).
#[tokio::test]
async fn test_cross_module_drop_subset_unestablished_module_irrelevant() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        let base = base_config(helper)?;
        set_modules(
            helper,
            &base,
            "\nmodules:\n  core:\n    paths: [\"schema/core/**\"]\n  billing:\n    paths: [\"schema/billing/**\"]\n    depends_on: [core]\n",
        )?;
        helper.write_schema_file(
            "core/accounts.sql",
            "CREATE TABLE accounts (id SERIAL PRIMARY KEY);",
        )?;
        helper.write_schema_file(
            "billing/invoices.sql",
            "-- require: core/accounts.sql\n\
             CREATE TABLE invoices (id SERIAL PRIMARY KEY, account_id INT REFERENCES accounts(id));",
        )?;
        helper
            .command()
            .args(["migrate", "new", "initial"])
            .assert()
            .success();

        // SUBSET target: core only — billing never deployed (no invoices here).
        helper
            .command()
            .args([
                "migrate",
                "provision",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "core",
            ])
            .assert()
            .success();
        let pool = helper.connect_to_dev_db().await?;
        assert!(table_exists(&pool, "accounts").await?);
        assert!(!table_exists(&pool, "invoices").await?);

        // Cross-module drop: remove accounts (billing's history references it).
        next_version_tick();
        std::fs::remove_file(helper.project_root.join("schema/core/accounts.sql"))?;
        helper.write_schema_file(
            "billing/invoices.sql",
            "CREATE TABLE invoices (id SERIAL PRIMARY KEY, account_id INT);",
        )?;
        helper
            .command()
            .args(["migrate", "new", "drop_accounts", "--create-baseline"])
            .assert()
            .success();

        // Apply core on the SUBSET target: core's DROP runs; billing's section
        // is irrelevant (not established) and no coupling error fires.
        helper
            .command()
            .args([
                "migrate",
                "apply",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "core",
            ])
            .assert()
            .success();
        assert!(
            !table_exists(&pool, "accounts").await?,
            "core's drop ran on the SUBSET target"
        );
        assert!(
            !table_exists(&pool, "invoices").await?,
            "billing was never deployed here"
        );
        assert_eq!(
            subscription_modules(&pool).await?,
            vec!["core".to_string()],
            "billing stays unestablished — its sections were irrelevant"
        );

        pool.close().await;
        Ok(())
    })
    .await
}

/// Cross-module-drop × BEHIND: a fully-established target that is behind V
/// applies the drop migration in version order; the enforced re-anchor baseline
/// at V is a plain checkpoint (no `remaps`) and stays inert — it is never a
/// crossing and never applied as content.
#[tokio::test]
async fn test_cross_module_drop_behind_baseline_is_inert() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        let base = base_config(helper)?;
        set_modules(
            helper,
            &base,
            "\nmodules:\n  core:\n    paths: [\"schema/core/**\"]\n  billing:\n    paths: [\"schema/billing/**\"]\n    depends_on: [core]\n",
        )?;
        helper.write_schema_file(
            "core/accounts.sql",
            "CREATE TABLE accounts (id SERIAL PRIMARY KEY);",
        )?;
        helper.write_schema_file(
            "billing/invoices.sql",
            "-- require: core/accounts.sql\n\
             CREATE TABLE invoices (id SERIAL PRIMARY KEY, account_id INT REFERENCES accounts(id));",
        )?;
        helper
            .command()
            .args(["migrate", "new", "initial"])
            .assert()
            .success();

        // Fully establish core + billing at v1.
        helper
            .command()
            .args([
                "migrate",
                "apply",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "all",
            ])
            .assert()
            .success();
        let pool = helper.connect_to_dev_db().await?;
        assert!(table_exists(&pool, "accounts").await?);
        assert!(table_exists(&pool, "invoices").await?);

        // Cross-module drop at v2 (+ enforced baseline). The target does NOT
        // apply it yet — it is BEHIND.
        next_version_tick();
        std::fs::remove_file(helper.project_root.join("schema/core/accounts.sql"))?;
        helper.write_schema_file(
            "billing/invoices.sql",
            "CREATE TABLE invoices (id SERIAL PRIMARY KEY, account_id INT);",
        )?;
        helper
            .command()
            .args(["migrate", "new", "drop_accounts", "--create-baseline"])
            .assert()
            .success();

        // Apply the pending v2 migration (in version order). The baseline at v2
        // is inert: it is not a re-anchor, so nothing crosses.
        helper
            .command()
            .args([
                "migrate",
                "apply",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "all",
            ])
            .assert()
            .success();
        assert!(
            !table_exists(&pool, "accounts").await?,
            "the v2 migration dropped accounts"
        );
        assert!(
            table_exists(&pool, "invoices").await?,
            "billing kept invoices (only the FK went away)"
        );
        assert_eq!(
            crossing_watermark(&pool).await?,
            None,
            "a plain checkpoint baseline is inert — it is never a crossing"
        );
        assert_eq!(
            subscription_modules(&pool).await?,
            vec!["billing".to_string(), "core".to_string()],
            "the subscription is untouched by the inert baseline"
        );

        pool.close().await;
        Ok(())
    })
    .await
}

/// Split × BEHIND: a target subscribed to `app` but behind the split
/// re-anchor V finishes app's sections < V first (an intermediate app change),
/// then crosses at V — version order enforces "finish the source's sections ≤ V
/// before the crossing" naturally. After apply: the intermediate change landed,
/// the subscription grew to {app, metrics}, and the watermark is V.
#[tokio::test]
async fn test_split_behind_finishes_source_sections_before_crossing() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        let base = base_config(helper)?;
        set_modules(
            helper,
            &base,
            "\nmodules:\n  app:\n    paths: [\"schema/app/**\"]\n",
        )?;
        helper.write_schema_file(
            "app/users.sql",
            "CREATE TABLE users (id SERIAL PRIMARY KEY);",
        )?;
        helper.write_schema_file(
            "app/events.sql",
            "CREATE TABLE events (id SERIAL PRIMARY KEY);",
        )?;
        helper
            .command()
            .args(["migrate", "new", "initial", "--create-baseline"])
            .assert()
            .success();

        // Establish app from the baseline (watermark = the provision baseline).
        helper
            .command()
            .args([
                "migrate",
                "provision",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "app",
            ])
            .assert()
            .success();
        let pool = helper.connect_to_dev_db().await?;
        assert_eq!(subscription_modules(&pool).await?, vec!["app".to_string()]);

        // An intermediate app-only migration (< V): adds a column to users.
        next_version_tick();
        helper.write_schema_file(
            "app/users.sql",
            "CREATE TABLE users (id SERIAL PRIMARY KEY, tag TEXT);",
        )?;
        helper
            .command()
            .args(["migrate", "new", "app_change"])
            .assert()
            .success();

        // The split at V > the intermediate: events moves into brand-new metrics.
        next_version_tick();
        std::fs::remove_file(helper.project_root.join("schema/app/events.sql"))?;
        helper.write_schema_file(
            "metrics/events.sql",
            "CREATE TABLE events (id SERIAL PRIMARY KEY);",
        )?;
        set_modules(
            helper,
            &base,
            "\nmodules:\n  app:\n    paths: [\"schema/app/**\"]\n  metrics:\n    paths: [\"schema/metrics/**\"]\n",
        )?;
        helper
            .command()
            .args(["migrate", "new", "split", "--create-baseline"])
            .assert()
            .success();
        let re_anchor_version = latest_baseline_version(helper)?;

        // The target is BEHIND (it applied only the provision baseline). Apply
        // app: version order runs the intermediate app change first, THEN the
        // split at V crosses (source app established; metrics brand-new →
        // auto-subscribe).
        helper
            .command()
            .args([
                "migrate",
                "apply",
                "--target-url",
                &helper.dev_database_url,
                "--modules",
                "app",
            ])
            .assert()
            .success();
        let tag_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_name = 'users' AND column_name = 'tag')",
        )
        .fetch_one(&pool)
        .await?;
        assert!(
            tag_exists,
            "app's intermediate section (< V) ran before the crossing"
        );
        assert_eq!(
            subscription_modules(&pool).await?,
            vec!["app".to_string(), "metrics".to_string()],
            "the crossing at V auto-subscribed the brand-new metrics"
        );
        assert_eq!(
            crossing_watermark(&pool).await?,
            Some(re_anchor_version as i64),
            "the watermark advanced to V once app's sections ≤ V were settled"
        );

        pool.close().await;
        Ok(())
    })
    .await
}
