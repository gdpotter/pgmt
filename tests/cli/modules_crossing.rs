//! Acceptance tests for the stored subscription + crossing loop + wholeness
//! membranes (modules.md §13, §16, §19): re-anchors are consumed exactly once
//! per target, at the crossing, rewriting the stored subscription; wholeness
//! failures are strong membranes with adopt-first guidance.
//!
//! Together with `modules_deploy.rs`/`modules_generation.rs` (ordinary
//! migrations, new modules, cross-module drops, coupling, adoption paths,
//! the non-module characterization pin) these cover the §16 dynamics grid;
//! this file owns the re-anchor rows (modularization, split, merge, demotion,
//! irrelevant crossings) across LEGACY / SUBSET / BEHIND targets.

use crate::helpers::cli::{CliTestHelper, with_cli_helper};
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

/// Distinct migration versions are timestamps: force the clock forward
/// between generation calls.
fn next_version_tick() {
    std::thread::sleep(std::time::Duration::from_millis(1100));
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

/// `(version, subscription_before, subscription_after)` of recorded crossings.
async fn crossing_events(pool: &sqlx::PgPool) -> Result<Vec<(i64, String, String)>> {
    Ok(sqlx::query_as(
        "SELECT version, subscription_before, subscription_after
         FROM public.pgmt_migrations_events WHERE event = 'crossing' ORDER BY id",
    )
    .fetch_all(pool)
    .await?)
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

/// §19c end-to-end: a LEGACY target (all-default pre-module history) meets a
/// modularization re-anchor. The next bare `apply` consumes the crossing —
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
        let events = crossing_events(&pool).await?;
        assert_eq!(events.len(), 1, "exactly one crossing recorded: {events:?}");
        assert_eq!(
            events[0],
            (
                re_anchor_version as i64,
                "(base only)".to_string(),
                "analytics,app".to_string()
            )
        );

        // Skipped means skipped: the module columns don't exist yet.
        let email_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_name = 'users' AND column_name = 'email')",
        )
        .fetch_one(&pool)
        .await?;
        assert!(!email_exists, "bare apply never grows the target's surface");

        // Consumed once: a second bare apply records no new crossing.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();
        assert_eq!(crossing_events(&pool).await?.len(), 1);

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
/// initializing the watermark to W with NO crossing events.
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
            "provision initializes the watermark to its baseline's version"
        );
        assert_eq!(
            crossing_events(&pool).await?.len(),
            0,
            "provision never crosses"
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

/// Move into a PRE-EXISTING module via per-section adoption (§14). Provenance-
/// cut stamps a plain `b` section (retained `z`) + a remap `b_2 remaps="a"`
/// (acquired `y`). On a target subscribed to `a` but not `b`, bare apply hits
/// the needed-modules gate (§13) — the crossing would relabel `y` into the
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
        // the unsubscribed pre-existing b (needed-modules gate, §13). The
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
        assert_eq!(crossing_events(&pool).await?.len(), 0);

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
        assert_eq!(crossing_events(&pool).await?.len(), 1);

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

/// Merge `a, b → c` on a target subscribed to both: the crossing collapses
/// the subscription to {c}; the wholly-absorbed sources drop out.
#[tokio::test]
async fn test_merge_with_both_sources_collapses() -> Result<()> {
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

        // Merge: both files move into module c; a and b disappear.
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
        let baseline_sql =
            helper.read_baseline_file(&format!("baseline_{re_anchor_version}.sql"))?;
        // Provenance-cut: c is brand-new, all acquired — one remap section per
        // source, never a comma-list, never self.
        assert!(
            baseline_sql.contains(r#"module="c" remaps="a""#)
                && baseline_sql.contains(r#"module="c" remaps="b""#),
            "merge splits into one remap section per source:\n{baseline_sql}"
        );
        assert!(
            !baseline_sql.contains("remaps=\"a,b\""),
            "the comma-list form is retired:\n{baseline_sql}"
        );

        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();
        let pool = helper.connect_to_dev_db().await?;
        assert_eq!(
            subscription_modules(&pool).await?,
            vec!["c".to_string()],
            "both sources whole -> c subscribed, absorbed a and b removed"
        );
        assert_eq!(
            crossing_watermark(&pool).await?,
            Some(re_anchor_version as i64)
        );

        // Source-holding variant of §19e: the target held BOTH sources, so
        // every acquisition section in migration V recorded `satisfied` —
        // nothing executed, the crossing relabelled.
        let statuses: Vec<String> = sqlx::query_scalar(
            "SELECT status FROM public.pgmt_migrations_sections
             WHERE NOT is_baseline AND migration_version = $1",
        )
        .bind(re_anchor_version as i64)
        .fetch_all(&pool)
        .await?;
        assert!(
            !statuses.is_empty() && statuses.iter().all(|st| st == "satisfied"),
            "both sources held -> every acquisition section satisfied: {statuses:?}"
        );

        pool.close().await;
        Ok(())
    })
    .await
}

/// Demotion `a → (unmoduled)` runs BOTH ways on plain apply (§12/§16): with
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
        // unmoduled remap section `remaps="a"` (no self-inclusion, no
        // comma-list).
        assert!(
            baseline_sql.contains(r#"remaps="a""#),
            "demotion: an unmoduled remap section records the demoted module as source:\n{baseline_sql}"
        );
        assert!(
            !baseline_sql.contains("remaps=\"(unmoduled),a\""),
            "the self-included comma-list form is retired:\n{baseline_sql}"
        );
        // The same-version MIGRATION carries the acquisition delta (§12): an
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
        // everyone (§16 demotion cell) — then the crossing commits and the
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
        let events = crossing_events(&pool).await?;
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].1, events[0].2,
            "crossing recorded with before == after"
        );

        pool.close().await;
        Ok(())
    })
    .await
}

/// §19e / §16 merge cell: full merge `a, b → c` with `b` REMOVED from
/// pgmt.yaml in the same change, on a target holding only `a`-side content —
/// PLAIN APPLY crosses. Migration V carries the acquisition delta (§12): the
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

        // SUBSET target: a only (b never deployed — no y here).
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
        // source, each with the reviewer-facing audience comment (§11).
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

        // PLAIN APPLY crosses (§16 merge cell: "runs"): the remaps="b" section
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
        assert_eq!(crossing_events(&pool).await?.len(), 1);

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

        // Consumed once: another bare apply records nothing new.
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success();
        assert_eq!(subscription_modules(&pool).await?, vec!["c".to_string()]);
        assert_eq!(crossing_events(&pool).await?.len(), 1);

        pool.close().await;
        Ok(())
    })
    .await
}

/// Fresh provision from a re-anchor applies BOTH its plain and its remap
/// sections (the source is not established on a fresh target, so remap-section
/// objects are created, not relabelled) and pins every section row completed.
/// Provision never crosses: the watermark initializes to the baseline's
/// version with no crossing events.
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

        // Provision never crosses: watermark = baseline version, no events.
        assert_eq!(
            crossing_watermark(&pool).await?,
            Some(re_anchor_version as i64),
            "provision initializes the watermark to the baseline's version"
        );
        assert_eq!(
            crossing_events(&pool).await?.len(),
            0,
            "provision never crosses"
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
