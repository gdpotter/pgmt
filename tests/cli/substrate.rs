//! Gated regression tests for image-provided substrate (Supabase, PostGIS).
//!
//! These run a real platform Postgres image as pgmt's Docker shadow and verify
//! that `migrate baseline` captures only the user's schemas, never the substrate
//! the image provides. They're slow (large image pulls + init scripts) and need
//! Docker, so they only run when `PGMT_SUBSTRATE_TESTS` is set — CI runs them in
//! a dedicated job, and they no-op in the normal matrix.
//!
//! Unlike the rest of the CLI tests, these don't use `CliTestHelper`: baseline
//! generation runs entirely against the Docker shadow and never connects to a
//! dev database, so there's no plain-Postgres instance to provision (only
//! Docker is needed). Pointing `dev_url` at a vanilla `postgres` would also
//! misrepresent the project — a real Supabase project's dev DB is itself a
//! Supabase instance, substrate and all.

use anyhow::{Context, Result};
use std::fs;
use tempfile::TempDir;

const SUPABASE_IMAGE: &str = "supabase/postgres:17.6.1.081";

/// `migrate baseline` never dials the dev database, so this is a syntactically
/// valid placeholder that is never connected to.
const PLACEHOLDER_DEV_URL: &str = "postgres://pgmt:pgmt@127.0.0.1:1/unused";

/// A managed table with a foreign key into Supabase's `auth` substrate.
const PROFILES_SCHEMA: &str = r#"CREATE TABLE public.profiles (
    id uuid PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
    display_name text
);"#;

fn substrate_tests_enabled() -> bool {
    std::env::var("PGMT_SUBSTRATE_TESTS").is_ok_and(|v| !v.is_empty())
}

/// Build a `pgmt.yaml` for a Supabase-image Docker shadow, appending the given
/// `objects:` section (pass `""` for no scoping at all).
fn supabase_config(objects_section: &str) -> String {
    format!(
        r#"databases:
  dev_url: {PLACEHOLDER_DEV_URL}
  shadow:
    docker:
      image: {SUPABASE_IMAGE}
      environment:
        POSTGRES_USER: supabase_admin
        POSTGRES_PASSWORD: postgres

directories:
  schema_dir: schema/
  migrations_dir: migrations/
  baselines_dir: schema_baselines/
{objects_section}"#
    )
}

/// Scaffold a throwaway project with `config_yaml` and `schema_files`, run
/// `pgmt migrate baseline`, and return the single generated baseline's SQL.
fn baseline_for(config_yaml: &str, schema_files: &[(&str, &str)]) -> Result<String> {
    let project = TempDir::new()?;
    let root = project.path();
    fs::create_dir_all(root.join("schema"))?;
    fs::create_dir_all(root.join("migrations"))?;
    fs::create_dir_all(root.join("schema_baselines"))?;
    fs::write(root.join("pgmt.yaml"), config_yaml)?;
    for (name, content) in schema_files {
        fs::write(root.join("schema").join(name), content)?;
    }

    // `migrate baseline` checkpoints the migration log, so create it first.
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("pgmt");
    cmd.current_dir(root).args(["migrate", "new", "initial"]);
    cmd.assert().success();
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("pgmt");
    cmd.current_dir(root).args(["migrate", "baseline"]);
    cmd.assert().success();

    let mut baselines: Vec<_> = fs::read_dir(root.join("schema_baselines"))?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "sql"))
        .collect();
    baselines.sort();
    assert_eq!(baselines.len(), 1, "expected exactly one baseline file");

    fs::read_to_string(&baselines[0]).context("failed to read baseline file")
}

/// Assert a baseline (re)creates none of the platform substrate.
fn assert_excludes_substrate(baseline: &str) {
    for forbidden in [
        "CREATE SCHEMA auth",
        "CREATE SCHEMA storage",
        "CREATE SCHEMA realtime",
        "CREATE TABLE auth.",
        "CREATE TABLE storage.",
        "storage.objects",
    ] {
        assert!(
            !baseline.contains(forbidden),
            "baseline must not include substrate ({forbidden}):\n{baseline}"
        );
    }
}

/// A baseline generated against a Supabase-image shadow contains only the
/// managed (`public`) objects — never the platform substrate — even though the
/// managed table carries an FK into `auth.users` (which resolves because the
/// shadow runs the Supabase image).
#[test]
fn test_supabase_substrate_excluded_from_baseline() -> Result<()> {
    if !substrate_tests_enabled() {
        eprintln!("skipping Supabase substrate test (set PGMT_SUBSTRATE_TESTS=1 to run)");
        return Ok(());
    }

    let config = supabase_config(
        r#"
objects:
  include:
    schemas:
      - public
"#,
    );
    let baseline = baseline_for(&config, &[("profiles.sql", PROFILES_SCHEMA)])?;

    // The managed table is created...
    assert!(
        baseline.contains("profiles"),
        "baseline should create the managed public.profiles table:\n{baseline}"
    );
    // ...and the FK into substrate is preserved (substrate exists in the target)...
    assert!(
        baseline.contains(r#"REFERENCES "auth"."users""#),
        "baseline should keep the FK reference to auth.users:\n{baseline}"
    );
    // ...but no substrate is (re)created.
    assert_excludes_substrate(&baseline);

    Ok(())
}

/// The payoff of diffing against the shadow base: with **no `objects` scoping at
/// all** (pgmt nominally manages everything), `migrate baseline` still excludes
/// the platform substrate — because it's subtracted as the shadow's pre-schema
/// state, not because a predicate named it.
#[test]
fn test_supabase_substrate_excluded_without_objects_scoping() -> Result<()> {
    if !substrate_tests_enabled() {
        eprintln!("skipping Supabase substrate test (set PGMT_SUBSTRATE_TESTS=1 to run)");
        return Ok(());
    }

    let baseline = baseline_for(&supabase_config(""), &[("profiles.sql", PROFILES_SCHEMA)])?;

    assert!(
        baseline.contains("profiles"),
        "baseline should create the managed public.profiles table:\n{baseline}"
    );
    assert_excludes_substrate(&baseline);

    Ok(())
}
