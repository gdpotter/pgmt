//! Binary-level tests for connection resolution precedence:
//! CLI flag > `PGMT_*` env > pgmt.yaml, plus the required-value errors.
//!
//! These spawn the real binary so the full ladder (clap parsing, env lookup,
//! yaml fallback) is exercised exactly as users hit it. Unreachable hosts
//! prove which URL won — the failure names the host — without needing a
//! database.

use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use tempfile::TempDir;

fn pgmt_in(dir: &TempDir) -> Command {
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("pgmt");
    cmd.current_dir(dir.path());
    // Deterministic regardless of the test runner's environment
    cmd.env_remove("PGMT_DEV_URL");
    cmd.env_remove("PGMT_SHADOW_URL");
    cmd.env_remove("PGMT_TARGET_URL");
    cmd.env_remove("DEV_DATABASE_URL");
    cmd.env_remove("TARGET_DATABASE_URL");
    cmd
}

fn project_with_yaml(yaml: &str) -> TempDir {
    let dir = TempDir::new().unwrap();
    fs::create_dir_all(dir.path().join("schema")).unwrap();
    fs::write(dir.path().join("pgmt.yaml"), yaml).unwrap();
    dir
}

#[test]
fn test_dev_url_required_error_lists_all_sources() {
    let dir = project_with_yaml("directories:\n  schema_dir: schema\n");

    pgmt_in(&dir)
        .args(["apply", "--dry-run"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "No development database configured",
        ))
        .stderr(predicate::str::contains("--dev-url"))
        .stderr(predicate::str::contains("PGMT_DEV_URL"))
        .stderr(predicate::str::contains("databases.dev_url"));
}

#[test]
fn test_yaml_provides_dev_url() {
    let dir = project_with_yaml("databases:\n  dev_url: postgres://yamlhost:1/db\n");

    // Resolution succeeds and the connection failure names the yaml host
    pgmt_in(&dir)
        .args(["apply", "--dry-run"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("yamlhost"));
}

#[test]
fn test_env_overrides_yaml_for_dev_url() {
    let dir = project_with_yaml("databases:\n  dev_url: postgres://yamlhost:1/db\n");

    pgmt_in(&dir)
        .args(["apply", "--dry-run"])
        .env("PGMT_DEV_URL", "postgres://envhost:1/db")
        .assert()
        .failure()
        .stderr(predicate::str::contains("envhost"))
        .stderr(predicate::str::contains("yamlhost").not());
}

#[test]
fn test_cli_flag_overrides_env_for_dev_url() {
    let dir = project_with_yaml("databases:\n  dev_url: postgres://yamlhost:1/db\n");

    pgmt_in(&dir)
        .args(["apply", "--dry-run", "--dev-url", "postgres://clihost:1/db"])
        .env("PGMT_DEV_URL", "postgres://envhost:1/db")
        .assert()
        .failure()
        .stderr(predicate::str::contains("clihost"))
        .stderr(predicate::str::contains("envhost").not());
}

/// The pre-rename names were dropped, not demoted: an ambient
/// DEV_DATABASE_URL from another project must not silently connect pgmt
/// anywhere. With nothing else configured this is a config error.
#[test]
fn test_legacy_env_names_are_not_read() {
    let dir = project_with_yaml("directories:\n  schema_dir: schema\n");

    pgmt_in(&dir)
        .args(["apply", "--dry-run"])
        .env("DEV_DATABASE_URL", "postgres://legacyhost:1/db")
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "No development database configured",
        ))
        .stderr(predicate::str::contains("legacyhost").not());
}

#[test]
fn test_target_required_error_lists_all_sources() {
    let dir = project_with_yaml("databases:\n  dev_url: postgres://yamlhost:1/db\n");

    pgmt_in(&dir)
        .args(["migrate", "apply"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("No target database configured"))
        .stderr(predicate::str::contains("--target-url"))
        .stderr(predicate::str::contains("PGMT_TARGET_URL"));
}

#[test]
fn test_env_overrides_yaml_for_target_url() {
    let dir = project_with_yaml(
        "databases:\n  dev_url: postgres://yamlhost:1/db\n  target_url: postgres://yamltarget:1/db\n",
    );
    // migrate apply returns early without a migrations directory
    fs::create_dir_all(dir.path().join("migrations")).unwrap();

    pgmt_in(&dir)
        .args(["migrate", "apply"])
        .env("PGMT_TARGET_URL", "postgres://envtarget:1/db")
        .assert()
        .failure()
        .stderr(predicate::str::contains("envtarget"))
        .stderr(predicate::str::contains("yamltarget").not());
}
