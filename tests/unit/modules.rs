//! Config-load validation of `modules:` declarations.
//!
//! Validation runs in `ConfigBuilder::resolve`, so every command fails fast
//! on a broken declaration — like any other yaml error. Attribution machinery
//! itself is unit-tested in `src/modules.rs`.

use anyhow::Result;
use pgmt::config::builder::ConfigBuilder;
use pgmt::config::types::ConfigInput;

fn resolve_yaml(yaml: &str) -> Result<pgmt::config::Config> {
    let input: ConfigInput = serde_yaml::from_str(yaml)?;
    ConfigBuilder::new().with_file(input).resolve()
}

#[test]
fn test_modules_absent_is_feature_off() -> Result<()> {
    let config = resolve_yaml("directories:\n  schema_dir: schema/\n")?;
    assert!(!config.modules.is_enabled());
    Ok(())
}

#[test]
fn test_modules_parse_and_resolve() -> Result<()> {
    let config = resolve_yaml(
        r#"
modules:
  core:
    paths: ["schema/core/**"]
  billing:
    paths: ["schema/billing/**", "schema/shared_billing/**"]
    depends_on: [core]
"#,
    )?;
    assert!(config.modules.is_enabled());
    let billing = &config.modules.modules["billing"];
    assert_eq!(billing.paths.len(), 2);
    assert_eq!(billing.depends_on, vec!["core"]);
    Ok(())
}

#[test]
fn test_module_name_grammar_enforced() {
    for bad in ["Core", "1core", "core-api", "core api", "cöre", ""] {
        let yaml = format!("modules:\n  \"{}\":\n    paths: [\"x/**\"]\n", bad);
        let err = resolve_yaml(&yaml).unwrap_err().to_string();
        assert!(
            err.contains("invalid module name"),
            "name '{}' should be rejected: {err}",
            bad
        );
    }
}

#[test]
fn test_module_name_default_reserved() {
    let err = resolve_yaml("modules:\n  default:\n    paths: [\"x/**\"]\n")
        .unwrap_err()
        .to_string();
    assert!(err.contains("reserved"), "{err}");
}

#[test]
fn test_module_requires_paths() {
    let err = resolve_yaml("modules:\n  core: {}\n")
        .unwrap_err()
        .to_string();
    assert!(err.contains("at least one entry in `paths`"), "{err}");
}

#[test]
fn test_depends_on_must_reference_declared_module() {
    let err = resolve_yaml("modules:\n  billing:\n    paths: [\"b/**\"]\n    depends_on: [core]\n")
        .unwrap_err()
        .to_string();
    assert!(err.contains("undeclared module 'core'"), "{err}");
}

#[test]
fn test_self_dependency_rejected() {
    let err = resolve_yaml("modules:\n  core:\n    paths: [\"c/**\"]\n    depends_on: [core]\n")
        .unwrap_err()
        .to_string();
    assert!(err.contains("cannot depend on itself"), "{err}");
}

#[test]
fn test_dependency_cycle_rejected() {
    let err = resolve_yaml(
        r#"
modules:
  a:
    paths: ["a/**"]
    depends_on: [b]
  b:
    paths: ["b/**"]
    depends_on: [c]
  c:
    paths: ["c/**"]
    depends_on: [a]
"#,
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("acyclic"), "{err}");
    assert!(err.contains("->"), "should name the cycle: {err}");
}

#[test]
fn test_conflicts_are_symmetrized() -> Result<()> {
    // Declared on one side only; the reverse edge is filled in.
    let config = resolve_yaml(
        r#"
modules:
  us:
    paths: ["us/**"]
    conflicts_with: [eu]
  eu:
    paths: ["eu/**"]
"#,
    )?;
    assert_eq!(config.modules.modules["eu"].conflicts_with, vec!["us"]);
    Ok(())
}

#[test]
fn test_depend_and_conflict_on_same_module_rejected() {
    let err = resolve_yaml(
        r#"
modules:
  a:
    paths: ["a/**"]
    depends_on: [b]
    conflicts_with: [b]
  b:
    paths: ["b/**"]
"#,
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("both depends on and conflicts with"), "{err}");
}

#[test]
fn test_invalid_glob_rejected() {
    let err = resolve_yaml("modules:\n  core:\n    paths: [\"[\"]\n")
        .unwrap_err()
        .to_string();
    assert!(err.contains("invalid path glob"), "{err}");
}
