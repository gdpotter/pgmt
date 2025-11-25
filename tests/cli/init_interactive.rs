//! CLI tests for pgmt init command
//! These tests verify the init command integration and basic functionality

use anyhow::Result;
use pgmt::commands::init::import::ImportSource;
use std::fs;
use tempfile::TempDir;
// Only run interactive tests on Unix-like systems where expectrl is available

// TODO: Implement interactive tests once the CliTestHelper is properly accessible
// The tests below would require the full interactive pgmt init command to be implemented

#[cfg(not(windows))]
#[tokio::test]
async fn test_init_interactive_help() -> anyhow::Result<()> {
    use crate::helpers::cli::with_cli_helper;
    use expectrl::Eof;

    with_cli_helper(async |helper| {
        // Test init help command interactively
        let mut session = helper.interactive_command(&["init", "--help"]).await?;

        // Wait for the help output
        session.expect("Initialize")?;
        session.expect(Eof)?;

        Ok(())
    })
    .await
}

#[cfg(not(windows))]
#[tokio::test]
async fn test_init_command_with_flags() -> anyhow::Result<()> {
    use crate::helpers::cli::with_cli_helper;

    with_cli_helper(async |helper| {
        // Test init command with database URL flag (should work without prompting)
        let result = helper
            .command()
            .args(["init", "--dev-url", "postgres://test/db", "--help"])
            .assert();

        result.success();

        Ok(())
    })
    .await
}

#[cfg(windows)]
#[test]
fn test_init_windows_compatibility() {
    // Test basic init functionality that works on Windows
    use assert_cmd::Command;

    let mut cmd = Command::cargo_bin("pgmt").unwrap();
    cmd.args(["init", "--help"]);

    // Should succeed and show init help
    cmd.assert().success();
}

#[test]
fn test_init_cli_help_non_interactive() {
    // This test can run on all platforms using assert_cmd
    use assert_cmd::Command;

    let mut cmd = Command::cargo_bin("pgmt").unwrap();
    cmd.args(["init", "--help"]);

    let output = cmd.assert().success();
    let stdout = std::str::from_utf8(&output.get_output().stdout).unwrap();

    // Verify help text contains key information
    assert!(stdout.contains("init"));
    assert!(stdout.contains("Initialize") || stdout.contains("initialize"));
}

#[test]
fn test_init_command_exists_in_cli() {
    // Verify that the init subcommand is recognized
    use assert_cmd::Command;

    let mut cmd = Command::cargo_bin("pgmt").unwrap();
    cmd.args(["help", "init"]);

    // Should succeed and show init help
    cmd.assert().success();
}

#[test]
fn test_pgmt_version() {
    use assert_cmd::Command;

    let mut cmd = Command::cargo_bin("pgmt").unwrap();
    cmd.args(["--version"]);

    cmd.assert().success();
}

/// Test the init workflow components that can be tested without full interactivity
#[test]
fn test_init_workflow_components() -> Result<()> {
    use pgmt::commands::init::{BaselineCreationConfig, InitOptions, ObjectManagementConfig};
    use pgmt::prompts::ShadowDatabaseInput;
    use std::path::PathBuf;

    let temp_dir = TempDir::new()?;
    let project_path = temp_dir.path().to_path_buf();

    // Test that we can create InitOptions struct
    let options = InitOptions {
        project_dir: project_path.clone(),
        dev_database_url: "postgres://localhost/test".to_string(),
        shadow_config: ShadowDatabaseInput::Auto,
        schema_dir: PathBuf::from("schema"),
        import_source: None,
        object_config: ObjectManagementConfig::default(),
        baseline_config: BaselineCreationConfig::default(),
        tracking_table: pgmt::config::types::TrackingTable::default(),
        roles_file: None,
    };

    // Test project structure creation
    pgmt::commands::init::project::create_project_structure(
        &options.project_dir,
        &options.schema_dir,
    )?;

    // Verify structure was created
    assert!(project_path.join("schema").exists());
    assert!(project_path.join("migrations").exists());
    assert!(project_path.join("schema_baselines").exists());

    // Test config file generation
    pgmt::commands::init::project::generate_config_file(&options, &options.project_dir)?;

    // Verify config was created
    assert!(project_path.join("pgmt.yaml").exists());

    let config_content = fs::read_to_string(project_path.join("pgmt.yaml"))?;
    assert!(config_content.contains("postgres://localhost/test"));
    assert!(config_content.contains("auto: true"));

    Ok(())
}

/// Test error handling in init workflow
#[test]
fn test_init_workflow_error_handling() -> Result<()> {
    use pgmt::commands::init::{BaselineCreationConfig, InitOptions, ObjectManagementConfig};
    use pgmt::prompts::ShadowDatabaseInput;
    use std::path::PathBuf;

    // Test with non-existent import file
    let temp_dir = TempDir::new()?;
    let project_path = temp_dir.path().to_path_buf();
    let non_existent_file = temp_dir.path().join("does_not_exist.sql");

    let options = InitOptions {
        project_dir: project_path.clone(),
        dev_database_url: "postgres://localhost/test".to_string(),
        shadow_config: ShadowDatabaseInput::Auto,
        schema_dir: PathBuf::from("schema"),
        import_source: Some(ImportSource::SqlFile(non_existent_file)),
        object_config: ObjectManagementConfig::default(),
        baseline_config: BaselineCreationConfig::default(),
        tracking_table: pgmt::config::types::TrackingTable::default(),
        roles_file: None,
    };

    // Project structure creation should still work
    let result = pgmt::commands::init::project::create_project_structure(
        &options.project_dir,
        &options.schema_dir,
    );
    assert!(result.is_ok());

    // Config generation should work
    let result =
        pgmt::commands::init::project::generate_config_file(&options, &options.project_dir);
    assert!(result.is_ok());

    // Import validation would be handled separately in the import logic

    Ok(())
}

#[cfg(not(windows))]
#[tokio::test]
async fn test_init_with_dev_url_flag() -> Result<()> {
    use crate::helpers::cli::with_cli_helper;

    with_cli_helper(async |helper| {
        // Test init with --dev-url flag (should work without prompting)
        let result = helper
            .command()
            .args([
                "init",
                "--dev-url",
                "postgres://localhost/test_db",
                "--help",
            ])
            .assert();

        // Should succeed (help flag prevents actual init)
        result.success();

        Ok(())
    })
    .await
}

/// Test that CLI handles existing config files appropriately
#[tokio::test]
async fn test_init_with_existing_config() -> Result<()> {
    use crate::helpers::cli::with_cli_helper;

    with_cli_helper(async |helper| {
        helper.init_project()?;

        // Test init in directory that already has a pgmt.yaml
        let result = helper
            .command()
            .args(["init", "--dev-url", "postgres://test/db"])
            .assert();

        let output = result.get_output();
        let stdout_str = std::str::from_utf8(&output.stdout).unwrap_or("");
        let stderr_str = std::str::from_utf8(&output.stderr).unwrap_or("");

        // Should handle existing config appropriately (provide some feedback)
        let has_meaningful_output = !stdout_str.is_empty() || !stderr_str.is_empty();
        assert!(
            has_meaningful_output,
            "Init should provide feedback when config already exists"
        );

        Ok(())
    })
    .await
}
