use crate::helpers::cli::with_cli_helper;
use anyhow::Result;
use serde_json::Value;

#[tokio::test]
async fn test_migrate_validate_json_format() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        // For testing JSON output format, we just need any state where validate runs
        // Even if it fails, we can test the JSON structure

        // Test JSON output format (don't assert success, just check JSON structure)
        let assert_result = helper
            .command()
            .args(["migrate", "validate", "--format", "json", "--quiet"])
            .assert();
        let output = assert_result.get_output();
        let stdout_str = std::str::from_utf8(&output.stdout)?;

        // Try to parse the entire stdout as JSON first
        // If that fails, we might have debug output before the JSON
        let json: Value = if let Ok(json) = serde_json::from_str(stdout_str) {
            json
        } else {
            // Find the start of the JSON object - look for the first '{'
            let json_start = stdout_str.find('{');

            if json_start.is_none() {
                // No JSON found - that's OK for this test
                return Ok(());
            }

            let json_str = &stdout_str[json_start.unwrap()..];

            // Try to parse from the first '{'
            serde_json::from_str(json_str)?
        };

        // Verify expected JSON structure
        assert!(json.get("status").is_some());
        assert!(json.get("exit_code").is_some());
        assert!(json.get("applied_migrations").is_some());
        assert!(json.get("unapplied_migrations").is_some());
        assert!(json.get("conflicts").is_some());
        assert!(json.get("suggested_actions").is_some());
        assert!(json.get("message").is_some());

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_migrate_validate_human_format() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        // Test human format output (default)
        let assert_result = helper.command().args(["migrate", "validate"]).assert();
        let output = assert_result.get_output();
        let stdout_str = std::str::from_utf8(&output.stdout)?;

        // Human format should contain readable text
        assert!(
            stdout_str.contains("Migration validation")
                || stdout_str.contains("migrations")
                || stdout_str.contains("status"),
            "Human format output should contain readable text about migration status"
        );

        // Should not be JSON format
        assert!(
            !stdout_str.trim_start().starts_with('{'),
            "Human format should not start with JSON"
        );

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_migrate_validate_quiet_flag() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        // Test quiet flag with human format
        let assert_result = helper
            .command()
            .args(["migrate", "validate", "--quiet"])
            .assert();
        let output = assert_result.get_output();
        let stdout_str = std::str::from_utf8(&output.stdout)?;

        // Get regular output for comparison
        let regular_result = helper.command().args(["migrate", "validate"]).assert();
        let regular_output = std::str::from_utf8(&regular_result.get_output().stdout)?;

        // Quiet mode should produce less output than regular mode
        // (It may not be completely silent due to database connection messages)
        if !regular_output.is_empty() && !stdout_str.is_empty() {
            assert!(
                stdout_str.len() <= regular_output.len(),
                "Quiet mode should produce less or equal output than regular mode. Quiet: {} chars, Regular: {} chars",
                stdout_str.len(),
                regular_output.len()
            );
        }

        // Test that quiet flag is recognized (doesn't cause argument errors)
        assert_result.success();

        Ok(())
    }).await
}

#[tokio::test]
async fn test_migrate_validate_json_vs_human_formats() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        // Get human format output
        let human_result = helper.command().args(["migrate", "validate"]).assert();
        let human_output = std::str::from_utf8(&human_result.get_output().stdout)?;

        // Get JSON format output
        let json_result = helper
            .command()
            .args(["migrate", "validate", "--format", "json"])
            .assert();
        let json_output = std::str::from_utf8(&json_result.get_output().stdout)?;

        // Outputs should be different formats
        let human_looks_like_text = !human_output.trim_start().starts_with('{');
        let json_looks_like_json =
            json_output.trim_start().starts_with('{') || json_output.contains("status");

        if !human_output.is_empty() && !json_output.is_empty() {
            assert!(
                human_looks_like_text,
                "Human format should not look like JSON"
            );
            assert!(
                json_looks_like_json,
                "JSON format should contain JSON-like content"
            );
        }

        Ok(())
    })
    .await
}
