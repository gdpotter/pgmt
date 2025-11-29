/// Tests for pgmt debug dependencies command
use crate::helpers::cli::with_cli_helper;
use anyhow::Result;
use predicates::prelude::*;

mod debug_dependencies_tests {
    use super::*;

    /// Test debug dependencies outputs valid JSON
    #[tokio::test]
    async fn test_debug_dependencies_outputs_json() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Create a simple schema with a dependency
            helper.write_schema_file(
                "users.sql",
                "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100));",
            )?;

            helper.write_schema_file(
                "orders.sql",
                r#"-- require: users.sql
CREATE TABLE orders (id SERIAL PRIMARY KEY, user_id INT REFERENCES users(id));"#,
            )?;

            // Run debug dependencies with JSON format
            helper
                .command()
                .args(["debug", "dependencies", "--format", "json"])
                .assert()
                .success()
                .stdout(predicate::str::contains("\"objects\""))
                .stdout(predicate::str::contains("\"file_mappings\""))
                .stdout(predicate::str::contains("\"file_dependencies\""));

            Ok(())
        })
        .await
    }

    /// Test debug dependencies with text format
    #[tokio::test]
    async fn test_debug_dependencies_outputs_text() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL PRIMARY KEY);")?;

            helper
                .command()
                .args(["debug", "dependencies", "--format", "text"])
                .assert()
                .success()
                .stdout(predicate::str::contains("Dependency Report"));

            Ok(())
        })
        .await
    }
}
