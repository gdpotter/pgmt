use crate::helpers::harness::PgTestInstance;
use anyhow::{Context, Result};
use assert_cmd::Command;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;
use uuid::Uuid;

/// Modern CLI test helper that combines assert_cmd and expectrl for comprehensive testing
pub struct CliTestHelper {
    pub temp_dir: TempDir,
    pub project_root: PathBuf,
    pub pg_instance: PgTestInstance,
    pub dev_database_url: String,
    pub shadow_database_url: String,
    dev_db_name: String,
    shadow_db_name: String,
}

impl CliTestHelper {
    /// Create a new CLI test environment with temporary project and databases
    pub async fn new() -> Self {
        // Load .env file for test environment configuration
        dotenv::dotenv().ok();

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let project_root = temp_dir.path().to_path_buf();

        let pg_instance = PgTestInstance::new().await;

        // Create two separate database names for dev and shadow
        let _port = pg_instance.get_host_port();
        let dev_db_name = format!("test_dev_{}", Uuid::new_v4().simple());
        let shadow_db_name = format!("test_shadow_{}", Uuid::new_v4().simple());

        // Create the databases manually using the PostgreSQL instance
        let base_pool = sqlx::PgPool::connect(&pg_instance.base_url)
            .await
            .expect("Failed to connect to postgres");

        sqlx::query(&format!("CREATE DATABASE \"{}\"", dev_db_name))
            .execute(&base_pool)
            .await
            .expect("Failed to create dev database");
        sqlx::query(&format!("CREATE DATABASE \"{}\"", shadow_db_name))
            .execute(&base_pool)
            .await
            .expect("Failed to create shadow database");

        base_pool.close().await;

        let dev_database_url = if let Some(last_slash) = pg_instance.base_url.rfind('/') {
            format!("{}/{}", &pg_instance.base_url[..last_slash], dev_db_name)
        } else {
            format!("{}/{}", pg_instance.base_url, dev_db_name)
        };
        let shadow_database_url = if let Some(last_slash) = pg_instance.base_url.rfind('/') {
            format!("{}/{}", &pg_instance.base_url[..last_slash], shadow_db_name)
        } else {
            format!("{}/{}", pg_instance.base_url, shadow_db_name)
        };

        Self {
            temp_dir,
            project_root,
            pg_instance,
            dev_database_url,
            shadow_database_url,
            dev_db_name,
            shadow_db_name,
        }
    }

    /// Cleanup test databases manually
    pub async fn cleanup(&self) {
        let base_url = self.pg_instance.base_url.clone();
        let dev_db_name = self.dev_db_name.clone();
        let shadow_db_name = self.shadow_db_name.clone();

        // Best-effort cleanup with timeout
        let cleanup_future = async move {
            if let Ok(pool) = sqlx::PgPool::connect(&base_url).await {
                let _ = sqlx::query(&format!(
                    "DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)",
                    dev_db_name
                ))
                .execute(&pool)
                .await;
                let _ = sqlx::query(&format!(
                    "DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)",
                    shadow_db_name
                ))
                .execute(&pool)
                .await;
                pool.close().await;
            }
        };

        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), cleanup_future).await;
    }

    /// Initialize a pgmt project structure
    pub fn init_project(&self) -> Result<()> {
        // Create project directories
        fs::create_dir_all(self.project_root.join("schema"))?;
        fs::create_dir_all(self.project_root.join("migrations"))?;
        fs::create_dir_all(self.project_root.join("schema_baselines"))?;

        // Create pgmt.yaml config file with new structured format
        let config_content = format!(
            r#"databases:
  dev_url: {}
  shadow:
    auto: false
    url: {}

directories:
  schema_dir: schema/
  migrations_dir: migrations/
  baselines_dir: schema_baselines/
  roles_file: roles.sql

objects:
  include:
    schemas: []
    tables: []
  exclude:
    schemas: ["pg_*", "information_schema"]
    tables: []
  comments: true
  grants: true
  triggers: true
  extensions: true

migration:
  default_mode: safe_only
  validate_baseline_consistency: true
  create_baselines_by_default: false

docker:
  auto_cleanup: true
  check_system_identifier: true
"#,
            self.dev_database_url, self.shadow_database_url
        );

        fs::write(self.project_root.join("pgmt.yaml"), config_content)?;

        Ok(())
    }

    /// Create a command for non-interactive CLI testing using assert_cmd
    /// This is the preferred method for testing straightforward commands
    pub fn command(&self) -> Command {
        let mut cmd = Command::cargo_bin("pgmt").unwrap();
        cmd.current_dir(&self.project_root);
        cmd
    }

    /// Create an interactive session for testing prompts using expectrl
    /// Use this for commands that require user input
    #[cfg(not(windows))]
    pub async fn interactive_command(&self, args: &[&str]) -> Result<expectrl::Session> {
        use std::process::Command;

        // Use the pre-built binary instead of cargo run for better reliability
        let binary_path = env!("CARGO_BIN_EXE_pgmt");

        let mut cmd = Command::new(binary_path);
        cmd.args(args).current_dir(&self.project_root);

        // Use Session::spawn with Command
        let mut session = expectrl::Session::spawn(cmd)?;
        session.set_expect_timeout(Some(std::time::Duration::from_secs(30)));
        Ok(session)
    }

    /// Write a schema file to the project
    pub fn write_schema_file(&self, filename: &str, content: &str) -> Result<()> {
        let schema_path = self.project_root.join("schema").join(filename);
        if let Some(parent) = schema_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(schema_path, content)?;
        Ok(())
    }

    /// Write a roles.sql file to the project root
    pub fn write_roles_file(&self, content: &str) -> Result<()> {
        fs::write(self.project_root.join("roles.sql"), content)?;
        Ok(())
    }

    /// Write multiple schema files at once
    pub fn write_schema_files(&self, files: &[(&str, &str)]) -> Result<()> {
        for (filename, content) in files {
            self.write_schema_file(filename, content)?;
        }
        Ok(())
    }

    /// Get the path to the migrations directory
    pub fn migrations_dir(&self) -> PathBuf {
        self.project_root.join("migrations")
    }

    /// Get the path to the baselines directory
    pub fn baselines_dir(&self) -> PathBuf {
        self.project_root.join("schema_baselines")
    }

    /// List all migration files
    pub fn list_migration_files(&self) -> Result<Vec<String>> {
        let migrations_dir = self.migrations_dir();
        if !migrations_dir.exists() {
            return Ok(vec![]);
        }

        let mut files = Vec::new();
        for entry in fs::read_dir(migrations_dir)? {
            let entry = entry?;
            if let Some(filename) = entry.file_name().to_str()
                && filename.ends_with(".sql")
            {
                files.push(filename.to_string());
            }
        }
        files.sort();
        Ok(files)
    }

    /// List all baseline files
    pub fn list_baseline_files(&self) -> Result<Vec<String>> {
        let baselines_dir = self.baselines_dir();
        if !baselines_dir.exists() {
            return Ok(vec![]);
        }

        let mut files = Vec::new();
        for entry in fs::read_dir(baselines_dir)? {
            let entry = entry?;
            if let Some(filename) = entry.file_name().to_str()
                && filename.ends_with(".sql")
            {
                files.push(filename.to_string());
            }
        }
        files.sort();
        Ok(files)
    }

    /// Read the content of a migration file
    pub fn read_migration_file(&self, filename: &str) -> Result<String> {
        let path = self.migrations_dir().join(filename);
        fs::read_to_string(path).context("Failed to read migration file")
    }

    /// Write a migration file directly (for testing scenarios with existing migrations)
    pub fn write_migration_file(&self, filename: &str, content: &str) -> Result<()> {
        let migrations_dir = self.migrations_dir();
        fs::create_dir_all(&migrations_dir)?;
        let path = migrations_dir.join(filename);
        fs::write(path, content).context("Failed to write migration file")
    }

    /// Read the content of a baseline file
    pub fn read_baseline_file(&self, filename: &str) -> Result<String> {
        let path = self.baselines_dir().join(filename);
        fs::read_to_string(path).context("Failed to read baseline file")
    }

    /// Connect to the dev database and return a connection pool
    pub async fn connect_to_dev_db(&self) -> Result<sqlx::PgPool> {
        sqlx::PgPool::connect(&self.dev_database_url)
            .await
            .context("Failed to connect to dev database")
    }

    /// Connect to the shadow database and return a connection pool
    pub async fn connect_to_shadow_db(&self) -> Result<sqlx::PgPool> {
        sqlx::PgPool::connect(&self.shadow_database_url)
            .await
            .context("Failed to connect to shadow database")
    }

    /// Verify that a table exists in the dev database
    pub async fn table_exists_in_dev(&self, schema: &str, table: &str) -> Result<bool> {
        let pool = self.connect_to_dev_db().await?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)"
        )
        .bind(schema)
        .bind(table)
        .fetch_one(&pool)
        .await?;
        pool.close().await;
        Ok(exists)
    }

    /// Get the comment on a table in the dev database
    pub async fn get_table_comment_in_dev(
        &self,
        schema: &str,
        table: &str,
    ) -> Result<Option<String>> {
        let pool = self.connect_to_dev_db().await?;
        let comment: Option<Option<String>> = sqlx::query_scalar(
            "SELECT obj_description(c.oid) FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relname = $2",
        )
        .bind(schema)
        .bind(table)
        .fetch_optional(&pool)
        .await?;
        pool.close().await;
        Ok(comment.flatten())
    }
}

/// Run a CLI test with automatic database cleanup
///
/// This is the idiomatic pattern for CLI testing with guaranteed cleanup.
/// Similar to `with_test_db` for database tests.
///
/// # Example
/// ```
/// #[tokio::test]
/// async fn test_migrate_new() -> Result<()> {
///     with_cli_helper(async |helper| {
///         helper.init_project()?;
///         helper.command()
///             .args(&["migrate", "new", "test"])
///             .assert()
///             .success();
///         Ok(())
///     }).await
/// }
/// ```
pub async fn with_cli_helper<F, R>(test_fn: F) -> R
where
    F: std::ops::AsyncFnOnce(&CliTestHelper) -> R,
{
    let helper = CliTestHelper::new().await;
    let result = test_fn(&helper).await;
    helper.cleanup().await;
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cli_helper_setup() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Verify project structure was created
            assert!(helper.project_root.join("pgmt.yaml").exists());
            assert!(helper.project_root.join("schema").exists());
            assert!(helper.project_root.join("migrations").exists());
            assert!(helper.project_root.join("schema_baselines").exists());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_creation() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Test that we can create a command
            let mut cmd = helper.command();
            cmd.arg("--help");

            Ok(())
        })
        .await
    }
}
