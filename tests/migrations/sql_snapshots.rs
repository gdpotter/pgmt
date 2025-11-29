//! SQL output snapshot tests using insta
//!
//! These tests verify that SQL generation is stable and produces expected output.
//! Snapshot testing helps catch unintended changes to SQL formatting.
//!
//! To update snapshots after intentional changes:
//! ```bash
//! cargo insta review
//! ```

use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use insta::assert_snapshot;
use pgmt::diff::operations::SqlRenderer;

/// Helper to render migration steps to a single SQL string for snapshotting
fn render_steps_to_sql(steps: &[pgmt::diff::operations::MigrationStep]) -> String {
    steps
        .iter()
        .flat_map(|step| step.to_sql())
        .map(|rendered| rendered.sql)
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// Snapshot test for CREATE TABLE SQL output
#[tokio::test]
async fn test_snapshot_create_table() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE SCHEMA app"],
            &[],
            &["CREATE TABLE app.users (
                id SERIAL PRIMARY KEY,
                email VARCHAR(255) NOT NULL UNIQUE,
                name TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )"],
            |steps, _| {
                let sql = render_steps_to_sql(steps);
                assert_snapshot!("create_table_users", sql);
                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Snapshot test for CREATE TABLE with constraints
#[tokio::test]
async fn test_snapshot_create_table_with_constraints() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE SCHEMA app"],
            &[],
            &["CREATE TABLE app.orders (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                status VARCHAR(20) CHECK (status IN ('pending', 'completed', 'cancelled')),
                total NUMERIC(10, 2) NOT NULL CHECK (total >= 0),
                created_at TIMESTAMP DEFAULT NOW()
            )"],
            |steps, _| {
                let sql = render_steps_to_sql(steps);
                assert_snapshot!("create_table_orders_with_constraints", sql);
                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Snapshot test for ALTER TABLE ADD COLUMN
#[tokio::test]
async fn test_snapshot_alter_table_add_column() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &[
                "CREATE SCHEMA app",
                "CREATE TABLE app.users (id SERIAL PRIMARY KEY)",
            ],
            &[],
            &["ALTER TABLE app.users ADD COLUMN email VARCHAR(255) NOT NULL"],
            |steps, _| {
                let sql = render_steps_to_sql(steps);
                assert_snapshot!("alter_table_add_column", sql);
                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Snapshot test for CREATE INDEX
#[tokio::test]
async fn test_snapshot_create_index() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &[
                "CREATE SCHEMA app",
                "CREATE TABLE app.users (id SERIAL PRIMARY KEY, email TEXT, name TEXT)",
            ],
            &[],
            &[
                "CREATE INDEX idx_users_email ON app.users(email)",
                "CREATE UNIQUE INDEX idx_users_name ON app.users(name) WHERE name IS NOT NULL",
            ],
            |steps, _| {
                let sql = render_steps_to_sql(steps);
                assert_snapshot!("create_indexes", sql);
                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Snapshot test for CREATE FUNCTION
#[tokio::test]
async fn test_snapshot_create_function() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE SCHEMA app"],
            &[],
            &[
                "CREATE OR REPLACE FUNCTION app.add_numbers(a INTEGER, b INTEGER)
                RETURNS INTEGER
                LANGUAGE SQL
                IMMUTABLE
                AS $$ SELECT a + b $$",
            ],
            |steps, _| {
                let sql = render_steps_to_sql(steps);
                assert_snapshot!("create_function", sql);
                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Snapshot test for CREATE VIEW
#[tokio::test]
async fn test_snapshot_create_view() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    // Skip on PG < 16 due to pg_get_viewdef() formatting differences
    if helper.pg_major_version().await < 16 {
        eprintln!("Skipping test_snapshot_create_view (requires PostgreSQL 16+)");
        return Ok(());
    }

    helper
        .run_migration_test(
            &[
                "CREATE SCHEMA app",
                "CREATE TABLE app.users (id SERIAL PRIMARY KEY, email TEXT, active BOOLEAN)",
            ],
            &[],
            &["CREATE VIEW app.active_users AS
                SELECT id, email
                FROM app.users
                WHERE active = true"],
            |steps, _| {
                let sql = render_steps_to_sql(steps);
                assert_snapshot!("create_view", sql);
                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Snapshot test for CREATE TYPE (enum)
#[tokio::test]
async fn test_snapshot_create_enum_type() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE SCHEMA app"],
            &[],
            &["CREATE TYPE app.status AS ENUM ('pending', 'active', 'completed', 'cancelled')"],
            |steps, _| {
                let sql = render_steps_to_sql(steps);
                assert_snapshot!("create_enum_type", sql);
                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Snapshot test for CREATE TRIGGER
#[tokio::test]
async fn test_snapshot_create_trigger() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &[
                "CREATE SCHEMA app",
                "CREATE TABLE app.users (id SERIAL PRIMARY KEY, updated_at TIMESTAMP)",
                "CREATE OR REPLACE FUNCTION app.update_timestamp()
                    RETURNS TRIGGER
                    LANGUAGE plpgsql
                    AS $$ BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$",
            ],
            &[],
            &["CREATE TRIGGER update_users_timestamp
                BEFORE UPDATE ON app.users
                FOR EACH ROW
                EXECUTE FUNCTION app.update_timestamp()"],
            |steps, _| {
                let sql = render_steps_to_sql(steps);
                assert_snapshot!("create_trigger", sql);
                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Snapshot test for DROP TABLE
#[tokio::test]
async fn test_snapshot_drop_table() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &["CREATE SCHEMA app"],
            &["CREATE TABLE app.old_table (id INT)"],
            &[],
            |steps, _| {
                let sql = render_steps_to_sql(steps);
                assert_snapshot!("drop_table", sql);
                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Snapshot test for COMMENT ON TABLE
#[tokio::test]
async fn test_snapshot_comment_on_table() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &[
                "CREATE SCHEMA app",
                "CREATE TABLE app.users (id SERIAL PRIMARY KEY)",
            ],
            &[],
            &["COMMENT ON TABLE app.users IS 'Stores user account information'"],
            |steps, _| {
                let sql = render_steps_to_sql(steps);
                assert_snapshot!("comment_on_table", sql);
                Ok(())
            },
        )
        .await?;

    Ok(())
}

/// Snapshot test for complex migration with multiple steps
#[tokio::test]
async fn test_snapshot_complex_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper
        .run_migration_test(
            &[],
            &[],
            &[
                "CREATE SCHEMA app",
                "CREATE TYPE app.priority AS ENUM ('low', 'medium', 'high')",
                "CREATE TABLE app.tasks (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    priority app.priority DEFAULT 'medium',
                    created_at TIMESTAMP DEFAULT NOW()
                )",
                "CREATE INDEX idx_tasks_priority ON app.tasks(priority)",
                "COMMENT ON TABLE app.tasks IS 'Task management table'",
            ],
            |steps, _| {
                let sql = render_steps_to_sql(steps);
                assert_snapshot!("complex_migration_multiple_objects", sql);
                Ok(())
            },
        )
        .await?;

    Ok(())
}
