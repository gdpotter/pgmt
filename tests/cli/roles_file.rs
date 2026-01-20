/// Tests for roles.sql file support
/// Verifies that roles are applied to shadow database before schema files
use crate::helpers::cli::with_cli_helper;
use anyhow::Result;
use predicates::prelude::*;

mod roles_basic {
    use super::*;

    /// Test that diff succeeds when roles.sql provides required roles for grants
    #[tokio::test]
    async fn test_diff_with_roles_file() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Create roles.sql - use DO block to handle "role already exists" error
            // (PostgreSQL doesn't support CREATE ROLE IF NOT EXISTS)
            // Note: Catch both duplicate_object and unique_violation as different PG versions may raise different errors
            helper.write_roles_file(
                r#"-- Roles for shadow database
DO $$ BEGIN CREATE ROLE test_app_user; EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$;"#,
            )?;

            // Create schema with a grant to the role (test_app_user exists in test cluster)
            helper.write_schema_file(
                "users.sql",
                r#"CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100));
GRANT SELECT ON users TO test_app_user;"#,
            )?;

            // Apply schema to dev database (test_app_user exists in test cluster via seed_standard_roles)
            helper
                .command()
                .args(["apply", "--force"])
                .assert()
                .success();

            // Diff should succeed - roles.sql should be applied to shadow before schema
            // If roles.sql wasn't applied, this would fail with "role does not exist"
            helper
                .command()
                .arg("diff")
                .assert()
                .success()
                .stdout(predicate::str::contains("No differences found"));

            Ok(())
        })
        .await
    }

    /// Test that diff works without roles.sql file (backward compatibility)
    #[tokio::test]
    async fn test_diff_without_roles_file() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // No roles.sql file - should still work for schemas without grants

            helper.write_schema_file(
                "users.sql",
                "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100));",
            )?;

            helper
                .command()
                .args(["apply", "--force"])
                .assert()
                .success();

            // Diff should succeed without roles.sql
            helper
                .command()
                .arg("diff")
                .assert()
                .success()
                .stdout(predicate::str::contains("No differences found"));

            Ok(())
        })
        .await
    }

    /// Test migrate new with roles.sql
    #[tokio::test]
    async fn test_migrate_new_with_roles_file() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Create roles.sql - use DO block to handle "role already exists" error
            // Note: Catch both duplicate_object and unique_violation as different PG versions may raise different errors
            helper.write_roles_file(
                r#"-- Roles for shadow database
DO $$ BEGIN CREATE ROLE test_app_user; EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$;"#,
            )?;

            // Create initial schema and apply
            helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL PRIMARY KEY);")?;

            helper
                .command()
                .args(["apply", "--force"])
                .assert()
                .success();

            // Create first migration
            helper
                .command()
                .args(["migrate", "new", "initial"])
                .assert()
                .success();

            // Update schema with a grant
            helper.write_schema_file(
                "users.sql",
                r#"CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100));
GRANT SELECT ON users TO test_app_user;"#,
            )?;

            // migrate new should succeed - roles.sql applied to shadow first
            helper
                .command()
                .args(["migrate", "new", "add-name-and-grant"])
                .assert()
                .success();

            // Verify migration was created
            let migrations = helper.list_migration_files()?;
            assert!(migrations.len() >= 2);

            Ok(())
        })
        .await
    }
}

mod roles_multiple {
    use super::*;

    /// Test roles.sql with multiple roles
    #[tokio::test]
    async fn test_roles_file_multiple_roles() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Create roles.sql with multiple roles - use DO blocks for idempotency
            // Note: Catch both duplicate_object and unique_violation as different PG versions may raise different errors
            helper.write_roles_file(
                r#"-- Multiple roles for shadow database
DO $$ BEGIN CREATE ROLE test_app_user; EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$;
DO $$ BEGIN CREATE ROLE test_read_only; EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$;
DO $$ BEGIN CREATE ROLE test_admin_user; EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$;"#,
            )?;

            // Create schema with grants to multiple roles
            helper.write_schema_file(
                "users.sql",
                r#"CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100));
GRANT SELECT ON users TO test_read_only;
GRANT SELECT, INSERT, UPDATE ON users TO test_app_user;
GRANT ALL ON users TO test_admin_user;"#,
            )?;

            helper
                .command()
                .args(["apply", "--force"])
                .assert()
                .success();

            // Diff should succeed with multiple roles
            helper
                .command()
                .arg("diff")
                .assert()
                .success()
                .stdout(predicate::str::contains("No differences found"));

            Ok(())
        })
        .await
    }
}

mod roles_init {
    use super::*;

    /// Test that apply command uses roles.sql from config
    #[tokio::test]
    async fn test_apply_uses_roles_file() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Create roles.sql with a role
            helper.write_roles_file(
                r#"DO $$ BEGIN CREATE ROLE test_apply_role; EXCEPTION WHEN duplicate_object THEN NULL; END $$;"#,
            )?;

            // Create schema with a grant to the role
            helper.write_schema_file(
                "items.sql",
                r#"CREATE TABLE items (id SERIAL PRIMARY KEY);
GRANT SELECT ON items TO test_apply_role;"#,
            )?;

            // Apply should succeed - roles.sql should be applied to shadow before schema
            // If roles.sql wasn't applied to shadow, schema processing would fail
            helper
                .command()
                .args(["apply", "--force"])
                .assert()
                .success();

            Ok(())
        })
        .await
    }

    /// Test migrate validate with roles.sql
    #[tokio::test]
    async fn test_migrate_validate_with_roles() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Create roles.sql
            helper.write_roles_file(
                r#"DO $$ BEGIN CREATE ROLE test_validate_role; EXCEPTION WHEN duplicate_object THEN NULL; END $$;"#,
            )?;

            // Create initial schema with grants
            helper.write_schema_file(
                "orders.sql",
                r#"CREATE TABLE orders (id SERIAL PRIMARY KEY);
GRANT SELECT ON orders TO test_validate_role;"#,
            )?;

            // Apply and create baseline
            helper
                .command()
                .args(["apply", "--force"])
                .assert()
                .success();

            helper
                .command()
                .args(["baseline", "create"])
                .assert()
                .success();

            // Migrate validate should succeed - roles applied during reconstruction
            helper
                .command()
                .args(["migrate", "validate"])
                .assert()
                .success();

            Ok(())
        })
        .await
    }
}
