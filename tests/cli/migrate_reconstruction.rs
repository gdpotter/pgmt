use crate::helpers::cli::with_cli_helper;
use anyhow::Result;
use predicates::prelude::*;

/// Tests for migration reconstruction behavior when no baselines exist
mod migration_reconstruction_tests {
    use super::*;

    /// Test that migrate new correctly reconstructs from existing migrations
    #[tokio::test]
    async fn test_migrate_new_reconstructs_from_migration_chain() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Create initial schema with a table
            helper.write_schema_file(
                "users.sql",
                "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL);",
            )?;

            // Generate first migration
            helper
                .command()
                .args(["migrate", "new", "create_users_table"])
                .assert()
                .success();

            // Verify we have one migration
            let migrations = helper.list_migration_files()?;
            assert_eq!(migrations.len(), 1);
            assert!(migrations[0].contains("create_users_table"));

            // Add a new column to the schema
            helper.write_schema_file(
                "users.sql",
                r#"CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT
);"#,
            )?;

            // Generate second migration - this should reconstruct the schema from the first migration
            helper
                .command()
                .args(["migrate", "new", "add_email_column"])
                .assert()
                .success()
                .stdout(predicate::str::contains(
                    "Applying 1 existing migration(s) to reconstruct state",
                ))
                .stdout(predicate::str::contains("Applying V"))
                .stdout(predicate::str::contains("create_users_table"));

            // Verify we now have two migrations
            let migrations = helper.list_migration_files()?;
            assert_eq!(migrations.len(), 2);

            // Verify the second migration contains ADD COLUMN (not CREATE TABLE)
            let second_migration = migrations
                .iter()
                .find(|m| m.contains("add_email_column"))
                .unwrap();
            let migration_content = helper.read_migration_file(second_migration)?;
            assert!(migration_content.contains("ADD COLUMN"));
            assert!(migration_content.contains("email"));
            // Should NOT contain CREATE TABLE since it's an incremental change
            assert!(!migration_content.contains("CREATE TABLE"));

            Ok(())
        })
        .await
    }

    /// Test migrate new with no existing migrations (fresh project)
    #[tokio::test]
    async fn test_migrate_new_with_no_existing_migrations() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Create initial schema
            helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL PRIMARY KEY);")?;

            // Generate first migration
            helper
                .command()
                .args(["migrate", "new", "initial_schema"])
                .assert()
                .success()
                .stdout(predicate::str::contains(
                    "No existing migrations found, starting from empty schema",
                ));

            // Verify migration contains full CREATE statement
            let migrations = helper.list_migration_files()?;
            assert_eq!(migrations.len(), 1);

            let migration_content = helper.read_migration_file(&migrations[0])?;
            assert!(migration_content.contains("CREATE TABLE"));
            assert!(migration_content.contains("users"));

            Ok(())
        })
        .await
    }

    /// Test migrate update correctly reconstructs previous state
    #[tokio::test]
    async fn test_migrate_update_reconstructs_previous_state() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Create V1: users table
            helper.write_schema_file("users.sql", "CREATE TABLE users (id SERIAL PRIMARY KEY);")?;
            helper
                .command()
                .args(["migrate", "new", "create_users"])
                .assert()
                .success();

            // Add delay to ensure different timestamps
            tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

            // Create V2: add posts table
            helper.write_schema_file(
                "posts.sql",
                "CREATE TABLE posts (id SERIAL PRIMARY KEY, title TEXT);",
            )?;
            helper
                .command()
                .args(["migrate", "new", "create_posts"])
                .assert()
                .success();

            // Now modify the schema and update the latest migration
            helper.write_schema_file(
                "posts.sql",
                r#"CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    author_id INTEGER
);"#,
            )?;

            // Update should reconstruct state from V1 migration, not from empty
            helper
                .command()
                .args(["migrate", "update"])
                .assert()
                .success()
                .stdout(predicate::str::contains(
                    "Applying 1 existing migration(s) before V",
                ));

            // Verify the updated migration includes both posts table changes and NOT users table
            let migrations = helper.list_migration_files()?;
            let posts_migration = migrations
                .iter()
                .find(|m| m.contains("create_posts"))
                .unwrap();
            let migration_content = helper.read_migration_file(posts_migration)?;

            // Should contain posts table creation (since this is what changed since V1)
            assert!(migration_content.contains("CREATE TABLE"));
            assert!(migration_content.contains("posts"));
            assert!(migration_content.contains("author_id"));

            Ok(())
        })
        .await
    }

    /// Test scenario described in the bug report: clone repo with existing migrations
    #[tokio::test]
    async fn test_clone_repo_scenario() -> Result<()> {
        with_cli_helper(async |helper| {
            helper.init_project()?;

            // Simulate existing migration files from a cloned repo (V1 and V2)
            // V1: Create users table
            helper.write_migration_file(
                "V1234567890_create_users.sql",
                "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL);",
            )?;

            // V2: Add posts table
            helper.write_migration_file(
                "V1234567891_add_posts.sql",
                "CREATE TABLE posts (id SERIAL PRIMARY KEY, title TEXT NOT NULL, user_id INTEGER);",
            )?;

            // Now set up current schema state (what the developer wants)
            helper.write_schema_files(&[
                (
                    "users.sql",
                    "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL);",
                ),
                (
                    "posts.sql",
                    "CREATE TABLE posts (id SERIAL PRIMARY KEY, title TEXT NOT NULL);",
                ), // note: user_id removed
            ])?;

            // Developer makes a change: removes user_id column from posts
            // When they run migrate new, it should:
            // 1. Reconstruct state from V1 + V2 (users + posts with user_id)
            // 2. Compare against current schema (users + posts without user_id)
            // 3. Generate DROP COLUMN migration
            helper
                .command()
                .args(["migrate", "new", "remove_user_id_from_posts"])
                .assert()
                .success()
                .stdout(predicate::str::contains(
                    "Applying 2 existing migration(s) to reconstruct state",
                ));

            // Verify the new migration contains only the DROP COLUMN, not full table recreation
            let migrations = helper.list_migration_files()?;
            let drop_migration = migrations
                .iter()
                .find(|m| m.contains("remove_user_id_from_posts"))
                .unwrap();
            let migration_content = helper.read_migration_file(drop_migration)?;

            assert!(
                migration_content.contains("DROP COLUMN")
                    || migration_content.contains("ALTER TABLE")
            );
            assert!(migration_content.contains("user_id"));
            // Should NOT recreate the entire schema
            assert!(!migration_content.contains("CREATE TABLE users"));
            assert!(!migration_content.contains("CREATE TABLE posts"));

            Ok(())
        })
        .await
    }
}
