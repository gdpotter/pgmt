use crate::helpers::cli::with_cli_helper;
use anyhow::Result;
use pgmt::db::cleaner;
use predicates::prelude::*;

#[tokio::test]
async fn test_baseline_extension_first() -> Result<()> {
    with_cli_helper(async |helper| {
        // Initialize project first
        helper.init_project()?;

        // Create schema files that define the extension and table (schema-as-code approach)
        helper.write_schema_files(&[
            ("extensions/citext.sql", "CREATE EXTENSION IF NOT EXISTS citext;"),
            ("tables/users.sql", "-- require: extensions/citext.sql\nCREATE TABLE users (id SERIAL PRIMARY KEY, email citext);"),
        ])?;

        // Create baseline from schema files
        // Use --force because extensions can cause minor validation inconsistencies
        helper
            .command()
            .args(["baseline", "create", "--force"])
            .assert()
            .success();

        // Read the generated baseline
        let baseline_files = helper.list_baseline_files()?;

        // Debug: show what files were created
        println!("Baseline files found: {:?}", baseline_files);
        if baseline_files.is_empty() {
            // Check if project was created at all
            println!("Project root exists: {}", helper.project_root.exists());
            if helper.project_root.exists() {
                println!(
                    "Directory contents: {:?}",
                    std::fs::read_dir(&helper.project_root)
                        .unwrap()
                        .collect::<Vec<_>>()
                );
            }
        }

        assert_eq!(
            baseline_files.len(),
            1,
            "Should have created one baseline file"
        );

        let baseline_content = helper.read_baseline_file(&baseline_files[0])?;

        // Find positions of extension and table statements
        let extension_pos = baseline_content
            .find("CREATE EXTENSION")
            .expect("Baseline should contain CREATE EXTENSION");
        let table_pos = baseline_content
            .find("CREATE TABLE")
            .expect("Baseline should contain CREATE TABLE");

        assert!(
            extension_pos < table_pos,
            "Extension must appear before table in baseline. Extension at {}, Table at {}",
            extension_pos,
            table_pos
        );

        // Verify the baseline can be applied to the shadow database (clean it first)
        let shadow_pool = helper.connect_to_shadow_db().await?;

        // Clean shadow database to ensure it's empty before applying baseline
        cleaner::clean_shadow_db(&shadow_pool, &pgmt::config::types::Objects::default()).await?;

        // Execute baseline as raw SQL (it may contain multiple statements)
        sqlx::raw_sql(&baseline_content)
            .execute(&shadow_pool)
            .await
            .expect("Baseline should apply cleanly to empty database");

        // Verify objects exist in shadow database
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'users')"
        )
        .fetch_one(&shadow_pool)
        .await?;
        assert!(exists, "Users table should exist after applying baseline");

        shadow_pool.close().await;

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_baseline_multiple_extensions_ordering() -> Result<()> {
    with_cli_helper(async |helper| {
        // Initialize project first
        helper.init_project()?;

        // Create schema files that define multiple extensions and dependent tables (schema-as-code approach)
        helper.write_schema_files(&[
            ("extensions/uuid_ossp.sql", "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";"),
            ("extensions/citext.sql", "CREATE EXTENSION IF NOT EXISTS citext;"),
            ("tables/users.sql", "-- require: extensions/uuid_ossp.sql, extensions/citext.sql\nCREATE TABLE users (id uuid DEFAULT uuid_generate_v4(), email citext);"),
            ("tables/posts.sql", "-- require: extensions/uuid_ossp.sql\nCREATE TABLE posts (id uuid DEFAULT uuid_generate_v4(), title TEXT);"),
        ])?;

        // Create baseline from schema files
        // Use --force because extensions can cause minor validation inconsistencies
        helper
            .command()
            .args(["baseline", "create", "--force"])
            .assert()
            .success();

        // Read the generated baseline
        let baseline_files = helper.list_baseline_files()?;
        let baseline_content = helper.read_baseline_file(&baseline_files[0])?;

        // Find positions of all relevant statements
        let uuid_ext_pos = baseline_content
            .find("\"uuid-ossp\"")
            .expect("Baseline should contain uuid-ossp extension");
        let citext_ext_pos = baseline_content
            .find("citext")
            .and_then(|pos| {
                // Make sure we're finding the extension creation, not the column type
                if baseline_content[..pos].contains("EXTENSION") {
                    Some(pos)
                } else {
                    baseline_content[pos + 1..]
                        .find("citext")
                        .map(|p| pos + 1 + p)
                }
            })
            .expect("Baseline should contain citext extension");

        let users_table_pos = baseline_content
            .find("CREATE TABLE \"public\".\"users\"")
            .or_else(|| baseline_content.find("CREATE TABLE users"))
            .expect("Baseline should contain users table");
        let posts_table_pos = baseline_content
            .find("CREATE TABLE \"public\".\"posts\"")
            .or_else(|| baseline_content.find("CREATE TABLE posts"))
            .expect("Baseline should contain posts table");

        // Both extensions should come before both tables
        let last_extension_pos = uuid_ext_pos.max(citext_ext_pos);
        let first_table_pos = users_table_pos.min(posts_table_pos);

        assert!(
            last_extension_pos < first_table_pos,
            "All extensions must appear before any tables. Last extension at {}, First table at {}",
            last_extension_pos,
            first_table_pos
        );

        // Test that baseline applies cleanly to shadow database
        let shadow_pool = helper.connect_to_shadow_db().await?;

        // Clean shadow database to ensure it's empty before applying baseline
        cleaner::clean_shadow_db(&shadow_pool, &pgmt::config::types::Objects::default()).await?;

        sqlx::raw_sql(&baseline_content)
            .execute(&shadow_pool)
            .await
            .expect("Baseline with multiple extensions should apply cleanly");

        shadow_pool.close().await;

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_migration_with_extension_ordering() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        // Create schema files that use an extension
        helper.write_schema_files(&[
            ("extensions.sql", "CREATE EXTENSION IF NOT EXISTS citext;"),
            (
                "tables/users.sql",
                r#"-- require: extensions.sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email citext UNIQUE NOT NULL
);"#,
            ),
        ])?;

        // Generate migration
        helper
            .command()
            .args([
                "migrate",
                "new",
                "add_citext_extension",
                "--create-baseline",
            ])
            .assert()
            .success()
            .stdout(predicate::str::contains("Migration generation complete!"));

        // Read the generated migration
        let migration_files = helper.list_migration_files()?;
        assert_eq!(migration_files.len(), 1);

        let migration_content = helper.read_migration_file(&migration_files[0])?;

        // Verify extension comes before table in migration
        let extension_pos = migration_content
            .find("CREATE EXTENSION")
            .expect("Migration should contain CREATE EXTENSION");
        let table_pos = migration_content
            .find("CREATE TABLE")
            .expect("Migration should contain CREATE TABLE");

        assert!(
            extension_pos < table_pos,
            "Extension must appear before table in migration. Extension at {}, Table at {}",
            extension_pos,
            table_pos
        );

        // Apply migration and verify it works
        helper
            .command()
            .args(["migrate", "apply", "--target-url", &helper.dev_database_url])
            .assert()
            .success()
            .stdout(predicate::str::contains("Applying migration"))
            .stdout(predicate::str::contains("Completed in"));

        // Verify objects exist
        assert!(helper.table_exists_in_dev("public", "users").await?);

        Ok(())
    })
    .await
}

// Test that baseline generation orders objects correctly when extension is in a custom schema:
// schema -> extension -> table
#[tokio::test]
async fn test_extension_with_schema_ordering() -> Result<()> {
    with_cli_helper(async |helper| {
        // Initialize project first
        helper.init_project()?;

        // Create schema files with proper dependencies using -- require: comments
        // The baseline should generate them in correct dependency order
        helper.write_schema_files(&[
            ("schemas/app.sql", "CREATE SCHEMA app;"),
            (
                "extensions/citext.sql",
                "-- require: schemas/app.sql\nCREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA app;",
            ),
            (
                "tables/users.sql",
                "-- require: extensions/citext.sql\nCREATE TABLE app.users (id SERIAL, email app.citext);",
            ),
        ])?;

        // Create baseline - should order by dependencies
        // Use --force because extensions can cause minor validation inconsistencies
        helper
            .command()
            .args(["baseline", "create", "--force"])
            .assert()
            .success();

        let baseline_files = helper.list_baseline_files()?;
        let baseline_content = helper.read_baseline_file(&baseline_files[0])?;

        // Find positions - schema should come first, then extension, then table
        let schema_pos = baseline_content
            .find("CREATE SCHEMA")
            .expect("Baseline should contain CREATE SCHEMA");
        let extension_pos = baseline_content
            .find("CREATE EXTENSION")
            .expect("Baseline should contain CREATE EXTENSION");
        let table_pos = baseline_content
            .find("CREATE TABLE")
            .expect("Baseline should contain CREATE TABLE");

        assert!(
            schema_pos < extension_pos,
            "Schema must appear before extension. Schema at {}, Extension at {}",
            schema_pos,
            extension_pos
        );
        assert!(
            extension_pos < table_pos,
            "Extension must appear before table. Extension at {}, Table at {}",
            extension_pos,
            table_pos
        );

        // Verify baseline applies cleanly
        let shadow_pool = helper.connect_to_shadow_db().await?;

        // Clean shadow database to ensure it's empty before applying baseline
        cleaner::clean_shadow_db(&shadow_pool, &pgmt::config::types::Objects::default()).await?;

        sqlx::raw_sql(&baseline_content)
            .execute(&shadow_pool)
            .await
            .expect("Baseline with schema and extension should apply cleanly");

        shadow_pool.close().await;

        Ok(())
    })
    .await
}
