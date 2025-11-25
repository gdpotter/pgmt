use crate::helpers::harness::with_test_db;
use anyhow::Result;
use pgmt::commands::migrate::section_executor::{ExecutionMode, SectionExecutor};
use pgmt::config::types::TrackingTable;
use pgmt::migration::{parse_migration_sections, validate_sections};
use pgmt::migration_tracking::section_tracking::{
    SectionStatus, ensure_section_tracking_table, get_section_status, initialize_sections,
};
use pgmt::progress::SectionReporter;
use std::path::Path;

#[tokio::test]
async fn test_basic_section_execution() -> Result<()> {
    with_test_db(async |db| {
        // Create a simple table for testing
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            .await;

        let migration_sql = r#"
-- pgmt:section name="add_email"
-- pgmt:  mode="transactional"
-- pgmt:  timeout="30s"
ALTER TABLE users ADD COLUMN email TEXT;
"#;

        // Parse sections
        let sections = parse_migration_sections(Path::new("test.sql"), migration_sql)?;

        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].name, "add_email");

        // Validate sections
        validate_sections(&sections)?;

        // Setup section tracking
        let tracking_table = TrackingTable::default();
        ensure_section_tracking_table(db.pool(), &tracking_table).await?;
        initialize_sections(db.pool(), &tracking_table, 1, &sections).await?;

        // Execute sections
        let reporter = SectionReporter::new(sections.len(), false);
        let mut executor = SectionExecutor::new(
            db.pool().clone(),
            tracking_table.clone(),
            reporter,
            ExecutionMode::Production,
        );

        let result = executor.execute_section(1, &sections[0]).await?;

        assert_eq!(result.status, SectionStatus::Completed);
        assert_eq!(result.attempts, 1);

        // Verify the column was added
        let column_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'users' AND column_name = 'email'
            )",
        )
        .fetch_one(db.pool())
        .await?;

        assert!(column_exists, "Email column should exist");

        // Verify section status was recorded
        let status = get_section_status(db.pool(), &tracking_table, 1, "add_email").await?;
        assert_eq!(status, Some(SectionStatus::Completed));

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_legacy_migration_compatibility() -> Result<()> {
    with_test_db(async |db| {
        // Legacy migration without section directives
        let migration_sql = "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT);";

        // Parse sections - should auto-wrap in default section
        let sections = parse_migration_sections(Path::new("legacy.sql"), migration_sql)?;

        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].name, "default");
        assert_eq!(sections[0].sql, migration_sql);

        // Setup and execute
        let tracking_table = TrackingTable::default();
        ensure_section_tracking_table(db.pool(), &tracking_table).await?;
        initialize_sections(db.pool(), &tracking_table, 1, &sections).await?;

        let reporter = SectionReporter::new(sections.len(), false);
        let mut executor = SectionExecutor::new(
            db.pool().clone(),
            tracking_table,
            reporter,
            ExecutionMode::Production,
        );

        executor.execute_section(1, &sections[0]).await?;

        // Verify table was created
        let table_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'products'
            )",
        )
        .fetch_one(db.pool())
        .await?;

        assert!(table_exists, "Products table should exist");

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_resume_capability() -> Result<()> {
    with_test_db(async |db| {
        db.execute("CREATE TABLE inventory (id INTEGER PRIMARY KEY)")
            .await;

        let migration_sql = r#"
-- pgmt:section name="section1"
-- pgmt:  mode="transactional"
-- pgmt:  timeout="30s"
ALTER TABLE inventory ADD COLUMN quantity INTEGER;

-- pgmt:section name="section2"
-- pgmt:  mode="transactional"
-- pgmt:  timeout="30s"
ALTER TABLE inventory ADD COLUMN location TEXT;
"#;

        let sections = parse_migration_sections(Path::new("test.sql"), migration_sql)?;

        let tracking_table = TrackingTable::default();
        ensure_section_tracking_table(db.pool(), &tracking_table).await?;
        initialize_sections(db.pool(), &tracking_table, 1, &sections).await?;

        // Execute only first section
        let reporter = SectionReporter::new(sections.len(), false);
        let mut executor = SectionExecutor::new(
            db.pool().clone(),
            tracking_table.clone(),
            reporter,
            ExecutionMode::Production,
        );

        executor.execute_section(1, &sections[0]).await?;

        // Verify first section completed
        let status1 = get_section_status(db.pool(), &tracking_table, 1, "section1").await?;
        assert_eq!(status1, Some(SectionStatus::Completed));

        // Now execute both sections - first should be skipped
        let reporter2 = SectionReporter::new(sections.len(), false);
        let mut executor2 = SectionExecutor::new(
            db.pool().clone(),
            tracking_table.clone(),
            reporter2,
            ExecutionMode::Production,
        );

        let result1 = executor2.execute_section(1, &sections[0]).await?;
        assert_eq!(
            result1.attempts, 0,
            "Section1 should be skipped (0 attempts)"
        );

        let result2 = executor2.execute_section(1, &sections[1]).await?;
        assert_eq!(result2.attempts, 1, "Section2 should execute (1 attempt)");

        // Verify both columns exist
        let quantity_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'inventory' AND column_name = 'quantity'
            )",
        )
        .fetch_one(db.pool())
        .await?;

        let location_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'inventory' AND column_name = 'location'
            )",
        )
        .fetch_one(db.pool())
        .await?;

        assert!(
            quantity_exists && location_exists,
            "Both columns should exist"
        );

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_non_transactional_section() -> Result<()> {
    with_test_db(async |db| {
        // Create table with data for concurrent index creation
        db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, email TEXT)")
            .await;
        db.execute("INSERT INTO customers (id, email) VALUES (1, 'test@example.com')")
            .await;

        let migration_sql = r#"
-- pgmt:section name="create_index"
-- pgmt:  mode="non-transactional"
-- pgmt:  timeout="30s"
-- pgmt:  retry_attempts="3"
-- pgmt:  retry_delay="1s"
CREATE INDEX CONCURRENTLY idx_customers_email ON customers(email);
"#;

        let sections = parse_migration_sections(Path::new("test.sql"), migration_sql)?;
        validate_sections(&sections)?;

        let tracking_table = TrackingTable::default();
        ensure_section_tracking_table(db.pool(), &tracking_table).await?;
        initialize_sections(db.pool(), &tracking_table, 1, &sections).await?;

        let reporter = SectionReporter::new(sections.len(), false);
        let mut executor = SectionExecutor::new(
            db.pool().clone(),
            tracking_table,
            reporter,
            ExecutionMode::Production,
        );

        let result = executor.execute_section(1, &sections[0]).await?;

        assert_eq!(result.status, SectionStatus::Completed);

        // Verify index was created
        let index_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM pg_indexes
                WHERE tablename = 'customers' AND indexname = 'idx_customers_email'
            )",
        )
        .fetch_one(db.pool())
        .await?;

        assert!(index_exists, "Concurrent index should exist");

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_multiple_sections_with_different_modes() -> Result<()> {
    with_test_db(async |db| {
        let migration_sql = r#"
-- pgmt:section name="create_table"
-- pgmt:  mode="transactional"
-- pgmt:  timeout="30s"
CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);

-- pgmt:section name="insert_data"
-- pgmt:  mode="transactional"
-- pgmt:  timeout="30s"
INSERT INTO items (id, name) VALUES (1, 'Item 1'), (2, 'Item 2');

-- pgmt:section name="create_index"
-- pgmt:  mode="non-transactional"
-- pgmt:  timeout="30s"
-- pgmt:  retry_attempts="5"
-- pgmt:  retry_delay="2s"
CREATE INDEX CONCURRENTLY idx_items_name ON items(name);
"#;

        let sections = parse_migration_sections(Path::new("test.sql"), migration_sql)?;
        validate_sections(&sections)?;

        let tracking_table = TrackingTable::default();
        ensure_section_tracking_table(db.pool(), &tracking_table).await?;
        initialize_sections(db.pool(), &tracking_table, 1, &sections).await?;

        let reporter = SectionReporter::new(sections.len(), false);
        let mut executor = SectionExecutor::new(
            db.pool().clone(),
            tracking_table,
            reporter,
            ExecutionMode::Production,
        );

        // Execute all sections
        for section in &sections {
            executor.execute_section(1, section).await?;
        }

        // Verify data was inserted
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM items")
            .fetch_one(db.pool())
            .await?;

        assert_eq!(count, 2, "Should have 2 items");

        // Verify index exists
        let index_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM pg_indexes
                WHERE tablename = 'items' AND indexname = 'idx_items_name'
            )",
        )
        .fetch_one(db.pool())
        .await?;

        assert!(index_exists, "Index should exist");

        Ok(())
    })
    .await
}
