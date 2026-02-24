use crate::helpers::harness::with_test_db;
/// Integration tests for migration tracking functionality
/// Tests the core migration tracking functions with real databases
use anyhow::Result;
use pgmt::config::types::TrackingTable;
use pgmt::migration_tracking::{ensure_tracking_table_exists, version_to_db};
use sqlx::Row;
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::test]
async fn test_migration_tracking_functions() -> Result<()> {
    with_test_db(async |db| {
        let tracking_table = TrackingTable {
            schema: "public".to_string(),
            name: "pgmt_migrations".to_string(),
        };

        // Test ensure tracking table exists
        ensure_tracking_table_exists(db.pool(), &tracking_table).await?;

        // Test recording baseline as applied
        let version = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let checksum = pgmt::migration_tracking::calculate_checksum("CREATE TABLE test (id INT);");

        pgmt::migration_tracking::record_baseline_as_applied(
            db.pool(),
            &tracking_table,
            version,
            "test baseline",
            &checksum,
        )
        .await?;

        // Test checking if migration is applied
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM \"public\".\"pgmt_migrations\" WHERE version = $1",
        )
        .bind(version_to_db(version)?)
        .fetch_one(db.pool())
        .await?;
        assert!(count > 0, "Baseline should be marked as applied");

        // Test getting applied migrations
        let rows = sqlx::query(
            "SELECT version, description, checksum FROM \"public\".\"pgmt_migrations\" ORDER BY version",
        )
        .fetch_all(db.pool())
        .await?;
        assert_eq!(rows.len(), 1);
        let row_version: i64 = rows[0].get("version");
        let row_desc: String = rows[0].get("description");
        let row_checksum: String = rows[0].get("checksum");
        assert_eq!(row_version, version_to_db(version)?);
        assert_eq!(row_desc, "test baseline");
        assert_eq!(row_checksum, checksum);

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_baseline_creation_prevents_recreation() -> Result<()> {
    with_test_db(async |db| {
        // Create a table in the database
        db.execute("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
            .await;

        let tracking_table = TrackingTable {
            schema: "public".to_string(),
            name: "pgmt_migrations".to_string(),
        };

        // Record this as a baseline
        let version = 1234567890u64;
        let baseline_sql = "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL);";
        let checksum = pgmt::migration_tracking::calculate_checksum(baseline_sql);

        pgmt::migration_tracking::record_baseline_as_applied(
            db.pool(),
            &tracking_table,
            version,
            "initial baseline",
            &checksum,
        )
        .await?;

        // Now test that the migration is marked as applied
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM \"public\".\"pgmt_migrations\" WHERE version = $1",
        )
        .bind(version_to_db(version)?)
        .fetch_one(db.pool())
        .await?;
        assert!(count > 0, "Baseline should be marked as applied");

        // Simulate what a future 'pgmt migrate new' would check
        let rows =
            sqlx::query("SELECT version FROM \"public\".\"pgmt_migrations\" ORDER BY version")
                .fetch_all(db.pool())
                .await?;
        assert_eq!(rows.len(), 1);
        let row_version: i64 = rows[0].get("version");
        assert_eq!(row_version, version_to_db(version)?);

        // This proves that the baseline is properly recorded and would prevent recreation
        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_checksum_calculation_consistency() -> Result<()> {
    // Test that checksum calculation is consistent
    let content = "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL);";

    let checksum1 = pgmt::migration_tracking::calculate_checksum(content);
    let checksum2 = pgmt::migration_tracking::calculate_checksum(content);

    assert_eq!(checksum1, checksum2, "Checksums should be consistent");

    // Different content should produce different checksums
    let different_content = "CREATE TABLE products (id SERIAL PRIMARY KEY, title TEXT NOT NULL);";
    let different_checksum = pgmt::migration_tracking::calculate_checksum(different_content);

    assert_ne!(
        checksum1, different_checksum,
        "Different content should produce different checksums"
    );

    Ok(())
}

#[tokio::test]
async fn test_multiple_baselines_ordering() -> Result<()> {
    with_test_db(async |db| {
        let tracking_table = TrackingTable {
            schema: "public".to_string(),
            name: "pgmt_migrations".to_string(),
        };

        // Record multiple baselines/migrations in non-chronological order
        let version1 = 1000u64;
        let version2 = 2000u64;
        let version3 = 1500u64; // Out of order insertion

        pgmt::migration_tracking::record_baseline_as_applied(
            db.pool(),
            &tracking_table,
            version1,
            "first baseline",
            "checksum1",
        )
        .await?;

        pgmt::migration_tracking::record_baseline_as_applied(
            db.pool(),
            &tracking_table,
            version2,
            "second baseline",
            "checksum2",
        )
        .await?;

        pgmt::migration_tracking::record_baseline_as_applied(
            db.pool(),
            &tracking_table,
            version3,
            "third baseline",
            "checksum3",
        )
        .await?;

        // Get applied migrations - should be ordered by version
        let rows =
            sqlx::query("SELECT version FROM \"public\".\"pgmt_migrations\" ORDER BY version")
                .fetch_all(db.pool())
                .await?;
        assert_eq!(rows.len(), 3);

        // Verify chronological ordering
        let v0: i64 = rows[0].get("version");
        let v1: i64 = rows[1].get("version");
        let v2: i64 = rows[2].get("version");
        assert_eq!(v0, version_to_db(version1)?); // 1000
        assert_eq!(v1, version_to_db(version3)?); // 1500
        assert_eq!(v2, version_to_db(version2)?); // 2000

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_tracking_table_custom_schema() -> Result<()> {
    with_test_db(async |db| {
        // Create custom schema
        db.execute("CREATE SCHEMA internal").await;

        let tracking_table = TrackingTable {
            schema: "internal".to_string(),
            name: "migrations_log".to_string(),
        };

        // Test with custom schema/name
        ensure_tracking_table_exists(db.pool(), &tracking_table).await?;

        let version = 1234567890u64;
        pgmt::migration_tracking::record_baseline_as_applied(
            db.pool(),
            &tracking_table,
            version,
            "custom schema baseline",
            "test_checksum",
        )
        .await?;

        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM \"internal\".\"migrations_log\" WHERE version = $1",
        )
        .bind(version_to_db(version)?)
        .fetch_one(db.pool())
        .await?;
        assert!(count > 0, "Should work with custom schema and table name");

        Ok(())
    })
    .await
}
