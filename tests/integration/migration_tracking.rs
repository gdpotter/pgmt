use crate::helpers::harness::with_test_db;
use anyhow::Result;
use pgmt::config::types::TrackingTable;
use pgmt::migration_tracking::{
    calculate_checksum, ensure_tracking_table_exists, record_baseline_as_applied, version_to_db,
};
use sqlx::Row;

#[tokio::test]
async fn test_ensure_tracking_table_exists() -> Result<()> {
    with_test_db(async |db| {
        let tracking_table = TrackingTable {
            schema: "public".to_string(),
            name: "test_migrations".to_string(),
        };

        ensure_tracking_table_exists(db.pool(), &tracking_table).await?;

        // Verify table was created
        let result = sqlx::query("SELECT COUNT(*) as count FROM information_schema.tables WHERE table_name = 'test_migrations'")
            .fetch_one(db.pool())
            .await?;

        let count: i64 = result.get("count");
        assert_eq!(count, 1);

        Ok(())
    }).await
}

#[tokio::test]
async fn test_record_baseline_as_applied() -> Result<()> {
    with_test_db(async |db| {
        let tracking_table = TrackingTable {
            schema: "public".to_string(),
            name: "test_migrations".to_string(),
        };

        let checksum = calculate_checksum("CREATE TABLE test (id INT);");

        record_baseline_as_applied(
            db.pool(),
            &tracking_table,
            1234567890,
            "baseline",
            &checksum,
        )
        .await?;

        // Verify record was inserted
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM \"public\".\"test_migrations\" WHERE version = $1",
        )
        .bind(version_to_db(1234567890u64)?)
        .fetch_one(db.pool())
        .await?;
        assert!(count > 0);

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_get_applied_migrations() -> Result<()> {
    with_test_db(async |db| {
        let tracking_table = TrackingTable {
            schema: "public".to_string(),
            name: "test_migrations".to_string(),
        };

        // Record a baseline
        let checksum = calculate_checksum("CREATE TABLE test (id INT);");
        record_baseline_as_applied(
            db.pool(),
            &tracking_table,
            1234567890,
            "baseline",
            &checksum,
        )
        .await?;

        // Get applied migrations
        let rows = sqlx::query(
            "SELECT version, description, checksum FROM \"public\".\"test_migrations\" ORDER BY version",
        )
        .fetch_all(db.pool())
        .await?;

        assert_eq!(rows.len(), 1);
        let version: i64 = rows[0].get("version");
        let description: String = rows[0].get("description");
        let row_checksum: String = rows[0].get("checksum");
        assert_eq!(version, 1234567890);
        assert_eq!(description, "baseline");
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
        let checksum = calculate_checksum(baseline_sql);

        record_baseline_as_applied(
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

    let checksum1 = calculate_checksum(content);
    let checksum2 = calculate_checksum(content);

    assert_eq!(checksum1, checksum2, "Checksums should be consistent");

    // Different content should produce different checksums
    let different_content = "CREATE TABLE products (id SERIAL PRIMARY KEY, title TEXT NOT NULL);";
    let different_checksum = calculate_checksum(different_content);

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

        record_baseline_as_applied(
            db.pool(),
            &tracking_table,
            version1,
            "first baseline",
            "checksum1",
        )
        .await?;

        record_baseline_as_applied(
            db.pool(),
            &tracking_table,
            version2,
            "second baseline",
            "checksum2",
        )
        .await?;

        record_baseline_as_applied(
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
        record_baseline_as_applied(
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
