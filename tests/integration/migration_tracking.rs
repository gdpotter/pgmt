use crate::helpers::harness::with_test_db;
use anyhow::Result;
use pgmt::config::types::TrackingTable;
use pgmt::migration::section_parser::MigrationSection;
use pgmt::migration_tracking::{
    TrackingStore, calculate_checksum, ensure_section_tracking_table, ensure_tracking_table_exists,
    register_baseline_start, register_migration_start, version_to_db,
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

/// A tracking table created by an older pgmt version (no `applied_by`, no
/// `is_baseline`) must be migrated up to the current shape idempotently, with
/// existing rows backfilled to is_baseline = FALSE.
#[tokio::test]
async fn test_ensure_tracking_table_migrates_legacy_schema() -> Result<()> {
    with_test_db(async |db| {
        let tracking_table = TrackingTable {
            schema: "public".to_string(),
            name: "legacy_migrations".to_string(),
        };

        // Legacy shape: no applied_by, no is_baseline, non-TZ timestamp.
        db.execute(
            "CREATE TABLE \"public\".\"legacy_migrations\" (\
                version BIGINT PRIMARY KEY, \
                description TEXT NOT NULL, \
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \
                checksum TEXT NOT NULL)",
        )
        .await;
        db.execute(
            "INSERT INTO \"public\".\"legacy_migrations\" (version, description, checksum) \
             VALUES (1, 'legacy', 'abc')",
        )
        .await;

        // Evolve to the current shape.
        ensure_tracking_table_exists(db.pool(), &tracking_table).await?;

        // Existing row backfilled to FALSE (and is_baseline is now NOT NULL).
        let is_baseline: bool = sqlx::query_scalar(
            "SELECT is_baseline FROM \"public\".\"legacy_migrations\" WHERE version = 1",
        )
        .fetch_one(db.pool())
        .await?;
        assert!(
            !is_baseline,
            "legacy rows should backfill to is_baseline = FALSE"
        );

        // applied_by reconciled too.
        let has_applied_by: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
             WHERE table_schema = 'public' AND table_name = 'legacy_migrations' \
             AND column_name = 'applied_by')",
        )
        .fetch_one(db.pool())
        .await?;
        assert!(has_applied_by, "applied_by should be reconciled");

        // The single-column PK is migrated to (version, is_baseline).
        let pk_columns: Vec<String> = sqlx::query_scalar(
            "SELECT a.attname::text
             FROM pg_index i
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
             WHERE i.indrelid = '\"public\".\"legacy_migrations\"'::regclass AND i.indisprimary
             ORDER BY array_position(i.indkey, a.attnum)",
        )
        .fetch_all(db.pool())
        .await?;
        assert_eq!(
            pk_columns,
            vec!["version".to_string(), "is_baseline".to_string()],
            "PK should be migrated to (version, is_baseline)"
        );

        // Idempotent: a second call is a no-op and must not error.
        ensure_tracking_table_exists(db.pool(), &tracking_table).await?;

        Ok(())
    })
    .await
}

/// The composite PK allows a migration row and a baseline row at the SAME
/// version — a baseline generated alongside a migration (`migrate new
/// --create-baseline`) covers it, and both must be trackable independently.
#[tokio::test]
async fn test_same_version_migration_and_baseline_rows() -> Result<()> {
    with_test_db(async |db| {
        let tracking_table = TrackingTable {
            schema: "public".to_string(),
            name: "paired_migrations".to_string(),
        };
        ensure_tracking_table_exists(db.pool(), &tracking_table).await?;

        register_migration_start(db.pool(), &tracking_table, 1234, "migration", "aaa", &[], &[])
            .await?;
        register_baseline_start(db.pool(), &tracking_table, 1234, "baseline", "bbb", &[]).await?;

        let rows: Vec<(i64, bool)> = sqlx::query_as(
            "SELECT version, is_baseline FROM \"public\".\"paired_migrations\" ORDER BY is_baseline",
        )
        .fetch_all(db.pool())
        .await?;
        assert_eq!(rows, vec![(1234, false), (1234, true)]);

        Ok(())
    })
    .await
}

/// A sections table created by an older pgmt version (no `is_baseline`, PK
/// without it) must be migrated to the current shape idempotently, with
/// existing rows backfilled to is_baseline = FALSE.
#[tokio::test]
async fn test_ensure_section_table_migrates_legacy_schema() -> Result<()> {
    with_test_db(async |db| {
        let tracking_table = TrackingTable {
            schema: "public".to_string(),
            name: "legacy_sec_migrations".to_string(),
        };

        // Legacy sections shape: no is_baseline, two-column PK.
        db.execute(
            "CREATE TABLE \"public\".\"legacy_sec_migrations_sections\" (\
                migration_version BIGINT NOT NULL, \
                section_name TEXT NOT NULL, \
                section_order INT NOT NULL, \
                status TEXT NOT NULL, \
                started_at TIMESTAMP WITH TIME ZONE, \
                completed_at TIMESTAMP WITH TIME ZONE, \
                attempts INT DEFAULT 0, \
                last_error TEXT, \
                rows_affected BIGINT, \
                duration_ms BIGINT, \
                PRIMARY KEY (migration_version, section_name))",
        )
        .await;
        db.execute(
            "INSERT INTO \"public\".\"legacy_sec_migrations_sections\" \
             (migration_version, section_name, section_order, status) \
             VALUES (1, 'default', 0, 'completed')",
        )
        .await;

        ensure_section_tracking_table(db.pool(), &tracking_table).await?;

        // Existing row backfilled to FALSE, column NOT NULL.
        let is_baseline: bool = sqlx::query_scalar(
            "SELECT is_baseline FROM \"public\".\"legacy_sec_migrations_sections\" \
             WHERE migration_version = 1",
        )
        .fetch_one(db.pool())
        .await?;
        assert!(!is_baseline, "legacy section rows backfill to FALSE");

        // PK migrated to (migration_version, is_baseline, section_name).
        let pk_columns: Vec<String> = sqlx::query_scalar(
            "SELECT a.attname::text
             FROM pg_index i
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
             WHERE i.indrelid = '\"public\".\"legacy_sec_migrations_sections\"'::regclass
                   AND i.indisprimary
             ORDER BY array_position(i.indkey, a.attnum)",
        )
        .fetch_all(db.pool())
        .await?;
        assert_eq!(
            pk_columns,
            vec![
                "migration_version".to_string(),
                "is_baseline".to_string(),
                "section_name".to_string()
            ]
        );

        // Idempotent second call.
        ensure_section_tracking_table(db.pool(), &tracking_table).await?;

        Ok(())
    })
    .await
}

/// Build a minimal remap section for the atomicity tests.
fn remap_section(name: &str, module: &str, source: &str) -> MigrationSection {
    use std::time::Duration;
    MigrationSection {
        name: name.to_string(),
        description: None,
        mode: pgmt::migration::section_parser::TransactionMode::Transactional,
        timeout: Duration::from_secs(30),
        lock_timeout: None,
        retry_config: None,
        sql: format!("-- {name}"),
        raw_header: format!("-- pgmt:section name={name} module={module} remaps={source}"),
        module: Some(module.to_string()),
        remaps: Some(source.to_string()),
        start_line: 1,
    }
}

/// The introducing evolve creates the section table AND backfills synthetic
/// `default` rows for every pre-existing (legacy) main row in ONE transaction.
/// A crash can therefore never leave the table present with the backfill lost
/// — the state that would brick every legacy version at apply's "tracking row
/// but no section rows" bail. We can't crash a transaction from a test, so we
/// assert the observable invariant it guarantees: after a SINGLE call to
/// `ensure_section_tracking_table` on a genuine legacy target (main rows, no
/// section table), the synthetic rows exist.
#[tokio::test]
async fn test_introducing_evolve_backfills_atomically() -> Result<()> {
    with_test_db(async |db| {
        let tracking_table = TrackingTable {
            schema: "public".to_string(),
            name: "legacy_backfill".to_string(),
        };

        // Genuine legacy shape: a main table with rows, NO section table.
        ensure_tracking_table_exists(db.pool(), &tracking_table).await?;
        db.execute(
            "INSERT INTO \"public\".\"legacy_backfill\" (version, description, checksum, is_baseline) \
             VALUES (100, 'first', 'aaa', FALSE), (200, 'second', 'bbb', FALSE)",
        )
        .await;

        // One call introduces the section table and backfills atomically.
        ensure_section_tracking_table(db.pool(), &tracking_table).await?;

        let rows: Vec<(i64, String, String)> = sqlx::query_as(
            "SELECT migration_version, section_name, status \
             FROM \"public\".\"legacy_backfill_sections\" ORDER BY migration_version",
        )
        .fetch_all(db.pool())
        .await?;
        assert_eq!(
            rows,
            vec![
                (100, "default".to_string(), "completed".to_string()),
                (200, "default".to_string(), "completed".to_string()),
            ],
            "every legacy main row gets one synthetic completed 'default' section"
        );

        Ok(())
    })
    .await
}

/// An inherited-bricked target (a section table already exists but a legacy
/// main row has zero section rows — the pre-fix crash could leave this) is NOT
/// speculatively healed: the introducing backfill runs only when the section
/// table is absent, and once it exists a zero-section main row is a legitimate
/// state. The hard apply-time bail is the correct response for a genuinely
/// corrupt row, so `ensure_section_tracking_table` leaves the bricked row
/// untouched rather than forging content.
#[tokio::test]
async fn test_existing_section_table_never_backfills() -> Result<()> {
    with_test_db(async |db| {
        let tracking_table = TrackingTable {
            schema: "public".to_string(),
            name: "inherited_brick".to_string(),
        };

        ensure_tracking_table_exists(db.pool(), &tracking_table).await?;
        db.execute(
            "INSERT INTO \"public\".\"inherited_brick\" (version, description, checksum, is_baseline) \
             VALUES (100, 'legacy', 'aaa', FALSE)",
        )
        .await;
        // A section table that ALREADY exists (current shape), with no rows for
        // the legacy main row — the inherited-bricked state.
        db.execute(
            "CREATE TABLE \"public\".\"inherited_brick_sections\" (\
                migration_version BIGINT NOT NULL, is_baseline BOOLEAN NOT NULL, \
                section_name TEXT NOT NULL, section_order INT NOT NULL, status TEXT NOT NULL, \
                started_at TIMESTAMP WITH TIME ZONE, completed_at TIMESTAMP WITH TIME ZONE, \
                attempts INT DEFAULT 0, last_error TEXT, rows_affected BIGINT, \
                duration_ms BIGINT, checksum TEXT, mode TEXT, module TEXT, applied_by TEXT, \
                pgmt_version TEXT, \
                PRIMARY KEY (migration_version, is_baseline, section_name))",
        )
        .await;

        ensure_section_tracking_table(db.pool(), &tracking_table).await?;

        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM \"public\".\"inherited_brick_sections\"",
        )
        .fetch_one(db.pool())
        .await?;
        assert_eq!(
            count, 0,
            "an existing section table is never backfilled (no speculative healing)"
        );

        Ok(())
    })
    .await
}

/// First registration of an acquisition migration whose to-run set is empty
/// (all remap sources already held) writes the main row AND the satisfied
/// section rows in ONE transaction, so a crash can't leave a main row with zero
/// section rows — the state apply hard-bails on as corrupt. Asserts both landed
/// together (single snapshot) and that no main-row-without-sections exists.
#[tokio::test]
async fn test_register_migration_start_folds_satisfied_atomically() -> Result<()> {
    with_test_db(async |db| {
        let tracking_table = TrackingTable {
            schema: "public".to_string(),
            name: "acq_migrations".to_string(),
        };
        ensure_tracking_table_exists(db.pool(), &tracking_table).await?;
        ensure_section_tracking_table(db.pool(), &tracking_table).await?;

        // Acquisition-shaped registration: empty to-run, one satisfied remap.
        let satisfied = vec![(0i32, remap_section("analytics", "analytics", "app"))];
        register_migration_start(
            db.pool(),
            &tracking_table,
            1700000000,
            "acquire analytics from app",
            "ccc",
            &[],
            &satisfied,
        )
        .await?;

        // Single snapshot: the main row and its satisfied section row both exist.
        let main_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM \"public\".\"acq_migrations\" WHERE version = 1700000000",
        )
        .fetch_one(db.pool())
        .await?;
        assert_eq!(main_count, 1, "the main row exists");

        let sections: Vec<(String, String)> = sqlx::query_as(
            "SELECT section_name, status FROM \"public\".\"acq_migrations_sections\" \
             WHERE migration_version = 1700000000",
        )
        .fetch_all(db.pool())
        .await?;
        assert_eq!(
            sections,
            vec![("analytics".to_string(), "satisfied".to_string())],
            "the satisfied section row committed with the main row"
        );

        // A main row with zero section rows must never exist for this version.
        let bricked: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM \"public\".\"acq_migrations\" m \
             WHERE NOT EXISTS (SELECT 1 FROM \"public\".\"acq_migrations_sections\" s \
                 WHERE s.migration_version = m.version AND s.is_baseline = m.is_baseline))",
        )
        .fetch_one(db.pool())
        .await?;
        assert!(!bricked, "no main row exists without section rows");

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_register_baseline_start() -> Result<()> {
    with_test_db(async |db| {
        let tracking_table = TrackingTable {
            schema: "public".to_string(),
            name: "test_migrations".to_string(),
        };

        let checksum = calculate_checksum("CREATE TABLE test (id INT);");

        register_baseline_start(
            db.pool(),
            &tracking_table,
            1234567890,
            "baseline",
            &checksum,
            &[],
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
        register_baseline_start(
            db.pool(),
            &tracking_table,
            1234567890,
            "baseline",
            &checksum,
            &[],
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

        register_baseline_start(
            db.pool(),
            &tracking_table,
            version,
            "initial baseline",
            &checksum,
            &[],
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

        register_baseline_start(
            db.pool(),
            &tracking_table,
            version1,
            "first baseline",
            "checksum1",
            &[],
        )
        .await?;

        register_baseline_start(
            db.pool(),
            &tracking_table,
            version2,
            "second baseline",
            "checksum2",
            &[],
        )
        .await?;

        register_baseline_start(
            db.pool(),
            &tracking_table,
            version3,
            "third baseline",
            "checksum3",
            &[],
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
        register_baseline_start(
            db.pool(),
            &tracking_table,
            version,
            "custom schema baseline",
            "test_checksum",
            &[],
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

/// Regression: a target whose baseline coverage is recorded entirely as
/// `satisfied` rows (a module adopted through a re-anchor under the
/// per-section adoption rule — its remap sections' objects are already present under
/// the source's name, so nothing ran but the sections are covered) must be
/// recognized as ESTABLISHED. Counting only `completed` here made
/// `target_is_established` misread such a target as fresh, sending provision
/// down the fresh path against a populated database.
#[tokio::test]
async fn test_target_is_established_counts_satisfied_baseline_coverage() -> Result<()> {
    with_test_db(async |db| {
        let tracking_table = TrackingTable {
            schema: "public".to_string(),
            name: "pgmt_migrations".to_string(),
        };
        ensure_tracking_table_exists(db.pool(), &tracking_table).await?;
        ensure_section_tracking_table(db.pool(), &tracking_table).await?;

        let store = TrackingStore::new(db.pool(), &tracking_table)?;

        // No rows yet → not established.
        assert!(!store.target_is_established().await?);

        // A baseline at 1400 whose only section is `satisfied` (source-held
        // remap adoption): objects present, nothing ran. This is a finished,
        // covered baseline — the target is established.
        sqlx::query(
            "INSERT INTO public.pgmt_migrations_sections
                 (migration_version, is_baseline, section_name, section_order, status, module)
             VALUES (1400, TRUE, 'billing', 0, 'satisfied', 'billing')",
        )
        .execute(db.pool())
        .await?;
        assert!(
            store.target_is_established().await?,
            "a satisfied-only baseline covers the target and must count as established"
        );

        Ok(())
    })
    .await
}

/// A half-applied baseline (a non-covered section) must NOT count as
/// established: a failed provision has to be able to resume through the fresh
/// path. Guards the negative side of the covered predicate.
#[tokio::test]
async fn test_target_is_established_ignores_half_applied_baseline() -> Result<()> {
    with_test_db(async |db| {
        let tracking_table = TrackingTable {
            schema: "public".to_string(),
            name: "pgmt_migrations".to_string(),
        };
        ensure_tracking_table_exists(db.pool(), &tracking_table).await?;
        ensure_section_tracking_table(db.pool(), &tracking_table).await?;
        let store = TrackingStore::new(db.pool(), &tracking_table)?;

        // One covered section but another still failed at the same baseline
        // version → the baseline did not finish, so not established.
        for (name, status) in [("base", "completed"), ("billing", "failed")] {
            sqlx::query(
                "INSERT INTO public.pgmt_migrations_sections
                     (migration_version, is_baseline, section_name, section_order, status)
                 VALUES (1400, TRUE, $1, 0, $2)",
            )
            .bind(name)
            .bind(status)
            .execute(db.pool())
            .await?;
        }
        assert!(!store.target_is_established().await?);

        // A completed *migration* section, by contrast, always establishes.
        sqlx::query(
            "INSERT INTO public.pgmt_migrations_sections
                 (migration_version, is_baseline, section_name, section_order, status)
             VALUES (1500, FALSE, 'base', 0, 'completed')",
        )
        .execute(db.pool())
        .await?;
        assert!(store.target_is_established().await?);

        Ok(())
    })
    .await
}
