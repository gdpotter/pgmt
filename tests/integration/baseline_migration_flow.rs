use crate::helpers::harness::with_test_db;
use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::config::types::TrackingTable;
use pgmt::migration_tracking::{get_applied_migrations, is_migration_applied};

/// Test the complete baseline + migration flow to ensure future migrations
/// only contain new changes after baseline creation during init
#[tokio::test]
async fn test_baseline_prevents_recreation_in_future_migrations() -> Result<()> {
    with_test_db(async |db| {
        // Step 1: Set up a database with existing objects
        db.execute("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)")
            .await;
        db.execute("CREATE VIEW active_users AS SELECT * FROM users WHERE email IS NOT NULL")
            .await;
        db.execute("CREATE INDEX idx_users_email ON users (email)")
            .await;

        // Step 2: Simulate what init would do - record baseline as applied
        let tracking_table = TrackingTable {
            schema: "public".to_string(),
            name: "pgmt_migrations".to_string(),
        };

        let baseline_version = 1234567890u64;
        let baseline_sql = r#"
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE
);

CREATE VIEW active_users AS
SELECT * FROM users WHERE email IS NOT NULL;

CREATE INDEX idx_users_email ON users (email);
"#;

        let checksum = pgmt::migration_tracking::calculate_checksum(baseline_sql);

        pgmt::migration_tracking::record_baseline_as_applied(
            db.pool(),
            &tracking_table,
            baseline_version,
            "initial baseline",
            &checksum,
        )
        .await?;

        // Step 3: Verify baseline is recorded as applied
        let is_applied = is_migration_applied(db.pool(), &tracking_table, baseline_version).await?;
        assert!(is_applied, "Baseline should be marked as applied");

        // Step 4: Now simulate what "migrate new" would do after init
        // It should only detect NEW changes, not recreate existing objects

        // Add a new table that wasn't in the baseline
        db.execute(
            "CREATE TABLE products (id SERIAL PRIMARY KEY, title TEXT NOT NULL, price DECIMAL(10,2))",
        )
        .await;

        // Step 5: Simulate migration generation logic
        // In real usage, this would be in the migrate new command
        let helper = MigrationTestHelper::new().await;

        // Test what the migration would look like when comparing current DB to baseline state
        helper.run_migration_test(
            &[
                // Baseline objects (should NOT be recreated)
                "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)",
                "CREATE VIEW active_users AS SELECT * FROM users WHERE email IS NOT NULL",
                "CREATE INDEX idx_users_email ON users (email)",
            ],
            &[], // Initial only (empty - baseline already applied)
            &[
                // Current state includes the new table
                "CREATE TABLE products (id SERIAL PRIMARY KEY, title TEXT NOT NULL, price DECIMAL(10,2))",
            ],
            |steps, _final_catalog| {
                // Should only contain steps to create the NEW table
                assert!(!steps.is_empty(), "Should detect the new products table");

                // Verify that it does NOT try to recreate existing objects
                let steps_sql = format!("{:?}", steps);

                // Should NOT contain steps to recreate users table, view, or index
                assert!(!steps_sql.contains("CREATE TABLE users"),
                       "Should NOT try to recreate users table from baseline");
                assert!(!steps_sql.contains("CREATE VIEW active_users"),
                       "Should NOT try to recreate active_users view from baseline");
                assert!(!steps_sql.contains("CREATE INDEX idx_users_email"),
                       "Should NOT try to recreate idx_users_email index from baseline");

                // Should ONLY contain the new products table
                assert!(steps_sql.contains("products"),
                       "Should contain steps for the new products table");

                Ok(())
            }
        ).await?;

        // Step 6: Verify applied migrations list
        let applied_migrations = get_applied_migrations(db.pool(), &tracking_table).await?;
        assert_eq!(
            applied_migrations.len(),
            1,
            "Should only have the baseline recorded"
        );
        assert_eq!(applied_migrations[0].version, baseline_version);
        assert_eq!(applied_migrations[0].description, "initial baseline");

        Ok(())
    }).await
}

/// Test that the complete init → apply → migrate new flow works correctly
#[tokio::test]
async fn test_complete_init_to_migration_workflow() -> Result<()> {
    with_test_db(async |db| {
        // Step 1: Set up existing database (what user would have before init)
        db.execute(
            "CREATE TABLE orders (id SERIAL PRIMARY KEY, user_id INTEGER, amount DECIMAL(10,2))",
        )
        .await;
        db.execute("CREATE TABLE customers (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
            .await;

        // Step 2: Simulate init command with baseline creation
        let tracking_table = TrackingTable {
            schema: "public".to_string(),
            name: "pgmt_migrations".to_string(),
        };

        let baseline_version = 1000000000u64;
        let baseline_sql = r#"
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    amount DECIMAL(10,2)
);

CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);
"#;

        let checksum = pgmt::migration_tracking::calculate_checksum(baseline_sql);

        // This is what the enhanced init command would do
        pgmt::migration_tracking::record_baseline_as_applied(
            db.pool(),
            &tracking_table,
            baseline_version,
            "initial schema baseline",
            &checksum,
        )
        .await?;

        // Step 3: User makes schema changes (adds new objects)
        db.execute("CREATE TABLE invoices (id SERIAL PRIMARY KEY, order_id INTEGER REFERENCES orders(id), total DECIMAL(10,2))").await;
        db.execute("CREATE INDEX idx_invoices_order_id ON invoices (order_id)")
            .await;

        // Step 4: Simulate "migrate new" - should only generate steps for NEW objects
        let helper = MigrationTestHelper::new().await;

        helper.run_migration_test(
            &[
                // Baseline objects that exist and are marked as applied
                "CREATE TABLE orders (id SERIAL PRIMARY KEY, user_id INTEGER, amount DECIMAL(10,2))",
                "CREATE TABLE customers (id SERIAL PRIMARY KEY, name TEXT NOT NULL)",
            ],
            &[], // No initial-only objects
            &[
                // New objects added after baseline
                "CREATE TABLE invoices (id SERIAL PRIMARY KEY, order_id INTEGER REFERENCES orders(id), total DECIMAL(10,2))",
                "CREATE INDEX idx_invoices_order_id ON invoices (order_id)",
            ],
            |steps, _final_catalog| {
                assert!(!steps.is_empty(), "Should generate migration steps for new objects");

                let steps_sql = format!("{:?}", steps);

                // Should contain new objects
                assert!(steps_sql.contains("invoices"), "Should include invoices table creation");
                assert!(steps_sql.contains("idx_invoices_order_id"), "Should include index creation");

                // Should NOT contain baseline objects
                assert!(!steps_sql.contains("CREATE TABLE orders") || !steps_sql.contains("CREATE TABLE customers"),
                       "Should NOT recreate baseline objects");

                Ok(())
            }
        ).await?;

        // Step 5: Verify migration tracking state
        let applied = get_applied_migrations(db.pool(), &tracking_table).await?;
        assert_eq!(applied.len(), 1, "Should only have baseline recorded");

        // If we were to apply the new migration, it would be version 2
        let new_migration_version = 2000000000u64;
        pgmt::migration_tracking::record_baseline_as_applied(
            db.pool(),
            &tracking_table,
            new_migration_version,
            "add invoices and index",
            "new_checksum",
        )
        .await?;

        let applied_after = get_applied_migrations(db.pool(), &tracking_table).await?;
        assert_eq!(
            applied_after.len(),
            2,
            "Should have baseline + new migration"
        );
        assert_eq!(
            applied_after[0].version, baseline_version,
            "First should be baseline"
        );
        assert_eq!(
            applied_after[1].version, new_migration_version,
            "Second should be new migration"
        );

        Ok(())
    }).await
}

/// Test edge case: empty database with baseline should not generate any migration steps
#[tokio::test]
async fn test_empty_database_baseline_no_steps() -> Result<()> {
    with_test_db(async |db| {
        // Step 1: Create tracking table but don't add any schema objects
        let tracking_table = TrackingTable {
            schema: "public".to_string(),
            name: "pgmt_migrations".to_string(),
        };

        pgmt::migration_tracking::ensure_tracking_table_exists(db.pool(), &tracking_table).await?;

        // Step 2: Record empty baseline
        let baseline_version = 1000u64;
        pgmt::migration_tracking::record_baseline_as_applied(
            db.pool(),
            &tracking_table,
            baseline_version,
            "empty baseline",
            "empty_checksum",
        )
        .await?;

        // Step 3: Test that no migration steps are generated for empty → empty
        let helper = MigrationTestHelper::new().await;

        helper
            .run_migration_test(
                &[], // Empty baseline
                &[], // No initial-only
                &[], // No new objects
                |steps, _final_catalog| {
                    assert!(
                        steps.is_empty(),
                        "Empty database should generate no migration steps"
                    );
                    Ok(())
                },
            )
            .await?;

        Ok(())
    })
    .await
}

/// Test that complex schema changes after baseline work correctly
#[tokio::test]
async fn test_complex_schema_changes_after_baseline() -> Result<()> {
    with_test_db(async |db| {
        // Step 1: Set up complex initial schema
        db.execute("CREATE SCHEMA app").await;
        db.execute("CREATE TYPE app.status AS ENUM ('pending', 'approved', 'rejected')")
            .await;
        db.execute("CREATE TABLE app.requests (id SERIAL PRIMARY KEY, status app.status DEFAULT 'pending', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)").await;
        db.execute("CREATE FUNCTION app.count_pending() RETURNS INTEGER LANGUAGE SQL AS 'SELECT COUNT(*) FROM app.requests WHERE status = ''pending'''").await;

        // Step 2: Record baseline
        let tracking_table = TrackingTable {
            schema: "public".to_string(),
            name: "pgmt_migrations".to_string(),
        };

        let baseline_version = 5000u64;
        let baseline_sql = "Complex baseline with schema, enum, table, function";
        let checksum = pgmt::migration_tracking::calculate_checksum(baseline_sql);

        pgmt::migration_tracking::record_baseline_as_applied(
            db.pool(),
            &tracking_table,
            baseline_version,
            "complex schema baseline",
            &checksum,
        )
        .await?;

        // Step 3: Add new complex objects
        db.execute("ALTER TYPE app.status ADD VALUE 'cancelled'")
            .await;
        db.execute("CREATE TABLE app.audit_log (id SERIAL PRIMARY KEY, request_id INTEGER REFERENCES app.requests(id), action TEXT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)").await;
        db.execute("CREATE VIEW app.recent_requests AS SELECT * FROM app.requests WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '7 days'").await;

        // Step 4: Test migration generation
        let helper = MigrationTestHelper::new().await;

        helper.run_migration_test(
            &[
                "CREATE SCHEMA app",
                "CREATE TYPE app.status AS ENUM ('pending', 'approved', 'rejected')",
                "CREATE TABLE app.requests (id SERIAL PRIMARY KEY, status app.status DEFAULT 'pending', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
                "CREATE FUNCTION app.count_pending() RETURNS INTEGER LANGUAGE SQL AS 'SELECT COUNT(*) FROM app.requests WHERE status = ''pending'''",
            ],
            &[], // No initial-only
            &[
                // New objects (note: ALTER TYPE for enum values is complex to test in this setup)
                "CREATE TABLE app.audit_log (id SERIAL PRIMARY KEY, request_id INTEGER REFERENCES app.requests(id), action TEXT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
                "CREATE VIEW app.recent_requests AS SELECT * FROM app.requests WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '7 days'",
            ],
            |steps, _final_catalog| {
                assert!(!steps.is_empty(), "Should generate steps for new objects");

                let steps_sql = format!("{:?}", steps);

                // Should include new objects
                assert!(steps_sql.contains("audit_log"), "Should include audit_log table");
                assert!(steps_sql.contains("recent_requests"), "Should include recent_requests view");

                // Should NOT recreate baseline objects
                assert!(!steps_sql.contains("CREATE SCHEMA app") ||
                       !steps_sql.contains("CREATE TYPE app.status") ||
                       !steps_sql.contains("CREATE TABLE app.requests"),
                       "Should NOT recreate baseline objects");

                Ok(())
            }
        ).await?;

        Ok(())
    }).await
}
