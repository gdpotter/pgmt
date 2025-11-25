use crate::helpers::harness::{PgTestInstance, TestDatabase};
use anyhow::Result;
use pgmt::catalog::Catalog;
use pgmt::diff::operations::{MigrationStep, SqlRenderer};
use pgmt::diff::{cascade, diff_all, diff_order};

/// Helper for migration tests that eliminates boilerplate setup
pub struct MigrationTestHelper {
    pg: PgTestInstance,
}

impl MigrationTestHelper {
    pub async fn new() -> Self {
        Self {
            pg: PgTestInstance::new().await,
        }
    }

    /// Set up initial and target databases for migration testing
    pub async fn setup_migration_test(&self) -> (TestDatabase, TestDatabase) {
        let initial_db = self.pg.create_test_database().await;
        let target_db = self.pg.create_test_database().await;
        (initial_db, target_db)
    }

    /// Run the full migration pipeline: diff_all -> cascade::expand -> diff_order
    pub async fn run_migration_pipeline(
        &self,
        initial_catalog: &Catalog,
        target_catalog: &Catalog,
    ) -> Result<Vec<MigrationStep>> {
        let mut steps = diff_all(initial_catalog, target_catalog);
        steps = cascade::expand(steps, initial_catalog, target_catalog);
        steps = diff_order(steps, initial_catalog, target_catalog)?;
        Ok(steps)
    }

    /// Execute migration steps on a database
    pub async fn execute_migration(
        &self,
        db: &TestDatabase,
        steps: &[MigrationStep],
    ) -> Result<()> {
        for step in steps {
            let sql_list = step.to_sql();
            for rendered in sql_list {
                db.execute(&rendered.sql).await;
            }
        }
        Ok(())
    }

    /// Complete end-to-end migration test with clear 3-vector approach:
    /// 1. Setup initial and target databases with provided SQL
    /// 2. Load catalogs
    /// 3. Run migration pipeline
    /// 4. Apply migration to fresh database
    /// 5. Verify final state matches target
    /// 6. Return migration steps for additional assertions
    pub async fn run_migration_test<F>(
        &self,
        both_dbs_sql: &[&str],     // SQL to run on both initial and target DBs
        initial_only_sql: &[&str], // Additional SQL to run only on initial DB
        target_only_sql: &[&str],  // Additional SQL to run only on target DB
        verification: F,
    ) -> Result<Vec<MigrationStep>>
    where
        F: FnOnce(&[MigrationStep], &Catalog) -> Result<()>,
    {
        // Setup databases
        let (initial_db, target_db) = self.setup_migration_test().await;

        // Apply common SQL to both databases
        for sql in both_dbs_sql {
            initial_db.execute(sql).await;
            target_db.execute(sql).await;
        }

        // Apply initial-only SQL to initial database
        for sql in initial_only_sql {
            initial_db.execute(sql).await;
        }

        // Apply target-only SQL to target database
        for sql in target_only_sql {
            target_db.execute(sql).await;
        }

        // Load catalogs
        let initial_catalog = Catalog::load(initial_db.pool()).await?;
        let target_catalog = Catalog::load(target_db.pool()).await?;

        // Run migration pipeline
        let steps = self
            .run_migration_pipeline(&initial_catalog, &target_catalog)
            .await?;

        // Apply migration to fresh database (starting from initial state)
        let fresh_db = self.pg.create_test_database().await;
        for sql in both_dbs_sql {
            fresh_db.execute(sql).await;
        }
        for sql in initial_only_sql {
            fresh_db.execute(sql).await;
        }
        self.execute_migration(&fresh_db, &steps).await?;

        // Verify final state
        let final_catalog = Catalog::load(fresh_db.pool()).await?;
        verification(&steps, &final_catalog)?;

        // Cleanup test databases
        initial_db.cleanup().await;
        target_db.cleanup().await;
        fresh_db.cleanup().await;

        Ok(steps)
    }
}

impl MigrationTestHelper {
    pub fn new_sync() -> Self {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(Self::new())
    }
}
