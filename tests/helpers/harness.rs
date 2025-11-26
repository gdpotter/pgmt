use sqlx::PgPool;
use uuid::Uuid;

/// Simple test instance that connects to an external PostgreSQL database
/// Requires DATABASE_URL environment variable to be set
pub struct PgTestInstance {
    pub base_url: String,
}

/// Test database with an isolated database for testing
pub struct TestDatabase {
    pool: PgPool,
    db_name: String,
    base_url: String,
}

impl TestDatabase {
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Acquire a connection from the pool for direct fetch function calls
    pub async fn conn(&self) -> sqlx::pool::PoolConnection<sqlx::Postgres> {
        self.pool.acquire().await.unwrap()
    }

    /// Execute arbitrary SQL - perfect for test setup
    pub async fn execute(&self, sql: &str) {
        use sqlx::Executor;
        self.pool
            .execute(sql)
            .await
            .unwrap_or_else(|e| panic!("Failed to execute SQL: {}\nError: {}", sql, e));
    }

    /// Cleanup the test database - best effort async cleanup
    pub async fn cleanup(self) {
        // Close the connection pool first
        self.pool.close().await;

        // Connect to postgres database and drop the test database
        // Use best-effort with timeout to avoid hanging tests
        let db_name = self.db_name.clone();
        let base_url = self.base_url.clone();

        let cleanup_future = async move {
            if let Ok(pool) = PgPool::connect(&base_url).await {
                let drop_sql = format!("DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)", db_name);
                let _ = sqlx::query(&drop_sql).execute(&pool).await;
                pool.close().await;
            }
        };

        // Timeout after 5 seconds to prevent hanging
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), cleanup_future).await;
    }

    /// Seed standard roles for grant testing
    /// These roles are created once per PostgreSQL cluster and used across all tests
    async fn seed_standard_roles(&self) {
        // Create standard test roles, ignoring errors if they already exist
        let roles = [
            "test_app_user",
            "test_admin_user",
            "test_read_only",
            "test_write_user",
            "test_group",
        ];

        for role_name in &roles {
            // Try to create the role, ignore error if it already exists
            let create_sql = match *role_name {
                "test_app_user" => "CREATE ROLE test_app_user LOGIN",
                "test_admin_user" => "CREATE ROLE test_admin_user LOGIN CREATEDB",
                "test_read_only" => "CREATE ROLE test_read_only NOLOGIN",
                "test_write_user" => "CREATE ROLE test_write_user LOGIN",
                "test_group" => "CREATE ROLE test_group NOLOGIN",
                _ => continue,
            };

            let _ = sqlx::query(create_sql).execute(&self.pool).await;
        }
    }
}

impl PgTestInstance {
    pub async fn new() -> Self {
        // Load .env file for test environment configuration
        dotenv::dotenv().ok();

        let base_url = std::env::var("DATABASE_URL")
            .expect("DATABASE_URL environment variable is required for testing. Run: ./scripts/test-setup.sh");

        // Verify we can connect to the database
        let test_pool = PgPool::connect(&base_url).await
            .expect("Failed to connect to test database. Make sure PostgreSQL is running and DATABASE_URL is correct.");
        test_pool.close().await;

        Self { base_url }
    }

    pub fn get_host_port(&self) -> u16 {
        // Extract port from DATABASE_URL: postgres://postgres:postgres@localhost:5432/postgres
        let url_parts: Vec<&str> = self.base_url.split(':').collect();
        if url_parts.len() >= 4 {
            let port_part = url_parts[3].split('/').next().unwrap();
            port_part.parse().unwrap_or(5432)
        } else {
            5432 // default PostgreSQL port
        }
    }

    pub async fn create_test_database(&self) -> TestDatabase {
        // Generate a random database name
        let db_name = format!("test_{}", Uuid::new_v4().simple());

        // Connect to the base postgres database to create new database
        let base_pool = PgPool::connect(&self.base_url)
            .await
            .expect("Failed to connect to PostgreSQL for database creation");

        // Create new database
        sqlx::query(&format!("CREATE DATABASE \"{}\"", db_name))
            .execute(&base_pool)
            .await
            .expect("Failed to create test database");

        base_pool.close().await;

        // Connect to the newly created database
        let db_url = if let Some(last_slash) = self.base_url.rfind('/') {
            format!("{}/{}", &self.base_url[..last_slash], db_name)
        } else {
            format!("{}/{}", self.base_url, db_name)
        };

        let pool = PgPool::connect(&db_url)
            .await
            .expect("Failed to connect to newly created test database");

        let db = TestDatabase {
            pool,
            db_name,
            base_url: self.base_url.clone(),
        };

        // Seed standard roles for grant testing
        db.seed_standard_roles().await;

        db
    }
}

/// Run a test with automatic database cleanup
///
/// This is the idiomatic Rust pattern for resource cleanup with async code.
/// Similar to Java's try-with-resources or Python's context managers.
///
/// # Example
/// ```
/// #[tokio::test]
/// async fn test_something() {
///     with_test_db(async |db| {
///         db.execute("CREATE TABLE users (id INT)").await;
///         let tables = fetch_tables(db.pool()).await.unwrap();
///         assert_eq!(tables.len(), 1);
///     }).await;
///     // Database automatically cleaned up here!
/// }
/// ```
pub async fn with_test_db<F, R>(test_fn: F) -> R
where
    F: std::ops::AsyncFnOnce(&TestDatabase) -> R,
{
    let pg = PgTestInstance::new().await;
    let db = pg.create_test_database().await;

    // Run the test with a reference to the database
    let result = test_fn(&db).await;

    // Cleanup happens here - best effort (ignore errors)
    db.cleanup().await;

    result
}
