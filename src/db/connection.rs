use anyhow::Result;
use sqlx::PgPool;
use std::time::Duration;

/// Database connection configuration
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Maximum number of retries for database connections
    pub max_retries: u32,
    /// Delay between connection retries
    pub retry_delay: Duration,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            retry_delay: Duration::from_millis(200),
        }
    }
}

/// Connect to database with retry logic
///
/// This function handles common connection scenarios like Docker container startup timing,
/// network issues, and other transient connection problems.
pub async fn connect_with_retry(url: &str) -> Result<PgPool> {
    connect_with_retry_config(url, &ConnectionConfig::default()).await
}

/// Connect to database with custom retry configuration
pub async fn connect_with_retry_config(url: &str, config: &ConnectionConfig) -> Result<PgPool> {
    let mut last_error = None;

    for attempt in 0..=config.max_retries {
        match PgPool::connect(url).await {
            Ok(pool) => {
                if attempt > 0 {
                    println!(
                        "✅ Connected to database (after {} retry{})",
                        attempt,
                        if attempt == 1 { "" } else { "ies" }
                    );
                } else {
                    println!("✅ Connected to database");
                }
                return Ok(pool);
            }
            Err(e) => {
                last_error = Some(e);
                if attempt < config.max_retries {
                    if attempt == 0 {
                        println!("🔄 Database not ready, retrying...");
                    }
                    tokio::time::sleep(config.retry_delay).await;
                }
            }
        }
    }

    Err(anyhow::anyhow!(
        "Failed to connect to database after {} attempts: {}",
        config.max_retries + 1,
        last_error.unwrap()
    ))
}

/// Initialize database session with proper schema context for migration execution
///
/// This sets up the PostgreSQL session with standard settings that ensure
/// consistent behavior across different environments and migration scenarios.
pub async fn initialize_database_session(pool: &PgPool) -> Result<()> {
    // Set search_path to ensure proper schema resolution
    // This is critical for migrations that don't specify schemas explicitly
    sqlx::query("SET search_path = public, pg_catalog")
        .execute(pool)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to set search_path: {}", e))?;

    // Set standard_conforming_strings to ensure consistent string handling
    sqlx::query("SET standard_conforming_strings = on")
        .execute(pool)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to set standard_conforming_strings: {}", e))?;

    // Log current session settings for debugging
    let search_path: (String,) = sqlx::query_as("SHOW search_path")
        .fetch_one(pool)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to query search_path: {}", e))?;

    println!(
        "🔧 Database session initialized with search_path: {}",
        search_path.0
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_config_default() {
        let config = ConnectionConfig::default();
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.retry_delay, Duration::from_millis(200));
    }

    #[test]
    fn test_connection_config_custom() {
        let config = ConnectionConfig {
            max_retries: 3,
            retry_delay: Duration::from_millis(100),
        };
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.retry_delay, Duration::from_millis(100));
    }
}
