use anyhow::{Context, Result};
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::time::Duration;
use tracing::info;
use tracing::log::LevelFilter;

/// Mask password in database URL for display
pub fn mask_url_password(url: &str) -> String {
    // Handle case where URL doesn't contain ://
    if !url.contains("://") {
        return url.to_string();
    }

    // Split on :// to get protocol and rest
    let parts: Vec<&str> = url.splitn(2, "://").collect();
    if parts.len() != 2 {
        return url.to_string();
    }

    let protocol = parts[0];
    let rest = parts[1];

    // Check if there's user info (user:pass@host or user@host)
    if let Some(at_pos) = rest.find('@') {
        let user_info = &rest[..at_pos];
        let host_and_path = &rest[at_pos + 1..];

        // Check if there's a password (user:pass)
        if let Some(colon_pos) = user_info.find(':') {
            let username = &user_info[..colon_pos];
            return format!("{}://{}:***@{}", protocol, username, host_and_path);
        }
    }

    url.to_string()
}

/// Connect to a database with a 5-second timeout and enriched error messages
///
/// Use this for one-shot connections where retry logic is not needed.
/// The `label` describes the database role (e.g., "development database", "target database")
/// and is included in error messages along with the masked URL.
pub async fn connect_to_database(url: &str, label: &str) -> Result<PgPool> {
    PgPoolOptions::new()
        .acquire_timeout(Duration::from_secs(5))
        .connect(url)
        .await
        .with_context(|| format!("Failed to connect to {} at {}", label, mask_url_password(url)))
}

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
                    info!(
                        "✅ Connected to database (after {} retry{})",
                        attempt,
                        if attempt == 1 { "" } else { "ies" }
                    );
                } else {
                    info!("✅ Connected to database");
                }
                return Ok(pool);
            }
            Err(e) => {
                last_error = Some(e);
                if attempt < config.max_retries {
                    if attempt == 0 {
                        info!("🔄 Database not ready, retrying...");
                    }
                    tokio::time::sleep(config.retry_delay).await;
                }
            }
        }
    }

    Err(anyhow::anyhow!(
        "Failed to connect to database at {} after {} attempts: {}",
        mask_url_password(url),
        config.max_retries + 1,
        last_error.unwrap()
    ))
}

/// Connect to database with retry logic and no slow query logging
///
/// Use this for operations where slow queries are expected (e.g., large schema imports).
/// This prevents sqlx from logging the entire SQL content when queries exceed 1 second.
pub async fn connect_with_retry_quiet(url: &str) -> Result<PgPool> {
    connect_with_retry_config_quiet(url, &ConnectionConfig::default()).await
}

/// Connect to database with custom retry configuration and no slow query logging
pub async fn connect_with_retry_config_quiet(
    url: &str,
    config: &ConnectionConfig,
) -> Result<PgPool> {
    use sqlx::ConnectOptions;
    use std::str::FromStr;

    let mut last_error = None;

    // Parse connection options and disable slow statement logging
    let connect_options = PgConnectOptions::from_str(url)
        .map_err(|e| anyhow::anyhow!("Invalid database URL: {}", e))?
        .log_slow_statements(LevelFilter::Off, Duration::from_secs(0));

    for attempt in 0..=config.max_retries {
        let result = PgPoolOptions::new()
            .connect_with(connect_options.clone())
            .await;

        match result {
            Ok(pool) => {
                if attempt > 0 {
                    info!(
                        "✅ Connected to database (after {} retry{})",
                        attempt,
                        if attempt == 1 { "" } else { "ies" }
                    );
                } else {
                    info!("✅ Connected to database");
                }
                return Ok(pool);
            }
            Err(e) => {
                last_error = Some(e);
                if attempt < config.max_retries {
                    if attempt == 0 {
                        info!("🔄 Database not ready, retrying...");
                    }
                    tokio::time::sleep(config.retry_delay).await;
                }
            }
        }
    }

    Err(anyhow::anyhow!(
        "Failed to connect to database at {} after {} attempts: {}",
        mask_url_password(url),
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

    info!(
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

    #[test]
    fn test_mask_url_password() {
        // URL with password
        assert_eq!(
            mask_url_password("postgres://user:secret@localhost:5432/mydb"),
            "postgres://user:***@localhost:5432/mydb"
        );

        // URL without password
        assert_eq!(
            mask_url_password("postgres://user@localhost/mydb"),
            "postgres://user@localhost/mydb"
        );

        // URL without any auth
        assert_eq!(
            mask_url_password("postgres://localhost/mydb"),
            "postgres://localhost/mydb"
        );

        // Invalid URL (no protocol)
        assert_eq!(mask_url_password("not a url"), "not a url");
    }
}
