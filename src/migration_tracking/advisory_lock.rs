//! Cross-process/cross-machine mutual exclusion for `migrate apply` and
//! `migrate provision`.
//!
//! Two deployers pointed at the same database could otherwise both read the
//! tracking table, both decide the same sections are pending, and both execute
//! them. We serialize the whole run behind a PostgreSQL **session-scoped**
//! advisory lock.
//!
//! The lock lives on a *dedicated* connection ([`MigrationLock`] owns it), never
//! a pooled one: a session advisory lock is tied to the backend connection, and
//! a leaked lock on a connection returned to an `sqlx` pool would poison later
//! acquisitions when the pool hands that connection out again. On the happy path
//! we unlock and close explicitly ([`MigrationLock::release`]); on error/panic
//! paths the connection is dropped, ending the session and letting PostgreSQL
//! release the lock for us.

use crate::config::types::TrackingTable;
use crate::migration_tracking::format_tracking_table_name;
use anyhow::{Context, Result};
use sqlx::postgres::PgConnectOptions;
use sqlx::{ConnectOptions, Connection, PgConnection};
use std::str::FromStr;
use std::time::Duration;
use tracing::log::LevelFilter;

/// Derive the advisory-lock key for a tracking table.
///
/// The key is derived from the *formatted* (schema-qualified, quoted) tracking
/// table name so that:
///
/// * distinct tracking tables (different schema and/or name) get distinct keys
///   and never serialize against each other, while
/// * `apply` and `provision` targeting the *same* tracking table derive the
///   *same* key and therefore exclude one another.
///
/// We hash with MD5 and take the first 8 bytes big-endian as the `i64` key. MD5
/// is used purely as a stable hash here, not for security. `std`'s `DefaultHasher`
/// is deliberately avoided: it is explicitly not stable across releases, and the
/// key must stay identical across pgmt versions or two versions would fail to
/// exclude each other.
pub fn advisory_lock_key(tracking_table: &TrackingTable) -> Result<i64> {
    let name = format_tracking_table_name(tracking_table)?;
    Ok(advisory_lock_key_from_name(&name))
}

/// Inner key derivation over the already-formatted name (see [`advisory_lock_key`]).
fn advisory_lock_key_from_name(formatted_name: &str) -> i64 {
    let digest = md5::compute(formatted_name.as_bytes());
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest.0[..8]);
    i64::from_be_bytes(bytes)
}

/// RAII guard holding a session-scoped advisory lock on its own connection.
///
/// Acquire with [`MigrationLock::acquire`] before touching the tracking table.
/// Keep the guard alive for the entire run; call [`MigrationLock::release`] on
/// success. Dropping the guard (error/panic) also releases the lock by closing
/// the connection.
pub struct MigrationLock {
    conn: Option<PgConnection>,
    key: i64,
}

impl MigrationLock {
    /// Open a dedicated connection to `url` and take the migration advisory lock.
    ///
    /// Tries `pg_try_advisory_lock` first; if the lock is already held by another
    /// pgmt process, prints a one-line notice and then blocks on
    /// `pg_advisory_lock` until it becomes available.
    pub async fn acquire(url: &str, tracking_table: &TrackingTable) -> Result<Self> {
        let key = advisory_lock_key(tracking_table)?;

        // Blocking on `pg_advisory_lock` is the whole point of this connection,
        // so exempt it from sqlx's slow-statement instrumentation — otherwise a
        // normal lock wait surfaces a WARN ("slow statement ... pg_advisory_lock")
        // that interleaves with the reporter's clean output. Genuinely slow user
        // DDL still warns: that runs on the pooled connections, not this one.
        let mut conn = PgConnectOptions::from_str(url)
            .context("Invalid database URL for the migration advisory lock")?
            .log_slow_statements(LevelFilter::Off, Duration::from_secs(0))
            .connect()
            .await
            .context("Failed to open dedicated connection for the migration advisory lock")?;

        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(key)
            .fetch_one(&mut conn)
            .await
            .context("Failed to attempt the migration advisory lock")?;

        if !acquired {
            println!(
                "Another pgmt operation is running against this database \
                 (advisory lock {} held); waiting...",
                key
            );
            sqlx::query("SELECT pg_advisory_lock($1)")
                .bind(key)
                .execute(&mut conn)
                .await
                .context("Failed to acquire the migration advisory lock")?;
        }

        Ok(Self {
            conn: Some(conn),
            key,
        })
    }

    /// Explicitly release the lock and close the connection.
    ///
    /// Unlock failures are non-fatal: the lock is also released when the session
    /// ends, so we log-and-continue rather than surface a spurious error after
    /// the real work already succeeded.
    pub async fn release(mut self) -> Result<()> {
        if let Some(mut conn) = self.conn.take() {
            if let Err(e) = sqlx::query("SELECT pg_advisory_unlock($1)")
                .bind(self.key)
                .execute(&mut conn)
                .await
            {
                tracing::warn!(
                    "Failed to explicitly release migration advisory lock {} \
                     (will be released when the session ends): {}",
                    self.key,
                    e
                );
            }
            // Best-effort graceful close; dropping would also end the session.
            let _ = conn.close().await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tracking(schema: &str, name: &str) -> TrackingTable {
        TrackingTable {
            schema: schema.to_string(),
            name: name.to_string(),
        }
    }

    #[test]
    fn test_default_tracking_table_key_is_stable() {
        // Pinned literal: if this ever changes, two pgmt versions would fail to
        // exclude each other against the same database. Derived from
        // MD5(`"public"."pgmt_migrations"`), first 8 bytes big-endian.
        let key = advisory_lock_key(&tracking("public", "pgmt_migrations")).unwrap();
        assert_eq!(key, 4779231646775632092);
    }

    #[test]
    fn test_distinct_schema_or_name_yields_distinct_keys() {
        let base = advisory_lock_key(&tracking("public", "pgmt_migrations")).unwrap();
        let other_schema = advisory_lock_key(&tracking("app", "pgmt_migrations")).unwrap();
        let other_name = advisory_lock_key(&tracking("public", "other_migrations")).unwrap();

        assert_ne!(base, other_schema);
        assert_ne!(base, other_name);
        assert_ne!(other_schema, other_name);
    }

    #[test]
    fn test_apply_and_provision_share_key_for_same_table() {
        // Same tracking table -> same key regardless of caller. This is what
        // makes apply and provision mutually exclusive.
        let a = advisory_lock_key(&tracking("public", "pgmt_migrations")).unwrap();
        let b = advisory_lock_key(&tracking("public", "pgmt_migrations")).unwrap();
        assert_eq!(a, b);
    }
}
