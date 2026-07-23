//! Ephemeral shadow branches.
//!
//! The shadow *source* database — a Docker container's init database, or the
//! database named in `shadow.url` — is treated as a read-only baseline that
//! pgmt never writes to. Each run works on a branch: a `CREATE DATABASE ...
//! TEMPLATE source` copy that is dropped again at process exit. The source
//! stays pristine by construction, every branch starts from the *current*
//! baseline, and on external servers cleanup leaves things exactly as pgmt
//! found them.

use anyhow::{Result, anyhow};
use once_cell::sync::Lazy;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Executor, PgPool};
use std::str::FromStr;
use std::sync::Mutex;
use std::time::Duration;
use tracing::{debug, warn};

use crate::render::quote_ident;

/// Branches created by this process: (maintenance options, branch name).
/// Dropped by `cleanup_all_branches` at process exit. A killed process can
/// orphan a branch — they're recognizable by the `pgmt_branch_` prefix and
/// safe to drop once nothing is connected.
#[allow(clippy::type_complexity)]
static BRANCH_REGISTRY: Lazy<Mutex<Vec<(PgConnectOptions, String)>>> =
    Lazy::new(|| Mutex::new(Vec::new()));

/// Maintenance database for CREATE/DROP DATABASE: those statements cannot run
/// from the database being copied or dropped.
pub fn admin_db_name(source_db: &str) -> &'static str {
    if source_db == "postgres" {
        "template1"
    } else {
        "postgres"
    }
}

fn new_branch_name() -> String {
    format!("pgmt_branch_{}", uuid::Uuid::new_v4().simple())
}

/// Create an ephemeral branch of `source_db` and register it for drop at
/// process exit. The source must have no active connections (PostgreSQL
/// requires this of `CREATE DATABASE ... TEMPLATE`); pgmt itself never
/// connects to it.
pub async fn create_branch(admin: &PgPool, source_db: &str) -> Result<String> {
    let branch = new_branch_name();
    let create_start = std::time::Instant::now();
    admin
        .execute(sqlx::AssertSqlSafe(format!(
            "CREATE DATABASE {} TEMPLATE {}",
            quote_ident(&branch),
            quote_ident(source_db)
        )))
        .await
        .map_err(|e| {
            anyhow!(
                "Failed to branch shadow database from {}: {} \
                 (the source must have no active connections)",
                source_db,
                e
            )
        })?;
    debug!(
        "Branched shadow {} from {} in {:?}",
        branch,
        source_db,
        create_start.elapsed()
    );

    let admin_options = (*admin.connect_options()).clone();
    BRANCH_REGISTRY
        .lock()
        .unwrap()
        .push((admin_options.clone(), branch.clone()));
    super::cleaner::mark_branch_provisioned(
        admin_options.get_host(),
        admin_options.get_port(),
        &branch,
    );

    Ok(branch)
}

/// Branch an external `shadow.url` source (declared `reset: branch`) and
/// return the connection string for the branch. The named database itself is
/// never touched.
pub async fn branch_url(url: &str) -> Result<String> {
    let options = PgConnectOptions::from_str(url)
        .map_err(|e| anyhow!("Invalid shadow database URL: {}", e))?;
    let source_db = options
        .get_database()
        .ok_or_else(|| anyhow!("Shadow database URL has no database name"))?
        .to_string();

    let admin = admin_pool(options.clone().database(admin_db_name(&source_db))).await?;
    let result = create_branch(&admin, &source_db).await;
    admin.close().await;
    let branch = result.map_err(|e| {
        anyhow!(
            "{}\nreset: branch requires CREATEDB and a source database used only \
             by pgmt; use reset: clean if this server's lifecycle belongs to \
             something else.",
            e
        )
    })?;

    rewrite_database(url, &source_db, &branch)
}

/// Drop the ephemeral branch a pool is connected to, reclaiming it as soon as
/// a phase finishes instead of holding every branch until process exit.
///
/// No-op for non-branch shadows (an external `reset: clean` URL): those are not
/// in the branch registry, so their lifecycle belongs to whoever provisioned
/// them and we never drop them. The pool is always closed.
pub async fn drop_branch(pool: PgPool) -> Result<()> {
    let options = (*pool.connect_options()).clone();
    let host = options.get_host().to_string();
    let port = options.get_port();
    let database = options.get_database().unwrap_or_default().to_string();

    // Close our connections first — DROP DATABASE requires no active sessions.
    pool.close().await;

    if !super::cleaner::take_branch_provisioned(&host, port, &database) {
        // Not an ephemeral branch we own; leave the external database alone.
        return Ok(());
    }

    let admin = admin_pool(options.database(admin_db_name(&database))).await?;
    let result = admin
        .execute(sqlx::AssertSqlSafe(format!(
            "DROP DATABASE IF EXISTS {} WITH (FORCE)",
            quote_ident(&database)
        )))
        .await;
    admin.close().await;
    result.map_err(|e| anyhow!("Failed to drop shadow branch {}: {}", database, e))?;

    // Forget it so the process-exit sweep doesn't try to drop it again.
    BRANCH_REGISTRY
        .lock()
        .unwrap()
        .retain(|(_, branch)| branch != &database);
    debug!("Dropped shadow branch {}", database);
    Ok(())
}

async fn admin_pool(options: PgConnectOptions) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(options)
        .await
        .map_err(|e| {
            anyhow!(
                "Failed to open maintenance connection for shadow branching: {}",
                e
            )
        })
}

/// Swap the database segment of a postgres URL, preserving everything else
/// (credentials, query parameters) verbatim.
fn rewrite_database(url: &str, source_db: &str, branch: &str) -> Result<String> {
    let needle = format!("/{}", source_db);
    if let Some(pos) = url.rfind(&needle) {
        // Ensure the match is the path segment (end of string or followed by '?')
        let after = pos + needle.len();
        if after == url.len() || url[after..].starts_with('?') {
            return Ok(format!("{}/{}{}", &url[..pos], branch, &url[after..]));
        }
    }
    Err(anyhow!(
        "Could not derive branch URL from shadow URL (database segment not found)"
    ))
}

/// Drop every branch this process created. Called at process exit, while any
/// warm shadow containers are still running.
pub async fn cleanup_all_branches() -> Result<()> {
    let branches = {
        let mut registry = BRANCH_REGISTRY.lock().unwrap();
        std::mem::take(&mut *registry)
    };
    if branches.is_empty() {
        return Ok(());
    }

    debug!("Dropping {} shadow branch(es)", branches.len());
    for (admin_options, branch) in branches {
        match admin_pool(admin_options).await {
            Ok(admin) => {
                if let Err(e) = admin
                    .execute(sqlx::AssertSqlSafe(format!(
                        "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                        quote_ident(&branch)
                    )))
                    .await
                {
                    warn!("Failed to drop shadow branch {}: {}", branch, e);
                }
                admin.close().await;
            }
            // The server may already be gone (container removed, CI teardown)
            Err(e) => debug!("Skipping branch {} cleanup: {}", branch, e),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewrite_database() {
        assert_eq!(
            rewrite_database("postgres://u:p@h:5/src", "src", "br").unwrap(),
            "postgres://u:p@h:5/br"
        );
        assert_eq!(
            rewrite_database("postgres://u:p@h:5/src?sslmode=disable", "src", "br").unwrap(),
            "postgres://u:p@h:5/br?sslmode=disable"
        );
        // A username matching the db name must not be rewritten.
        assert_eq!(
            rewrite_database("postgres://src:p@h:5/src", "src", "br").unwrap(),
            "postgres://src:p@h:5/br"
        );
    }
}
