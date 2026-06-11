//! Template-based shadow database reset.
//!
//! The shadow's baseline state is snapshotted into a template database once,
//! and every later run recreates the shadow from it — a file-level copy that
//! cannot miss anything (unlike enumerating and dropping objects) and
//! preserves whatever the baseline provides (image init scripts, platform
//! schemas). Used unconditionally for Docker-managed shadows and opt-in for
//! external `shadow.url` databases via `reset: template`.

use anyhow::{Result, anyhow};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Executor, PgPool};
use std::str::FromStr;
use std::time::Duration;
use tracing::debug;

use crate::render::quote_ident;

/// Template database name for a given work database. Derived (not random) so
/// every run finds the previous snapshot; uniqueness follows from work
/// database names being unique per server. Long names get a deterministic
/// hash suffix to stay inside PostgreSQL's 63-byte identifier limit.
pub fn template_db_name(work_db: &str) -> String {
    const PREFIX: &str = "pgmt_template_";
    const MAX_IDENT: usize = 63;
    let name = format!("{PREFIX}{work_db}");
    if name.len() <= MAX_IDENT {
        return name;
    }
    let hash = fnv1a(work_db.as_bytes());
    let keep = MAX_IDENT - PREFIX.len() - 17; // "_" + 16 hex chars
    format!("{PREFIX}{}_{hash:016x}", &work_db[..keep])
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in bytes {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Maintenance database for CREATE/DROP DATABASE: those statements cannot run
/// from the database being copied or dropped.
pub fn admin_db_name(work_db: &str) -> &'static str {
    if work_db == "postgres" {
        "template1"
    } else {
        "postgres"
    }
}

pub async fn template_exists(admin: &PgPool, work_db: &str) -> Result<bool> {
    Ok(
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
            .bind(template_db_name(work_db))
            .fetch_one(admin)
            .await?,
    )
}

/// Snapshot the work database's current state as the pristine template.
/// The work database must have no other connections.
pub async fn snapshot(admin: &PgPool, work_db: &str) -> Result<()> {
    admin
        .execute(
            format!(
                "CREATE DATABASE {} TEMPLATE {}",
                quote_ident(&template_db_name(work_db)),
                quote_ident(work_db)
            )
            .as_str(),
        )
        .await
        .map_err(|e| anyhow!("Failed to snapshot pristine shadow template: {}", e))?;
    debug!(
        "Snapshotted pristine state of {} into {}",
        work_db,
        template_db_name(work_db)
    );
    Ok(())
}

/// Recreate the work database from the pristine template.
pub async fn reset(admin: &PgPool, work_db: &str) -> Result<()> {
    let reset_start = std::time::Instant::now();
    admin
        .execute(
            format!(
                "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                quote_ident(work_db)
            )
            .as_str(),
        )
        .await
        .map_err(|e| anyhow!("Failed to drop shadow database for reset: {}", e))?;
    admin
        .execute(
            format!(
                "CREATE DATABASE {} TEMPLATE {}",
                quote_ident(work_db),
                quote_ident(&template_db_name(work_db))
            )
            .as_str(),
        )
        .await
        .map_err(|e| anyhow!("Failed to recreate shadow database from template: {}", e))?;
    debug!("Shadow reset from template in {:?}", reset_start.elapsed());
    Ok(())
}

/// Bring an external `shadow.url` database (declared `reset: template`) to its
/// baseline: snapshot on first contact, reset from the snapshot afterwards.
pub async fn ensure_reset_by_url(url: &str) -> Result<()> {
    let options = PgConnectOptions::from_str(url)
        .map_err(|e| anyhow!("Invalid shadow database URL: {}", e))?;
    let work_db = options
        .get_database()
        .ok_or_else(|| anyhow!("Shadow database URL has no database name"))?
        .to_string();

    let admin = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(options.clone().database(admin_db_name(&work_db)))
        .await
        .map_err(|e| {
            anyhow!(
                "Failed to open maintenance connection for shadow reset \
                 (reset: template needs access to the {} database): {}",
                admin_db_name(&work_db),
                e
            )
        })?;

    let result = if template_exists(&admin, &work_db).await? {
        reset(&admin, &work_db).await
    } else {
        // First contact: the database's current state becomes the baseline.
        snapshot(&admin, &work_db).await
    };
    admin.close().await;
    result.map_err(|e| {
        anyhow!(
            "{}\nreset: template requires CREATEDB and a database used only by pgmt; \
             use reset: clean if this server's lifecycle belongs to something else.",
            e
        )
    })?;

    super::cleaner::mark_template_provisioned(
        options.get_host(),
        options.get_port(),
        &work_db,
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_template_db_name_is_deterministic_and_bounded() {
        assert_eq!(template_db_name("pgmt_shadow"), "pgmt_template_pgmt_shadow");
        assert_eq!(template_db_name("postgres"), "pgmt_template_postgres");

        // Long names stay within PostgreSQL's identifier limit but remain
        // deterministic and distinct.
        let long_a = "a".repeat(60);
        let long_b = format!("{}b", "a".repeat(59));
        let name_a = template_db_name(&long_a);
        let name_b = template_db_name(&long_b);
        assert!(name_a.len() <= 63);
        assert_eq!(name_a, template_db_name(&long_a));
        assert_ne!(name_a, name_b);
    }
}
