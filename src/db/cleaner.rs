use crate::config::filter::ObjectFilter;
use crate::config::types::{Objects, TrackingTable};
use crate::render::quote_ident;
use once_cell::sync::Lazy;
use sqlx::{Executor, PgPool, Row};
use std::collections::HashSet;
use std::format;
use std::sync::Mutex;
use tracing::debug;

/// Shadow branches created this process (`db::branch`): fresh copies of the
/// pristine source, so the scoped clean below has nothing to do — and would
/// destroy the source-provided substrate (image init scripts, platform
/// schemas) the branch inherits. External `shadow.url` databases in the
/// default `reset: clean` mode are never registered: their lifecycle belongs
/// to whoever provisioned them, and the scoped clean is the contract there.
static BRANCH_PROVISIONED: Lazy<Mutex<HashSet<(String, u16, String)>>> =
    Lazy::new(|| Mutex::new(HashSet::new()));

/// Record that the database at host:port/database is a freshly-created shadow
/// branch.
pub fn mark_branch_provisioned(host: &str, port: u16, database: &str) {
    BRANCH_PROVISIONED
        .lock()
        .unwrap()
        .insert((host.to_string(), port, database.to_string()));
}

fn is_branch_provisioned(pool: &PgPool) -> bool {
    let options = pool.connect_options();
    let key = (
        options.get_host().to_string(),
        options.get_port(),
        options.get_database().unwrap_or_default().to_string(),
    );
    BRANCH_PROVISIONED.lock().unwrap().contains(&key)
}

/// Clean the shadow database by dropping managed schemas.
///
/// Only schemas that pgmt manages (as determined by the object filter config)
/// will be dropped. Schemas excluded from management (e.g. Supabase's `auth`,
/// `storage`, `realtime`) are preserved, allowing custom shadow database images
/// to provide platform-specific schemas that user schema files can reference.
///
/// Shadow branches (Docker-managed shadows and `reset: branch` URLs) are
/// fresh copies of the pristine source and are skipped entirely.
pub async fn clean_shadow_db(pool: &PgPool, objects: &Objects) -> anyhow::Result<()> {
    if is_branch_provisioned(pool) {
        debug!("Shadow is a fresh branch of its source; skipping clean");
        return Ok(());
    }

    let filter = ObjectFilter::new(objects, &TrackingTable::default());

    // Find all non-system schemas
    let schemas = sqlx::query(
        "SELECT schema_name
         FROM information_schema.schemata
         WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')"
    )
        .fetch_all(pool)
        .await?;

    for row in schemas {
        let schema_name: &str = row.get("schema_name");
        if filter.should_include_schema(schema_name) {
            // This is a managed schema — drop it for a clean slate.
            // Reassign ownership first so non-superuser connections (e.g.
            // supabase_admin) can drop schemas they don't own.
            let quoted = quote_ident(schema_name);
            let reassign = format!("ALTER SCHEMA {} OWNER TO CURRENT_USER", quoted);
            let drop_stmt = format!("DROP SCHEMA IF EXISTS {} CASCADE", quoted);
            pool.execute(reassign.as_str()).await?;
            pool.execute(drop_stmt.as_str()).await?;
        } else {
            debug!("Preserving non-managed schema: {}", schema_name);
        }
    }

    // Recreate the public schema (PostgreSQL best practice)
    pool.execute("CREATE SCHEMA IF NOT EXISTS public").await?;
    pool.execute("GRANT ALL ON SCHEMA public TO PUBLIC").await?;

    Ok(())
}
