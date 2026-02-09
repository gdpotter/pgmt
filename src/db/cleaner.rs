use crate::config::filter::ObjectFilter;
use crate::config::types::{Objects, TrackingTable};
use crate::render::quote_ident;
use sqlx::{Executor, PgPool, Row};
use std::format;
use tracing::debug;

/// Clean the shadow database by dropping managed schemas.
///
/// Only schemas that pgmt manages (as determined by the object filter config)
/// will be dropped. Schemas excluded from management (e.g. Supabase's `auth`,
/// `storage`, `realtime`) are preserved, allowing custom shadow database images
/// to provide platform-specific schemas that user schema files can reference.
pub async fn clean_shadow_db(pool: &PgPool, objects: &Objects) -> anyhow::Result<()> {
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
