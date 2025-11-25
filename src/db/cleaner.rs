use crate::render::quote_ident;
use sqlx::{Executor, PgPool, Row};
use std::format;

pub async fn clean_shadow_db(pool: &PgPool) -> anyhow::Result<()> {
    // Drop all non-system schemas, including public
    let schemas = sqlx::query(
        "SELECT schema_name
         FROM information_schema.schemata
         WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')"
    )
        .fetch_all(pool)
        .await?;

    for row in schemas {
        let schema_name: &str = row.get("schema_name");
        let drop_stmt = format!("DROP SCHEMA IF EXISTS {} CASCADE", quote_ident(schema_name));
        pool.execute(drop_stmt.as_str()).await?;
    }

    // Recreate the public schema (PostgreSQL best practice)
    pool.execute("CREATE SCHEMA IF NOT EXISTS public").await?;
    pool.execute("GRANT ALL ON SCHEMA public TO PUBLIC").await?;
    pool.execute("GRANT ALL ON SCHEMA public TO postgres")
        .await?;

    Ok(())
}
