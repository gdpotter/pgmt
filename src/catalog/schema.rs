use super::comments::Commentable;
use anyhow::Result;
use sqlx::postgres::PgConnection;
use tracing::info;

#[derive(Debug, Clone)]
pub struct Schema {
    pub name: String,
    pub comment: Option<String>, // comment on the schema
}

impl Schema {}

impl Commentable for Schema {
    fn comment(&self) -> &Option<String> {
        &self.comment
    }
}

pub async fn fetch(conn: &mut PgConnection) -> Result<Vec<Schema>> {
    info!("Fetching schemas...");
    let rows = sqlx::query!(
        r#"
        SELECT
            n.nspname as "name!",
            d.description as "comment?"
        FROM pg_namespace n
        LEFT JOIN pg_description d ON d.objoid = n.oid AND d.objsubid = 0
        WHERE n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
          AND n.nspname NOT LIKE 'pg_temp_%'
          AND n.nspname NOT LIKE 'pg_toast_temp_%'
        ORDER BY n.nspname
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    let schemas: Vec<Schema> = rows
        .into_iter()
        .map(|row| Schema {
            name: row.name,
            comment: row.comment,
        })
        .collect();

    Ok(schemas)
}
