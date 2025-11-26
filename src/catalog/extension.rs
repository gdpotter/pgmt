use anyhow::Result;
use sqlx::postgres::PgConnection;
use tracing::info;

use crate::catalog::{DependsOn, comments::Commentable, id::DbObjectId};

/// Represents a PostgreSQL extension
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Extension {
    pub name: String,
    pub schema: String,
    pub version: String,
    pub relocatable: bool,
    pub comment: Option<String>,
    pub depends_on: Vec<DbObjectId>,
}

impl DependsOn for Extension {
    fn id(&self) -> DbObjectId {
        DbObjectId::Extension {
            name: self.name.clone(),
        }
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}

impl Commentable for Extension {
    fn comment(&self) -> &Option<String> {
        &self.comment
    }
}

/// Fetch all extensions from the database
pub async fn fetch(conn: &mut PgConnection) -> Result<Vec<Extension>> {
    info!("Fetching extensions...");
    let extensions = sqlx::query!(
        r#"
        SELECT
            e.extname AS name,
            n.nspname AS schema,
            e.extversion AS version,
            e.extrelocatable AS relocatable,

            -- Comments
            d.description AS "comment?"

        FROM pg_extension e
        JOIN pg_namespace n ON e.extnamespace = n.oid
        LEFT JOIN pg_description d ON d.objoid = e.oid AND d.objsubid = 0

        -- Exclude built-in extensions that come with PostgreSQL
        WHERE e.extname NOT IN ('plpgsql')

        ORDER BY e.extname
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    let mut result = Vec::new();

    for row in extensions {
        // Track schema dependency when extension is installed in a non-public schema
        let mut depends_on = Vec::new();
        if row.schema != "public" {
            depends_on.push(DbObjectId::Schema {
                name: row.schema.clone(),
            });
        }

        let extension = Extension {
            name: row.name,
            schema: row.schema,
            version: row.version,
            relocatable: row.relocatable,
            comment: row.comment,
            depends_on,
        };

        result.push(extension);
    }

    Ok(result)
}
