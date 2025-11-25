use anyhow::Result;
use sqlx::PgPool;

use crate::catalog::{DependsOn, comments::Commentable, id::DbObjectId};

/// Represents a PostgreSQL trigger
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trigger {
    pub schema: String,
    pub table_name: String,
    pub name: String,
    pub function_schema: String,
    pub function_name: String,
    pub comment: Option<String>,
    pub depends_on: Vec<DbObjectId>,

    /// Complete trigger definition from pg_get_triggerdef()
    /// This is the authoritative source for trigger recreation
    pub definition: String,
}

impl DependsOn for Trigger {
    fn id(&self) -> DbObjectId {
        DbObjectId::Trigger {
            schema: self.schema.clone(),
            table: self.table_name.clone(),
            name: self.name.clone(),
        }
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}

impl Commentable for Trigger {
    fn comment(&self) -> &Option<String> {
        &self.comment
    }
}

/// Fetch all triggers from the database
pub async fn fetch(pool: &PgPool) -> Result<Vec<Trigger>> {
    let triggers = sqlx::query!(
        r#"
        SELECT
            tn.nspname AS table_schema,
            c.relname AS table_name,
            t.tgname AS trigger_name,

            -- Function details
            p.proname AS function_name,
            fn.nspname AS function_schema,

            -- Comments
            d.description AS "comment?",

            -- Complete trigger definition (authoritative source)
            pg_get_triggerdef(t.oid) AS "definition!"

        FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        JOIN pg_namespace tn ON c.relnamespace = tn.oid
        JOIN pg_proc p ON t.tgfoid = p.oid
        JOIN pg_namespace fn ON p.pronamespace = fn.oid
        LEFT JOIN pg_description d ON d.objoid = t.oid AND d.objsubid = 0

        WHERE tn.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND NOT t.tgisinternal  -- Exclude system-generated triggers
          AND c.relkind IN ('r', 'v', 'm')  -- Regular tables, views, materialized views

        ORDER BY tn.nspname, c.relname, t.tgname
        "#
    )
    .fetch_all(pool)
    .await?;

    let mut result = Vec::new();

    for row in triggers {
        // Build dependencies
        let depends_on = vec![
            // Triggers depend on their table
            DbObjectId::Table {
                schema: row.table_schema.clone(),
                name: row.table_name.clone(),
            },
            // Triggers depend on their function
            DbObjectId::Function {
                schema: row.function_schema.clone(),
                name: row.function_name.clone(),
            },
        ];

        let trigger = Trigger {
            schema: row.table_schema,
            table_name: row.table_name,
            name: row.trigger_name,
            function_schema: row.function_schema,
            function_name: row.function_name,
            comment: row.comment,
            depends_on,
            definition: row.definition,
        };

        result.push(trigger);
    }

    Ok(result)
}
