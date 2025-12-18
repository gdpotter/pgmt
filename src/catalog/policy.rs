use anyhow::Result;
use sqlx::postgres::PgConnection;
use tracing::info;

use crate::catalog::{DependsOn, comments::Commentable, id::DbObjectId};

/// Command type for RLS policies
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyCommand {
    All,    // 'a' or '*' - applies to all commands
    Select, // 'r' - SELECT only
    Insert, // 'a' - INSERT only
    Update, // 'w' - UPDATE only
    Delete, // 'd' - DELETE only
}

/// Represents a PostgreSQL Row-Level Security policy
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Policy {
    pub schema: String,
    pub table_name: String,
    pub name: String,

    /// Command type this policy applies to
    pub command: PolicyCommand,

    /// true = PERMISSIVE, false = RESTRICTIVE
    pub permissive: bool,

    /// Roles this policy applies to (empty = PUBLIC)
    pub roles: Vec<String>,

    /// USING expression (for SELECT, UPDATE, DELETE)
    pub using_expr: Option<String>,

    /// WITH CHECK expression (for INSERT, UPDATE)
    pub with_check_expr: Option<String>,

    /// Comment on the policy
    pub comment: Option<String>,

    /// Dependencies (primarily the table)
    pub depends_on: Vec<DbObjectId>,
}

impl DependsOn for Policy {
    fn id(&self) -> DbObjectId {
        DbObjectId::Policy {
            schema: self.schema.clone(),
            table: self.table_name.clone(),
            name: self.name.clone(),
        }
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}

impl Commentable for Policy {
    fn comment(&self) -> &Option<String> {
        &self.comment
    }
}

/// Fetch all RLS policies from the database
pub async fn fetch(conn: &mut PgConnection) -> Result<Vec<Policy>> {
    info!("Fetching RLS policies...");

    let policies = sqlx::query!(
        r#"
        SELECT
            n.nspname AS schema_name,
            c.relname AS table_name,
            p.polname AS policy_name,
            p.polcmd::text AS "command!",
            p.polpermissive AS "permissive!",
            COALESCE(
                ARRAY(
                    SELECT rolname FROM pg_roles
                    WHERE oid = ANY(p.polroles)
                    ORDER BY rolname
                ),
                '{}'::text[]
            ) AS "roles!: Vec<String>",
            pg_get_expr(p.polqual, p.polrelid) AS "using_expr?",
            pg_get_expr(p.polwithcheck, p.polrelid) AS "with_check_expr?",
            d.description AS "comment?"
        FROM pg_policy p
        JOIN pg_class c ON p.polrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        LEFT JOIN pg_description d ON d.objoid = p.oid AND d.objsubid = 0
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        ORDER BY n.nspname, c.relname, p.polname
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    let mut result = Vec::new();

    for row in policies {
        // Parse command type
        let command = match row.command.as_str() {
            "*" => PolicyCommand::All,
            "r" => PolicyCommand::Select,
            "a" => PolicyCommand::Insert,
            "w" => PolicyCommand::Update,
            "d" => PolicyCommand::Delete,
            _ => PolicyCommand::All, // Default fallback
        };

        // Build dependencies
        let depends_on = vec![
            // Policies depend on their table
            DbObjectId::Table {
                schema: row.schema_name.clone(),
                name: row.table_name.clone(),
            },
        ];

        let policy = Policy {
            schema: row.schema_name,
            table_name: row.table_name,
            name: row.policy_name,
            command,
            permissive: row.permissive,
            roles: row.roles,
            using_expr: row.using_expr,
            with_check_expr: row.with_check_expr,
            comment: row.comment,
            depends_on,
        };

        result.push(policy);
    }

    Ok(result)
}
