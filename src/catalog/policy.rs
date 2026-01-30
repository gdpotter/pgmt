use anyhow::Result;
use sqlx::postgres::PgConnection;
use sqlx::postgres::types::Oid;
use std::collections::HashMap;
use tracing::info;

use crate::catalog::utils::is_system_schema;
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

/// Row returned from the policy dependencies query.
struct PolicyDependencyRow {
    /// Column name when refobjsubid > 0 (column-level dependency)
    column_name: String,
    /// Schema of the referenced table
    table_schema: String,
    /// Name of the referenced table
    table_name: String,
}

/// Fetch all policy column-level dependencies in a single query.
///
/// Returns a HashMap keyed by policy OID, containing column dependencies for each policy.
/// PostgreSQL tracks column-level dependencies via `pg_depend` with `refobjsubid > 0`.
async fn fetch_all_policy_dependencies(
    conn: &mut PgConnection,
) -> Result<HashMap<Oid, Vec<PolicyDependencyRow>>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            p.oid AS "policy_oid!",
            a.attname AS "column_name!",
            n.nspname AS "table_schema!",
            c.relname AS "table_name!"
        FROM pg_policy p
        JOIN pg_depend d ON d.objid = p.oid AND d.classid = 'pg_policy'::regclass
        JOIN pg_class c ON d.refobjid = c.oid AND d.refclassid = 'pg_class'::regclass
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = d.refobjsubid
        WHERE d.refobjsubid > 0
          AND d.deptype = 'n'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        ORDER BY p.oid, n.nspname, c.relname, a.attname
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    // Group by policy_oid
    let mut deps_by_policy: HashMap<Oid, Vec<PolicyDependencyRow>> = HashMap::new();
    for row in rows {
        deps_by_policy
            .entry(row.policy_oid)
            .or_default()
            .push(PolicyDependencyRow {
                column_name: row.column_name,
                table_schema: row.table_schema,
                table_name: row.table_name,
            });
    }
    Ok(deps_by_policy)
}

/// Populate policy column-level dependencies using pg_depend.
///
/// PostgreSQL tracks column-level dependencies for RLS policies via `pg_depend`
/// with `refobjsubid > 0`. This enables precise cascade handling: only policies
/// that reference a changed column need to be dropped and recreated.
fn populate_policy_dependencies(
    policies: &mut [Policy],
    policy_oids: &[Oid],
    deps_map: &HashMap<Oid, Vec<PolicyDependencyRow>>,
) {
    for (policy, oid) in policies.iter_mut().zip(policy_oids.iter()) {
        let Some(deps) = deps_map.get(oid) else {
            continue;
        };

        for dep in deps {
            // Skip system schemas
            if is_system_schema(&dep.table_schema) {
                continue;
            }

            // Add column-level dependency
            let col_dep_id = DbObjectId::Column {
                schema: dep.table_schema.clone(),
                table: dep.table_name.clone(),
                column: dep.column_name.clone(),
            };
            if !policy.depends_on.contains(&col_dep_id) {
                policy.depends_on.push(col_dep_id);
            }
        }
    }
}

/// Fetch all RLS policies from the database
pub async fn fetch(conn: &mut PgConnection) -> Result<Vec<Policy>> {
    info!("Fetching RLS policies...");

    let policies = sqlx::query!(
        r#"
        SELECT
            p.oid AS "policy_oid!",
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
    let mut policy_oids = Vec::new();

    for row in &policies {
        // Parse command type
        let command = match row.command.as_str() {
            "*" => PolicyCommand::All,
            "r" => PolicyCommand::Select,
            "a" => PolicyCommand::Insert,
            "w" => PolicyCommand::Update,
            "d" => PolicyCommand::Delete,
            _ => PolicyCommand::All, // Default fallback
        };

        // Build dependencies - start with table dependency
        let depends_on = vec![
            // Policies depend on their table
            DbObjectId::Table {
                schema: row.schema_name.clone(),
                name: row.table_name.clone(),
            },
        ];

        let policy = Policy {
            schema: row.schema_name.clone(),
            table_name: row.table_name.clone(),
            name: row.policy_name.clone(),
            command,
            permissive: row.permissive,
            roles: row.roles.clone(),
            using_expr: row.using_expr.clone(),
            with_check_expr: row.with_check_expr.clone(),
            comment: row.comment.clone(),
            depends_on,
        };

        policy_oids.push(row.policy_oid);
        result.push(policy);
    }

    // Phase 2: Populate column-level dependencies using pg_depend
    if !result.is_empty() {
        info!("Fetching policy dependencies...");
        let deps_map = fetch_all_policy_dependencies(&mut *conn).await?;
        populate_policy_dependencies(&mut result, &policy_oids, &deps_map);
    }

    Ok(result)
}
