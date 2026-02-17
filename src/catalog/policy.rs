use anyhow::Result;
use sqlx::postgres::PgConnection;
use sqlx::postgres::types::Oid;
use std::collections::{HashMap, HashSet};
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
    /// Schema of the referenced table/view
    table_schema: String,
    /// Name of the referenced table/view
    table_name: String,
    /// relkind of the referenced object ('r'=table, 'v'=view, etc.)
    relkind: String,
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
            c.relname AS "table_name!",
            c.relkind::text AS "relkind!"
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
                relkind: row.relkind,
            });
    }
    Ok(deps_by_policy)
}

/// Populate policy column-level dependencies using pg_depend.
///
/// PostgreSQL tracks column-level dependencies for RLS policies via `pg_depend`
/// with `refobjsubid > 0`. This enables precise cascade handling: only policies
/// that reference a changed column need to be dropped and recreated.
///
/// Also derives object-level dependencies (views, tables) from column-level deps,
/// since PostgreSQL may only record column-level entries for referenced objects
/// without separate object-level entries.
fn populate_policy_dependencies(
    policies: &mut [Policy],
    policy_oids: &[Oid],
    deps_map: &HashMap<Oid, Vec<PolicyDependencyRow>>,
) {
    for (policy, oid) in policies.iter_mut().zip(policy_oids.iter()) {
        let Some(deps) = deps_map.get(oid) else {
            continue;
        };

        // The parent table is already in depends_on
        let parent_table = DbObjectId::Table {
            schema: policy.schema.clone(),
            name: policy.table_name.clone(),
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

            // Derive object-level dependency from the column's parent object
            let obj_dep = match dep.relkind.as_str() {
                "v" | "m" => DbObjectId::View {
                    schema: dep.table_schema.clone(),
                    name: dep.table_name.clone(),
                },
                "r" | "p" => {
                    let table_dep = DbObjectId::Table {
                        schema: dep.table_schema.clone(),
                        name: dep.table_name.clone(),
                    };
                    // Skip the policy's own parent table (already hard-coded)
                    if table_dep == parent_table {
                        continue;
                    }
                    table_dep
                }
                _ => continue,
            };
            if !policy.depends_on.contains(&obj_dep) {
                policy.depends_on.push(obj_dep);
            }
        }
    }
}

/// Fetch all policy object-level dependencies in a single query.
///
/// Returns a HashMap keyed by policy OID, containing object-level dependencies.
/// These are dependencies where `refobjsubid = 0`, covering views, tables, and
/// functions referenced in USING/WITH CHECK expressions.
async fn fetch_all_policy_object_dependencies(
    conn: &mut PgConnection,
) -> Result<HashMap<Oid, Vec<PolicyObjectDependencyRow>>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            p.oid AS "policy_oid!",
            cls.relkind::text AS "cls_relkind?",
            cls_n.nspname AS "cls_schema?",
            cls.relname AS "cls_name?",
            proc.proname AS "proc_name?",
            proc_n.nspname AS "proc_schema?",
            pg_catalog.pg_get_function_identity_arguments(proc.oid) AS "proc_args?",
            ext_procs.extname AS "proc_extension_name?"
        FROM pg_policy p
        JOIN pg_depend d ON d.objid = p.oid AND d.classid = 'pg_policy'::regclass
        LEFT JOIN pg_class cls ON d.refclassid = 'pg_class'::regclass AND d.refobjid = cls.oid
        LEFT JOIN pg_namespace cls_n ON cls.relnamespace = cls_n.oid
        LEFT JOIN pg_proc proc ON d.refclassid = 'pg_proc'::regclass AND d.refobjid = proc.oid
        LEFT JOIN pg_namespace proc_n ON proc.pronamespace = proc_n.oid
        LEFT JOIN (
            SELECT DISTINCT dep.objid AS proc_oid, e.extname
            FROM pg_depend dep
            JOIN pg_extension e ON dep.refobjid = e.oid
            WHERE dep.deptype = 'e'
        ) ext_procs ON ext_procs.proc_oid = proc.oid
        WHERE d.refobjsubid = 0
          AND d.deptype = 'n'
          AND (cls.oid IS NOT NULL OR proc.oid IS NOT NULL)
        ORDER BY p.oid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    let mut deps_by_policy: HashMap<Oid, Vec<PolicyObjectDependencyRow>> = HashMap::new();
    for row in rows {
        deps_by_policy
            .entry(row.policy_oid)
            .or_default()
            .push(PolicyObjectDependencyRow {
                cls_relkind: row.cls_relkind,
                cls_schema: row.cls_schema,
                cls_name: row.cls_name,
                proc_name: row.proc_name,
                proc_schema: row.proc_schema,
                proc_args: row.proc_args,
                proc_extension_name: row.proc_extension_name,
            });
    }
    Ok(deps_by_policy)
}

/// Row returned from the policy object-level dependencies query.
struct PolicyObjectDependencyRow {
    cls_relkind: Option<String>,
    cls_schema: Option<String>,
    cls_name: Option<String>,
    proc_name: Option<String>,
    proc_schema: Option<String>,
    proc_args: Option<String>,
    proc_extension_name: Option<String>,
}

/// Populate policy object-level dependencies (views, tables, functions) using pg_depend.
///
/// This complements `populate_policy_dependencies` (which handles column-level deps)
/// by resolving object-level references from USING/WITH CHECK expressions. Without this,
/// policies that reference views or functions would not have those dependencies tracked,
/// causing incorrect ordering of DROP operations.
fn populate_policy_object_dependencies(
    policies: &mut [Policy],
    policy_oids: &[Oid],
    deps_map: &HashMap<Oid, Vec<PolicyObjectDependencyRow>>,
) {
    for (policy, oid) in policies.iter_mut().zip(policy_oids.iter()) {
        let Some(deps) = deps_map.get(oid) else {
            continue;
        };

        // The parent table is already in depends_on; track it to skip duplicates
        let parent_table = DbObjectId::Table {
            schema: policy.schema.clone(),
            name: policy.table_name.clone(),
        };

        for dep in deps {
            // Resolve table/view references
            if let (Some(relkind), Some(schema), Some(name)) =
                (&dep.cls_relkind, &dep.cls_schema, &dep.cls_name)
            {
                if is_system_schema(schema) {
                    continue;
                }
                let dep_id = match relkind.as_str() {
                    "v" | "m" => DbObjectId::View {
                        schema: schema.clone(),
                        name: name.clone(),
                    },
                    "r" | "p" => {
                        let table_dep = DbObjectId::Table {
                            schema: schema.clone(),
                            name: name.clone(),
                        };
                        // Skip the policy's own parent table (already hard-coded)
                        if table_dep == parent_table {
                            continue;
                        }
                        table_dep
                    }
                    _ => continue,
                };
                if !policy.depends_on.contains(&dep_id) {
                    policy.depends_on.push(dep_id);
                }
                continue;
            }

            // Resolve function references
            if let (Some(name), Some(schema), Some(args)) =
                (&dep.proc_name, &dep.proc_schema, &dep.proc_args)
            {
                if is_system_schema(schema) {
                    continue;
                }
                let dep_id = if let Some(ext_name) = &dep.proc_extension_name {
                    DbObjectId::Extension {
                        name: ext_name.clone(),
                    }
                } else {
                    DbObjectId::Function {
                        schema: schema.clone(),
                        name: name.clone(),
                        arguments: args.clone(),
                    }
                };
                if !policy.depends_on.contains(&dep_id) {
                    policy.depends_on.push(dep_id);
                }
            }
        }
    }

    // Deduplicate dependencies for each policy
    for policy in policies.iter_mut() {
        let unique_deps: HashSet<_> = policy.depends_on.drain(..).collect();
        policy.depends_on.extend(unique_deps);
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

    // Phase 2: Populate dependencies using pg_depend
    if !result.is_empty() {
        info!("Fetching policy dependencies...");
        let col_deps_map = fetch_all_policy_dependencies(&mut *conn).await?;
        populate_policy_dependencies(&mut result, &policy_oids, &col_deps_map);

        let obj_deps_map = fetch_all_policy_object_dependencies(&mut *conn).await?;
        populate_policy_object_dependencies(&mut result, &policy_oids, &obj_deps_map);
    }

    Ok(result)
}
