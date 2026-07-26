//! Raw RLS policy rows and their conversion into logical policies.
//!
//! The fetches keep the OIDs the converter resolves with, plus the outputs of
//! the server-side functions that cannot be computed in Rust: `pg_get_expr` for
//! the USING and WITH CHECK expressions, `pg_get_function_identity_arguments`
//! for the identity of a function such an expression calls, and the role names
//! `polroles` resolves to. Everything else — schema-name resolution,
//! extension-ownership and system-schema exclusion, dependency derivation,
//! comment attachment — happens in the converter, where the OIDs die.
//!
//! A policy's dependencies come from `pg_depend`, in two shapes: the
//! column-level edges (`refobjsubid > 0`) that make a cascade precise enough to
//! recreate only the policies referencing a changed column, and the object-level
//! edges (`refobjsubid = 0`) that catch the views, tables and functions an
//! expression names. Both are needed: PostgreSQL may record a referenced object
//! only through its columns.

use anyhow::{Context, Result};
use sqlx::postgres::PgConnection;
use sqlx::postgres::types::Oid;
use std::collections::BTreeMap;
use tracing::info;

use super::exclusion::{Converted, Excluded, ExclusionReason};
use super::oid_index::OidIndex;
use super::shared::{SharedCatalog, class};
use crate::catalog::id::DbObjectId;
use crate::catalog::policy::{Policy, PolicyCommand};
use crate::catalog::{DependsOn, utils::is_system_schema};

/// One `pg_policy` row, before names are resolved and OIDs are discarded.
#[derive(Debug, Clone)]
pub struct RawPolicy {
    pub oid: Oid,
    pub name: String,
    /// The table the policy is on. Its OID is what decides extension ownership:
    /// a policy gets no `deptype = 'e'` row of its own, only its table does.
    pub table_oid: Oid,
    pub table_namespace: Oid,
    pub table_name: String,
    /// `pg_policy.polcmd`: '*' all, 'r' select, 'a' insert, 'w' update,
    /// 'd' delete.
    pub command: String,
    /// `pg_policy.polpermissive`: PERMISSIVE when true, RESTRICTIVE when false.
    pub permissive: bool,
    /// The role names `polroles` resolves to; empty means PUBLIC.
    pub roles: Vec<String>,
    /// `pg_get_expr(polqual, polrelid)`.
    pub using_expr: Option<String>,
    /// `pg_get_expr(polwithcheck, polrelid)`.
    pub with_check_expr: Option<String>,
}

/// One column-level `pg_depend` edge from a policy to a column its expressions
/// read.
#[derive(Debug, Clone)]
pub struct RawPolicyColumnDependency {
    pub policy_oid: Oid,
    pub relation_namespace: Oid,
    pub relation_name: String,
    /// `relkind` of the relation the column belongs to.
    pub relkind: String,
    /// The column's name, which only `pg_attribute` can give: the edge addresses
    /// it by attnum under its relation.
    pub column_name: String,
}

/// One object-level `pg_depend` edge from a policy to a relation or routine its
/// expressions name.
#[derive(Debug, Clone)]
pub struct RawPolicyObjectDependency {
    pub policy_oid: Oid,

    /// Referenced relation (`pg_class`).
    pub relation_namespace: Option<Oid>,
    pub relation_name: Option<String>,
    pub relkind: Option<String>,

    /// Referenced routine (`pg_proc`).
    pub function_oid: Option<Oid>,
    pub function_namespace: Option<Oid>,
    pub function_name: Option<String>,
    pub function_args: Option<String>,
}

/// Everything the policy converter reads out of `pg_catalog`.
#[derive(Debug, Clone, Default)]
pub struct RawPolicies {
    pub policies: Vec<RawPolicy>,
    pub column_dependencies: Vec<RawPolicyColumnDependency>,
    pub object_dependencies: Vec<RawPolicyObjectDependency>,
}

/// Fetch every policy and every dependency edge out of one, unresolved and
/// unfiltered.
pub async fn fetch(conn: &mut PgConnection) -> Result<RawPolicies> {
    info!("Fetching RLS policies...");
    let policies = fetch_policies(&mut *conn).await?;

    // Both edge fetches scan pg_depend for every policy at once; with no policy
    // there is nothing for them to find.
    let (column_dependencies, object_dependencies) = if policies.is_empty() {
        (Vec::new(), Vec::new())
    } else {
        info!("Fetching policy dependencies...");
        (
            fetch_column_dependencies(&mut *conn).await?,
            fetch_object_dependencies(&mut *conn).await?,
        )
    };

    Ok(RawPolicies {
        policies,
        column_dependencies,
        object_dependencies,
    })
}

/// Fetch policies and convert them into the logical catalog, with each policy's
/// comment attached through the OID index.
pub async fn load(conn: &mut PgConnection, shared: &SharedCatalog) -> Result<Vec<Policy>> {
    Ok(load_with_exclusions(conn, shared).await?.objects)
}

/// The same load, keeping the named reason for every raw row that did not become
/// a policy.
pub async fn load_with_exclusions(
    conn: &mut PgConnection,
    shared: &SharedCatalog,
) -> Result<Converted<Policy>> {
    let raw = fetch(conn).await?;
    let mut converted = convert(&raw, shared)?;

    // Identity first, then the index, then the OID-addressed state: a comment
    // can only be attached to an object whose identity is already known.
    let oids = OidIndex::from_pairs(
        converted
            .objects
            .iter()
            .map(|(oid, policy)| (*oid, policy.id())),
    )?;
    let comments = oids.object_comments(&shared.descriptions, class::PG_POLICY);
    for (_, policy) in &mut converted.objects {
        policy.comment = comments.get(&policy.id()).map(|text| text.to_string());
    }

    Ok(converted.map(|(_, policy)| policy))
}

/// Resolve raw policies into logical ones, keeping each policy's OID beside it so
/// OID-addressed state can still be attached before the identities cross the
/// firewall.
///
/// Policies on a system table and policies whose table belongs to an extension
/// are dropped here, each with its named reason, along with the dependency edges
/// belonging to them.
pub fn convert(raw: &RawPolicies, shared: &SharedCatalog) -> Result<Converted<(Oid, Policy)>> {
    // The policies that survive filtering, by OID, so every dependency edge can
    // be routed to its policy (or dropped with it).
    let mut kept: BTreeMap<u32, usize> = BTreeMap::new();
    let mut converted: Converted<(Oid, Policy)> = Converted::new();

    for row in &raw.policies {
        let schema = shared
            .namespaces
            .name(row.table_namespace)
            .with_context(|| format!("policy {} has no namespace entry", row.name))?;

        if is_system_schema(schema) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "policy",
                schema,
                &row.name,
                ExclusionReason::SystemSchema,
            ));
            continue;
        }
        // A policy never carries extension membership itself, even when an
        // extension script created it; only its table does.
        if let Some(extension) = shared.extensions.owner_of_relation_subobject(row.table_oid) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "policy",
                schema,
                &row.name,
                ExclusionReason::ExtensionOwned {
                    extension: extension.to_string(),
                },
            ));
            continue;
        }

        kept.insert(row.oid.0, converted.objects.len());
        converted.objects.push((
            row.oid,
            Policy {
                schema: schema.to_string(),
                table_name: row.table_name.clone(),
                name: row.name.clone(),
                command: command(&row.command),
                permissive: row.permissive,
                roles: row.roles.clone(),
                using_expr: row.using_expr.clone(),
                with_check_expr: row.with_check_expr.clone(),
                comment: None,
                depends_on: vec![DbObjectId::Table {
                    schema: schema.to_string(),
                    name: row.table_name.clone(),
                }],
            },
        ));
    }

    for row in &raw.column_dependencies {
        let Some(&idx) = kept.get(&row.policy_oid.0) else {
            continue;
        };
        let Some(schema) = shared.namespaces.name(row.relation_namespace) else {
            continue;
        };
        if is_system_schema(schema) {
            continue;
        }

        let (_, policy) = &mut converted.objects[idx];
        push_dependency(
            policy,
            DbObjectId::Column {
                schema: schema.to_string(),
                table: row.relation_name.clone(),
                column: row.column_name.clone(),
            },
        );
        // PostgreSQL may record a referenced relation only through its columns,
        // so the relation the column belongs to is a dependency of its own.
        if let Some(dep) = relation_dependency(&row.relkind, schema, &row.relation_name, policy) {
            push_dependency(policy, dep);
        }
    }

    for row in &raw.object_dependencies {
        let Some(&idx) = kept.get(&row.policy_oid.0) else {
            continue;
        };
        let (_, policy) = &mut converted.objects[idx];

        if let (Some(namespace), Some(name), Some(relkind)) =
            (row.relation_namespace, &row.relation_name, &row.relkind)
        {
            let Some(schema) = shared.namespaces.name(namespace) else {
                continue;
            };
            if is_system_schema(schema) {
                continue;
            }
            if let Some(dep) = relation_dependency(relkind, schema, name, policy) {
                push_dependency(policy, dep);
            }
            continue;
        }

        if let (Some(oid), Some(namespace), Some(name), Some(args)) = (
            row.function_oid,
            row.function_namespace,
            &row.function_name,
            &row.function_args,
        ) {
            let Some(schema) = shared.namespaces.name(namespace) else {
                continue;
            };
            if is_system_schema(schema) {
                continue;
            }
            // An extension-provided function is filtered from the catalog, so the
            // policy depends on what creates it.
            let dep = match shared.extensions.owner(class::PG_PROC, oid) {
                Some(extension) => DbObjectId::Extension {
                    name: extension.to_string(),
                },
                None => DbObjectId::Function {
                    schema: schema.to_string(),
                    name: name.clone(),
                    arguments: args.clone(),
                },
            };
            push_dependency(policy, dep);
        }
    }

    // The raw fetch orders by OID; ordering by name is what callers see.
    converted.objects.sort_by(|(_, a), (_, b)| {
        (&a.schema, &a.table_name, &a.name).cmp(&(&b.schema, &b.table_name, &b.name))
    });

    Ok(converted)
}

/// The command a `polcmd` char names. An unrecognized char is treated as ALL,
/// the widest reading and the one a policy without a command clause has.
fn command(polcmd: &str) -> PolicyCommand {
    match polcmd {
        "r" => PolicyCommand::Select,
        "a" => PolicyCommand::Insert,
        "w" => PolicyCommand::Update,
        "d" => PolicyCommand::Delete,
        _ => PolicyCommand::All,
    }
}

/// The dependency a reference to a relation creates, or nothing when the
/// relation is the policy's own table (already a dependency) or a kind a policy
/// cannot meaningfully depend on.
fn relation_dependency(
    relkind: &str,
    schema: &str,
    name: &str,
    policy: &Policy,
) -> Option<DbObjectId> {
    match relkind {
        "v" | "m" => Some(DbObjectId::View {
            schema: schema.to_string(),
            name: name.to_string(),
        }),
        "r" | "p" => {
            if schema == policy.schema && name == policy.table_name {
                return None;
            }
            Some(DbObjectId::Table {
                schema: schema.to_string(),
                name: name.to_string(),
            })
        }
        _ => None,
    }
}

/// Add a dependency the policy does not already carry, keeping the order edges
/// were derived in.
fn push_dependency(policy: &mut Policy, dep: DbObjectId) {
    if !policy.depends_on.contains(&dep) {
        policy.depends_on.push(dep);
    }
}

async fn fetch_policies(conn: &mut PgConnection) -> Result<Vec<RawPolicy>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            p.oid AS "oid!",
            p.polname AS "name!",
            c.oid AS "table_oid!",
            c.relnamespace AS "table_namespace!",
            c.relname AS "table_name!",
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
            pg_catalog.pg_get_expr(p.polqual, p.polrelid) AS "using_expr?",
            pg_catalog.pg_get_expr(p.polwithcheck, p.polrelid) AS "with_check_expr?"
        FROM pg_policy p
        JOIN pg_class c ON p.polrelid = c.oid
        ORDER BY p.oid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawPolicy {
            oid: row.oid,
            name: row.name,
            table_oid: row.table_oid,
            table_namespace: row.table_namespace,
            table_name: row.table_name,
            command: row.command,
            permissive: row.permissive,
            roles: row.roles,
            using_expr: row.using_expr,
            with_check_expr: row.with_check_expr,
        })
        .collect())
}

async fn fetch_column_dependencies(
    conn: &mut PgConnection,
) -> Result<Vec<RawPolicyColumnDependency>> {
    // The attnum-to-name correspondence is `pg_attribute`'s alone, so the column
    // name is resolved here rather than in the converter.
    let rows = sqlx::query!(
        r#"
        SELECT
            p.oid AS "policy_oid!",
            c.relnamespace AS "relation_namespace!",
            c.relname AS "relation_name!",
            c.relkind::text AS "relkind!",
            a.attname AS "column_name!"
        FROM pg_policy p
        JOIN pg_depend d ON d.objid = p.oid AND d.classid = 'pg_policy'::regclass
        JOIN pg_class c ON d.refobjid = c.oid AND d.refclassid = 'pg_class'::regclass
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = d.refobjsubid
        WHERE d.refobjsubid > 0
          AND d.deptype = 'n'
        ORDER BY p.oid, c.relname, a.attname
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawPolicyColumnDependency {
            policy_oid: row.policy_oid,
            relation_namespace: row.relation_namespace,
            relation_name: row.relation_name,
            relkind: row.relkind,
            column_name: row.column_name,
        })
        .collect())
}

async fn fetch_object_dependencies(
    conn: &mut PgConnection,
) -> Result<Vec<RawPolicyObjectDependency>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            p.oid AS "policy_oid!",
            cls.relnamespace AS "relation_namespace?",
            cls.relname AS "relation_name?",
            cls.relkind::text AS "relkind?",
            proc.oid AS "function_oid?",
            proc.pronamespace AS "function_namespace?",
            proc.proname AS "function_name?",
            pg_catalog.pg_get_function_identity_arguments(proc.oid) AS "function_args?"
        FROM pg_policy p
        JOIN pg_depend d ON d.objid = p.oid AND d.classid = 'pg_policy'::regclass
        LEFT JOIN pg_class cls ON d.refclassid = 'pg_class'::regclass AND d.refobjid = cls.oid
        LEFT JOIN pg_proc proc ON d.refclassid = 'pg_proc'::regclass AND d.refobjid = proc.oid
        WHERE d.refobjsubid = 0
          AND d.deptype = 'n'
          AND (cls.oid IS NOT NULL OR proc.oid IS NOT NULL)
        ORDER BY p.oid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawPolicyObjectDependency {
            policy_oid: row.policy_oid,
            relation_namespace: row.relation_namespace,
            relation_name: row.relation_name,
            relkind: row.relkind,
            function_oid: row.function_oid,
            function_namespace: row.function_namespace,
            function_name: row.function_name,
            function_args: row.function_args,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_every_polcmd_char_names_a_command() {
        assert_eq!(command("*"), PolicyCommand::All);
        assert_eq!(command("r"), PolicyCommand::Select);
        assert_eq!(command("a"), PolicyCommand::Insert);
        assert_eq!(command("w"), PolicyCommand::Update);
        assert_eq!(command("d"), PolicyCommand::Delete);
    }
}
