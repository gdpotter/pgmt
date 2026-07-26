//! Raw view rows and their conversion into logical views.
//!
//! The fetches keep the OIDs and attnums the converter resolves with, plus the
//! outputs of the server-side functions that cannot be computed in Rust:
//! `pg_get_viewdef` for the view body, `format_type` for a column's rendered
//! type and for an operator's operand types, and
//! `pg_get_function_identity_arguments` for the routines a view calls.
//! Everything else — schema-name resolution, extension-ownership and
//! system-schema exclusion, type classification, dependency derivation, comment
//! attachment — happens in the converter, where the OIDs die.

use anyhow::{Context, Result, anyhow};
use sqlx::postgres::PgConnection;
use sqlx::postgres::types::Oid;
use std::collections::BTreeMap;
use tracing::info;

use super::dedup_preserving_order;
use super::exclusion::{Converted, Excluded, ExclusionReason, is_system_schema};
use super::oid_index::OidIndex;
use super::shared::{ResolvedType, SharedCatalog, class};
use crate::catalog::id::DbObjectId;
use crate::catalog::view::{View, ViewColumn};

/// One `pg_class` row of `relkind = 'v'`, before names are resolved and OIDs are
/// discarded.
#[derive(Debug, Clone)]
pub struct RawView {
    pub oid: Oid,
    pub namespace: Oid,
    pub name: String,
    /// `pg_get_viewdef(oid, true)` — the view body as the server renders it.
    /// Absent for a view in a system schema, which the converter excludes:
    /// rendering the whole of `information_schema` costs more than the load.
    pub definition: Option<String>,
    pub reloptions: Option<Vec<String>>,
}

/// One `pg_attribute` row of a view.
#[derive(Debug, Clone)]
pub struct RawViewColumn {
    pub attrelid: Oid,
    pub attnum: i32,
    pub name: String,
    /// `atttypid`, unresolved: an array's own OID, not its element type's.
    pub type_oid: Oid,
    /// `format_type(atttypid, atttypmod)` — carries type modifiers and array
    /// brackets, which no Rust-side reconstruction can recover.
    pub formatted_type: String,
    pub attndims: i32,
}

/// One `pg_depend` edge of a view's rewrite rule: what the view body references.
///
/// The referenced object is identified by the catalog table it lives in, and
/// only the columns of that catalog are populated.
#[derive(Debug, Clone)]
pub struct RawViewDependency {
    pub view_oid: Oid,
    /// Name of the `pg_catalog` table the reference addresses (`pg_class`,
    /// `pg_type`, `pg_proc`, `pg_operator`, …).
    pub ref_class: String,
    pub ref_oid: Oid,

    /// Referenced relation (`pg_class`).
    pub relation_kind: Option<String>,
    pub relation_namespace: Option<Oid>,
    pub relation_name: Option<String>,

    /// Referenced routine (`pg_proc`).
    pub function_namespace: Option<Oid>,
    pub function_name: Option<String>,
    pub function_args: Option<String>,

    /// Referenced operator (`pg_operator`), with its `format_type` operands.
    pub operator_namespace: Option<Oid>,
    pub operator_name: Option<String>,
    pub operator_left_type: Option<String>,
    pub operator_right_type: Option<String>,
}

/// Everything the view converter reads out of `pg_catalog`.
#[derive(Debug, Clone, Default)]
pub struct RawViews {
    pub views: Vec<RawView>,
    pub columns: Vec<RawViewColumn>,
    pub dependencies: Vec<RawViewDependency>,
}

/// A converted view, still beside the OID the comment pass addresses it by.
#[derive(Debug, Clone)]
pub struct ConvertedView {
    pub oid: Oid,
    pub view: View,
    /// The attnum of each column of `view`, positionally aligned. Column
    /// comments are addressed by attnum in `pg_description` but by name in the
    /// logical world; this is the correspondence, and it does not outlive the
    /// conversion.
    pub column_attnums: Vec<i32>,
}

/// Fetch every view, view column and view dependency edge in the database,
/// unresolved and unfiltered.
pub async fn fetch(conn: &mut PgConnection) -> Result<RawViews> {
    info!("Fetching views...");
    let views = fetch_views(&mut *conn).await?;
    info!("Fetching view columns...");
    let columns = fetch_columns(&mut *conn).await?;
    info!("Fetching view dependencies...");
    let dependencies = fetch_dependencies(&mut *conn).await?;

    Ok(RawViews {
        views,
        columns,
        dependencies,
    })
}

/// Fetch views and convert them into the logical catalog, with the view's own
/// comment and its column comments attached through the OID index.
pub async fn load(conn: &mut PgConnection, shared: &SharedCatalog) -> Result<Vec<View>> {
    Ok(load_with_exclusions(conn, shared)
        .await?
        .log_and_take_objects("view"))
}

/// The same load, keeping the named reason for every raw row that did not
/// become a view.
pub async fn load_with_exclusions(
    conn: &mut PgConnection,
    shared: &SharedCatalog,
) -> Result<Converted<View>> {
    let raw = fetch(conn).await?;
    let mut converted = convert(&raw, shared)?;

    // Identity first, then the index, then the OID-addressed state: a comment
    // can only be attached to an object whose identity is already known.
    let index = OidIndex::from_pairs(
        class::PG_CLASS,
        converted
            .objects
            .iter()
            .map(|entry| (entry.oid, entry.view.id())),
    )?;
    let view_comments = index.object_comments(&shared.descriptions, class::PG_CLASS);
    let column_comments = index.subobject_comments(&shared.descriptions, class::PG_CLASS);

    for entry in &mut converted.objects {
        let id = entry.view.id();
        entry.view.comment = view_comments.get(&id).map(|text| text.to_string());

        if let Some(by_attnum) = column_comments.get(&id) {
            for (column, attnum) in entry.view.columns.iter_mut().zip(&entry.column_attnums) {
                column.comment = by_attnum.get(attnum).map(|text| text.to_string());
            }
        }
    }

    converted.index = index;

    Ok(converted.map(|entry| entry.view))
}

/// Resolve raw views into logical ones, keeping each view's OID (and its
/// columns' attnums) beside it so OID-addressed state can still be attached
/// before the identities cross the firewall.
///
/// Views in a system schema and views owned by an extension are dropped here,
/// each recorded with its named reason, along with the columns and dependency
/// edges belonging to them.
pub fn convert(raw: &RawViews, shared: &SharedCatalog) -> Result<Converted<ConvertedView>> {
    let namespaces = &shared.namespaces;

    // The views that survive filtering, by OID, so every column and dependency
    // row can be routed to its view (or dropped with it).
    let mut kept: BTreeMap<u32, usize> = BTreeMap::new();
    let mut converted: Converted<ConvertedView> = Converted::new();

    for row in &raw.views {
        let schema = namespaces
            .name(row.namespace)
            .with_context(|| format!("view {} has no namespace entry", row.name))?;

        if is_system_schema(schema) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "view",
                schema,
                &row.name,
                ExclusionReason::SystemSchema,
            ));
            continue;
        }
        if let Some(extension) = shared.extensions.owner(class::PG_CLASS, row.oid) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "view",
                schema,
                &row.name,
                ExclusionReason::ExtensionOwned {
                    extension: extension.to_string(),
                },
            ));
            continue;
        }

        // Only a row the fetch skipped rendering — that is, one excluded above —
        // may lack a definition; a view that converts without one would be
        // rendered as an empty `CREATE VIEW`.
        let definition = row.definition.clone().ok_or_else(|| {
            anyhow!(
                "view {}.{} was fetched without a definition",
                schema,
                row.name
            )
        })?;

        let (security_invoker, security_barrier) = parse_view_options(&row.reloptions);

        kept.insert(row.oid.0, converted.objects.len());
        converted.objects.push(ConvertedView {
            oid: row.oid,
            view: View {
                schema: schema.to_string(),
                name: row.name.clone(),
                definition,
                columns: Vec::new(),
                comment: None,
                security_invoker,
                security_barrier,
                depends_on: Vec::new(),
            },
            column_attnums: Vec::new(),
        });
    }

    for row in &raw.columns {
        let Some(&idx) = kept.get(&row.attrelid.0) else {
            continue;
        };
        let entry = &mut converted.objects[idx];
        entry.view.columns.push(ViewColumn {
            name: row.name.clone(),
            type_: Some(build_column_type(
                &row.formatted_type,
                shared.resolve_type(row.type_oid).as_ref(),
                row.attndims,
            )),
            comment: None,
        });
        entry.column_attnums.push(row.attnum);
    }

    for row in &raw.dependencies {
        let Some(&idx) = kept.get(&row.view_oid.0) else {
            continue;
        };
        let entry = &mut converted.objects[idx];
        let view_id = entry.view.id();
        if let Some(dep) = dependency(row, shared)
            && dep != view_id
        {
            entry.view.depends_on.push(dep);
        }
    }

    for entry in &mut converted.objects {
        // A view body references the same object once per column it uses.
        dedup_preserving_order(&mut entry.view.depends_on);

        // The `public` schema is assumed to exist rather than depended on.
        if entry.view.schema != "public" {
            entry.view.depends_on.push(DbObjectId::Schema {
                name: entry.view.schema.clone(),
            });
        }
    }

    // The raw fetches order by OID; ordering by name is what callers see.
    converted
        .objects
        .sort_by(|a, b| (&a.view.schema, &a.view.name).cmp(&(&b.view.schema, &b.view.name)));

    Ok(converted)
}

/// The object a view's rewrite-rule edge depends on, or `None` for a reference
/// pgmt does not track (a system type, a built-in routine or operator, a
/// relation kind that is neither table nor view).
fn dependency(row: &RawViewDependency, shared: &SharedCatalog) -> Option<DbObjectId> {
    let namespaces = &shared.namespaces;

    if let Some(relkind) = row.relation_kind.as_deref() {
        let schema = row.relation_namespace.and_then(|ns| namespaces.name(ns))?;
        let name = row.relation_name.clone()?;
        return match relkind {
            "r" | "p" => Some(DbObjectId::Table {
                schema: schema.to_string(),
                name,
            }),
            "v" | "m" => Some(DbObjectId::View {
                schema: schema.to_string(),
                name,
            }),
            _ => None,
        };
    }

    if row.ref_class == class::PG_TYPE
        && let Some(dep) = shared
            .resolve_type(row.ref_oid)
            .and_then(|t| t.dependency())
    {
        return Some(dep);
    }

    // A routine or operator an extension provides is depended on through the
    // extension; a built-in one, living in a system schema, not at all.
    if let (Some(namespace), Some(name), Some(args)) = (
        row.function_namespace,
        &row.function_name,
        &row.function_args,
    ) {
        let schema = namespaces.name(namespace)?;
        if is_system_schema(schema) {
            return None;
        }
        return Some(match shared.extensions.owner(class::PG_PROC, row.ref_oid) {
            Some(extension) => DbObjectId::Extension {
                name: extension.to_string(),
            },
            None => DbObjectId::Function {
                schema: schema.to_string(),
                name: name.clone(),
                arguments: args.clone(),
            },
        });
    }

    if let (Some(namespace), Some(name)) = (row.operator_namespace, &row.operator_name) {
        let schema = namespaces.name(namespace)?;
        if is_system_schema(schema) {
            return None;
        }
        return Some(
            match shared.extensions.owner(class::PG_OPERATOR, row.ref_oid) {
                Some(extension) => DbObjectId::Extension {
                    name: extension.to_string(),
                },
                None => DbObjectId::Operator {
                    schema: schema.to_string(),
                    name: name.clone(),
                    arguments: format!(
                        "{}, {}",
                        row.operator_left_type.as_deref().unwrap_or("NONE"),
                        row.operator_right_type.as_deref().unwrap_or("NONE")
                    ),
                },
            },
        );
    }

    None
}

/// Render a column's type, schema-qualifying a user-defined one and preserving
/// array brackets. An extension-provided or built-in type keeps the server's
/// rendering: extension types resolve through the extension's schema and are
/// never qualified by pgmt.
fn build_column_type(
    formatted_type: &str,
    resolved: Option<&ResolvedType<'_>>,
    attndims: i32,
) -> String {
    let Some(resolved) = resolved else {
        return formatted_type.to_string();
    };
    if resolved.extension.is_some() {
        return formatted_type.to_string();
    }
    let Some(schema) = resolved.schema.filter(|s| !is_system_schema(s)) else {
        return formatted_type.to_string();
    };

    let brackets = if attndims > 0 {
        "[]".repeat(attndims as usize)
    } else if formatted_type.ends_with("[]") {
        "[]".to_string()
    } else {
        String::new()
    };
    format!("\"{}\".\"{}\"{}", schema, resolved.name, brackets)
}

/// Read `security_invoker` and `security_barrier` out of a view's reloptions.
fn parse_view_options(reloptions: &Option<Vec<String>>) -> (bool, bool) {
    let mut security_invoker = false;
    let mut security_barrier = false;

    if let Some(opts) = reloptions {
        for opt in opts {
            if opt == "security_invoker=true" || opt == "security_invoker=on" {
                security_invoker = true;
            } else if opt == "security_barrier=true" || opt == "security_barrier=on" {
                security_barrier = true;
            }
        }
    }

    (security_invoker, security_barrier)
}

async fn fetch_views(conn: &mut PgConnection) -> Result<Vec<RawView>> {
    // The system-schema test mirrors `exclusion::sql::not_a_system_namespace`,
    // spelled out because `sqlx::query!` takes a literal: the row still has to
    // arrive for the converter to account for its exclusion, but rendering the
    // body of a view the converter drops is pure cost.
    let rows = sqlx::query!(
        r#"
        SELECT
            c.oid AS "oid!",
            c.relnamespace AS "namespace!",
            c.relname AS "name!",
            CASE
                WHEN n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                 AND n.nspname NOT LIKE 'pg_temp_%'
                 AND n.nspname NOT LIKE 'pg_toast_temp_%'
                THEN pg_catalog.pg_get_viewdef(c.oid, true)
            END AS "definition?",
            c.reloptions AS "reloptions?"
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relkind = 'v'
        ORDER BY c.oid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawView {
            oid: row.oid,
            namespace: row.namespace,
            name: row.name,
            definition: row.definition,
            reloptions: row.reloptions,
        })
        .collect())
}

async fn fetch_columns(conn: &mut PgConnection) -> Result<Vec<RawViewColumn>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            a.attrelid AS "attrelid!",
            a.attnum AS "attnum!",
            a.attname AS "name!",
            a.atttypid AS "type_oid!",
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS "formatted_type!",
            COALESCE(a.attndims, 0)::int AS "attndims!: i32"
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid AND c.relkind = 'v'
        WHERE a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attrelid, a.attnum
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawViewColumn {
            attrelid: row.attrelid,
            attnum: row.attnum as i32,
            name: row.name,
            type_oid: row.type_oid,
            formatted_type: row.formatted_type,
            attndims: row.attndims,
        })
        .collect())
}

async fn fetch_dependencies(conn: &mut PgConnection) -> Result<Vec<RawViewDependency>> {
    // What a view body references is recorded against its rewrite rule, not
    // against the view: `pg_rewrite` is the edge's origin and `pg_depend` names
    // the target, once per column of the target that the body uses.
    let rows = sqlx::query!(
        r#"
        SELECT
            r.ev_class AS "view_oid!",
            refcl.relname AS "ref_class!",
            d.refobjid AS "ref_oid!",

            cls.relkind::text AS "relation_kind?",
            cls.relnamespace AS "relation_namespace?",
            cls.relname AS "relation_name?",

            proc.pronamespace AS "function_namespace?",
            proc.proname AS "function_name?",
            pg_catalog.pg_get_function_identity_arguments(proc.oid) AS "function_args?",

            op.oprnamespace AS "operator_namespace?",
            op.oprname AS "operator_name?",
            CASE WHEN op.oprleft = 0 THEN NULL ELSE format_type(op.oprleft, NULL) END AS "operator_left_type?",
            CASE WHEN op.oprright = 0 THEN NULL ELSE format_type(op.oprright, NULL) END AS "operator_right_type?"

        FROM pg_rewrite r
        JOIN pg_class vc ON vc.oid = r.ev_class AND vc.relkind = 'v'
        JOIN pg_depend d
          ON d.classid = 'pg_rewrite'::regclass::oid
         AND d.objid = r.oid
        JOIN pg_class refcl ON refcl.oid = d.refclassid

        LEFT JOIN pg_class cls
          ON d.refclassid = 'pg_class'::regclass::oid
         AND d.refobjid = cls.oid

        LEFT JOIN pg_proc proc
          ON d.refclassid = 'pg_proc'::regclass::oid
         AND d.refobjid = proc.oid

        LEFT JOIN pg_operator op
          ON d.refclassid = 'pg_operator'::regclass::oid
         AND d.refobjid = op.oid

        ORDER BY r.ev_class, refcl.relname, d.refobjid, d.refobjsubid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawViewDependency {
            view_oid: row.view_oid,
            ref_class: row.ref_class,
            ref_oid: row.ref_oid,
            relation_kind: row.relation_kind,
            relation_namespace: row.relation_namespace,
            relation_name: row.relation_name,
            function_namespace: row.function_namespace,
            function_name: row.function_name,
            function_args: row.function_args,
            operator_namespace: row.operator_namespace,
            operator_name: row.operator_name,
            operator_left_type: row.operator_left_type,
            operator_right_type: row.operator_right_type,
        })
        .collect())
}
