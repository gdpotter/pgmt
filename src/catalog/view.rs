//! src/catalog/view.rs
//! Fetch views and their dependencies via pg_depend + pg_rewrite
use super::comments::Commentable;
use super::id::{DbObjectId, DependsOn};
use super::utils::is_system_schema;
use anyhow::Result;
use sqlx::postgres::PgConnection;
use sqlx::postgres::types::Oid;
use std::collections::{HashMap, HashSet};
use tracing::info;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ViewColumn {
    pub name: String,
    pub type_: Option<String>, // PostgreSQL doesn't always expose this directly
}

#[derive(Debug, Clone)]
pub struct View {
    pub schema: String,
    pub name: String,
    pub definition: String, // raw `SELECT …`
    pub columns: Vec<ViewColumn>,
    pub comment: Option<String>,     // comment on the view
    pub security_invoker: bool,      // PG 15+: execute with invoker's permissions (default: false)
    pub security_barrier: bool,      // prevent predicate pushdown for security (default: false)
    pub depends_on: Vec<DbObjectId>, // populated from pg_depend
}

impl View {
    pub fn id(&self) -> DbObjectId {
        DbObjectId::View {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }
}

impl DependsOn for View {
    fn id(&self) -> DbObjectId {
        DbObjectId::View {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}

impl Commentable for View {
    fn comment(&self) -> &Option<String> {
        &self.comment
    }
}

#[derive(sqlx::FromRow)]
struct RawView {
    view_oid: Oid,
    schema: String,
    name: String,
    definition: String,
    comment: Option<String>,
    reloptions: Option<Vec<String>>,
}

/// Build column type string, schema-qualifying custom types and preserving array brackets
fn build_column_type(
    formatted_type: &str,
    type_schema: &Option<String>,
    type_name: &Option<String>,
    attndims: i32,
    is_extension_type: bool,
) -> String {
    // Extension and system types use format_type directly
    if is_extension_type {
        return formatted_type.to_string();
    }
    if type_schema.as_ref().is_some_and(|s| is_system_schema(s)) || type_schema.is_none() {
        return formatted_type.to_string();
    }

    // Custom types need schema qualification
    if let (Some(schema), Some(name)) = (type_schema, type_name) {
        if attndims > 0 {
            format!(
                "\"{}\".\"{}\"{}",
                schema,
                name,
                "[]".repeat(attndims as usize)
            )
        } else if formatted_type.ends_with("[]") {
            format!("\"{}\".\"{}\"{}", schema, name, "[]")
        } else {
            format!("\"{}\".\"{}\"", schema, name)
        }
    } else {
        formatted_type.to_string()
    }
}

/// Parse reloptions to extract security_invoker and security_barrier
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

/// Fetch all non-system views, then populate `depends_on` via pg_depend.
pub async fn fetch(conn: &mut PgConnection) -> Result<Vec<View>> {
    // 1. Fetch view OIDs + definitions
    info!("Fetching views...");
    let raw: Vec<RawView> = sqlx::query_as!(
        RawView,
        r#"
        SELECT
          c.oid                    AS "view_oid!",
          n.nspname                AS "schema!",
          c.relname                AS "name!",
          pg_catalog.pg_get_viewdef(c.oid, true) AS "definition!",
          d.description            AS "comment?",
          c.reloptions             AS "reloptions?"
        FROM pg_class c
        JOIN pg_namespace n
          ON c.relnamespace = n.oid
        LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0
        WHERE c.relkind = 'v'                             -- only views
          AND n.nspname NOT IN ('pg_catalog','information_schema', 'pg_toast')
          -- Exclude views that belong to extensions
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend dep
              WHERE dep.objid = c.oid
              AND dep.deptype = 'e'
          )
        ORDER BY n.nspname, c.relname
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    info!("Fetching view columns...");
    // Use pg_attribute + pg_type for consistent array handling with other catalog types
    let column_rows = sqlx::query!(
        r#"
        SELECT
            n.nspname AS "schema!",
            c.relname AS "view_name!",
            a.attname AS "column_name!",
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS "data_type!",
            COALESCE(a.attndims, 0)::int AS "attndims!: i32",
            -- Resolve array element type schema/name for custom type handling
            CASE
                WHEN t.typelem != 0 THEN elem_tn.nspname
                ELSE tn.nspname
            END AS "type_schema?",
            CASE
                WHEN t.typelem != 0 THEN elem_t.typname
                ELSE t.typname
            END AS "type_name?",
            -- Check if type (or element type for arrays) is from an extension
            ext_types.extname IS NOT NULL AS "is_extension_type!: bool"
        FROM pg_attribute a
        JOIN pg_class c ON a.attrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        LEFT JOIN pg_type t ON a.atttypid = t.oid
        LEFT JOIN pg_namespace tn ON t.typnamespace = tn.oid
        -- Element type for array attributes
        LEFT JOIN pg_type elem_t ON t.typelem = elem_t.oid AND t.typelem != 0
        LEFT JOIN pg_namespace elem_tn ON elem_t.typnamespace = elem_tn.oid
        -- Extension type lookup
        LEFT JOIN (
            SELECT DISTINCT dep.objid AS type_oid, e.extname
            FROM pg_depend dep
            JOIN pg_extension e ON dep.refobjid = e.oid
            WHERE dep.deptype = 'e'
        ) ext_types ON ext_types.type_oid = COALESCE(NULLIF(t.typelem, 0::oid), t.oid)
        WHERE c.relkind = 'v'
          AND a.attnum > 0
          AND NOT a.attisdropped
          AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        ORDER BY n.nspname, c.relname, a.attnum
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    let mut columns_by_view: HashMap<(String, String), Vec<ViewColumn>> = HashMap::new();
    for col in column_rows {
        let key = (col.schema.clone(), col.view_name.clone());
        let type_str = build_column_type(
            &col.data_type,
            &col.type_schema,
            &col.type_name,
            col.attndims,
            col.is_extension_type,
        );
        columns_by_view.entry(key).or_default().push(ViewColumn {
            name: col.column_name,
            type_: Some(type_str),
        });
    }

    // Build initial View structs (empty depends_on) and index map
    let mut views: Vec<View> = raw
        .iter()
        .map(|r| {
            let key = (r.schema.clone(), r.name.clone());
            let columns = columns_by_view.remove(&key).unwrap_or_default();
            let (security_invoker, security_barrier) = parse_view_options(&r.reloptions);

            View {
                schema: r.schema.clone(),
                name: r.name.clone(),
                definition: r.definition.clone(),
                columns,
                comment: r.comment.clone(),
                security_invoker,
                security_barrier,
                depends_on: Vec::new(),
            }
        })
        .collect();

    let mut oid_to_idx: HashMap<Oid, usize> = HashMap::with_capacity(raw.len());
    let view_oids: Vec<Oid> = raw
        .into_iter()
        .enumerate()
        .map(|(i, r)| {
            oid_to_idx.insert(r.view_oid, i);
            r.view_oid
        })
        .collect();

    info!("Fetching view dependencies...");
    let deps = sqlx::query!(
        r#"
        SELECT
          r.ev_class                     AS "view_oid!",         -- the view itself
          d.refclassid                   AS "refclassid!",       -- kind of object
          d.refobjid                     AS "refobjid!",


          -- Table or view reference
          cls.relkind::text             AS "cls_relkind",
          cls_n.nspname                 AS "cls_schema",
          cls.relname                   AS "cls_name",

          -- Type reference (resolve array element type)
          CASE
            WHEN typ.typelem != 0 THEN elem_typ.typname
            ELSE typ.typname
          END AS "typ_name",
          CASE
            WHEN typ.typelem != 0 THEN elem_typ_n.nspname
            ELSE typ_n.nspname
          END AS "typ_schema",
          ext_types.extname AS "typ_extension_name?",
          -- Get typtype to distinguish domains ('d') from other types
          CASE
            WHEN typ.typelem != 0 THEN elem_typ.typtype::text
            ELSE typ.typtype::text
          END AS "typ_typtype?",

          -- Function reference
          proc.proname                  AS "proc_name",
          proc_n.nspname                AS "proc_schema",
          pg_catalog.pg_get_function_identity_arguments(proc.oid) AS "proc_args?",
          ext_procs.extname AS "proc_extension_name?"

        FROM pg_rewrite r
        JOIN pg_depend d
          ON d.classid = 'pg_rewrite'::regclass::oid
         AND d.objid    = r.oid

        -- Table/view reference
        LEFT JOIN pg_class cls
          ON d.refclassid = 'pg_class'::regclass::oid
         AND d.refobjid   = cls.oid

        LEFT JOIN pg_namespace cls_n
          ON cls.relnamespace = cls_n.oid

        -- Type reference
        LEFT JOIN pg_type typ
          ON d.refclassid = 'pg_type'::regclass::oid
         AND d.refobjid   = typ.oid

        LEFT JOIN pg_namespace typ_n
          ON typ.typnamespace = typ_n.oid

        -- Element type for array types
        LEFT JOIN pg_type elem_typ
          ON typ.typelem = elem_typ.oid AND typ.typelem != 0

        LEFT JOIN pg_namespace elem_typ_n
          ON elem_typ.typnamespace = elem_typ_n.oid

        -- Extension type lookup: compute once as derived table, then hash join
        LEFT JOIN (
            SELECT DISTINCT dep.objid AS type_oid, e.extname
            FROM pg_depend dep
            JOIN pg_extension e ON dep.refobjid = e.oid
            WHERE dep.deptype = 'e'
        ) ext_types ON ext_types.type_oid = COALESCE(NULLIF(typ.typelem, 0::oid), typ.oid)

        -- Function reference
        LEFT JOIN pg_proc proc
          ON d.refclassid = 'pg_proc'::regclass::oid
         AND d.refobjid   = proc.oid

        LEFT JOIN pg_namespace proc_n
          ON proc.pronamespace = proc_n.oid

        -- Extension function lookup: compute once as derived table, then hash join
        LEFT JOIN (
            SELECT DISTINCT dep.objid AS proc_oid, e.extname
            FROM pg_depend dep
            JOIN pg_extension e ON dep.refobjid = e.oid
            WHERE dep.deptype = 'e'
        ) ext_procs ON ext_procs.proc_oid = proc.oid

        WHERE r.ev_class = ANY($1)
        "#,
        &view_oids,
    )
    .fetch_all(&mut *conn)
    .await?;

    // 3. Map each dependency row into the corresponding View.depends_on
    for d in deps {
        if let Some(&idx) = oid_to_idx.get(&d.view_oid) {
            let view_id = views[idx].id();
            let v = &mut views[idx].depends_on;

            // Table or nested view?
            if let Some(relkind) = d.cls_relkind.as_deref() {
                let dep = match relkind {
                    "r" | "p" => DbObjectId::Table {
                        schema: d.cls_schema.unwrap(),
                        name: d.cls_name.unwrap(),
                    },
                    "v" | "m" => DbObjectId::View {
                        schema: d.cls_schema.unwrap(),
                        name: d.cls_name.unwrap(),
                    },
                    _ => continue, // skip other relkinds
                };
                if dep != view_id {
                    v.push(dep);
                }
                continue;
            }

            // Custom type, domain, or extension type?
            // Type name is already resolved to element type for arrays via SQL
            if let (Some(name), Some(ns)) = (&d.typ_name, &d.typ_schema) {
                if !is_system_schema(ns) {
                    // If type is from an extension, depend on the extension instead
                    if let Some(ext_name) = &d.typ_extension_name {
                        v.push(DbObjectId::Extension {
                            name: ext_name.clone(),
                        });
                    } else if d.typ_typtype.as_deref() == Some("d") {
                        // Domain type
                        v.push(DbObjectId::Domain {
                            schema: ns.to_string(),
                            name: name.to_string(),
                        });
                    } else {
                        v.push(DbObjectId::Type {
                            schema: ns.to_string(),
                            name: name.to_string(),
                        });
                    }
                }
                continue;
            }

            // Function or extension function?
            if let (Some(name), Some(ns), Some(args)) = (&d.proc_name, &d.proc_schema, &d.proc_args)
                && !is_system_schema(ns)
            {
                // If function is from an extension, depend on the extension instead
                if let Some(ext_name) = &d.proc_extension_name {
                    v.push(DbObjectId::Extension {
                        name: ext_name.clone(),
                    });
                } else {
                    v.push(DbObjectId::Function {
                        schema: ns.to_string(),
                        name: name.to_string(),
                        arguments: args.to_string(),
                    });
                }
            }
        }
    }

    // Deduplicate dependencies for each view
    for view in &mut views {
        let unique_deps: HashSet<_> = view.depends_on.drain(..).collect();
        view.depends_on.extend(unique_deps);

        // Add implicit schema dependency (every view depends on its schema existing)
        // Only add if it's not the default 'public' schema
        if view.schema != "public" {
            view.depends_on.push(DbObjectId::Schema {
                name: view.schema.clone(),
            });
        }
    }

    Ok(views)
}
