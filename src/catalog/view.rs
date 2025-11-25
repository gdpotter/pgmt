//! src/catalog/view.rs
//! Fetch views and their dependencies via pg_depend + pg_rewrite
use super::comments::Commentable;
use super::id::{DbObjectId, DependsOn};
use super::utils::is_system_schema;
use anyhow::Result;
use sqlx::PgPool;
use sqlx::postgres::types::Oid;
use std::collections::{HashMap, HashSet};

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
}

fn normalize_type(data_type: &str, udt_name: &str) -> String {
    if data_type == "ARRAY" {
        match udt_name {
            "_int4" => "integer[]".to_string(),
            "_text" => "text[]".to_string(),
            "_varchar" => "character varying[]".to_string(),
            "_bool" => "boolean[]".to_string(),
            _ => format!("{}[]", udt_name.trim_start_matches('_')),
        }
    } else {
        data_type.to_string()
    }
}

/// Fetch all non-system views, then populate `depends_on` via pg_depend.
pub async fn fetch(pool: &PgPool) -> Result<Vec<View>> {
    // 1. Fetch view OIDs + definitions
    let raw: Vec<RawView> = sqlx::query_as!(
        RawView,
        r#"
        SELECT
          c.oid                    AS "view_oid!",
          n.nspname                AS "schema!",
          c.relname                AS "name!",
          pg_catalog.pg_get_viewdef(c.oid, true) AS "definition!",
          d.description            AS "comment?"
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
    .fetch_all(pool)
    .await?;

    let column_rows = sqlx::query!(
        r#"
        SELECT
            table_schema AS "schema!",
            table_name AS "table_name!",
            column_name AS "name!",
            data_type AS "data_type!",
            udt_name AS "udt_name!"
        FROM information_schema.columns
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
          AND table_schema NOT LIKE 'pg_temp_%'
          AND table_name IN (
              SELECT table_name FROM information_schema.views
              WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                AND table_schema NOT LIKE 'pg_temp_%'
          )
        ORDER BY ordinal_position
        "#
    )
    .fetch_all(pool)
    .await?;

    let mut columns_by_view: HashMap<(String, String), Vec<ViewColumn>> = HashMap::new();
    for col in column_rows {
        let key = (col.schema.clone(), col.table_name.clone());
        columns_by_view.entry(key).or_default().push(ViewColumn {
            name: col.name,
            type_: Some(normalize_type(&col.data_type, &col.udt_name)),
        });
    }

    // Build initial View structs (empty depends_on) and index map
    let mut views: Vec<View> = raw
        .iter()
        .map(|r| {
            let key = (r.schema.clone(), r.name.clone());
            let columns = columns_by_view.remove(&key).unwrap_or_default();

            View {
                schema: r.schema.clone(),
                name: r.name.clone(),
                definition: r.definition.clone(),
                columns,
                comment: r.comment.clone(),
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

          -- Type reference
          typ.typname                   AS "typ_name",
          typ_n.nspname                 AS "typ_schema",
          (
            SELECT e.extname
            FROM pg_depend typ_dep
            JOIN pg_extension e ON typ_dep.refobjid = e.oid
            WHERE typ_dep.objid = typ.oid
            AND typ_dep.deptype = 'e'
            LIMIT 1
          ) AS "typ_extension_name?",

          -- Function reference
          proc.proname                  AS "proc_name",
          proc_n.nspname                AS "proc_schema",
          (
            SELECT e.extname
            FROM pg_depend proc_dep
            JOIN pg_extension e ON proc_dep.refobjid = e.oid
            WHERE proc_dep.objid = proc.oid
            AND proc_dep.deptype = 'e'
            LIMIT 1
          ) AS "proc_extension_name?"

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

        -- Function reference
        LEFT JOIN pg_proc proc
          ON d.refclassid = 'pg_proc'::regclass::oid
         AND d.refobjid   = proc.oid

        LEFT JOIN pg_namespace proc_n
          ON proc.pronamespace = proc_n.oid

        WHERE r.ev_class = ANY($1)
        "#,
        &view_oids,
    )
    .fetch_all(pool)
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

            // Custom type or extension type?
            if let (Some(name), Some(ns)) = (d.typ_name, d.typ_schema) {
                if !is_system_schema(&ns) {
                    // If type is from an extension, depend on the extension instead
                    if let Some(ext_name) = d.typ_extension_name {
                        v.push(DbObjectId::Extension { name: ext_name });
                    } else {
                        let base_type_name = if name.starts_with('_') {
                            name.trim_start_matches('_').to_string()
                        } else {
                            name
                        };
                        v.push(DbObjectId::Type {
                            schema: ns,
                            name: base_type_name,
                        });
                    }
                }
                continue;
            }

            // Function or extension function?
            if let (Some(name), Some(ns)) = (d.proc_name, d.proc_schema)
                && !is_system_schema(&ns)
            {
                // If function is from an extension, depend on the extension instead
                if let Some(ext_name) = d.proc_extension_name {
                    v.push(DbObjectId::Extension { name: ext_name });
                } else {
                    v.push(DbObjectId::Function {
                        schema: ns.to_string(),
                        name: name.to_string(),
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
