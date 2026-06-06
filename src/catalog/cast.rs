use anyhow::Result;
use sqlx::postgres::PgConnection;
use tracing::info;

use super::comments::Commentable;
use super::id::{DbObjectId, DependsOn};
use super::utils::{is_system_schema, resolve_type_dependency};

/// A user-defined PostgreSQL cast (`CREATE CAST`).
///
/// Casts are not schema-scoped; their identity is the (source type, target type)
/// pair. `source` and `target` are canonical `format_type` names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cast {
    pub source: String,
    pub target: String,
    /// The full reconstructed `CREATE CAST` statement (no trailing `;`).
    pub definition: String,
    pub comment: Option<String>,
    pub depends_on: Vec<DbObjectId>,
}

impl Cast {
    pub fn id(&self) -> DbObjectId {
        DbObjectId::Cast {
            source: self.source.clone(),
            target: self.target.clone(),
        }
    }
}

impl DependsOn for Cast {
    fn id(&self) -> DbObjectId {
        DbObjectId::Cast {
            source: self.source.clone(),
            target: self.target.clone(),
        }
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}

impl Commentable for Cast {
    fn comment(&self) -> &Option<String> {
        &self.comment
    }
}

/// Fetch all user-defined casts from the database.
pub async fn fetch(conn: &mut PgConnection) -> Result<Vec<Cast>> {
    info!("Fetching casts...");
    let rows = sqlx::query!(
        r#"
        SELECT
            format_type(c.castsource, NULL) AS "source!",
            format_type(c.casttarget, NULL) AS "target!",
            c.castcontext::text AS "context!",
            c.castmethod::text AS "method!",

            -- Implementing function (only for method = 'f').
            fn_ns.nspname AS "function_schema?",
            fn.proname AS "function_name?",
            pg_catalog.pg_get_function_identity_arguments(fn.oid) AS "function_args?",

            -- Source type dependency metadata (resolve array element type).
            CASE WHEN st.typelem != 0 THEN st_elem.typname ELSE st.typname END AS "source_dep_name?",
            CASE WHEN st.typelem != 0 THEN st_elem_n.nspname ELSE st_n.nspname END AS "source_dep_schema?",
            CASE WHEN st.typelem != 0 THEN st_elem.typtype::text ELSE st.typtype::text END AS "source_dep_typtype?",
            CASE WHEN st.typelem != 0 THEN st_elem_rel.relkind::text ELSE st_rel.relkind::text END AS "source_dep_relkind?",
            ext_source.extname AS "source_dep_extension?",

            -- Target type dependency metadata (resolve array element type).
            CASE WHEN tt.typelem != 0 THEN tt_elem.typname ELSE tt.typname END AS "target_dep_name?",
            CASE WHEN tt.typelem != 0 THEN tt_elem_n.nspname ELSE tt_n.nspname END AS "target_dep_schema?",
            CASE WHEN tt.typelem != 0 THEN tt_elem.typtype::text ELSE tt.typtype::text END AS "target_dep_typtype?",
            CASE WHEN tt.typelem != 0 THEN tt_elem_rel.relkind::text ELSE tt_rel.relkind::text END AS "target_dep_relkind?",
            ext_target.extname AS "target_dep_extension?",

            d.description AS "comment?"

        FROM pg_cast c

        -- Implementing function
        LEFT JOIN pg_proc fn ON c.castfunc = fn.oid AND c.castfunc != 0
        LEFT JOIN pg_namespace fn_ns ON fn.pronamespace = fn_ns.oid

        -- Source type (+ array element resolution)
        JOIN pg_type st ON c.castsource = st.oid
        JOIN pg_namespace st_n ON st.typnamespace = st_n.oid
        LEFT JOIN pg_type st_elem ON st.typelem = st_elem.oid AND st.typelem != 0
        LEFT JOIN pg_namespace st_elem_n ON st_elem.typnamespace = st_elem_n.oid
        LEFT JOIN pg_class st_rel ON st.typrelid = st_rel.oid AND st.typrelid != 0
        LEFT JOIN pg_class st_elem_rel ON st_elem.typrelid = st_elem_rel.oid AND st_elem.typrelid != 0
        LEFT JOIN (
            SELECT DISTINCT dep.objid AS type_oid, e.extname
            FROM pg_depend dep
            JOIN pg_extension e ON dep.refobjid = e.oid
            WHERE dep.deptype = 'e'
        ) ext_source ON ext_source.type_oid = COALESCE(NULLIF(st.typelem, 0::oid), st.oid)

        -- Target type (+ array element resolution)
        JOIN pg_type tt ON c.casttarget = tt.oid
        JOIN pg_namespace tt_n ON tt.typnamespace = tt_n.oid
        LEFT JOIN pg_type tt_elem ON tt.typelem = tt_elem.oid AND tt.typelem != 0
        LEFT JOIN pg_namespace tt_elem_n ON tt_elem.typnamespace = tt_elem_n.oid
        LEFT JOIN pg_class tt_rel ON tt.typrelid = tt_rel.oid AND tt.typrelid != 0
        LEFT JOIN pg_class tt_elem_rel ON tt_elem.typrelid = tt_elem_rel.oid AND tt_elem.typrelid != 0
        LEFT JOIN (
            SELECT DISTINCT dep.objid AS type_oid, e.extname
            FROM pg_depend dep
            JOIN pg_extension e ON dep.refobjid = e.oid
            WHERE dep.deptype = 'e'
        ) ext_target ON ext_target.type_oid = COALESCE(NULLIF(tt.typelem, 0::oid), tt.oid)

        -- Comment
        LEFT JOIN pg_description d
          ON d.objoid = c.oid
         AND d.classoid = 'pg_cast'::regclass
         AND d.objsubid = 0

        -- Keep only user casts. PostgreSQL requires ownership of the source or
        -- target type to create a cast, so every user cast has a non-system
        -- source or target type; built-in casts have both in pg_catalog. (Modern
        -- PostgreSQL identifies system objects by OID, not pg_depend pins, so a
        -- pin check is unreliable here.)
        WHERE (
            st_n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            OR tt_n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        )
        -- Exclude extension-owned casts.
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend dep
            WHERE dep.classid = 'pg_cast'::regclass
              AND dep.objid = c.oid
              AND dep.deptype = 'e'
        )

        ORDER BY format_type(c.castsource, NULL), format_type(c.casttarget, NULL)
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    let mut casts = Vec::new();

    for row in rows {
        // Dependencies: the source and target types and (for function casts) the
        // implementing function. Casts have no schema dependency of their own —
        // the types carry their schemas.
        let mut depends_on = Vec::new();

        if let Some(dep) = resolve_type_dependency(
            row.source_dep_schema.as_deref(),
            row.source_dep_name.as_deref(),
            row.source_dep_typtype.as_deref(),
            row.source_dep_relkind.as_deref(),
            row.source_dep_extension.is_some(),
            row.source_dep_extension.as_deref(),
        ) {
            depends_on.push(dep);
        }
        if let Some(dep) = resolve_type_dependency(
            row.target_dep_schema.as_deref(),
            row.target_dep_name.as_deref(),
            row.target_dep_typtype.as_deref(),
            row.target_dep_relkind.as_deref(),
            row.target_dep_extension.is_some(),
            row.target_dep_extension.as_deref(),
        ) {
            depends_on.push(dep);
        }

        let function = match (&row.function_schema, &row.function_name, &row.function_args) {
            (Some(schema), Some(name), Some(args)) => {
                if !is_system_schema(schema) {
                    depends_on.push(DbObjectId::Function {
                        schema: schema.clone(),
                        name: name.clone(),
                        arguments: args.clone(),
                    });
                }
                Some((schema.as_str(), name.as_str(), args.as_str()))
            }
            _ => None,
        };

        let definition = build_cast_definition(
            &row.source,
            &row.target,
            &row.method,
            &row.context,
            function,
        );

        // De-duplicate dependencies while preserving order.
        let mut seen = std::collections::HashSet::new();
        depends_on.retain(|d| seen.insert(d.clone()));

        casts.push(Cast {
            source: row.source,
            target: row.target,
            definition,
            comment: row.comment,
            depends_on,
        });
    }

    Ok(casts)
}

/// Qualify a function name with its schema, leaving system-schema objects
/// unqualified (matching how operators/aggregates render function references).
fn qualify(schema: &str, name: &str) -> String {
    if is_system_schema(schema) {
        name.to_string()
    } else {
        format!("{}.{}", schema, name)
    }
}

/// Reconstruct a `CREATE CAST` statement (no trailing `;`).
///
/// `method` is the `pg_cast.castmethod` char (`f` = WITH FUNCTION, `i` = WITH
/// INOUT, `b` = WITHOUT FUNCTION); `context` is `pg_cast.castcontext` (`e` =
/// explicit/default, `a` = AS ASSIGNMENT, `i` = AS IMPLICIT).
fn build_cast_definition(
    source: &str,
    target: &str,
    method: &str,
    context: &str,
    function: Option<(&str, &str, &str)>,
) -> String {
    let method_clause = match method {
        "f" => {
            let (schema, name, args) =
                function.expect("function-method cast must carry its implementing function");
            format!("WITH FUNCTION {}({})", qualify(schema, name), args)
        }
        "i" => "WITH INOUT".to_string(),
        // "b" and any unexpected value fall back to the no-function form.
        _ => "WITHOUT FUNCTION".to_string(),
    };

    let mut definition = format!("CREATE CAST ({} AS {}) {}", source, target, method_clause);
    match context {
        "a" => definition.push_str(" AS ASSIGNMENT"),
        "i" => definition.push_str(" AS IMPLICIT"),
        // "e" (explicit) is the default and needs no clause.
        _ => {}
    }
    definition
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_function_cast_explicit() {
        assert_eq!(
            build_cast_definition(
                "celsius",
                "fahrenheit",
                "f",
                "e",
                Some(("public", "c_to_f", "celsius"))
            ),
            "CREATE CAST (celsius AS fahrenheit) WITH FUNCTION public.c_to_f(celsius)"
        );
    }

    #[test]
    fn test_build_inout_cast_assignment() {
        assert_eq!(
            build_cast_definition("celsius", "text", "i", "a", None),
            "CREATE CAST (celsius AS text) WITH INOUT AS ASSIGNMENT"
        );
    }

    #[test]
    fn test_build_without_function_cast_implicit() {
        assert_eq!(
            build_cast_definition("widget", "gadget", "b", "i", None),
            "CREATE CAST (widget AS gadget) WITHOUT FUNCTION AS IMPLICIT"
        );
    }

    #[test]
    fn test_build_function_cast_uses_unqualified_system_function() {
        assert_eq!(
            build_cast_definition(
                "mytype",
                "integer",
                "f",
                "e",
                Some(("pg_catalog", "int4", "mytype"))
            ),
            "CREATE CAST (mytype AS integer) WITH FUNCTION int4(mytype)"
        );
    }
}
