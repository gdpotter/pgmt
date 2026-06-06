use anyhow::Result;
use sqlx::postgres::PgConnection;
use tracing::info;

use super::comments::Commentable;
use super::id::{DbObjectId, DependsOn};
use super::utils::{is_system_schema, resolve_type_dependency};

/// A user-defined PostgreSQL operator (`CREATE OPERATOR`).
///
/// An operator is identified by its schema, symbol, and the types of its two
/// operands. Prefix operators have no left operand; their left operand type is
/// recorded as `NONE` (matching the `DROP`/`COMMENT` `(left, right)` syntax).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Operator {
    pub schema: String,
    /// The operator symbol, e.g. `===` or `@>`.
    pub name: String,
    /// Canonical `"left, right"` operand-type string, with `NONE` for an absent
    /// operand (prefix operators). Matches the `(left, right)` form that
    /// `DROP OPERATOR` and `COMMENT ON OPERATOR` require.
    pub arguments: String,
    /// The full reconstructed `CREATE OPERATOR` statement (no trailing `;`).
    pub definition: String,
    pub comment: Option<String>,
    pub depends_on: Vec<DbObjectId>,
}

impl Operator {
    pub fn id(&self) -> DbObjectId {
        DbObjectId::Operator {
            schema: self.schema.clone(),
            name: self.name.clone(),
            arguments: self.arguments.clone(),
        }
    }
}

impl DependsOn for Operator {
    fn id(&self) -> DbObjectId {
        DbObjectId::Operator {
            schema: self.schema.clone(),
            name: self.name.clone(),
            arguments: self.arguments.clone(),
        }
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}

impl Commentable for Operator {
    fn comment(&self) -> &Option<String> {
        &self.comment
    }
}

/// Fetch all user-defined operators from the database.
pub async fn fetch(conn: &mut PgConnection) -> Result<Vec<Operator>> {
    info!("Fetching operators...");
    let rows = sqlx::query!(
        r#"
        SELECT
            n.nspname AS "schema!",
            o.oprname AS "name!",

            -- Formatted operand types (NULL for an absent operand). These drive both
            -- the `(left, right)` identity string and the LEFTARG/RIGHTARG clauses.
            CASE WHEN o.oprleft = 0 THEN NULL ELSE format_type(o.oprleft, NULL) END AS "left_type?",
            CASE WHEN o.oprright = 0 THEN NULL ELSE format_type(o.oprright, NULL) END AS "right_type?",

            -- Implementing function (oprcode) - always present.
            fn_ns.nspname AS "function_schema!",
            fn.proname AS "function_name!",
            pg_catalog.pg_get_function_identity_arguments(fn.oid) AS "function_args!",

            -- Left operand type dependency metadata (resolve array element type).
            CASE WHEN lt.typelem != 0 THEN lt_elem.typname ELSE lt.typname END AS "left_dep_name?",
            CASE WHEN lt.typelem != 0 THEN lt_elem_n.nspname ELSE lt_n.nspname END AS "left_dep_schema?",
            CASE WHEN lt.typelem != 0 THEN lt_elem.typtype::text ELSE lt.typtype::text END AS "left_dep_typtype?",
            CASE WHEN lt.typelem != 0 THEN lt_elem_rel.relkind::text ELSE lt_rel.relkind::text END AS "left_dep_relkind?",
            ext_left.extname AS "left_dep_extension?",

            -- Right operand type dependency metadata (resolve array element type).
            CASE WHEN rt.typelem != 0 THEN rt_elem.typname ELSE rt.typname END AS "right_dep_name?",
            CASE WHEN rt.typelem != 0 THEN rt_elem_n.nspname ELSE rt_n.nspname END AS "right_dep_schema?",
            CASE WHEN rt.typelem != 0 THEN rt_elem.typtype::text ELSE rt.typtype::text END AS "right_dep_typtype?",
            CASE WHEN rt.typelem != 0 THEN rt_elem_rel.relkind::text ELSE rt_rel.relkind::text END AS "right_dep_relkind?",
            ext_right.extname AS "right_dep_extension?",

            -- Commutator / negator operator identities (for rendering only).
            com_ns.nspname AS "commutator_schema?",
            com.oprname AS "commutator_name?",
            neg_ns.nspname AS "negator_schema?",
            neg.oprname AS "negator_name?",

            -- Restriction / join selectivity functions.
            rf_ns.nspname AS "restrict_schema?",
            rf.proname AS "restrict_name?",
            pg_catalog.pg_get_function_identity_arguments(rf.oid) AS "restrict_args?",
            jf_ns.nspname AS "join_schema?",
            jf.proname AS "join_name?",
            pg_catalog.pg_get_function_identity_arguments(jf.oid) AS "join_args?",

            o.oprcanhash AS "hashes!",
            o.oprcanmerge AS "merges!",

            d.description AS "comment?"

        FROM pg_operator o
        JOIN pg_namespace n ON o.oprnamespace = n.oid

        -- Implementing function
        JOIN pg_proc fn ON o.oprcode = fn.oid
        JOIN pg_namespace fn_ns ON fn.pronamespace = fn_ns.oid

        -- Left operand type (+ array element resolution)
        LEFT JOIN pg_type lt ON o.oprleft = lt.oid AND o.oprleft != 0
        LEFT JOIN pg_namespace lt_n ON lt.typnamespace = lt_n.oid
        LEFT JOIN pg_type lt_elem ON lt.typelem = lt_elem.oid AND lt.typelem != 0
        LEFT JOIN pg_namespace lt_elem_n ON lt_elem.typnamespace = lt_elem_n.oid
        LEFT JOIN pg_class lt_rel ON lt.typrelid = lt_rel.oid AND lt.typrelid != 0
        LEFT JOIN pg_class lt_elem_rel ON lt_elem.typrelid = lt_elem_rel.oid AND lt_elem.typrelid != 0
        LEFT JOIN (
            SELECT DISTINCT dep.objid AS type_oid, e.extname
            FROM pg_depend dep
            JOIN pg_extension e ON dep.refobjid = e.oid
            WHERE dep.deptype = 'e'
        ) ext_left ON ext_left.type_oid = COALESCE(NULLIF(lt.typelem, 0::oid), lt.oid)

        -- Right operand type (+ array element resolution)
        LEFT JOIN pg_type rt ON o.oprright = rt.oid AND o.oprright != 0
        LEFT JOIN pg_namespace rt_n ON rt.typnamespace = rt_n.oid
        LEFT JOIN pg_type rt_elem ON rt.typelem = rt_elem.oid AND rt.typelem != 0
        LEFT JOIN pg_namespace rt_elem_n ON rt_elem.typnamespace = rt_elem_n.oid
        LEFT JOIN pg_class rt_rel ON rt.typrelid = rt_rel.oid AND rt.typrelid != 0
        LEFT JOIN pg_class rt_elem_rel ON rt_elem.typrelid = rt_elem_rel.oid AND rt_elem.typrelid != 0
        LEFT JOIN (
            SELECT DISTINCT dep.objid AS type_oid, e.extname
            FROM pg_depend dep
            JOIN pg_extension e ON dep.refobjid = e.oid
            WHERE dep.deptype = 'e'
        ) ext_right ON ext_right.type_oid = COALESCE(NULLIF(rt.typelem, 0::oid), rt.oid)

        -- Commutator / negator
        LEFT JOIN pg_operator com ON o.oprcom = com.oid AND o.oprcom != 0
        LEFT JOIN pg_namespace com_ns ON com.oprnamespace = com_ns.oid
        LEFT JOIN pg_operator neg ON o.oprnegate = neg.oid AND o.oprnegate != 0
        LEFT JOIN pg_namespace neg_ns ON neg.oprnamespace = neg_ns.oid

        -- Selectivity functions
        LEFT JOIN pg_proc rf ON o.oprrest = rf.oid AND o.oprrest != 0
        LEFT JOIN pg_namespace rf_ns ON rf.pronamespace = rf_ns.oid
        LEFT JOIN pg_proc jf ON o.oprjoin = jf.oid AND o.oprjoin != 0
        LEFT JOIN pg_namespace jf_ns ON jf.pronamespace = jf_ns.oid

        -- Comment
        LEFT JOIN pg_description d
          ON d.objoid = o.oid
         AND d.classoid = 'pg_operator'::regclass
         AND d.objsubid = 0

        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        -- Exclude operators that belong to an extension.
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend dep
            WHERE dep.objid = o.oid
              AND dep.classid = 'pg_operator'::regclass
              AND dep.deptype = 'e'
        )

        ORDER BY n.nspname, o.oprname, o.oprleft, o.oprright
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    let mut operators = Vec::new();

    for row in rows {
        let arguments = format!(
            "{}, {}",
            row.left_type.as_deref().unwrap_or("NONE"),
            row.right_type.as_deref().unwrap_or("NONE")
        );

        // Dependencies: schema, the implementing function, the operand types, and
        // any user-defined selectivity functions. Commutator/negator are
        // intentionally excluded (they reference each other and resolve via shells).
        let mut depends_on = vec![DbObjectId::Schema {
            name: row.schema.clone(),
        }];

        if !is_system_schema(&row.function_schema) {
            depends_on.push(DbObjectId::Function {
                schema: row.function_schema.clone(),
                name: row.function_name.clone(),
                arguments: row.function_args.clone(),
            });
        }

        if let Some(dep) = resolve_type_dependency(
            row.left_dep_schema.as_deref(),
            row.left_dep_name.as_deref(),
            row.left_dep_typtype.as_deref(),
            row.left_dep_relkind.as_deref(),
            row.left_dep_extension.is_some(),
            row.left_dep_extension.as_deref(),
        ) {
            depends_on.push(dep);
        }
        if let Some(dep) = resolve_type_dependency(
            row.right_dep_schema.as_deref(),
            row.right_dep_name.as_deref(),
            row.right_dep_typtype.as_deref(),
            row.right_dep_relkind.as_deref(),
            row.right_dep_extension.is_some(),
            row.right_dep_extension.as_deref(),
        ) {
            depends_on.push(dep);
        }

        if let (Some(schema), Some(name), Some(args)) =
            (&row.restrict_schema, &row.restrict_name, &row.restrict_args)
            && !is_system_schema(schema)
        {
            depends_on.push(DbObjectId::Function {
                schema: schema.clone(),
                name: name.clone(),
                arguments: args.clone(),
            });
        }
        if let (Some(schema), Some(name), Some(args)) =
            (&row.join_schema, &row.join_name, &row.join_args)
            && !is_system_schema(schema)
        {
            depends_on.push(DbObjectId::Function {
                schema: schema.clone(),
                name: name.clone(),
                arguments: args.clone(),
            });
        }

        let definition = build_operator_definition(
            &row.schema,
            &row.name,
            &row.function_schema,
            &row.function_name,
            row.left_type.as_deref(),
            row.right_type.as_deref(),
            row.commutator_schema.as_deref(),
            row.commutator_name.as_deref(),
            row.negator_schema.as_deref(),
            row.negator_name.as_deref(),
            row.restrict_schema.as_deref(),
            row.restrict_name.as_deref(),
            row.join_schema.as_deref(),
            row.join_name.as_deref(),
            row.hashes,
            row.merges,
        );

        // De-duplicate dependencies while preserving order.
        let mut seen = std::collections::HashSet::new();
        depends_on.retain(|d| seen.insert(d.clone()));

        operators.push(Operator {
            schema: row.schema,
            name: row.name,
            arguments,
            definition,
            comment: row.comment,
            depends_on,
        });
    }

    Ok(operators)
}

/// Qualify a routine/operator name with its schema, leaving system-schema
/// objects unqualified (matching how aggregates render SFUNC etc.).
fn qualify(schema: &str, name: &str) -> String {
    if is_system_schema(schema) {
        name.to_string()
    } else {
        format!("{}.{}", schema, name)
    }
}

/// Reconstruct a `CREATE OPERATOR` statement (no trailing `;`).
#[allow(clippy::too_many_arguments)]
fn build_operator_definition(
    schema: &str,
    name: &str,
    function_schema: &str,
    function_name: &str,
    left_type: Option<&str>,
    right_type: Option<&str>,
    commutator_schema: Option<&str>,
    commutator_name: Option<&str>,
    negator_schema: Option<&str>,
    negator_name: Option<&str>,
    restrict_schema: Option<&str>,
    restrict_name: Option<&str>,
    join_schema: Option<&str>,
    join_name: Option<&str>,
    hashes: bool,
    merges: bool,
) -> String {
    let mut parts = vec![format!(
        "FUNCTION = {}",
        qualify(function_schema, function_name)
    )];

    if let Some(left) = left_type {
        parts.push(format!("LEFTARG = {}", left));
    }
    if let Some(right) = right_type {
        parts.push(format!("RIGHTARG = {}", right));
    }
    if let (Some(s), Some(n)) = (commutator_schema, commutator_name) {
        parts.push(format!("COMMUTATOR = OPERATOR({}.{})", s, n));
    }
    if let (Some(s), Some(n)) = (negator_schema, negator_name) {
        parts.push(format!("NEGATOR = OPERATOR({}.{})", s, n));
    }
    if let (Some(s), Some(n)) = (restrict_schema, restrict_name) {
        parts.push(format!("RESTRICT = {}", qualify(s, n)));
    }
    if let (Some(s), Some(n)) = (join_schema, join_name) {
        parts.push(format!("JOIN = {}", qualify(s, n)));
    }
    if hashes {
        parts.push("HASHES".to_string());
    }
    if merges {
        parts.push("MERGES".to_string());
    }

    format!(
        "CREATE OPERATOR {}.{} (\n    {}\n)",
        schema,
        name,
        parts.join(",\n    ")
    )
}
