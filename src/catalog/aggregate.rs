use anyhow::Result;
use sqlx::postgres::PgConnection;
use tracing::info;

use super::comments::Commentable;
use super::id::{DbObjectId, DependsOn};
use super::utils::is_system_schema;

/// Represents a PostgreSQL aggregate function
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Aggregate {
    pub schema: String,
    pub name: String,
    /// Formatted argument types (e.g., "integer, text")
    pub arguments: String,
    /// State transition type (STYPE)
    pub state_type: String,
    pub state_type_schema: String,
    /// State transition function (SFUNC)
    pub state_func: String,
    pub state_func_schema: String,
    /// Final function (FINALFUNC), optional
    pub final_func: Option<String>,
    pub final_func_schema: Option<String>,
    /// Combine function for parallel aggregation (COMBINEFUNC), optional
    pub combine_func: Option<String>,
    pub combine_func_schema: Option<String>,
    /// Initial state value (INITCOND), optional
    pub initial_value: Option<String>,
    /// Complete CREATE AGGREGATE statement (reconstructed)
    pub definition: String,
    pub comment: Option<String>,
    pub depends_on: Vec<DbObjectId>,
}

impl Aggregate {
    pub fn id(&self) -> DbObjectId {
        DbObjectId::Aggregate {
            schema: self.schema.clone(),
            name: self.name.clone(),
            arguments: self.arguments.clone(),
        }
    }
}

impl DependsOn for Aggregate {
    fn id(&self) -> DbObjectId {
        DbObjectId::Aggregate {
            schema: self.schema.clone(),
            name: self.name.clone(),
            arguments: self.arguments.clone(),
        }
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}

impl Commentable for Aggregate {
    fn comment(&self) -> &Option<String> {
        &self.comment
    }
}

/// Fetch all user-defined aggregate functions from the database
pub async fn fetch(conn: &mut PgConnection) -> Result<Vec<Aggregate>> {
    info!("Fetching aggregates...");
    let rows = sqlx::query!(
        r#"
        SELECT
            n.nspname AS "schema!",
            p.proname AS "name!",
            pg_catalog.pg_get_function_identity_arguments(p.oid) AS "arguments!",

            -- State type (STYPE) - resolve array element type if applicable
            CASE
                WHEN st.typelem != 0 THEN elem_st.typname
                ELSE st.typname
            END AS "state_type!",
            CASE
                WHEN st.typelem != 0 THEN elem_stn.nspname
                ELSE stn.nspname
            END AS "state_type_schema!",

            -- State transition function (SFUNC)
            tfunc.proname AS "state_func!",
            tfns.nspname AS "state_func_schema!",
            pg_catalog.pg_get_function_identity_arguments(tfunc.oid) AS "state_func_args!",

            -- Final function (FINALFUNC) - optional
            ffunc.proname AS "final_func?",
            ffns.nspname AS "final_func_schema?",
            pg_catalog.pg_get_function_identity_arguments(ffunc.oid) AS "final_func_args?",

            -- Combine function for parallel aggregation (COMBINEFUNC) - optional
            cfunc.proname AS "combine_func?",
            cfns.nspname AS "combine_func_schema?",
            pg_catalog.pg_get_function_identity_arguments(cfunc.oid) AS "combine_func_args?",

            -- Initial value (INITCOND) - optional
            agg.agginitval AS "initial_value?",

            -- Comment
            d.description AS "comment?"

        FROM pg_aggregate agg
        JOIN pg_proc p ON agg.aggfnoid = p.oid
        JOIN pg_namespace n ON p.pronamespace = n.oid

        -- State type
        JOIN pg_type st ON agg.aggtranstype = st.oid
        JOIN pg_namespace stn ON st.typnamespace = stn.oid
        -- Element type for array state types
        LEFT JOIN pg_type elem_st ON st.typelem = elem_st.oid AND st.typelem != 0
        LEFT JOIN pg_namespace elem_stn ON elem_st.typnamespace = elem_stn.oid

        -- State transition function
        JOIN pg_proc tfunc ON agg.aggtransfn = tfunc.oid
        JOIN pg_namespace tfns ON tfunc.pronamespace = tfns.oid

        -- Final function (optional)
        LEFT JOIN pg_proc ffunc ON agg.aggfinalfn = ffunc.oid AND agg.aggfinalfn != 0
        LEFT JOIN pg_namespace ffns ON ffunc.pronamespace = ffns.oid

        -- Combine function (optional)
        LEFT JOIN pg_proc cfunc ON agg.aggcombinefn = cfunc.oid AND agg.aggcombinefn != 0
        LEFT JOIN pg_namespace cfns ON cfunc.pronamespace = cfns.oid

        -- Comment
        LEFT JOIN pg_description d ON d.objoid = p.oid AND d.objsubid = 0

        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        -- Exclude aggregates that belong to extensions
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend dep
            WHERE dep.objid = p.oid
            AND dep.deptype = 'e'
        )

        ORDER BY n.nspname, p.proname
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    let mut aggregates = Vec::new();

    for row in rows {
        // Build dependencies directly (like triggers.rs)
        let mut depends_on = vec![
            // All objects depend on their schema
            DbObjectId::Schema {
                name: row.schema.clone(),
            },
        ];

        // Depend on state transition function
        if !is_system_schema(&row.state_func_schema) {
            depends_on.push(DbObjectId::Function {
                schema: row.state_func_schema.clone(),
                name: row.state_func.clone(),
                arguments: row.state_func_args.clone(),
            });
        }

        // Depend on final function if present
        if let (Some(ffunc), Some(ffunc_schema), Some(ffunc_args)) = (
            &row.final_func,
            &row.final_func_schema,
            &row.final_func_args,
        ) && !is_system_schema(ffunc_schema)
        {
            depends_on.push(DbObjectId::Function {
                schema: ffunc_schema.to_string(),
                name: ffunc.to_string(),
                arguments: ffunc_args.to_string(),
            });
        }

        // Depend on combine function if present
        if let (Some(cfunc), Some(cfunc_schema), Some(cfunc_args)) = (
            &row.combine_func,
            &row.combine_func_schema,
            &row.combine_func_args,
        ) && !is_system_schema(cfunc_schema)
        {
            depends_on.push(DbObjectId::Function {
                schema: cfunc_schema.to_string(),
                name: cfunc.to_string(),
                arguments: cfunc_args.to_string(),
            });
        }

        // Depend on state type if it's a custom type
        if !is_system_schema(&row.state_type_schema) {
            depends_on.push(DbObjectId::Type {
                schema: row.state_type_schema.clone(),
                name: row.state_type.clone(),
            });
        }

        // Reconstruct the CREATE AGGREGATE definition
        let definition = build_aggregate_definition(
            &row.schema,
            &row.name,
            &row.arguments,
            &row.state_func_schema,
            &row.state_func,
            &row.state_type_schema,
            &row.state_type,
            row.final_func.as_deref(),
            row.final_func_schema.as_deref(),
            row.combine_func.as_deref(),
            row.combine_func_schema.as_deref(),
            row.initial_value.as_deref(),
        );

        aggregates.push(Aggregate {
            schema: row.schema,
            name: row.name,
            arguments: row.arguments,
            state_type: row.state_type,
            state_type_schema: row.state_type_schema,
            state_func: row.state_func,
            state_func_schema: row.state_func_schema,
            final_func: row.final_func,
            final_func_schema: row.final_func_schema,
            combine_func: row.combine_func,
            combine_func_schema: row.combine_func_schema,
            initial_value: row.initial_value,
            definition,
            comment: row.comment,
            depends_on,
        });
    }

    Ok(aggregates)
}

/// Build a CREATE AGGREGATE statement from the component parts
#[allow(clippy::too_many_arguments)]
fn build_aggregate_definition(
    schema: &str,
    name: &str,
    arguments: &str,
    state_func_schema: &str,
    state_func: &str,
    state_type_schema: &str,
    state_type: &str,
    final_func: Option<&str>,
    final_func_schema: Option<&str>,
    combine_func: Option<&str>,
    combine_func_schema: Option<&str>,
    initial_value: Option<&str>,
) -> String {
    let mut parts = Vec::new();

    // SFUNC - state transition function
    let sfunc_qualified = if is_system_schema(state_func_schema) {
        state_func.to_string()
    } else {
        format!("{}.{}", state_func_schema, state_func)
    };
    parts.push(format!("SFUNC = {}", sfunc_qualified));

    // STYPE - state type
    let stype_qualified = if is_system_schema(state_type_schema) {
        state_type.to_string()
    } else {
        format!("{}.{}", state_type_schema, state_type)
    };
    parts.push(format!("STYPE = {}", stype_qualified));

    // FINALFUNC - optional
    if let (Some(ffunc), Some(ffunc_schema)) = (final_func, final_func_schema) {
        let ffunc_qualified = if is_system_schema(ffunc_schema) {
            ffunc.to_string()
        } else {
            format!("{}.{}", ffunc_schema, ffunc)
        };
        parts.push(format!("FINALFUNC = {}", ffunc_qualified));
    }

    // COMBINEFUNC - optional (for parallel aggregation)
    if let (Some(cfunc), Some(cfunc_schema)) = (combine_func, combine_func_schema) {
        let cfunc_qualified = if is_system_schema(cfunc_schema) {
            cfunc.to_string()
        } else {
            format!("{}.{}", cfunc_schema, cfunc)
        };
        parts.push(format!("COMBINEFUNC = {}", cfunc_qualified));
    }

    // INITCOND - optional
    if let Some(initval) = initial_value {
        // Quote the initial value as it's stored as text
        parts.push(format!("INITCOND = '{}'", initval.replace('\'', "''")));
    }

    format!(
        "CREATE AGGREGATE {}.{}({}) (\n    {}\n)",
        schema,
        name,
        arguments,
        parts.join(",\n    ")
    )
}
