//! Raw `pg_aggregate` rows and their conversion into logical aggregates.
//!
//! The fetch keeps the OIDs the converter classifies with, plus the outputs of
//! the server-side functions that cannot be computed in Rust: `format_type` for
//! the rendered state type, and `pg_get_function_identity_arguments` for the
//! aggregate's own signature — which *is* its identity — and for the routines it
//! names. Those render type names relative to the connection's `search_path`,
//! which is why the fetch and the shared state must run on one connection.
//! Everything else — schema-name resolution, extension-ownership and
//! system-schema exclusion, state-type classification, dependency derivation,
//! comment attachment — happens in the converter, where the OIDs die.

use anyhow::{Context, Result};
use sqlx::postgres::PgConnection;
use sqlx::postgres::types::Oid;
use tracing::info;

use super::exclusion::{Converted, Excluded, ExclusionReason};
use super::oid_index::OidIndex;
use super::shared::{SharedCatalog, class};
use crate::catalog::aggregate::Aggregate;
use crate::catalog::id::DbObjectId;
use crate::catalog::utils::is_system_schema;

/// Schemas whose aggregates are never pgmt's to manage.
const SYSTEM_SCHEMAS: [&str; 3] = ["pg_catalog", "information_schema", "pg_toast"];

/// One `pg_aggregate` row joined to the `pg_proc` row that names it, before
/// names are resolved and OIDs are discarded.
#[derive(Debug, Clone)]
pub struct RawAggregate {
    /// `aggfnoid`: the `pg_proc` entry an aggregate is addressed by, and the OID
    /// its comment and its extension membership are recorded under.
    pub oid: Oid,
    pub namespace: Oid,
    pub name: String,
    /// `pg_get_function_identity_arguments` — the signature an aggregate is
    /// identified by, rendered relative to the connection's `search_path`.
    pub arguments: String,

    /// `aggtranstype`, unresolved: an array's own OID, not its element type's.
    pub state_type_oid: Oid,
    /// `format_type(aggtranstype, NULL)` — carries array brackets, which no
    /// Rust-side reconstruction can recover.
    pub state_type_formatted: String,

    pub state_func_namespace: Oid,
    pub state_func_name: String,
    pub state_func_args: String,

    pub final_func_namespace: Option<Oid>,
    pub final_func_name: Option<String>,
    pub final_func_args: Option<String>,

    pub combine_func_namespace: Option<Oid>,
    pub combine_func_name: Option<String>,
    pub combine_func_args: Option<String>,

    /// `agginitval`, the INITCOND, stored as text.
    pub initial_value: Option<String>,
}

/// Fetch every aggregate in the database, unresolved and unfiltered.
pub async fn fetch(conn: &mut PgConnection) -> Result<Vec<RawAggregate>> {
    info!("Fetching aggregates...");
    let rows = sqlx::query!(
        r#"
        SELECT
            p.oid AS "oid!",
            p.pronamespace AS "namespace!",
            p.proname AS "name!",
            pg_catalog.pg_get_function_identity_arguments(p.oid) AS "arguments!",

            -- State type (STYPE), unresolved: classification happens in the converter.
            agg.aggtranstype AS "state_type_oid!",
            pg_catalog.format_type(agg.aggtranstype, NULL) AS "state_type_formatted!",

            -- State transition function (SFUNC) - always present.
            tfunc.pronamespace AS "state_func_namespace!",
            tfunc.proname AS "state_func_name!",
            pg_catalog.pg_get_function_identity_arguments(tfunc.oid) AS "state_func_args!",

            -- Final function (FINALFUNC) - optional.
            ffunc.pronamespace AS "final_func_namespace?",
            ffunc.proname AS "final_func_name?",
            pg_catalog.pg_get_function_identity_arguments(ffunc.oid) AS "final_func_args?",

            -- Combine function for parallel aggregation (COMBINEFUNC) - optional.
            cfunc.pronamespace AS "combine_func_namespace?",
            cfunc.proname AS "combine_func_name?",
            pg_catalog.pg_get_function_identity_arguments(cfunc.oid) AS "combine_func_args?",

            agg.agginitval AS "initial_value?"

        FROM pg_aggregate agg
        JOIN pg_proc p ON agg.aggfnoid = p.oid
        JOIN pg_proc tfunc ON agg.aggtransfn = tfunc.oid
        LEFT JOIN pg_proc ffunc ON agg.aggfinalfn = ffunc.oid AND agg.aggfinalfn != 0
        LEFT JOIN pg_proc cfunc ON agg.aggcombinefn = cfunc.oid AND agg.aggcombinefn != 0
        ORDER BY p.oid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawAggregate {
            oid: row.oid,
            namespace: row.namespace,
            name: row.name,
            arguments: row.arguments,
            state_type_oid: row.state_type_oid,
            state_type_formatted: row.state_type_formatted,
            state_func_namespace: row.state_func_namespace,
            state_func_name: row.state_func_name,
            state_func_args: row.state_func_args,
            final_func_namespace: row.final_func_namespace,
            final_func_name: row.final_func_name,
            final_func_args: row.final_func_args,
            combine_func_namespace: row.combine_func_namespace,
            combine_func_name: row.combine_func_name,
            combine_func_args: row.combine_func_args,
            initial_value: row.initial_value,
        })
        .collect())
}

/// Fetch aggregates and convert them into the logical catalog, with comments
/// attached through the OID index.
pub async fn load(conn: &mut PgConnection, shared: &SharedCatalog) -> Result<Vec<Aggregate>> {
    Ok(load_with_exclusions(conn, shared).await?.objects)
}

/// The same load, keeping the named reason for every raw row that did not
/// become an aggregate.
pub async fn load_with_exclusions(
    conn: &mut PgConnection,
    shared: &SharedCatalog,
) -> Result<Converted<Aggregate>> {
    let raw = fetch(conn).await?;
    let mut converted = convert(&raw, shared)?;

    // Identity first, then the index, then the OID-addressed state: a comment
    // can only be attached to an object whose identity is already known. An
    // aggregate's comment is keyed by the `pg_proc` entry that names it.
    let index = OidIndex::from_pairs(
        converted
            .objects
            .iter()
            .map(|(oid, aggregate)| (*oid, aggregate.id())),
    )?;
    let comments = index.object_comments(&shared.descriptions, class::PG_PROC);
    for (_, aggregate) in &mut converted.objects {
        aggregate.comment = comments.get(&aggregate.id()).map(|text| text.to_string());
    }

    Ok(converted.map(|(_, aggregate)| aggregate))
}

/// Resolve raw aggregates into logical ones, keeping each aggregate's OID beside
/// it so OID-addressed state can still be attached before the identities cross
/// the firewall.
///
/// Aggregates in a system schema and aggregates owned by an extension are
/// dropped here, each recorded with its named reason.
pub fn convert(
    raw: &[RawAggregate],
    shared: &SharedCatalog,
) -> Result<Converted<(Oid, Aggregate)>> {
    let namespaces = &shared.namespaces;
    let mut converted: Converted<(Oid, Aggregate)> = Converted::new();

    for row in raw {
        let schema = namespaces
            .name(row.namespace)
            .with_context(|| format!("aggregate {} has no namespace entry", row.name))?;

        if SYSTEM_SCHEMAS.contains(&schema) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "aggregate",
                schema,
                &row.name,
                ExclusionReason::SystemSchema,
            ));
            continue;
        }
        if let Some(extension) = shared.extensions.owner(class::PG_PROC, row.oid) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "aggregate",
                schema,
                &row.name,
                ExclusionReason::ExtensionOwned {
                    extension: extension.to_string(),
                },
            ));
            continue;
        }

        let state_func_schema = namespaces
            .name(row.state_func_namespace)
            .with_context(|| format!("aggregate {}.{} has no SFUNC schema", schema, row.name))?;
        let final_func_schema = row.final_func_namespace.and_then(|ns| namespaces.name(ns));
        let combine_func_schema = row
            .combine_func_namespace
            .and_then(|ns| namespaces.name(ns));

        // Dependencies: the schema, the routines the aggregate is built from, and
        // the state type.
        let mut depends_on = vec![DbObjectId::Schema {
            name: schema.to_string(),
        }];

        if !is_system_schema(state_func_schema) {
            depends_on.push(DbObjectId::Function {
                schema: state_func_schema.to_string(),
                name: row.state_func_name.clone(),
                arguments: row.state_func_args.clone(),
            });
        }
        for (routine_schema, routine_name, routine_args) in [
            (
                final_func_schema,
                &row.final_func_name,
                &row.final_func_args,
            ),
            (
                combine_func_schema,
                &row.combine_func_name,
                &row.combine_func_args,
            ),
        ] {
            if let (Some(routine_schema), Some(name), Some(args)) =
                (routine_schema, routine_name, routine_args)
                && !is_system_schema(routine_schema)
            {
                depends_on.push(DbObjectId::Function {
                    schema: routine_schema.to_string(),
                    name: name.clone(),
                    arguments: args.clone(),
                });
            }
        }

        let state_type = shared.resolve_type(row.state_type_oid);
        if let Some(dep) = state_type.as_ref().and_then(|t| t.dependency()) {
            depends_on.push(dep);
        }
        // The state type's name and schema are the element type's for an array
        // state type, which is what dependency tracking compares against.
        let state_type_name = state_type
            .as_ref()
            .map(|t| t.name.to_string())
            .unwrap_or_default();
        let state_type_schema = state_type
            .as_ref()
            .and_then(|t| t.schema)
            .unwrap_or_default()
            .to_string();

        let definition = build_aggregate_definition(
            schema,
            &row.name,
            &row.arguments,
            state_func_schema,
            &row.state_func_name,
            &state_type_schema,
            &row.state_type_formatted,
            row.final_func_name.as_deref(),
            final_func_schema,
            row.combine_func_name.as_deref(),
            combine_func_schema,
            row.initial_value.as_deref(),
        );

        converted.objects.push((
            row.oid,
            Aggregate {
                schema: schema.to_string(),
                name: row.name.clone(),
                arguments: row.arguments.clone(),
                state_type: state_type_name,
                state_type_schema,
                state_type_formatted: row.state_type_formatted.clone(),
                state_func: row.state_func_name.clone(),
                state_func_schema: state_func_schema.to_string(),
                final_func: row.final_func_name.clone(),
                final_func_schema: final_func_schema.map(String::from),
                combine_func: row.combine_func_name.clone(),
                combine_func_schema: combine_func_schema.map(String::from),
                initial_value: row.initial_value.clone(),
                definition,
                comment: None,
                depends_on,
            },
        ));
    }

    // The raw fetch orders by OID; ordering by name is what callers see, and a
    // stable sort keeps overloads of one name in creation order.
    converted
        .objects
        .sort_by(|(_, a), (_, b)| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));

    Ok(converted)
}

/// Qualify a routine name with its schema, leaving system-schema objects
/// unqualified.
fn qualify(schema: &str, name: &str) -> String {
    if is_system_schema(schema) {
        name.to_string()
    } else {
        format!("{}.{}", schema, name)
    }
}

/// Reconstruct a `CREATE AGGREGATE` statement (no trailing `;`).
#[allow(clippy::too_many_arguments)]
fn build_aggregate_definition(
    schema: &str,
    name: &str,
    arguments: &str,
    state_func_schema: &str,
    state_func: &str,
    state_type_schema: &str,
    state_type_formatted: &str,
    final_func: Option<&str>,
    final_func_schema: Option<&str>,
    combine_func: Option<&str>,
    combine_func_schema: Option<&str>,
    initial_value: Option<&str>,
) -> String {
    let mut parts = vec![format!(
        "SFUNC = {}",
        qualify(state_func_schema, state_func)
    )];

    // STYPE: a user-defined state type is schema-qualified, with the array
    // brackets `format_type` rendered reattached after the qualification.
    let stype = if is_system_schema(state_type_schema) {
        state_type_formatted.to_string()
    } else {
        let (base_type, array_suffix) = match state_type_formatted.strip_suffix("[]") {
            Some(_) => {
                let suffix_start = state_type_formatted
                    .rfind('[')
                    .unwrap_or(state_type_formatted.len());
                (
                    &state_type_formatted[..suffix_start],
                    &state_type_formatted[suffix_start..],
                )
            }
            None => (state_type_formatted, ""),
        };
        let unqualified = base_type.split('.').next_back().unwrap_or(base_type);
        format!("{}.{}{}", state_type_schema, unqualified, array_suffix)
    };
    parts.push(format!("STYPE = {}", stype));

    if let (Some(func), Some(func_schema)) = (final_func, final_func_schema) {
        parts.push(format!("FINALFUNC = {}", qualify(func_schema, func)));
    }
    if let (Some(func), Some(func_schema)) = (combine_func, combine_func_schema) {
        parts.push(format!("COMBINEFUNC = {}", qualify(func_schema, func)));
    }
    if let Some(initval) = initial_value {
        // INITCOND is stored as text and rendered as a quoted literal.
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
