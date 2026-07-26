//! Raw `pg_cast` rows and their conversion into logical casts.
//!
//! The fetch keeps the OIDs the converter classifies with, plus the outputs of
//! the server-side functions that cannot be computed in Rust: the `format_type`
//! names of the source and target types, which *are* a cast's identity, and the
//! identity-argument string of the implementing function. Everything else —
//! schema-name resolution, extension-ownership and built-in exclusion, type
//! classification, dependency derivation, comment attachment — happens in the
//! converter, where the OIDs die.

use anyhow::Result;
use sqlx::postgres::PgConnection;
use sqlx::postgres::types::Oid;
use tracing::info;

use super::dedup_preserving_order;
use super::exclusion::{Converted, Excluded, ExclusionReason, is_system_schema};
use super::oid_index::OidIndex;
use super::shared::{SharedCatalog, class};
use crate::catalog::cast::Cast;
use crate::catalog::id::DbObjectId;

/// One `pg_cast` row, before names are resolved and OIDs are discarded.
#[derive(Debug, Clone)]
pub struct RawCast {
    pub oid: Oid,
    /// `format_type` of each side. A cast is not schema-scoped: this pair is its
    /// identity.
    pub source: String,
    pub target: String,
    /// Source and target types, unresolved: an array's own OID, not its element
    /// type's. The converter classifies them through the shared type map.
    pub source_oid: Oid,
    pub target_oid: Oid,
    /// `pg_cast.castcontext`: 'e' explicit, 'a' assignment, 'i' implicit.
    pub context: String,
    /// `pg_cast.castmethod`: 'f' WITH FUNCTION, 'i' WITH INOUT, 'b' WITHOUT
    /// FUNCTION.
    pub method: String,

    /// Implementing function, present only for `castmethod = 'f'`.
    pub function_namespace: Option<Oid>,
    pub function_name: Option<String>,
    pub function_args: Option<String>,
}

/// Fetch every cast in the database, unresolved and unfiltered.
pub async fn fetch(conn: &mut PgConnection) -> Result<Vec<RawCast>> {
    info!("Fetching casts...");
    let rows = sqlx::query!(
        r#"
        SELECT
            c.oid AS "oid!",
            pg_catalog.format_type(c.castsource, NULL) AS "source!",
            pg_catalog.format_type(c.casttarget, NULL) AS "target!",
            c.castsource AS "source_oid!",
            c.casttarget AS "target_oid!",
            c.castcontext::text AS "context!",
            c.castmethod::text AS "method!",

            fn.pronamespace AS "function_namespace?",
            fn.proname AS "function_name?",
            pg_catalog.pg_get_function_identity_arguments(fn.oid) AS "function_args?"

        FROM pg_cast c
        LEFT JOIN pg_proc fn ON c.castfunc = fn.oid AND c.castfunc != 0
        ORDER BY c.oid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawCast {
            oid: row.oid,
            source: row.source,
            target: row.target,
            source_oid: row.source_oid,
            target_oid: row.target_oid,
            context: row.context,
            method: row.method,
            function_namespace: row.function_namespace,
            function_name: row.function_name,
            function_args: row.function_args,
        })
        .collect())
}

/// Fetch casts and convert them into the logical catalog, with comments attached
/// through the OID index.
pub async fn load(conn: &mut PgConnection, shared: &SharedCatalog) -> Result<Vec<Cast>> {
    Ok(load_with_exclusions(conn, shared).await?.objects)
}

/// The same load, keeping the named reason for every raw row that did not become
/// a cast.
pub async fn load_with_exclusions(
    conn: &mut PgConnection,
    shared: &SharedCatalog,
) -> Result<Converted<Cast>> {
    let raw = fetch(conn).await?;
    let mut converted = convert(&raw, shared)?;

    // Identity first, then the index, then the OID-addressed state: a comment
    // can only be attached to an object whose identity is already known.
    let index = OidIndex::from_pairs(
        class::PG_CAST,
        converted
            .objects
            .iter()
            .map(|(oid, cast)| (*oid, cast.id())),
    )?;
    let comments = index.object_comments(&shared.descriptions, class::PG_CAST);
    for (_, cast) in &mut converted.objects {
        cast.comment = comments.get(&cast.id()).map(|text| text.to_string());
    }

    Ok(converted.map(|(_, cast)| cast))
}

/// Resolve raw casts into logical ones, keeping each cast's OID beside it so
/// OID-addressed state can still be attached before the identities cross the
/// firewall.
///
/// Built-in casts and casts owned by an extension are dropped here, each
/// recorded with its named reason.
pub fn convert(raw: &[RawCast], shared: &SharedCatalog) -> Result<Converted<(Oid, Cast)>> {
    let mut converted: Converted<(Oid, Cast)> = Converted::new();

    for row in raw {
        let source = shared.resolve_type(row.source_oid);
        let target = shared.resolve_type(row.target_oid);

        // PostgreSQL requires ownership of the source or the target type to
        // create a cast, so every user cast has at least one side outside the
        // system schemas; a cast with both sides inside them is the server's.
        let user_side = [&source, &target]
            .into_iter()
            .flatten()
            .find(|side| side.schema.is_some_and(|schema| !is_system_schema(schema)));
        let Some(user_side) = user_side else {
            converted.excluded.push(Excluded::new(
                row.oid,
                "cast",
                source.as_ref().and_then(|s| s.schema).unwrap_or_default(),
                &cast_name(row),
                ExclusionReason::SystemSchema,
            ));
            continue;
        };
        if let Some(extension) = shared.extensions.owner(class::PG_CAST, row.oid) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "cast",
                user_side.schema.unwrap_or_default(),
                &cast_name(row),
                ExclusionReason::ExtensionOwned {
                    extension: extension.to_string(),
                },
            ));
            continue;
        }

        // Dependencies: the source and target types and (for a function cast) the
        // implementing function. A cast has no schema dependency of its own — the
        // types carry their schemas.
        let mut depends_on = Vec::new();
        for side in [&source, &target] {
            if let Some(dep) = side.as_ref().and_then(|t| t.dependency()) {
                depends_on.push(dep);
            }
        }

        let function_schema = row
            .function_namespace
            .and_then(|ns| shared.namespaces.name(ns));
        let function = match (function_schema, &row.function_name, &row.function_args) {
            (Some(schema), Some(name), Some(args)) => {
                if !is_system_schema(schema) {
                    depends_on.push(DbObjectId::Function {
                        schema: schema.to_string(),
                        name: name.clone(),
                        arguments: args.clone(),
                    });
                }
                Some((schema, name.as_str(), args.as_str()))
            }
            _ => None,
        };

        dedup_preserving_order(&mut depends_on);

        converted.objects.push((
            row.oid,
            Cast {
                source: row.source.clone(),
                target: row.target.clone(),
                definition: build_cast_definition(
                    &row.source,
                    &row.target,
                    &row.method,
                    &row.context,
                    function,
                ),
                comment: None,
                depends_on,
            },
        ));
    }

    // The raw fetch orders by OID; ordering by the identity pair is what callers
    // see.
    converted
        .objects
        .sort_by(|(_, a), (_, b)| (&a.source, &a.target).cmp(&(&b.source, &b.target)));

    Ok(converted)
}

/// How an excluded cast is named: it has no schema-qualified name of its own,
/// only the type pair it converts between.
fn cast_name(row: &RawCast) -> String {
    format!("{} AS {}", row.source, row.target)
}

/// Qualify a function name with its schema, leaving system-schema objects
/// unqualified (matching how operators and aggregates render function
/// references).
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
