//! Raw `pg_operator` rows and their conversion into logical operators.
//!
//! The fetch keeps the OIDs the converter classifies with, plus the outputs of
//! the server-side functions that cannot be computed in Rust: the `format_type`
//! operand strings that *are* an operator's identity, and the identity-argument
//! strings of the routines it names. Everything else — schema-name resolution,
//! extension-ownership and system-schema exclusion, dependency derivation,
//! comment attachment — happens in the converter, where the OIDs die.

use anyhow::{Context, Result};
use sqlx::postgres::PgConnection;
use sqlx::postgres::types::Oid;
use tracing::info;

use super::exclusion::{Converted, Excluded, ExclusionReason};
use super::oid_index::OidIndex;
use super::shared::{SharedCatalog, class};
use crate::catalog::id::DbObjectId;
use crate::catalog::operator::Operator;
use crate::catalog::utils::is_system_schema;

/// Schemas whose operators are never pgmt's to manage.
const SYSTEM_SCHEMAS: [&str; 3] = ["pg_catalog", "information_schema", "pg_toast"];

/// One `pg_operator` row, before names are resolved and OIDs are discarded.
#[derive(Debug, Clone)]
pub struct RawOperator {
    pub oid: Oid,
    pub namespace: Oid,
    pub name: String,

    /// `format_type` of each operand, `None` when the operand is absent.
    pub left_type: Option<String>,
    pub right_type: Option<String>,

    pub function_namespace: Oid,
    pub function_name: String,
    pub function_args: String,

    /// Operand types, unresolved: an array's own OID, not its element type's,
    /// and `0` for an absent operand. The converter classifies them through the
    /// shared type map.
    pub oprleft: Oid,
    pub oprright: Oid,

    pub commutator_namespace: Option<Oid>,
    pub commutator_name: Option<String>,
    pub negator_namespace: Option<Oid>,
    pub negator_name: Option<String>,

    pub restrict_namespace: Option<Oid>,
    pub restrict_name: Option<String>,
    pub restrict_args: Option<String>,
    pub join_namespace: Option<Oid>,
    pub join_name: Option<String>,
    pub join_args: Option<String>,

    pub hashes: bool,
    pub merges: bool,
}

/// Fetch every operator in the database, unresolved and unfiltered.
pub async fn fetch(conn: &mut PgConnection) -> Result<Vec<RawOperator>> {
    info!("Fetching operators...");
    let rows = sqlx::query!(
        r#"
        SELECT
            o.oid AS "oid!",
            o.oprnamespace AS "namespace!",
            o.oprname AS "name!",

            -- Formatted operand types (NULL for an absent operand). These drive both
            -- the `(left, right)` identity string and the LEFTARG/RIGHTARG clauses.
            CASE WHEN o.oprleft = 0 THEN NULL ELSE format_type(o.oprleft, NULL) END AS "left_type?",
            CASE WHEN o.oprright = 0 THEN NULL ELSE format_type(o.oprright, NULL) END AS "right_type?",

            -- Implementing function (oprcode) - always present.
            fn.pronamespace AS "function_namespace!",
            fn.proname AS "function_name!",
            pg_catalog.pg_get_function_identity_arguments(fn.oid) AS "function_args!",

            -- Operand types, unresolved: classification happens in the converter.
            o.oprleft AS "oprleft!",
            o.oprright AS "oprright!",

            -- Commutator / negator operator identities (for rendering only).
            com.oprnamespace AS "commutator_namespace?",
            com.oprname AS "commutator_name?",
            neg.oprnamespace AS "negator_namespace?",
            neg.oprname AS "negator_name?",

            -- Restriction / join selectivity functions.
            rf.pronamespace AS "restrict_namespace?",
            rf.proname AS "restrict_name?",
            pg_catalog.pg_get_function_identity_arguments(rf.oid) AS "restrict_args?",
            jf.pronamespace AS "join_namespace?",
            jf.proname AS "join_name?",
            pg_catalog.pg_get_function_identity_arguments(jf.oid) AS "join_args?",

            o.oprcanhash AS "hashes!",
            o.oprcanmerge AS "merges!"

        FROM pg_operator o

        -- Implementing function
        JOIN pg_proc fn ON o.oprcode = fn.oid

        -- Commutator / negator
        LEFT JOIN pg_operator com ON o.oprcom = com.oid AND o.oprcom != 0
        LEFT JOIN pg_operator neg ON o.oprnegate = neg.oid AND o.oprnegate != 0

        -- Selectivity functions
        LEFT JOIN pg_proc rf ON o.oprrest = rf.oid AND o.oprrest != 0
        LEFT JOIN pg_proc jf ON o.oprjoin = jf.oid AND o.oprjoin != 0

        ORDER BY o.oprnamespace, o.oprname, o.oprleft, o.oprright
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawOperator {
            oid: row.oid,
            namespace: row.namespace,
            name: row.name,
            left_type: row.left_type,
            right_type: row.right_type,
            function_namespace: row.function_namespace,
            function_name: row.function_name,
            function_args: row.function_args,
            oprleft: row.oprleft,
            oprright: row.oprright,
            commutator_namespace: row.commutator_namespace,
            commutator_name: row.commutator_name,
            negator_namespace: row.negator_namespace,
            negator_name: row.negator_name,
            restrict_namespace: row.restrict_namespace,
            restrict_name: row.restrict_name,
            restrict_args: row.restrict_args,
            join_namespace: row.join_namespace,
            join_name: row.join_name,
            join_args: row.join_args,
            hashes: row.hashes,
            merges: row.merges,
        })
        .collect())
}

/// Fetch operators and convert them into the logical catalog, with comments
/// attached through the OID index.
pub async fn load(conn: &mut PgConnection, shared: &SharedCatalog) -> Result<Vec<Operator>> {
    Ok(load_with_exclusions(conn, shared).await?.objects)
}

/// The same load, keeping the named reason for every raw row that did not
/// become an operator.
pub async fn load_with_exclusions(
    conn: &mut PgConnection,
    shared: &SharedCatalog,
) -> Result<Converted<Operator>> {
    let raw = fetch(conn).await?;
    let mut converted = convert(&raw, shared)?;

    // Identity first, then the index, then the OID-addressed state: a comment
    // can only be attached to an object whose identity is already known.
    let index = OidIndex::from_pairs(converted.objects.iter().map(|(oid, op)| (*oid, op.id())))?;
    let comments = index.object_comments(&shared.descriptions, class::PG_OPERATOR);
    for (_, operator) in &mut converted.objects {
        operator.comment = comments.get(&operator.id()).map(|text| text.to_string());
    }

    Ok(converted.map(|(_, operator)| operator))
}

/// Resolve raw operators into logical ones, keeping each operator's OID beside
/// it so OID-addressed state can still be attached before the identities cross
/// the firewall.
///
/// Operators in a system schema and operators owned by an extension are dropped
/// here, each recorded with its named reason.
pub fn convert(raw: &[RawOperator], shared: &SharedCatalog) -> Result<Converted<(Oid, Operator)>> {
    let namespaces = &shared.namespaces;
    let mut converted: Converted<(Oid, Operator)> = Converted::new();

    for row in raw {
        let schema = namespaces
            .name(row.namespace)
            .with_context(|| format!("operator {} has no namespace entry", row.name))?;

        if SYSTEM_SCHEMAS.contains(&schema) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "operator",
                schema,
                &row.name,
                ExclusionReason::SystemSchema,
            ));
            continue;
        }
        if let Some(extension) = shared.extensions.owner(class::PG_OPERATOR, row.oid) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "operator",
                schema,
                &row.name,
                ExclusionReason::ExtensionOwned {
                    extension: extension.to_string(),
                },
            ));
            continue;
        }

        let arguments = format!(
            "{}, {}",
            row.left_type.as_deref().unwrap_or("NONE"),
            row.right_type.as_deref().unwrap_or("NONE")
        );

        let function_schema = namespaces
            .name(row.function_namespace)
            .with_context(|| format!("operator {}.{} has no function schema", schema, row.name))?;
        let commutator_schema = row.commutator_namespace.and_then(|ns| namespaces.name(ns));
        let negator_schema = row.negator_namespace.and_then(|ns| namespaces.name(ns));
        let restrict_schema = row.restrict_namespace.and_then(|ns| namespaces.name(ns));
        let join_schema = row.join_namespace.and_then(|ns| namespaces.name(ns));

        // Dependencies: schema, the implementing function, the operand types, and
        // any user-defined selectivity functions. Commutator/negator are
        // intentionally excluded (they reference each other and resolve via shells).
        let mut depends_on = vec![DbObjectId::Schema {
            name: schema.to_string(),
        }];

        if !is_system_schema(function_schema) {
            depends_on.push(DbObjectId::Function {
                schema: function_schema.to_string(),
                name: row.function_name.clone(),
                arguments: row.function_args.clone(),
            });
        }

        // An absent operand is `0`, which resolves to nothing.
        for operand in [row.oprleft, row.oprright] {
            if let Some(dep) = shared.resolve_type(operand).and_then(|t| t.dependency()) {
                depends_on.push(dep);
            }
        }

        for (routine_schema, routine_name, routine_args) in [
            (restrict_schema, &row.restrict_name, &row.restrict_args),
            (join_schema, &row.join_name, &row.join_args),
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

        // De-duplicate dependencies while preserving order.
        let mut seen = std::collections::HashSet::new();
        depends_on.retain(|d| seen.insert(d.clone()));

        let definition = build_operator_definition(
            schema,
            &row.name,
            function_schema,
            &row.function_name,
            row.left_type.as_deref(),
            row.right_type.as_deref(),
            commutator_schema,
            row.commutator_name.as_deref(),
            negator_schema,
            row.negator_name.as_deref(),
            restrict_schema,
            row.restrict_name.as_deref(),
            join_schema,
            row.join_name.as_deref(),
            row.hashes,
            row.merges,
        );

        converted.objects.push((
            row.oid,
            Operator {
                schema: schema.to_string(),
                name: row.name.clone(),
                arguments,
                definition,
                comment: None,
                depends_on,
            },
        ));
    }

    // The raw fetch orders by namespace OID; ordering by schema name is what
    // callers see, and a stable sort keeps the rest of the raw ordering
    // (name, then operand OIDs) intact.
    converted
        .objects
        .sort_by(|(_, a), (_, b)| a.schema.cmp(&b.schema));

    Ok(converted)
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
