//! Raw `pg_proc` rows and their conversion into logical functions and
//! procedures.
//!
//! The fetches keep the OIDs the converter classifies with, plus the outputs of
//! the server-side functions that cannot be computed in Rust:
//! `pg_get_functiondef` for the body, `pg_get_function_result` for the rendered
//! return type, `format_type` for a parameter's rendered type, and
//! `pg_get_function_identity_arguments` for the signature that *is* a function's
//! identity. Those last ones render type names relative to the connection's
//! `search_path`, which is why the fetch and the shared state must run on one
//! connection. Everything else — schema-name resolution, extension-ownership and
//! system-schema exclusion, type classification, dependency derivation, comment
//! attachment — happens in the converter, where the OIDs die.

use anyhow::{Context, Result, anyhow};
use sqlx::postgres::PgConnection;
use sqlx::postgres::types::Oid;
use std::collections::BTreeMap;
use tracing::info;

use super::exclusion::{Converted, Excluded, ExclusionReason, is_system_schema};
use super::oid_index::OidIndex;
use super::shared::{SharedCatalog, class};
use crate::catalog::function::{Function, FunctionKind, FunctionParam};
use crate::catalog::id::DbObjectId;
use crate::render::quote_ident;

/// One `pg_proc` row of `prokind` function or procedure, before names are
/// resolved and OIDs are discarded.
#[derive(Debug, Clone)]
pub struct RawFunction {
    pub oid: Oid,
    pub namespace: Oid,
    pub name: String,
    /// `pg_proc.prokind`: 'f' function, 'p' procedure, 'w' window function.
    pub prokind: String,
    /// `pg_get_function_identity_arguments` — the signature a function is
    /// identified by, rendered relative to the connection's `search_path`.
    pub arguments: String,
    /// `pg_get_functiondef` — the complete `CREATE FUNCTION` statement.
    pub definition: String,
    /// `pg_get_function_result`, absent for a procedure.
    pub return_type: Option<String>,
    /// `prorettype`, unresolved: an array's own OID, not its element type's.
    pub return_type_oid: Oid,
    pub language: String,
    /// `pg_proc.provolatile`: 'i' immutable, 's' stable, 'v' volatile.
    pub volatility: String,
    pub is_strict: bool,
    pub security_definer: bool,
    /// `pg_proc.pronargs`: how many input parameters the routine declares.
    pub num_args: i16,
}

/// One input parameter of a routine, in declaration order.
#[derive(Debug, Clone)]
pub struct RawFunctionParameter {
    pub function_oid: Oid,
    /// 1-based position in `proargtypes`.
    pub ordinal: i32,
    /// The parameter's type, unresolved: an array's own OID, not its element
    /// type's.
    pub type_oid: Oid,
    /// `format_type(atttypid, NULL)` — carries array brackets, which no
    /// Rust-side reconstruction can recover.
    pub formatted_type: String,
    pub name: Option<String>,
    /// `pg_proc.proargmodes` entry: 'i' IN, 'o' OUT, 'b' INOUT, 'v' VARIADIC,
    /// 't' TABLE.
    pub mode: Option<String>,
}

/// One `pg_depend` edge from a routine to an object it references.
///
/// PostgreSQL records these only for what a function's *signature* names and,
/// for `BEGIN ATOMIC` bodies (PostgreSQL 14+), for what the body reads —
/// including the individual columns. A traditional `$$`-quoted body records
/// nothing, which is what `-- require:` comments in schema files are for.
///
/// The referenced object is identified by the catalog table it lives in, and
/// only the columns of that catalog are populated.
#[derive(Debug, Clone)]
pub struct RawFunctionDependency {
    pub function_oid: Oid,
    /// Name of the `pg_catalog` table the reference addresses (`pg_class`,
    /// `pg_type`, `pg_proc`).
    pub ref_class: String,
    pub ref_oid: Oid,

    /// Referenced relation (`pg_class`).
    pub relation_kind: Option<String>,
    pub relation_namespace: Option<Oid>,
    pub relation_name: Option<String>,
    /// The referenced column's name, when the edge addresses one
    /// (`refobjsubid > 0`).
    pub relation_column: Option<String>,

    /// Referenced routine (`pg_proc`).
    pub routine_namespace: Option<Oid>,
    pub routine_name: Option<String>,
    pub routine_args: Option<String>,
}

/// Everything the function converter reads out of `pg_catalog`.
#[derive(Debug, Clone, Default)]
pub struct RawFunctions {
    pub functions: Vec<RawFunction>,
    pub parameters: Vec<RawFunctionParameter>,
    pub dependencies: Vec<RawFunctionDependency>,
}

/// Fetch every function and procedure in the database, with their parameters
/// and dependency edges, unresolved and unfiltered.
pub async fn fetch(conn: &mut PgConnection) -> Result<RawFunctions> {
    info!("Fetching functions...");
    let functions = fetch_functions(&mut *conn).await?;
    info!("Fetching function parameters...");
    let parameters = fetch_parameters(&mut *conn).await?;
    info!("Fetching function dependencies...");
    let dependencies = fetch_dependencies(&mut *conn).await?;

    Ok(RawFunctions {
        functions,
        parameters,
        dependencies,
    })
}

/// Fetch functions and convert them into the logical catalog, with comments
/// attached through the OID index.
pub async fn load(conn: &mut PgConnection, shared: &SharedCatalog) -> Result<Vec<Function>> {
    Ok(load_with_exclusions(conn, shared).await?.objects)
}

/// The same load, keeping the named reason for every raw row that did not
/// become a function.
pub async fn load_with_exclusions(
    conn: &mut PgConnection,
    shared: &SharedCatalog,
) -> Result<Converted<Function>> {
    let raw = fetch(conn).await?;
    let mut converted = convert(&raw, shared)?;

    // Identity first, then the index, then the OID-addressed state: a comment
    // can only be attached to an object whose identity is already known.
    let index = OidIndex::from_pairs(
        class::PG_PROC,
        converted
            .objects
            .iter()
            .map(|(oid, function)| (*oid, function.id())),
    )?;
    let comments = index.object_comments(&shared.descriptions, class::PG_PROC);
    for (_, function) in &mut converted.objects {
        function.comment = comments.get(&function.id()).map(|text| text.to_string());
    }

    Ok(converted.map(|(_, function)| function))
}

/// Resolve raw routines into logical ones, keeping each routine's OID beside it
/// so OID-addressed state can still be attached before the identities cross the
/// firewall.
///
/// Routines in a system schema and routines owned by an extension are dropped
/// here, each recorded with its named reason, along with the parameters and
/// dependency edges belonging to them.
pub fn convert(raw: &RawFunctions, shared: &SharedCatalog) -> Result<Converted<(Oid, Function)>> {
    let namespaces = &shared.namespaces;
    let parameters = parameters_by_function(raw, shared);

    // The routines that survive filtering, by OID, so every dependency row can
    // be routed to its routine (or dropped with it).
    let mut kept: BTreeMap<u32, usize> = BTreeMap::new();
    let mut converted: Converted<(Oid, Function)> = Converted::new();

    for row in &raw.functions {
        let schema = namespaces
            .name(row.namespace)
            .with_context(|| format!("function {} has no namespace entry", row.name))?;

        if is_system_schema(schema) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "function",
                schema,
                &row.name,
                ExclusionReason::SystemSchema,
            ));
            continue;
        }
        if let Some(extension) = shared.extensions.owner(class::PG_PROC, row.oid) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "function",
                schema,
                &row.name,
                ExclusionReason::ExtensionOwned {
                    extension: extension.to_string(),
                },
            ));
            continue;
        }

        // Aggregates never reach here — they are their own object kind, fetched
        // from `pg_aggregate`. A window function is a function.
        let kind = match row.prokind.as_str() {
            "p" => FunctionKind::Procedure,
            _ => FunctionKind::Function,
        };

        let converted_parameters = parameters.get(&row.oid.0);
        let declared: Vec<FunctionParam> = converted_parameters
            .map(|params| params.iter().map(|p| p.param.clone()).collect())
            .unwrap_or_default();

        if declared.len() as i16 != row.num_args {
            return Err(anyhow!(
                "Function {}.{} has OUT/INOUT parameters which are not yet supported. \
                 Found {} IN parameters but function has {} total parameters.",
                schema,
                row.name,
                declared.len(),
                row.num_args
            ));
        }

        // The signature's own dependencies: the schema, each parameter type and
        // the return type. What the body references is added from `pg_depend`
        // below.
        let mut depends_on = vec![DbObjectId::Schema {
            name: schema.to_string(),
        }];
        if let Some(params) = converted_parameters {
            depends_on.extend(params.iter().filter_map(|p| p.dependency.clone()));
        }
        if let Some(dep) = shared
            .resolve_type(row.return_type_oid)
            .and_then(|t| t.dependency())
        {
            depends_on.push(dep);
        }

        kept.insert(row.oid.0, converted.objects.len());
        converted.objects.push((
            row.oid,
            Function {
                schema: schema.to_string(),
                name: row.name.clone(),
                kind: kind.clone(),
                arguments: row.arguments.clone(),
                parameters: declared,
                return_type: match kind {
                    FunctionKind::Procedure => None,
                    _ => row.return_type.clone(),
                },
                language: row.language.clone(),
                definition: row.definition.clone(),
                volatility: match row.volatility.as_str() {
                    "i" => "IMMUTABLE".to_string(),
                    "s" => "STABLE".to_string(),
                    _ => "VOLATILE".to_string(),
                },
                is_strict: row.is_strict,
                security_type: if row.security_definer {
                    "DEFINER".to_string()
                } else {
                    "INVOKER".to_string()
                },
                comment: None,
                depends_on,
            },
        ));
    }

    for row in &raw.dependencies {
        let Some(&idx) = kept.get(&row.function_oid.0) else {
            continue;
        };
        let (_, function) = &mut converted.objects[idx];
        let function_id = function.id();

        for dep in dependencies(row, shared) {
            if dep != function_id && !function.depends_on.contains(&dep) {
                function.depends_on.push(dep);
            }
        }
    }

    // The raw fetches order by OID; ordering by name is what callers see, and a
    // stable sort keeps overloads of one name in creation order.
    converted
        .objects
        .sort_by(|(_, a), (_, b)| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));

    Ok(converted)
}

/// A parameter, resolved: what the logical struct carries and the dependency the
/// parameter's type creates.
struct ConvertedParameter {
    param: FunctionParam,
    dependency: Option<DbObjectId>,
}

/// The parameters of each routine, in declaration order, keyed by the routine's
/// OID.
fn parameters_by_function(
    raw: &RawFunctions,
    shared: &SharedCatalog,
) -> BTreeMap<u32, Vec<ConvertedParameter>> {
    let mut by_function: BTreeMap<u32, Vec<ConvertedParameter>> = BTreeMap::new();

    for row in &raw.parameters {
        let resolved = shared.resolve_type(row.type_oid);

        // A user-defined type is rendered schema-qualified; a built-in or an
        // extension-provided one keeps the server's rendering (extension types
        // are resolved through the extension's schema, not qualified by pgmt).
        let data_type = match &resolved {
            Some(t) if t.extension.is_none() && t.schema.is_some_and(|s| !is_system_schema(s)) => {
                let base = format!(
                    "{}.{}",
                    quote_ident(t.schema.unwrap_or_default()),
                    quote_ident(t.name)
                );
                if t.is_array {
                    format!("{}[]", base)
                } else {
                    base
                }
            }
            _ => row.formatted_type.clone(),
        };

        by_function
            .entry(row.function_oid.0)
            .or_default()
            .push(ConvertedParameter {
                param: FunctionParam {
                    name: row.name.clone().filter(|name| !name.is_empty()),
                    data_type,
                    mode: row.mode.as_deref().map(parameter_mode),
                },
                dependency: resolved.as_ref().and_then(|t| t.dependency()),
            });
    }

    by_function
}

/// The `pg_proc.proargmodes` code, spelled the way `CREATE FUNCTION` spells it.
fn parameter_mode(mode: &str) -> String {
    match mode {
        "o" => "OUT".to_string(),
        "b" => "INOUT".to_string(),
        "v" => "VARIADIC".to_string(),
        "t" => "TABLE".to_string(),
        _ => "IN".to_string(),
    }
}

/// The objects one `pg_depend` edge of a routine depends on: a column-level
/// edge yields both the column and the relation it belongs to, every other kind
/// of edge at most one object.
///
/// A reference pgmt does not track — a built-in type or routine, a relation kind
/// that is neither table, view nor sequence — yields nothing.
fn dependencies(row: &RawFunctionDependency, shared: &SharedCatalog) -> Vec<DbObjectId> {
    let namespaces = &shared.namespaces;
    let mut deps = Vec::new();

    if row.ref_class == class::PG_CLASS {
        let (Some(relkind), Some(schema), Some(name)) = (
            row.relation_kind.as_deref(),
            row.relation_namespace.and_then(|ns| namespaces.name(ns)),
            row.relation_name.clone(),
        ) else {
            return deps;
        };
        if is_system_schema(schema) {
            return deps;
        }

        // A `BEGIN ATOMIC` body records what it reads at column granularity,
        // which is what lets a dropped column cascade into the routine. The
        // relation itself is depended on too, for ordering.
        if let Some(column) = &row.relation_column
            && matches!(relkind, "r" | "p" | "v" | "m")
        {
            deps.push(DbObjectId::Column {
                schema: schema.to_string(),
                table: name.clone(),
                column: column.clone(),
            });
        }

        match relkind {
            "r" | "p" => deps.push(DbObjectId::Table {
                schema: schema.to_string(),
                name,
            }),
            "v" | "m" => deps.push(DbObjectId::View {
                schema: schema.to_string(),
                name,
            }),
            "S" => deps.push(DbObjectId::Sequence {
                schema: schema.to_string(),
                name,
            }),
            _ => {}
        }
        return deps;
    }

    if row.ref_class == class::PG_TYPE {
        if let Some(dep) = shared
            .resolve_type(row.ref_oid)
            .and_then(|t| t.dependency())
        {
            deps.push(dep);
        }
        return deps;
    }

    if row.ref_class == class::PG_PROC
        && let (Some(namespace), Some(name), Some(args)) =
            (row.routine_namespace, &row.routine_name, &row.routine_args)
        && let Some(schema) = namespaces.name(namespace)
        && !is_system_schema(schema)
    {
        deps.push(DbObjectId::Function {
            schema: schema.to_string(),
            name: name.clone(),
            arguments: args.clone(),
        });
    }

    deps
}

async fn fetch_functions(conn: &mut PgConnection) -> Result<Vec<RawFunction>> {
    // Aggregates are their own object kind, reconstructed from `pg_aggregate`;
    // `pg_get_functiondef` cannot render them at all.
    let rows = sqlx::query!(
        r#"
        SELECT
            p.oid AS "oid!",
            p.pronamespace AS "namespace!",
            p.proname AS "name!",
            p.prokind::text AS "prokind!",
            pg_catalog.pg_get_function_identity_arguments(p.oid) AS "arguments!",
            pg_catalog.pg_get_functiondef(p.oid) AS "definition!",
            pg_catalog.pg_get_function_result(p.oid) AS "return_type?",
            p.prorettype AS "return_type_oid!",
            l.lanname AS "language!",
            p.provolatile::text AS "volatility!",
            p.proisstrict AS "is_strict!",
            p.prosecdef AS "security_definer!",
            p.pronargs AS "num_args!"
        FROM pg_proc p
        JOIN pg_language l ON p.prolang = l.oid
        WHERE p.prokind != 'a'
        ORDER BY p.oid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawFunction {
            oid: row.oid,
            namespace: row.namespace,
            name: row.name,
            prokind: row.prokind,
            arguments: row.arguments,
            definition: row.definition,
            return_type: row.return_type,
            return_type_oid: row.return_type_oid,
            language: row.language,
            volatility: row.volatility,
            is_strict: row.is_strict,
            security_definer: row.security_definer,
            num_args: row.num_args,
        })
        .collect())
}

async fn fetch_parameters(conn: &mut PgConnection) -> Result<Vec<RawFunctionParameter>> {
    // `proargtypes` holds the input parameters, in declaration order; the names
    // and modes are positional arrays alongside it.
    let rows = sqlx::query!(
        r#"
        SELECT
            p.oid AS "function_oid!",
            param_types.ordinal AS "ordinal!",
            param_types.type_oid AS "type_oid!",
            pg_catalog.format_type(param_types.type_oid, NULL) AS "formatted_type!",
            p.proargnames[param_types.ordinal] AS "name?",
            p.proargmodes[param_types.ordinal - 1]::text AS "mode?"
        FROM pg_proc p
        CROSS JOIN LATERAL unnest(p.proargtypes) WITH ORDINALITY AS param_types(type_oid, ordinal)
        WHERE p.prokind != 'a'
        ORDER BY p.oid, param_types.ordinal
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawFunctionParameter {
            function_oid: row.function_oid,
            ordinal: row.ordinal as i32,
            type_oid: row.type_oid,
            formatted_type: row.formatted_type,
            name: row.name,
            mode: row.mode,
        })
        .collect())
}

async fn fetch_dependencies(conn: &mut PgConnection) -> Result<Vec<RawFunctionDependency>> {
    // One edge can be recorded once per column of the target that the body
    // reads, so the rows arrive duplicated; DISTINCT collapses them and the
    // converter de-duplicates what survives resolution.
    let rows = sqlx::query!(
        r#"
        SELECT DISTINCT
            p.oid AS "function_oid!",
            refcl.relname AS "ref_class!",
            d.refobjid AS "ref_oid!",

            cls.relkind::text AS "relation_kind?",
            cls.relnamespace AS "relation_namespace?",
            cls.relname AS "relation_name?",
            cls_attr.attname AS "relation_column?",

            proc.pronamespace AS "routine_namespace?",
            proc.proname AS "routine_name?",
            pg_catalog.pg_get_function_identity_arguments(proc.oid) AS "routine_args?"

        FROM pg_proc p
        JOIN pg_depend d ON d.classid = 'pg_proc'::regclass::oid AND d.objid = p.oid
        JOIN pg_class refcl ON refcl.oid = d.refclassid

        LEFT JOIN pg_class cls
          ON d.refclassid = 'pg_class'::regclass::oid
         AND d.refobjid = cls.oid
        LEFT JOIN pg_attribute cls_attr
          ON cls_attr.attrelid = cls.oid
         AND cls_attr.attnum = d.refobjsubid
         AND d.refobjsubid > 0

        LEFT JOIN pg_proc proc
          ON d.refclassid = 'pg_proc'::regclass::oid
         AND d.refobjid = proc.oid

        WHERE p.prokind != 'a'
          AND d.deptype = 'n'
          AND d.refclassid IN (
            'pg_class'::regclass::oid,
            'pg_type'::regclass::oid,
            'pg_proc'::regclass::oid
          )
        ORDER BY p.oid, refcl.relname, d.refobjid, cls_attr.attname
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawFunctionDependency {
            function_oid: row.function_oid,
            ref_class: row.ref_class,
            ref_oid: row.ref_oid,
            relation_kind: row.relation_kind,
            relation_namespace: row.relation_namespace,
            relation_name: row.relation_name,
            relation_column: row.relation_column,
            routine_namespace: row.routine_namespace,
            routine_name: row.routine_name,
            routine_args: row.routine_args,
        })
        .collect())
}
