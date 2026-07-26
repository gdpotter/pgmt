//! Raw table rows and their conversion into logical tables.
//!
//! The fetches keep the OIDs and attnums the converter resolves with, plus the
//! outputs of the server-side functions that cannot be computed in Rust:
//! `format_type` for a column's rendered type, `pg_get_expr` for defaults and
//! generation expressions, `pg_get_function_identity_arguments` for the
//! routines a column's expression calls, and the aggregated column list of a
//! primary key. Everything else — schema-name resolution, extension-ownership
//! and system-schema exclusion, type classification, dependency derivation,
//! comment attachment — happens in the converter, where the OIDs die.

use anyhow::{Context, Result};
use sqlx::postgres::PgConnection;
use sqlx::postgres::types::Oid;
use std::collections::BTreeMap;
use tracing::info;

use super::exclusion::{Converted, Excluded, ExclusionReason, is_system_schema};
use super::oid_index::OidIndex;
use super::shared::{SharedCatalog, class};
use crate::catalog::id::DbObjectId;
use crate::catalog::table::{Column, IdentityKind, PrimaryKey, Table};
use crate::render::quote_ident;

/// One `pg_class` row of `relkind = 'r'`, before names are resolved and OIDs are
/// discarded.
#[derive(Debug, Clone)]
pub struct RawTable {
    pub oid: Oid,
    pub namespace: Oid,
    pub name: String,
    pub rls_enabled: bool,
    pub rls_forced: bool,
}

/// One `pg_attribute` row of a table, with its default/generation expression.
#[derive(Debug, Clone)]
pub struct RawColumn {
    pub attrelid: Oid,
    pub attnum: i32,
    pub name: String,
    /// `atttypid`, unresolved: an array's own OID, not its element type's.
    pub type_oid: Oid,
    /// `format_type(atttypid, atttypmod)` — carries type modifiers and array
    /// brackets, which no Rust-side reconstruction can recover.
    pub formatted_type: String,
    /// `pg_get_expr` of the `pg_attrdef` entry: the DEFAULT expression, or the
    /// generation expression when the column is `GENERATED ... STORED`.
    pub expression: Option<String>,
    pub attgenerated: Option<String>,
    pub attidentity: Option<String>,
    pub not_null: bool,
    pub attndims: i32,
}

/// One primary-key constraint, with its columns already aggregated in key order.
#[derive(Debug, Clone)]
pub struct RawPrimaryKey {
    pub oid: Oid,
    pub conrelid: Oid,
    pub name: String,
    pub columns: Vec<String>,
}

/// A sequence a column's default draws from (the `SERIAL` / `nextval` edge).
#[derive(Debug, Clone)]
pub struct RawColumnSequenceDependency {
    pub attrelid: Oid,
    pub attnum: i32,
    pub sequence_namespace: Oid,
    pub sequence_name: String,
}

/// A routine a column's default or generation expression calls.
#[derive(Debug, Clone)]
pub struct RawColumnFunctionDependency {
    pub attrelid: Oid,
    pub attnum: i32,
    pub function_oid: Oid,
    pub function_namespace: Oid,
    pub function_name: String,
    pub function_args: String,
}

/// Everything the table converter reads out of `pg_catalog`.
#[derive(Debug, Clone, Default)]
pub struct RawTables {
    pub tables: Vec<RawTable>,
    pub columns: Vec<RawColumn>,
    pub primary_keys: Vec<RawPrimaryKey>,
    pub sequence_dependencies: Vec<RawColumnSequenceDependency>,
    pub function_dependencies: Vec<RawColumnFunctionDependency>,
}

/// A converted table, still beside the OIDs the comment pass addresses it by.
#[derive(Debug, Clone)]
pub struct ConvertedTable {
    pub oid: Oid,
    pub table: Table,
    /// The attnum of each column of `table`, positionally aligned. Column
    /// comments are addressed by attnum in `pg_description` but by name in the
    /// logical world; this is the correspondence, and it does not outlive the
    /// conversion.
    pub column_attnums: Vec<i32>,
    /// OID of the primary-key constraint, whose comment the table carries.
    pub primary_key_oid: Option<Oid>,
}

/// Fetch every table, column, primary key and column-expression dependency in
/// the database, unresolved and unfiltered.
pub async fn fetch(conn: &mut PgConnection) -> Result<RawTables> {
    info!("Fetching tables...");
    let tables = fetch_tables(&mut *conn).await?;
    info!("Fetching table columns...");
    let columns = fetch_columns(&mut *conn).await?;
    info!("Fetching primary keys...");
    let primary_keys = fetch_primary_keys(&mut *conn).await?;
    info!("Fetching table sequence dependencies...");
    let sequence_dependencies = fetch_sequence_dependencies(&mut *conn).await?;
    info!("Fetching table function dependencies...");
    let function_dependencies = fetch_function_dependencies(&mut *conn).await?;

    Ok(RawTables {
        tables,
        columns,
        primary_keys,
        sequence_dependencies,
        function_dependencies,
    })
}

/// Fetch tables and convert them into the logical catalog, with the table's own
/// comment, its column comments and its primary key's comment attached through
/// the OID index.
#[allow(dead_code)]
pub async fn load(conn: &mut PgConnection, shared: &SharedCatalog) -> Result<Vec<Table>> {
    Ok(load_with_exclusions(conn, shared)
        .await?
        .log_and_take_objects("table"))
}

/// The same load, keeping the named reason for every raw row that did not
/// become a table.
pub async fn load_with_exclusions(
    conn: &mut PgConnection,
    shared: &SharedCatalog,
) -> Result<Converted<Table>> {
    let raw = fetch(conn).await?;
    let mut converted = convert(&raw, shared)?;

    // Identity first, then the index, then the OID-addressed state: a comment
    // can only be attached to an object whose identity is already known. Both
    // the table and its primary-key constraint are indexed, because
    // `pg_description` addresses their comments under different classes.
    let mut index = OidIndex::new();
    for entry in &converted.objects {
        index.insert(class::PG_CLASS, entry.oid, entry.table.id())?;
        if let (Some(pk_oid), Some(pk)) = (entry.primary_key_oid, &entry.table.primary_key) {
            index.insert(
                class::PG_CONSTRAINT,
                pk_oid,
                primary_key_id(&entry.table, pk),
            )?;
        }
    }

    let table_comments = index.object_comments(&shared.descriptions, class::PG_CLASS);
    let column_comments = index.subobject_comments(&shared.descriptions, class::PG_CLASS);
    let constraint_comments = index.object_comments(&shared.descriptions, class::PG_CONSTRAINT);

    for entry in &mut converted.objects {
        let id = entry.table.id();
        entry.table.comment = table_comments.get(&id).map(|text| text.to_string());

        if let Some(by_attnum) = column_comments.get(&id) {
            for (column, attnum) in entry.table.columns.iter_mut().zip(&entry.column_attnums) {
                column.comment = by_attnum.get(attnum).map(|text| text.to_string());
            }
        }

        let pk_comment = entry.table.primary_key.as_ref().and_then(|pk| {
            constraint_comments
                .get(&primary_key_id(&entry.table, pk))
                .map(|text| text.to_string())
        });
        if let Some(pk) = &mut entry.table.primary_key {
            pk.comment = pk_comment;
        }
    }

    converted.index = index;

    Ok(converted.map(|entry| entry.table))
}

/// The identity a primary key's comment is addressed by. Primary keys are not
/// separate `Constraint` objects in the logical catalog — the table carries the
/// constraint's comment — but `pg_description` still keys it by the constraint.
fn primary_key_id(table: &Table, pk: &PrimaryKey) -> DbObjectId {
    DbObjectId::Constraint {
        schema: table.schema.clone(),
        table: table.name.clone(),
        name: pk.name.clone(),
    }
}

/// Resolve raw tables into logical ones, keeping each table's OID (and its
/// columns' attnums) beside it so OID-addressed state can still be attached
/// before the identities cross the firewall.
///
/// Tables in a system schema and tables owned by an extension are dropped here,
/// each recorded with its named reason, along with the columns, keys and
/// dependencies belonging to them.
pub fn convert(raw: &RawTables, shared: &SharedCatalog) -> Result<Converted<ConvertedTable>> {
    let namespaces = &shared.namespaces;

    // The tables that survive filtering, by OID, so every per-column row can be
    // routed to its table (or dropped with it).
    let mut kept: BTreeMap<u32, usize> = BTreeMap::new();
    let mut converted: Converted<ConvertedTable> = Converted::new();

    for row in &raw.tables {
        let schema = namespaces
            .name(row.namespace)
            .with_context(|| format!("table {} has no namespace entry", row.name))?;

        if is_system_schema(schema) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "table",
                schema,
                &row.name,
                ExclusionReason::SystemSchema,
            ));
            continue;
        }
        if let Some(extension) = shared.extensions.owner(class::PG_CLASS, row.oid) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "table",
                schema,
                &row.name,
                ExclusionReason::ExtensionOwned {
                    extension: extension.to_string(),
                },
            ));
            continue;
        }

        let mut table = Table::new(
            schema.to_string(),
            row.name.clone(),
            Vec::new(),
            None,
            None,
            vec![DbObjectId::Schema {
                name: schema.to_string(),
            }],
        );
        table.rls_enabled = row.rls_enabled;
        table.rls_forced = row.rls_forced;

        kept.insert(row.oid.0, converted.objects.len());
        converted.objects.push(ConvertedTable {
            oid: row.oid,
            table,
            column_attnums: Vec::new(),
            primary_key_oid: None,
        });
    }

    let sequence_deps = sequence_dependencies(raw, shared);
    let function_deps = function_dependencies(raw, shared);

    for row in &raw.columns {
        let Some(&idx) = kept.get(&row.attrelid.0) else {
            continue;
        };

        let resolved = shared.resolve_type(row.type_oid);
        let mut depends_on = Vec::new();
        if let Some(dep) = resolved.as_ref().and_then(|t| t.dependency()) {
            depends_on.push(dep);
        }
        if let Some(functions) = function_deps.get(&(row.attrelid.0, row.attnum)) {
            depends_on.extend(functions.iter().cloned());
        }
        if let Some(sequences) = sequence_deps.get(&(row.attrelid.0, row.attnum)) {
            depends_on.extend(sequences.iter().cloned());
        }

        // A user-defined type is rendered schema-qualified; a built-in or an
        // extension-provided one keeps the server's rendering (extension types
        // are resolved through the extension's schema, not qualified by pgmt).
        let data_type = match &resolved {
            Some(t) if t.extension.is_none() && t.schema.is_some_and(|s| !is_system_schema(s)) => {
                format!(
                    "{}.{}{}",
                    quote_ident(t.schema.unwrap_or_default()),
                    quote_ident(t.name),
                    "[]".repeat(row.attndims as usize)
                )
            }
            _ => row.formatted_type.clone(),
        };

        let is_stored_generated = row.attgenerated.as_deref() == Some("s");
        converted.objects[idx].table.columns.push(Column {
            name: row.name.clone(),
            data_type,
            default: if is_stored_generated {
                None
            } else {
                row.expression.clone()
            },
            not_null: row.not_null,
            generated: if is_stored_generated {
                row.expression.clone()
            } else {
                None
            },
            identity: IdentityKind::from_attidentity(row.attidentity.as_deref()),
            comment: None,
            depends_on,
        });
        converted.objects[idx].column_attnums.push(row.attnum);
    }

    for row in &raw.primary_keys {
        let Some(&idx) = kept.get(&row.conrelid.0) else {
            continue;
        };
        converted.objects[idx].table.primary_key = Some(PrimaryKey {
            name: row.name.clone(),
            columns: row.columns.clone(),
            comment: None,
        });
        converted.objects[idx].primary_key_oid = Some(row.oid);
    }

    for entry in &mut converted.objects {
        entry.table.update_all_dependencies();
    }

    // The raw fetches order by OID; ordering by name is what callers see.
    converted
        .objects
        .sort_by(|a, b| (&a.table.schema, &a.table.name).cmp(&(&b.table.schema, &b.table.name)));

    Ok(converted)
}

/// The sequence dependency of each column default that draws from one, keyed by
/// `(table OID, attnum)`.
fn sequence_dependencies(
    raw: &RawTables,
    shared: &SharedCatalog,
) -> BTreeMap<(u32, i32), Vec<DbObjectId>> {
    let mut by_column: BTreeMap<(u32, i32), Vec<DbObjectId>> = BTreeMap::new();
    for row in &raw.sequence_dependencies {
        let Some(schema) = shared.namespaces.name(row.sequence_namespace) else {
            continue;
        };
        by_column
            .entry((row.attrelid.0, row.attnum))
            .or_default()
            .push(DbObjectId::Sequence {
                schema: schema.to_string(),
                name: row.sequence_name.clone(),
            });
    }
    by_column
}

/// The routine dependencies of each column default or generation expression,
/// keyed by `(table OID, attnum)`. An extension-provided routine is depended on
/// through its extension.
fn function_dependencies(
    raw: &RawTables,
    shared: &SharedCatalog,
) -> BTreeMap<(u32, i32), Vec<DbObjectId>> {
    let mut by_column: BTreeMap<(u32, i32), Vec<DbObjectId>> = BTreeMap::new();
    for row in &raw.function_dependencies {
        let Some(schema) = shared.namespaces.name(row.function_namespace) else {
            continue;
        };
        if is_system_schema(schema) {
            continue;
        }

        let dependency = match shared.extensions.owner(class::PG_PROC, row.function_oid) {
            Some(extension) => DbObjectId::Extension {
                name: extension.to_string(),
            },
            None => DbObjectId::Function {
                schema: schema.to_string(),
                name: row.function_name.clone(),
                arguments: row.function_args.clone(),
            },
        };
        by_column
            .entry((row.attrelid.0, row.attnum))
            .or_default()
            .push(dependency);
    }
    by_column
}

async fn fetch_tables(conn: &mut PgConnection) -> Result<Vec<RawTable>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            c.oid AS "oid!",
            c.relnamespace AS "namespace!",
            c.relname AS "name!",
            c.relrowsecurity AS "rls_enabled!",
            c.relforcerowsecurity AS "rls_forced!"
        FROM pg_class c
        WHERE c.relkind = 'r'
        ORDER BY c.oid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawTable {
            oid: row.oid,
            namespace: row.namespace,
            name: row.name,
            rls_enabled: row.rls_enabled,
            rls_forced: row.rls_forced,
        })
        .collect())
}

async fn fetch_columns(conn: &mut PgConnection) -> Result<Vec<RawColumn>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            a.attrelid AS "attrelid!",
            a.attnum AS "attnum!",
            a.attname AS "name!",
            a.atttypid AS "type_oid!",
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS "formatted_type!",
            pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) AS "expression?",
            a.attgenerated::text AS "attgenerated?",
            a.attidentity::text AS "attidentity?",
            a.attnotnull AS "not_null!",
            COALESCE(a.attndims, 0)::int AS "attndims!: i32"
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid AND c.relkind = 'r'
        LEFT JOIN pg_attrdef ad
          ON ad.adrelid = a.attrelid
         AND ad.adnum = a.attnum
        WHERE a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attrelid, a.attnum
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawColumn {
            attrelid: row.attrelid,
            attnum: row.attnum as i32,
            name: row.name,
            type_oid: row.type_oid,
            formatted_type: row.formatted_type,
            expression: row.expression,
            attgenerated: row.attgenerated,
            attidentity: row.attidentity,
            not_null: row.not_null,
            attndims: row.attndims,
        })
        .collect())
}

async fn fetch_primary_keys(conn: &mut PgConnection) -> Result<Vec<RawPrimaryKey>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            con.oid AS "oid!",
            con.conrelid AS "conrelid!",
            con.conname AS "name!",
            array_agg(a.attname ORDER BY array_position(con.conkey, a.attnum)) AS "columns!: Vec<String>"
        FROM pg_constraint con
        JOIN pg_attribute a
          ON a.attrelid = con.conrelid
         AND a.attnum = ANY(con.conkey)
        WHERE con.contype = 'p'
        GROUP BY con.oid, con.conrelid, con.conname
        ORDER BY con.oid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawPrimaryKey {
            oid: row.oid,
            conrelid: row.conrelid,
            name: row.name,
            columns: row.columns,
        })
        .collect())
}

async fn fetch_sequence_dependencies(
    conn: &mut PgConnection,
) -> Result<Vec<RawColumnSequenceDependency>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            a.attrelid AS "attrelid!",
            a.attnum AS "attnum!",
            seq.relnamespace AS "sequence_namespace!",
            seq.relname AS "sequence_name!"
        FROM pg_depend d
        JOIN pg_attrdef ad ON d.objid = ad.oid
        JOIN pg_attribute a ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
        JOIN pg_class seq ON d.refobjid = seq.oid
        WHERE d.refclassid = 'pg_class'::regclass
          AND seq.relkind = 'S'
        ORDER BY a.attrelid, a.attnum, seq.relnamespace, seq.relname
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawColumnSequenceDependency {
            attrelid: row.attrelid,
            attnum: row.attnum as i32,
            sequence_namespace: row.sequence_namespace,
            sequence_name: row.sequence_name,
        })
        .collect())
}

async fn fetch_function_dependencies(
    conn: &mut PgConnection,
) -> Result<Vec<RawColumnFunctionDependency>> {
    // PostgreSQL records the routines a column expression calls differently by
    // version: from v15 the edge hangs off the `pg_attrdef` entry as a NORMAL
    // dependency, before that off the column itself as an AUTO dependency.
    let rows = sqlx::query!(
        r#"
        SELECT DISTINCT
            a.attrelid AS "attrelid!",
            a.attnum AS "attnum!",
            pf.oid AS "function_oid!",
            pf.pronamespace AS "function_namespace!",
            pf.proname AS "function_name!",
            pg_catalog.pg_get_function_identity_arguments(pf.oid) AS "function_args!"
        FROM pg_depend d
        JOIN pg_attrdef ad ON d.classid = 'pg_attrdef'::regclass AND d.objid = ad.oid
        JOIN pg_attribute a ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
        JOIN pg_proc pf ON d.refclassid = 'pg_proc'::regclass AND d.refobjid = pf.oid
        WHERE (a.attgenerated = 's' OR a.atthasdef = true)
          AND d.deptype = 'n'

        UNION ALL

        SELECT DISTINCT
            a.attrelid AS "attrelid!",
            a.attnum AS "attnum!",
            pf.oid AS "function_oid!",
            pf.pronamespace AS "function_namespace!",
            pf.proname AS "function_name!",
            pg_catalog.pg_get_function_identity_arguments(pf.oid) AS "function_args!"
        FROM pg_depend d
        JOIN pg_attribute a ON d.classid = 'pg_class'::regclass
                             AND d.objid = a.attrelid
                             AND d.objsubid = a.attnum
        JOIN pg_proc pf ON d.refclassid = 'pg_proc'::regclass AND d.refobjid = pf.oid
        WHERE (a.attgenerated = 's' OR a.atthasdef = true)
          AND d.deptype = 'a'

        ORDER BY 1, 2, 3
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawColumnFunctionDependency {
            attrelid: row.attrelid,
            attnum: row.attnum as i32,
            function_oid: row.function_oid,
            function_namespace: row.function_namespace,
            function_name: row.function_name,
            function_args: row.function_args,
        })
        .collect())
}
