//! Raw index rows and their conversion into logical indexes.
//!
//! The fetches keep the OIDs the converter resolves with, plus the outputs of
//! the server-side functions that cannot be computed in Rust: `pg_get_indexdef`
//! per key column (the only faithful rendering of an expression index — no
//! reconstruction from `pg_attribute` recovers one), `pg_get_expr` for a partial
//! index's predicate, and `pg_get_function_identity_arguments` for the routines
//! an index expression calls. Everything else — schema-name resolution,
//! extension-ownership and system-schema exclusion, the constraint-backing
//! check, dependency derivation, comment attachment — happens in the converter,
//! where the OIDs die.

use anyhow::{Context, Result};
use sqlx::postgres::PgConnection;
use sqlx::postgres::types::Oid;
use std::collections::{BTreeMap, HashSet};
use tracing::info;

use super::exclusion::{Converted, Excluded, ExclusionReason};
use super::oid_index::OidIndex;
use super::shared::{SharedCatalog, class};
use crate::catalog::id::DbObjectId;
use crate::catalog::index::{Index, IndexColumn, IndexType};
use crate::catalog::utils::is_system_schema;

/// Schemas whose indexes are never pgmt's to manage.
const SYSTEM_SCHEMAS: [&str; 3] = ["pg_catalog", "information_schema", "pg_toast"];

/// One `pg_index` row with its `pg_class` metadata, before names are resolved
/// and OIDs are discarded.
#[derive(Debug, Clone)]
pub struct RawIndex {
    pub oid: Oid,
    pub namespace: Oid,
    pub name: String,
    /// The indexed relation. Its OID is what decides extension ownership: an
    /// index created by an extension script gets no `deptype = 'e'` row of its
    /// own, only its table does.
    pub table_oid: Oid,
    pub table_namespace: Oid,
    pub table_name: String,
    /// `pg_am.amname`: btree, hash, gist, gin, spgist, brin, or an
    /// extension-provided method.
    pub access_method: String,
    pub is_unique: bool,
    pub is_clustered: bool,
    pub is_valid: bool,
    /// `pg_get_expr(indpred, indrelid)` — the WHERE clause of a partial index.
    pub predicate: Option<String>,
    pub tablespace: Option<String>,
    pub reloptions: Option<Vec<String>>,
    /// Name of the primary-key, unique or exclusion constraint this index
    /// implements, if any.
    pub backing_constraint: Option<String>,
}

/// One key or INCLUDE column of an index, as `pg_get_indexdef` renders it.
#[derive(Debug, Clone)]
pub struct RawIndexColumn {
    pub index_oid: Oid,
    /// 1-based position within the index, key columns first.
    pub position: i32,
    /// `pg_get_indexdef(indexrelid, position, true)`: a column name, or the
    /// expression an expression index is built on.
    pub expression: String,
    pub collation: Option<String>,
    pub opclass: Option<String>,
    pub ordering: String,
    pub nulls_ordering: String,
    pub is_included: bool,
}

/// One `pg_depend` edge from an index to a type, routine or operator class its
/// definition uses.
#[derive(Debug, Clone)]
pub struct RawIndexDependency {
    pub index_oid: Oid,
    /// Name of the `pg_catalog` table the reference addresses (`pg_type`,
    /// `pg_proc`, `pg_opclass`).
    pub ref_class: String,
    pub ref_oid: Oid,

    /// Referenced type (`pg_type`).
    pub type_namespace: Option<Oid>,
    pub type_name: Option<String>,

    /// Referenced routine (`pg_proc`).
    pub function_namespace: Option<Oid>,
    pub function_name: Option<String>,
    pub function_args: Option<String>,
}

/// Everything the index converter reads out of `pg_catalog`.
#[derive(Debug, Clone, Default)]
pub struct RawIndexes {
    pub indexes: Vec<RawIndex>,
    pub columns: Vec<RawIndexColumn>,
    pub dependencies: Vec<RawIndexDependency>,
}

/// Fetch every index, index column and index dependency edge in the database,
/// unresolved and unfiltered.
pub async fn fetch(conn: &mut PgConnection) -> Result<RawIndexes> {
    info!("Fetching indexes...");
    let indexes = fetch_indexes(&mut *conn).await?;
    info!("Fetching index columns...");
    let columns = fetch_columns(&mut *conn).await?;
    info!("Fetching index dependencies...");
    let dependencies = fetch_dependencies(&mut *conn).await?;

    Ok(RawIndexes {
        indexes,
        columns,
        dependencies,
    })
}

/// Fetch indexes and convert them into the logical catalog, with each index's
/// comment attached through the OID index.
pub async fn load(conn: &mut PgConnection, shared: &SharedCatalog) -> Result<Vec<Index>> {
    Ok(load_with_exclusions(conn, shared).await?.objects)
}

/// The same load, keeping the named reason for every raw row that did not become
/// an index.
pub async fn load_with_exclusions(
    conn: &mut PgConnection,
    shared: &SharedCatalog,
) -> Result<Converted<Index>> {
    let raw = fetch(conn).await?;
    let mut converted = convert(&raw, shared)?;

    // Identity first, then the index, then the OID-addressed state: a comment
    // can only be attached to an object whose identity is already known. An
    // index's comment is addressed under `pg_class`, like any relation's.
    let oids = OidIndex::from_pairs(
        class::PG_CLASS,
        converted
            .objects
            .iter()
            .map(|(oid, index)| (*oid, index.id())),
    )?;
    let comments = oids.object_comments(&shared.descriptions, class::PG_CLASS);
    for (_, index) in &mut converted.objects {
        index.comment = comments.get(&index.id()).map(|text| text.to_string());
    }

    Ok(converted.map(|(_, index)| index))
}

/// Resolve raw indexes into logical ones, keeping each index's OID beside it so
/// OID-addressed state can still be attached before the identities cross the
/// firewall.
///
/// Indexes in a system schema or on a system table, indexes belonging to an
/// extension (through their own OID or their parent table's), and the indexes
/// implementing a constraint are dropped here, each with its named reason,
/// along with the columns and dependency edges belonging to them.
pub fn convert(raw: &RawIndexes, shared: &SharedCatalog) -> Result<Converted<(Oid, Index)>> {
    // The indexes that survive filtering, by OID, so every column and dependency
    // row can be routed to its index (or dropped with it).
    let mut kept: BTreeMap<u32, usize> = BTreeMap::new();
    let mut converted: Converted<(Oid, Index)> = Converted::new();

    for row in &raw.indexes {
        let schema = shared
            .namespaces
            .name(row.namespace)
            .with_context(|| format!("index {} has no namespace entry", row.name))?;
        let table_schema = shared
            .namespaces
            .name(row.table_namespace)
            .with_context(|| format!("table {} has no namespace entry", row.table_name))?;

        if SYSTEM_SCHEMAS.contains(&schema) || SYSTEM_SCHEMAS.contains(&table_schema) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "index",
                schema,
                &row.name,
                ExclusionReason::SystemSchema,
            ));
            continue;
        }
        // Ownership through the index's own OID covers a standalone extension
        // index; ownership through the table covers an index an extension script
        // created, which records membership only on the parent.
        let extension = shared
            .extensions
            .owner(class::PG_CLASS, row.oid)
            .or_else(|| shared.extensions.owner_of_relation_subobject(row.table_oid));
        if let Some(extension) = extension {
            converted.excluded.push(Excluded::new(
                row.oid,
                "index",
                schema,
                &row.name,
                ExclusionReason::ExtensionOwned {
                    extension: extension.to_string(),
                },
            ));
            continue;
        }
        if let Some(constraint) = &row.backing_constraint {
            converted.excluded.push(Excluded::new(
                row.oid,
                "index",
                schema,
                &row.name,
                ExclusionReason::ConstraintBackingIndex {
                    constraint: constraint.clone(),
                },
            ));
            continue;
        }

        kept.insert(row.oid.0, converted.objects.len());
        converted.objects.push((
            row.oid,
            Index {
                schema: schema.to_string(),
                name: row.name.clone(),
                table_schema: table_schema.to_string(),
                table_name: row.table_name.clone(),
                index_type: IndexType::from_string(&row.access_method),
                is_unique: row.is_unique,
                is_clustered: row.is_clustered,
                is_valid: row.is_valid,
                columns: Vec::new(),
                include_columns: Vec::new(),
                predicate: row.predicate.clone(),
                tablespace: row.tablespace.clone(),
                storage_parameters: storage_parameters(&row.reloptions),
                comment: None,
                depends_on: vec![DbObjectId::Table {
                    schema: table_schema.to_string(),
                    name: row.table_name.clone(),
                }],
            },
        ));
    }

    for row in &raw.columns {
        let Some(&idx) = kept.get(&row.index_oid.0) else {
            continue;
        };
        let (_, index) = &mut converted.objects[idx];
        if row.is_included {
            index.include_columns.push(row.expression.clone());
            continue;
        }

        // Only btree indexes order their keys; for every other access method the
        // direction and null placement are meaningless and are not rendered.
        let is_btree = index.index_type == IndexType::Btree;
        index.columns.push(IndexColumn {
            expression: row.expression.clone(),
            collation: row.collation.clone(),
            opclass: row.opclass.clone(),
            ordering: is_btree.then(|| row.ordering.clone()),
            nulls_ordering: is_btree.then(|| row.nulls_ordering.clone()),
        });
    }

    for row in &raw.dependencies {
        let Some(&idx) = kept.get(&row.index_oid.0) else {
            continue;
        };
        if let Some(dep) = dependency(row, shared) {
            converted.objects[idx].1.depends_on.push(dep);
        }
    }

    for (_, index) in &mut converted.objects {
        // Several referents can resolve to the same extension, and an index
        // references the same type once per column that has it.
        let mut seen = HashSet::new();
        index.depends_on.retain(|dep| seen.insert(dep.clone()));
    }

    // The raw fetch orders by OID; ordering by name is what callers see.
    converted
        .objects
        .sort_by(|(_, a), (_, b)| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));

    Ok(converted)
}

/// The dependency one `pg_depend` edge of an index creates.
///
/// An extension-provided referent (pg_trgm's `gin_trgm_ops`, fuzzystrmatch's
/// `soundex()`, citext) resolves to the extension itself: the object is filtered
/// from the catalog, so the index must depend on what creates it. Operator
/// classes are not catalog objects of their own, so an in-database one yields no
/// dependency at all.
fn dependency(row: &RawIndexDependency, shared: &SharedCatalog) -> Option<DbObjectId> {
    if let Some(extension) = shared.extensions.owner(&row.ref_class, row.ref_oid) {
        return Some(DbObjectId::Extension {
            name: extension.to_string(),
        });
    }

    match row.ref_class.as_str() {
        class::PG_TYPE => {
            let schema = shared.namespaces.name(row.type_namespace?)?;
            if is_system_schema(schema) {
                return None;
            }
            Some(DbObjectId::Type {
                schema: schema.to_string(),
                name: row.type_name.clone()?,
            })
        }
        class::PG_PROC => {
            let schema = shared.namespaces.name(row.function_namespace?)?;
            if is_system_schema(schema) {
                return None;
            }
            Some(DbObjectId::Function {
                schema: schema.to_string(),
                name: row.function_name.clone()?,
                arguments: row.function_args.clone().unwrap_or_default(),
            })
        }
        _ => None,
    }
}

/// The `WITH (...)` options of an index, from the `key=value` strings
/// `pg_class.reloptions` stores.
fn storage_parameters(reloptions: &Option<Vec<String>>) -> Vec<(String, String)> {
    reloptions
        .iter()
        .flatten()
        .filter_map(|option| {
            option
                .split_once('=')
                .map(|(key, value)| (key.to_string(), value.to_string()))
        })
        .collect()
}

async fn fetch_indexes(conn: &mut PgConnection) -> Result<Vec<RawIndex>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            i.oid AS "oid!",
            i.relnamespace AS "namespace!",
            i.relname AS "name!",
            t.oid AS "table_oid!",
            t.relnamespace AS "table_namespace!",
            t.relname AS "table_name!",
            am.amname AS "access_method!",
            idx.indisunique AS "is_unique!",
            idx.indisclustered AS "is_clustered!",
            idx.indisvalid AS "is_valid!",
            pg_catalog.pg_get_expr(idx.indpred, idx.indrelid) AS "predicate?",
            ts.spcname AS "tablespace?",
            i.reloptions AS "reloptions?",
            (
                SELECT con.conname
                FROM pg_constraint con
                WHERE con.conindid = i.oid
                  AND con.contype IN ('p', 'u', 'x')
                LIMIT 1
            ) AS "backing_constraint?"
        FROM pg_index idx
        JOIN pg_class i ON idx.indexrelid = i.oid
        JOIN pg_class t ON idx.indrelid = t.oid
        JOIN pg_am am ON i.relam = am.oid
        LEFT JOIN pg_tablespace ts ON i.reltablespace = ts.oid
        ORDER BY i.oid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawIndex {
            oid: row.oid,
            namespace: row.namespace,
            name: row.name,
            table_oid: row.table_oid,
            table_namespace: row.table_namespace,
            table_name: row.table_name,
            access_method: row.access_method,
            is_unique: row.is_unique,
            is_clustered: row.is_clustered,
            is_valid: row.is_valid,
            predicate: row.predicate,
            tablespace: row.tablespace,
            reloptions: row.reloptions,
            backing_constraint: row.backing_constraint,
        })
        .collect())
}

async fn fetch_columns(conn: &mut PgConnection) -> Result<Vec<RawIndexColumn>> {
    // `pg_get_indexdef` is asked for one column at a time: it is what renders an
    // expression index's expression, and `indkey` is 0 for such a column, so
    // there is nothing in `pg_attribute` to render instead.
    let rows = sqlx::query!(
        r#"
        SELECT
            idx.indexrelid AS "index_oid!",
            col_pos AS "position!",
            pg_catalog.pg_get_indexdef(idx.indexrelid, col_pos, true) AS "expression!",
            CASE
                WHEN c.collname IS NOT NULL AND c.collname != 'default'
                THEN quote_ident(cn.nspname) || '.' || quote_ident(c.collname)
                ELSE NULL
            END AS "collation?",
            CASE
                WHEN op.opcname IS NOT NULL
                THEN quote_ident(opn.nspname) || '.' || quote_ident(op.opcname)
                ELSE NULL
            END AS "opclass?",
            CASE
                WHEN idx.indoption[col_pos-1] & 1 = 1 THEN 'DESC'
                ELSE 'ASC'
            END AS "ordering!",
            CASE
                WHEN idx.indoption[col_pos-1] & 2 = 2 THEN 'NULLS FIRST'
                ELSE 'NULLS LAST'
            END AS "nulls_ordering!",
            col_pos > idx.indnkeyatts AS "is_included!"
        FROM pg_index idx
        CROSS JOIN generate_series(1, idx.indnatts) AS col_pos
        LEFT JOIN pg_attribute a ON a.attrelid = idx.indrelid
                                 AND a.attnum = idx.indkey[col_pos-1]
                                 AND idx.indkey[col_pos-1] > 0
        LEFT JOIN pg_collation c ON a.attcollation = c.oid
        LEFT JOIN pg_namespace cn ON c.collnamespace = cn.oid
        LEFT JOIN pg_opclass op ON col_pos <= array_length(idx.indclass, 1)
                                AND idx.indclass[col_pos-1] = op.oid
        LEFT JOIN pg_namespace opn ON op.opcnamespace = opn.oid
        ORDER BY idx.indexrelid, col_pos
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawIndexColumn {
            index_oid: row.index_oid,
            position: row.position,
            expression: row.expression,
            collation: row.collation,
            opclass: row.opclass,
            ordering: row.ordering,
            nulls_ordering: row.nulls_ordering,
            is_included: row.is_included,
        })
        .collect())
}

async fn fetch_dependencies(conn: &mut PgConnection) -> Result<Vec<RawIndexDependency>> {
    let rows = sqlx::query!(
        r#"
        SELECT DISTINCT
            d.objid AS "index_oid!",
            cl.relname AS "ref_class!",
            d.refobjid AS "ref_oid!",
            t.typnamespace AS "type_namespace?",
            t.typname AS "type_name?",
            p.pronamespace AS "function_namespace?",
            p.proname AS "function_name?",
            pg_catalog.pg_get_function_identity_arguments(p.oid) AS "function_args?"
        FROM pg_depend d
        JOIN pg_index idx ON idx.indexrelid = d.objid
        JOIN pg_class cl ON cl.oid = d.refclassid
        LEFT JOIN pg_type t ON d.refclassid = 'pg_type'::regclass AND d.refobjid = t.oid
        LEFT JOIN pg_proc p ON d.refclassid = 'pg_proc'::regclass AND d.refobjid = p.oid
        WHERE d.classid = 'pg_class'::regclass
          AND d.refclassid IN ('pg_type'::regclass, 'pg_proc'::regclass, 'pg_opclass'::regclass)
        ORDER BY d.objid, cl.relname, d.refobjid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawIndexDependency {
            index_oid: row.index_oid,
            ref_class: row.ref_class,
            ref_oid: row.ref_oid,
            type_namespace: row.type_namespace,
            type_name: row.type_name,
            function_namespace: row.function_namespace,
            function_name: row.function_name,
            function_args: row.function_args,
        })
        .collect())
}
