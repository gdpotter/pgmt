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

use anyhow::{Context, Result, anyhow};
use sqlx::postgres::PgConnection;
use sqlx::postgres::types::Oid;
use std::collections::BTreeMap;
use tracing::info;

use super::dedup_preserving_order;
use super::exclusion::{Converted, Excluded, ExclusionReason, is_system_schema};
use super::oid_index::OidIndex;
use super::reference::RawReference;
use super::shared::{SharedCatalog, class};
use crate::catalog::id::DbObjectId;
use crate::catalog::index::{Index, IndexColumn, IndexType};

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
    /// expression an expression index is built on. Absent for an index in a
    /// system schema or on a system table, which the converter excludes:
    /// rendering every system index costs more than the whole load.
    pub expression: Option<String>,
    pub collation: Option<String>,
    pub opclass: Option<String>,
    pub ordering: String,
    pub nulls_ordering: String,
    pub is_included: bool,
}

/// Everything the index converter reads out of `pg_catalog`.
#[derive(Debug, Clone, Default)]
pub struct RawIndexes {
    pub indexes: Vec<RawIndex>,
    pub columns: Vec<RawIndexColumn>,
    pub dependencies: Vec<RawReference>,
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
#[allow(dead_code)]
pub async fn load(conn: &mut PgConnection, shared: &SharedCatalog) -> Result<Vec<Index>> {
    Ok(load_with_exclusions(conn, shared)
        .await?
        .log_and_take_objects("index"))
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
    let index = OidIndex::from_pairs(
        class::PG_CLASS,
        converted
            .objects
            .iter()
            .map(|(oid, entry)| (*oid, entry.id())),
    )?;
    let comments = index.object_comments(&shared.descriptions, class::PG_CLASS);
    for (_, entry) in &mut converted.objects {
        entry.comment = comments.get(&entry.id()).map(|text| text.to_string());
    }

    converted.index = index;

    Ok(converted.map(|(_, entry)| entry))
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

        if is_system_schema(schema) || is_system_schema(table_schema) {
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
        // Only a row the fetch skipped rendering — a column of an index excluded
        // above — may lack an expression; a kept index with an unrendered column
        // would silently lose a key.
        let expression = row.expression.clone().ok_or_else(|| {
            anyhow!(
                "index {}.{} was fetched without an expression for column {}",
                index.schema,
                index.name,
                row.position
            )
        })?;

        if row.is_included {
            index.include_columns.push(expression);
            continue;
        }

        // Only btree indexes order their keys; for every other access method the
        // direction and null placement are meaningless and are not rendered.
        let is_btree = index.index_type == IndexType::Btree;
        index.columns.push(IndexColumn {
            expression,
            collation: row.collation.clone(),
            opclass: row.opclass.clone(),
            ordering: is_btree.then(|| row.ordering.clone()),
            nulls_ordering: is_btree.then(|| row.nulls_ordering.clone()),
        });
    }

    for row in &raw.dependencies {
        let Some(&idx) = kept.get(&row.source_oid.0) else {
            continue;
        };
        if let Some(dep) = row.dependency(shared) {
            converted.objects[idx].1.depends_on.push(dep);
        }
    }

    for (_, index) in &mut converted.objects {
        // Several referents can resolve to the same extension, and an index
        // references the same type once per column that has it.
        dedup_preserving_order(&mut index.depends_on);
    }

    // The raw fetch orders by OID; ordering by name is what callers see.
    converted
        .objects
        .sort_by(|(_, a), (_, b)| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));

    Ok(converted)
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
    // The `backing_constraint` subquery names the primary-key, unique or
    // exclusion constraint an index implements, which is what the converter
    // excludes the index for. Only those three contypes own an index: a foreign
    // key's `conindid` points at the *referenced* table's index, which stays a
    // user index of its own.
    //
    // `sqlx::query!` needs a string literal, so this cannot be interpolated from
    // `exclusion::sql::not_a_constraint_backing_index`, which spells the same
    // rule for the identity snapshot; the two are bound by this comment.
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
    //
    // The system-schema test mirrors `exclusion::sql::not_a_system_namespace`
    // for both the index and its table, spelled out because `sqlx::query!` takes
    // a literal: the converter drops those rows, and rendering each column of
    // every system index is pure cost.
    let rows = sqlx::query!(
        r#"
        SELECT
            idx.indexrelid AS "index_oid!",
            col_pos AS "position!",
            CASE
                WHEN n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                 AND n.nspname NOT LIKE 'pg_temp_%'
                 AND n.nspname NOT LIKE 'pg_toast_temp_%'
                 AND tn.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                 AND tn.nspname NOT LIKE 'pg_temp_%'
                 AND tn.nspname NOT LIKE 'pg_toast_temp_%'
                THEN pg_catalog.pg_get_indexdef(idx.indexrelid, col_pos, true)
            END AS "expression?",
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
        JOIN pg_class i ON idx.indexrelid = i.oid
        JOIN pg_namespace n ON i.relnamespace = n.oid
        JOIN pg_class t ON idx.indrelid = t.oid
        JOIN pg_namespace tn ON t.relnamespace = tn.oid
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

async fn fetch_dependencies(conn: &mut PgConnection) -> Result<Vec<RawReference>> {
    let rows = sqlx::query!(
        r#"
        SELECT DISTINCT
            d.objid AS "index_oid!",
            cl.relname AS "ref_class!",
            d.refobjid AS "ref_oid!",
            p.pronamespace AS "function_namespace?",
            p.proname AS "function_name?",
            pg_catalog.pg_get_function_identity_arguments(p.oid) AS "function_args?"
        FROM pg_depend d
        JOIN pg_index idx ON idx.indexrelid = d.objid
        JOIN pg_class cl ON cl.oid = d.refclassid
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
        .map(|row| RawReference {
            source_oid: row.index_oid,
            ref_class: row.ref_class,
            ref_oid: row.ref_oid,
            function_namespace: row.function_namespace,
            function_name: row.function_name,
            function_args: row.function_args,
            operator_namespace: None,
            operator_name: None,
            operator_left_type: None,
            operator_right_type: None,
        })
        .collect())
}
