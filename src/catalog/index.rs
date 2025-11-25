//! Fetch indexes via pg_catalog with support for all index types and features
use anyhow::Result;
use sqlx::PgPool;

use super::comments::Commentable;
use super::id::{DbObjectId, DependsOn};

/* ---------- Data structures ---------- */

#[derive(Debug, Clone, PartialEq)]
pub enum IndexType {
    Btree,
    Hash,
    Gist,
    Gin,
    Spgist,
    Brin,
    Custom(String), // For extension-provided index types
}

impl IndexType {
    fn from_string(s: &str) -> Self {
        match s {
            "btree" => IndexType::Btree,
            "hash" => IndexType::Hash,
            "gist" => IndexType::Gist,
            "gin" => IndexType::Gin,
            "spgist" => IndexType::Spgist,
            "brin" => IndexType::Brin,
            _ => IndexType::Custom(s.to_string()),
        }
    }
}

impl std::fmt::Display for IndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexType::Btree => write!(f, "btree"),
            IndexType::Hash => write!(f, "hash"),
            IndexType::Gist => write!(f, "gist"),
            IndexType::Gin => write!(f, "gin"),
            IndexType::Spgist => write!(f, "spgist"),
            IndexType::Brin => write!(f, "brin"),
            IndexType::Custom(s) => write!(f, "{}", s),
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndexColumn {
    pub expression: String, // Column name or expression
    pub collation: Option<String>,
    pub opclass: Option<String>,        // Operator class
    pub ordering: Option<String>,       // ASC/DESC for btree indexes
    pub nulls_ordering: Option<String>, // NULLS FIRST/LAST
}

#[derive(Debug, Clone)]
pub struct Index {
    pub schema: String,
    pub name: String,
    pub table_schema: String,
    pub table_name: String,
    pub index_type: IndexType,
    pub is_unique: bool,
    pub is_clustered: bool,
    pub is_valid: bool,
    pub columns: Vec<IndexColumn>,
    pub include_columns: Vec<String>, // INCLUDE columns for covering indexes
    pub predicate: Option<String>,    // WHERE clause for partial indexes
    pub tablespace: Option<String>,
    pub storage_parameters: Vec<(String, String)>, // WITH (...) options
    pub comment: Option<String>,
    pub depends_on: Vec<DbObjectId>,
}

impl Index {
    pub fn id(&self) -> DbObjectId {
        DbObjectId::Index {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }
}

impl DependsOn for Index {
    fn id(&self) -> DbObjectId {
        self.id()
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}

impl Commentable for Index {
    fn comment(&self) -> &Option<String> {
        &self.comment
    }
}

/* ---------- Fetch query (catalog-based) ---------- */

#[derive(Debug)]
struct IndexRow {
    schema: String,
    name: String,
    table_schema: String,
    table_name: String,
    index_type: String,
    is_unique: bool,
    is_clustered: bool,
    is_valid: bool,
    predicate: Option<String>,
    tablespace: Option<String>,
    comment: Option<String>,
}

async fn fetch_all_indexes(pool: &PgPool) -> Result<Vec<IndexRow>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            n.nspname AS "schema!",
            i.relname AS "name!",
            tn.nspname AS "table_schema!",
            t.relname AS "table_name!",
            am.amname AS "index_type!",
            idx.indisunique AS "is_unique!",
            idx.indisclustered AS "is_clustered!",
            idx.indisvalid AS "is_valid!",
            pg_catalog.pg_get_expr(idx.indpred, idx.indrelid) AS "predicate?",
            ts.spcname AS "tablespace?",
            d.description AS "comment?"
        FROM pg_index idx
        JOIN pg_class i ON idx.indexrelid = i.oid
        JOIN pg_namespace n ON i.relnamespace = n.oid
        JOIN pg_class t ON idx.indrelid = t.oid
        JOIN pg_namespace tn ON t.relnamespace = tn.oid
        JOIN pg_am am ON i.relam = am.oid
        LEFT JOIN pg_tablespace ts ON i.reltablespace = ts.oid
        LEFT JOIN pg_description d ON d.objoid = i.oid AND d.objsubid = 0
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND tn.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND NOT idx.indisprimary  -- We handle primary keys separately
          AND NOT EXISTS (  -- Exclude indexes that back constraints (handled by constraint catalog)
              SELECT 1 FROM pg_constraint c WHERE c.conindid = idx.indexrelid
          )
          -- Exclude indexes that belong to extensions
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend dep
              WHERE dep.objid = i.oid
              AND dep.deptype = 'e'
          )
        ORDER BY n.nspname, i.relname
        "#
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| IndexRow {
            schema: r.schema,
            name: r.name,
            table_schema: r.table_schema,
            table_name: r.table_name,
            index_type: r.index_type,
            is_unique: r.is_unique,
            is_clustered: r.is_clustered,
            is_valid: r.is_valid,
            predicate: r.predicate,
            tablespace: r.tablespace,
            comment: r.comment,
        })
        .collect())
}

#[derive(Debug)]
struct IndexColumnRow {
    schema: String,
    name: String,
    expression: String,
    collation: Option<String>,
    opclass: Option<String>,
    ordering: Option<String>,
    nulls_ordering: Option<String>,
    is_included: bool,
}

async fn fetch_index_columns(pool: &PgPool) -> Result<Vec<IndexColumnRow>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            n.nspname AS "schema!",
            i.relname AS "name!",
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
        JOIN pg_class i ON idx.indexrelid = i.oid
        JOIN pg_namespace n ON i.relnamespace = n.oid
        JOIN pg_class t ON idx.indrelid = t.oid
        JOIN pg_namespace tn ON t.relnamespace = tn.oid
        CROSS JOIN generate_series(1, idx.indnatts) AS col_pos
        LEFT JOIN pg_attribute a ON a.attrelid = t.oid
                                 AND a.attnum = idx.indkey[col_pos-1]
                                 AND idx.indkey[col_pos-1] > 0
        LEFT JOIN pg_collation c ON a.attcollation = c.oid
        LEFT JOIN pg_namespace cn ON c.collnamespace = cn.oid
        LEFT JOIN pg_opclass op ON col_pos <= array_length(idx.indclass, 1)
                                AND idx.indclass[col_pos-1] = op.oid
        LEFT JOIN pg_namespace opn ON op.opcnamespace = opn.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND tn.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND NOT idx.indisprimary
        ORDER BY n.nspname, i.relname, col_pos
        "#
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| IndexColumnRow {
            schema: r.schema,
            name: r.name,
            expression: r.expression,
            collation: r.collation,
            opclass: r.opclass,
            ordering: Some(r.ordering),
            nulls_ordering: Some(r.nulls_ordering),
            is_included: r.is_included,
        })
        .collect())
}

async fn fetch_index_storage_parameters(
    pool: &PgPool,
) -> Result<std::collections::BTreeMap<(String, String), Vec<(String, String)>>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            n.nspname AS "schema!",
            i.relname AS "name!",
            unnest(i.reloptions) AS "option_string?"
        FROM pg_class i
        JOIN pg_namespace n ON i.relnamespace = n.oid
        JOIN pg_index idx ON idx.indexrelid = i.oid
        JOIN pg_class t ON idx.indrelid = t.oid
        JOIN pg_namespace tn ON t.relnamespace = tn.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND tn.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND i.reloptions IS NOT NULL
          AND NOT idx.indisprimary
        "#
    )
    .fetch_all(pool)
    .await?;

    let mut storage_params: std::collections::BTreeMap<(String, String), Vec<(String, String)>> =
        std::collections::BTreeMap::new();

    for row in rows {
        if let Some(option_str) = row.option_string
            && let Some((key, value)) = option_str.split_once('=')
        {
            storage_params
                .entry((row.schema, row.name))
                .or_default()
                .push((key.to_string(), value.to_string()));
        }
    }

    Ok(storage_params)
}

async fn fetch_index_dependencies(
    pool: &PgPool,
) -> Result<std::collections::BTreeMap<(String, String), Vec<DbObjectId>>> {
    // Fetch dependencies on custom types and functions used in index expressions
    let deps = sqlx::query!(
        r#"
        SELECT DISTINCT
            n.nspname AS "index_schema!",
            i.relname AS "index_name!",
            CASE d.refclassid::regclass::text
                WHEN 'pg_type' THEN 'type'
                WHEN 'pg_proc' THEN 'function'
                ELSE 'unknown'
            END AS "dep_type!",
            dn.nspname AS "dep_schema!",
            CASE d.refclassid::regclass::text
                WHEN 'pg_type' THEN t.typname
                WHEN 'pg_proc' THEN p.proname
                ELSE 'unknown'
            END AS "dep_name!"
        FROM pg_depend d
        JOIN pg_class i ON d.objid = i.oid
        JOIN pg_namespace n ON i.relnamespace = n.oid
        JOIN pg_index idx ON idx.indexrelid = i.oid
        LEFT JOIN pg_type t ON d.refclassid = 'pg_type'::regclass AND d.refobjid = t.oid
        LEFT JOIN pg_namespace tn ON t.typnamespace = tn.oid
        LEFT JOIN pg_proc p ON d.refclassid = 'pg_proc'::regclass AND d.refobjid = p.oid
        LEFT JOIN pg_namespace pn ON p.pronamespace = pn.oid
        LEFT JOIN pg_namespace dn ON COALESCE(tn.oid, pn.oid) = dn.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND d.refclassid IN ('pg_type'::regclass, 'pg_proc'::regclass)
          AND dn.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND NOT idx.indisprimary
        "#
    )
    .fetch_all(pool)
    .await?;

    let mut dep_map: std::collections::BTreeMap<(String, String), Vec<DbObjectId>> =
        std::collections::BTreeMap::new();

    for row in deps {
        let key = (row.index_schema, row.index_name);
        let dep_id = match row.dep_type.as_str() {
            "type" => DbObjectId::Type {
                schema: row.dep_schema,
                name: row.dep_name,
            },
            "function" => DbObjectId::Function {
                schema: row.dep_schema,
                name: row.dep_name,
            },
            _ => continue,
        };
        dep_map.entry(key).or_default().push(dep_id);
    }

    Ok(dep_map)
}

pub async fn fetch(pool: &PgPool) -> Result<Vec<Index>> {
    let index_rows = fetch_all_indexes(pool).await?;
    let column_rows = fetch_index_columns(pool).await?;
    let storage_params = fetch_index_storage_parameters(pool).await?;
    let index_deps = fetch_index_dependencies(pool).await?;

    // Group columns by index
    let mut column_map: std::collections::BTreeMap<(String, String), Vec<IndexColumnRow>> =
        std::collections::BTreeMap::new();
    for col in column_rows {
        column_map
            .entry((col.schema.clone(), col.name.clone()))
            .or_default()
            .push(col);
    }

    let mut indexes = Vec::new();

    for row in index_rows {
        let key = (row.schema.clone(), row.name.clone());
        let empty_columns = Vec::new();
        let columns_for_index = column_map.get(&key).unwrap_or(&empty_columns);

        // Separate regular columns from INCLUDE columns
        let mut regular_columns = Vec::new();
        let mut include_columns = Vec::new();

        for col in columns_for_index {
            if col.is_included {
                include_columns.push(col.expression.clone());
            } else {
                regular_columns.push(IndexColumn {
                    expression: col.expression.clone(),
                    collation: col.collation.clone(),
                    opclass: col.opclass.clone(),
                    ordering: if row.index_type == "btree" {
                        col.ordering.clone()
                    } else {
                        None
                    },
                    nulls_ordering: if row.index_type == "btree" {
                        col.nulls_ordering.clone()
                    } else {
                        None
                    },
                });
            }
        }

        // Build dependencies: table + any type/function dependencies
        let mut depends_on = vec![DbObjectId::Table {
            schema: row.table_schema.clone(),
            name: row.table_name.clone(),
        }];

        if let Some(deps) = index_deps.get(&key) {
            depends_on.extend(deps.clone());
        }

        indexes.push(Index {
            schema: row.schema.clone(),
            name: row.name.clone(),
            table_schema: row.table_schema,
            table_name: row.table_name,
            index_type: IndexType::from_string(&row.index_type),
            is_unique: row.is_unique,
            is_clustered: row.is_clustered,
            is_valid: row.is_valid,
            columns: regular_columns,
            include_columns,
            predicate: row.predicate,
            tablespace: row.tablespace,
            storage_parameters: storage_params.get(&key).cloned().unwrap_or_default(),
            comment: row.comment,
            depends_on,
        });
    }

    Ok(indexes)
}
