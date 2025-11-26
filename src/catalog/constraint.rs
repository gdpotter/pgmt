//! Constraint catalog - fetch table constraints from PostgreSQL system catalogs
use anyhow::Result;
use sqlx::postgres::PgConnection;
use tracing::info;

use super::comments::Commentable;
use super::id::{DbObjectId, DependsOn};

/* ---------- Data structures ---------- */

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConstraintType {
    Unique {
        columns: Vec<String>,
    },
    ForeignKey {
        columns: Vec<String>,
        referenced_schema: String,
        referenced_table: String,
        referenced_columns: Vec<String>,
        on_delete: Option<String>,
        on_update: Option<String>,
        deferrable: bool,
        initially_deferred: bool,
    },
    Check {
        expression: String,
    },
    Exclusion {
        elements: Vec<String>,
        operator_classes: Vec<String>,
        operators: Vec<String>,
        index_method: String,
        predicate: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct Constraint {
    pub schema: String,
    pub table: String,
    pub name: String,
    pub constraint_type: ConstraintType,
    pub comment: Option<String>,
    pub depends_on: Vec<DbObjectId>,
}

impl Constraint {
    pub fn id(&self) -> DbObjectId {
        DbObjectId::Constraint {
            schema: self.schema.clone(),
            table: self.table.clone(),
            name: self.name.clone(),
        }
    }
}

impl DependsOn for Constraint {
    fn id(&self) -> DbObjectId {
        self.id()
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}

impl Commentable for Constraint {
    fn comment(&self) -> &Option<String> {
        &self.comment
    }
}

/* ---------- Fetch queries ---------- */

#[derive(Debug)]
struct ConstraintRow {
    schema_name: String,
    table_name: String,
    constraint_name: String,
    constraint_type: String,
    column_names: Option<Vec<String>>,
    foreign_schema: Option<String>,
    foreign_table: Option<String>,
    foreign_columns: Option<Vec<String>>,
    on_delete: Option<String>,
    on_update: Option<String>,
    deferrable: bool,
    initially_deferred: bool,
    check_clause: Option<String>,
    exclusion_elements: Option<Vec<String>>,
    exclusion_opcnames: Option<Vec<String>>,
    exclusion_operators: Option<Vec<String>>,
    index_method: Option<String>,
    predicate: Option<String>,
    constraint_comment: Option<String>,
}

async fn fetch_all_constraints(conn: &mut PgConnection) -> Result<Vec<ConstraintRow>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            n.nspname AS schema_name,
            cl.relname AS table_name,
            c.conname AS constraint_name,
            c.contype::text AS "constraint_type!",
            CASE
                WHEN c.contype IN ('u', 'f') THEN
                    ARRAY(
                        SELECT a.attname
                        FROM pg_attribute a
                        WHERE a.attrelid = c.conrelid
                          AND a.attnum = ANY(c.conkey)
                        ORDER BY array_position(c.conkey, a.attnum)
                    )
                ELSE NULL
            END AS "column_names?",

            -- Foreign key specific fields
            fn.nspname AS "foreign_schema?",
            fcl.relname AS "foreign_table?",
            CASE
                WHEN c.contype = 'f' THEN
                    ARRAY(
                        SELECT a.attname
                        FROM pg_attribute a
                        WHERE a.attrelid = c.confrelid
                          AND a.attnum = ANY(c.confkey)
                        ORDER BY array_position(c.confkey, a.attnum)
                    )
                ELSE NULL
            END AS "foreign_columns?",

            CASE c.confdeltype
                WHEN 'a' THEN NULL  -- NO ACTION (default)
                WHEN 'r' THEN 'RESTRICT'
                WHEN 'c' THEN 'CASCADE'
                WHEN 'n' THEN 'SET NULL'
                WHEN 'd' THEN 'SET DEFAULT'
                ELSE NULL
            END AS "on_delete?",

            CASE c.confupdtype
                WHEN 'a' THEN NULL  -- NO ACTION (default)
                WHEN 'r' THEN 'RESTRICT'
                WHEN 'c' THEN 'CASCADE'
                WHEN 'n' THEN 'SET NULL'
                WHEN 'd' THEN 'SET DEFAULT'
                ELSE NULL
            END AS "on_update?",

            c.condeferrable AS "deferrable!",
            c.condeferred AS "initially_deferred!",

            -- Check constraint specific
            CASE
                WHEN c.contype = 'c' THEN pg_get_constraintdef(c.oid, true)
                ELSE NULL
            END AS "check_clause?",

            -- Exclusion constraint specific
            CASE
                WHEN c.contype = 'x' THEN
                    ARRAY(
                        SELECT pg_get_indexdef(idx.indexrelid, col_pos, true)
                        FROM pg_index idx
                        CROSS JOIN generate_series(1, idx.indnatts) AS col_pos
                        WHERE idx.indexrelid = c.conindid
                        ORDER BY col_pos
                    )
                ELSE NULL
            END AS "exclusion_elements?",

            CASE
                WHEN c.contype = 'x' THEN
                    ARRAY(
                        SELECT opc.opcname
                        FROM pg_index idx
                        JOIN pg_opclass opc ON opc.oid = ANY(idx.indclass)
                        WHERE idx.indexrelid = c.conindid
                        ORDER BY opc.opcname
                    )
                ELSE NULL
            END AS "exclusion_opcnames?",

            CASE
                WHEN c.contype = 'x' THEN
                    ARRAY(
                        SELECT po.oprname
                        FROM pg_constraint exc
                        JOIN pg_operator po ON po.oid = ANY(exc.conexclop)
                        WHERE exc.oid = c.oid
                        ORDER BY po.oprname
                    )
                ELSE NULL
            END AS "exclusion_operators?",

            CASE
                WHEN c.contype = 'x' THEN
                    (SELECT am.amname
                     FROM pg_index idx
                     JOIN pg_class idx_cl ON idx.indexrelid = idx_cl.oid
                     JOIN pg_am am ON idx_cl.relam = am.oid
                     WHERE idx.indexrelid = c.conindid)
                ELSE NULL
            END AS "index_method?",

            CASE
                WHEN c.contype = 'x' THEN
                    (SELECT pg_get_expr(idx.indpred, idx.indrelid, true)
                     FROM pg_index idx
                     WHERE idx.indexrelid = c.conindid AND idx.indpred IS NOT NULL)
                ELSE NULL
            END AS "predicate?",

            d.description AS "constraint_comment?"

        FROM pg_constraint c
        JOIN pg_class cl ON c.conrelid = cl.oid
        JOIN pg_namespace n ON cl.relnamespace = n.oid
        LEFT JOIN pg_class fcl ON c.confrelid = fcl.oid
        LEFT JOIN pg_namespace fn ON fcl.relnamespace = fn.oid
        LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND cl.relkind = 'r'  -- Only regular tables
          AND c.contype IN ('u', 'f', 'c', 'x')  -- Unique, Foreign, Check, Exclusion (Primary keys handled by table catalog)
        ORDER BY n.nspname, cl.relname, c.conname
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| ConstraintRow {
            schema_name: r.schema_name,
            table_name: r.table_name,
            constraint_name: r.constraint_name,
            constraint_type: r.constraint_type,
            column_names: r.column_names,
            foreign_schema: r.foreign_schema,
            foreign_table: r.foreign_table,
            foreign_columns: r.foreign_columns,
            on_delete: r.on_delete,
            on_update: r.on_update,
            deferrable: r.deferrable,
            initially_deferred: r.initially_deferred,
            check_clause: r.check_clause,
            exclusion_elements: r.exclusion_elements,
            exclusion_opcnames: r.exclusion_opcnames,
            exclusion_operators: r.exclusion_operators,
            index_method: r.index_method,
            predicate: r.predicate,
            constraint_comment: r.constraint_comment,
        })
        .collect())
}

fn build_constraint_from_row(row: ConstraintRow) -> Result<Constraint> {
    let mut depends_on = vec![DbObjectId::Table {
        schema: row.schema_name.clone(),
        name: row.table_name.clone(),
    }];

    let constraint_type = match row.constraint_type.as_str() {
        "u" => ConstraintType::Unique {
            columns: row.column_names.unwrap_or_default(),
        },
        "f" => {
            let referenced_schema = row.foreign_schema.unwrap_or_default();
            let referenced_table = row.foreign_table.unwrap_or_default();

            // Add dependency on referenced table
            depends_on.push(DbObjectId::Table {
                schema: referenced_schema.clone(),
                name: referenced_table.clone(),
            });

            ConstraintType::ForeignKey {
                columns: row.column_names.unwrap_or_default(),
                referenced_schema,
                referenced_table,
                referenced_columns: row.foreign_columns.unwrap_or_default(),
                on_delete: row.on_delete,
                on_update: row.on_update,
                deferrable: row.deferrable,
                initially_deferred: row.initially_deferred,
            }
        }
        "c" => ConstraintType::Check {
            expression: row.check_clause.unwrap_or_default(),
        },
        "x" => ConstraintType::Exclusion {
            elements: row.exclusion_elements.unwrap_or_default(),
            operator_classes: row.exclusion_opcnames.unwrap_or_default(),
            operators: row.exclusion_operators.unwrap_or_default(),
            index_method: row.index_method.unwrap_or_default(),
            predicate: row.predicate,
        },
        _ => {
            return Err(anyhow::anyhow!(
                "Unknown constraint type: {}",
                row.constraint_type
            ));
        }
    };

    Ok(Constraint {
        schema: row.schema_name,
        table: row.table_name,
        name: row.constraint_name,
        constraint_type,
        comment: row.constraint_comment,
        depends_on,
    })
}

pub async fn fetch(conn: &mut PgConnection) -> Result<Vec<Constraint>> {
    info!("Fetching constraints...");
    let constraint_rows = fetch_all_constraints(&mut *conn).await?;

    let mut constraints = Vec::new();
    for row in constraint_rows {
        constraints.push(build_constraint_from_row(row)?);
    }

    Ok(constraints)
}
