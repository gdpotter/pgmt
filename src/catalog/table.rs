//! Fetch tables + columns (no constraints yet) via pg_catalog for BASE TABLEs
use anyhow::Result;
use sqlx::postgres::PgConnection;
use tracing::info;

use super::comments::Commentable;
use super::id::{DbObjectId, DependsOn};
use super::utils::is_system_schema;
use crate::render::quote_ident;
use itertools::Itertools;

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: String,
    pub default: Option<String>,
    pub not_null: bool,
    pub generated: Option<String>,
    pub comment: Option<String>,
    /// Dependencies for this column (e.g., functions used in generated expression)
    pub depends_on: Vec<DbObjectId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimaryKey {
    pub name: String,
    pub columns: Vec<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub schema: String,
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: Option<PrimaryKey>,
    pub comment: Option<String>,

    table_dependencies: Vec<DbObjectId>,

    all_dependencies: Vec<DbObjectId>,
}

impl Table {
    pub fn id(&self) -> DbObjectId {
        DbObjectId::Table {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }

    /// Compute all dependencies by aggregating table + unique column dependencies
    fn compute_all_dependencies(&self) -> Vec<DbObjectId> {
        let mut all_deps = self.table_dependencies.clone();

        for column in &self.columns {
            for col_dep in &column.depends_on {
                if !all_deps.contains(col_dep) {
                    all_deps.push(col_dep.clone());
                }
            }
        }

        all_deps
    }

    /// Update the computed all_dependencies field after columns have changed
    pub fn update_all_dependencies(&mut self) {
        self.all_dependencies = self.compute_all_dependencies();
    }

    /// Create a new Table with proper dependency computation
    #[allow(dead_code)]
    pub fn new(
        schema: String,
        name: String,
        columns: Vec<Column>,
        primary_key: Option<PrimaryKey>,
        comment: Option<String>,
        table_dependencies: Vec<DbObjectId>,
    ) -> Self {
        let mut table = Self {
            schema,
            name,
            columns,
            primary_key,
            comment,
            table_dependencies: table_dependencies.clone(),
            all_dependencies: table_dependencies,
        };
        table.update_all_dependencies();
        table
    }
}

impl DependsOn for Table {
    fn id(&self) -> DbObjectId {
        self.id()
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.all_dependencies
    }
}

impl Commentable for Table {
    fn comment(&self) -> &Option<String> {
        &self.comment
    }
}

async fn fetch_all_tables(
    conn: &mut PgConnection,
) -> Result<Vec<(String, String, Option<String>)>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            n.nspname AS table_schema,
            c.relname AS table_name,
            d.description AS "table_comment?"
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0
        WHERE c.relkind = 'r'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend dep
              WHERE dep.objid = c.oid
              AND dep.deptype = 'e'
          )
        ORDER BY n.nspname, c.relname
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| (r.table_schema, r.table_name, r.table_comment))
        .collect())
}

#[derive(Debug)]
struct ColumnRow {
    table_schema: String,
    table_name: String,
    column_name: String,
    data_type: String,
    type_schema: Option<String>,
    type_name: Option<String>,
    column_expr: Option<String>,
    attgenerated: Option<String>,
    not_null: bool,
    attndims: i32,
    column_comment: Option<String>,
    is_extension_type: bool,
    extension_name: Option<String>,
    type_typtype: Option<String>,
}

async fn fetch_table_columns(conn: &mut PgConnection) -> Result<Vec<ColumnRow>> {
    let rows = sqlx::query!(
        r#"
        SELECT
          n.nspname    AS table_schema,
          c.relname    AS table_name,
          a.attname    AS column_name,
          pg_catalog.format_type(a.atttypid, a.atttypmod) AS "data_type!",
          -- Resolve array element type schema/name correctly
          CASE
            WHEN t.typelem != 0 THEN elem_tn.nspname
            ELSE tn.nspname
          END AS "type_schema?",
          CASE
            WHEN t.typelem != 0 THEN elem_t.typname
            ELSE t.typname
          END AS "type_name?",
          pg_catalog.pg_get_expr(ad.adbin, ad.adrelid)  AS column_expr,
          a.attgenerated::text AS attgenerated,
          a.attnotnull AS "not_null!",
          COALESCE(a.attndims, 0)::int AS "attndims!: i32",
          d.description AS "column_comment?",
          -- Check if the type (or element type for arrays) is from an extension
          ext_types.extname IS NOT NULL AS "is_extension_type!: bool",
          ext_types.extname AS "extension_name?",
          -- Get typtype to distinguish domains ('d') from other types
          CASE
            WHEN t.typelem != 0 THEN elem_t.typtype::text
            ELSE t.typtype::text
          END AS "type_typtype?"
        FROM pg_attribute a
        LEFT JOIN pg_attrdef ad
          ON a.attrelid = ad.adrelid
         AND a.attnum   = ad.adnum
        LEFT JOIN pg_type t ON a.atttypid = t.oid
        LEFT JOIN pg_namespace tn ON t.typnamespace = tn.oid
        -- Element type for array types
        LEFT JOIN pg_type elem_t ON t.typelem = elem_t.oid AND t.typelem != 0
        LEFT JOIN pg_namespace elem_tn ON elem_t.typnamespace = elem_tn.oid
        LEFT JOIN pg_description d ON d.objoid = a.attrelid AND d.objsubid = a.attnum
        -- Extension type lookup: compute once as derived table, then hash join
        LEFT JOIN (
          SELECT DISTINCT dep.objid AS type_oid, e.extname
          FROM pg_depend dep
          JOIN pg_extension e ON dep.refobjid = e.oid
          WHERE dep.deptype = 'e'
        ) ext_types ON ext_types.type_oid = COALESCE(NULLIF(t.typelem, 0::oid), t.oid)
        JOIN pg_class c
          ON a.attrelid = c.oid
        JOIN pg_namespace n
          ON c.relnamespace = n.oid
        WHERE a.attnum > 0
          AND NOT a.attisdropped
          AND c.relkind = 'r'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        ORDER BY n.nspname, c.relname, a.attnum
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| ColumnRow {
            table_schema: r.table_schema,
            table_name: r.table_name,
            column_name: r.column_name,
            data_type: r.data_type,
            type_schema: r.type_schema,
            type_name: r.type_name,
            column_expr: r.column_expr,
            attgenerated: r.attgenerated,
            not_null: r.not_null,
            attndims: r.attndims,
            column_comment: r.column_comment,
            is_extension_type: r.is_extension_type,
            extension_name: r.extension_name,
            type_typtype: r.type_typtype,
        })
        .collect())
}

async fn fetch_sequence_dependencies(
    conn: &mut PgConnection,
) -> Result<std::collections::BTreeMap<(String, String, String), Vec<DbObjectId>>> {
    let sequence_deps = sqlx::query!(
        r#"
        SELECT
            n.nspname AS table_schema,
            c.relname AS table_name,
            a.attname AS column_name,
            seq_c.relname AS sequence_name,
            seq_n.nspname AS sequence_schema
        FROM pg_depend d
        JOIN pg_attrdef ad ON d.objid = ad.oid
        JOIN pg_attribute a ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
        JOIN pg_class c ON a.attrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_class seq_c ON d.refobjid = seq_c.oid
        JOIN pg_namespace seq_n ON seq_c.relnamespace = seq_n.oid
        WHERE d.refclassid = 'pg_class'::regclass
          AND seq_c.relkind = 'S'  -- Only sequences
          AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    let mut sequence_dep_map: std::collections::BTreeMap<
        (String, String, String),
        Vec<DbObjectId>,
    > = std::collections::BTreeMap::new();
    for row in sequence_deps {
        sequence_dep_map
            .entry((row.table_schema, row.table_name, row.column_name))
            .or_default()
            .push(DbObjectId::Sequence {
                schema: row.sequence_schema,
                name: row.sequence_name,
            });
    }

    Ok(sequence_dep_map)
}

async fn fetch_function_dependencies(
    conn: &mut PgConnection,
) -> Result<std::collections::BTreeMap<(String, String, String), Vec<DbObjectId>>> {
    // Query for function dependencies in both generated columns AND default expressions
    // PostgreSQL changed how it tracks these dependencies between versions:
    // - v12-14: Dependencies are on the column itself (pg_attribute)
    // - v15+: Dependencies are on pg_attrdef
    // This query handles both cases with a UNION ALL
    let function_deps = sqlx::query!(
        r#"
        -- PostgreSQL 15+ (including 16, 17): dependencies are on pg_attrdef (NORMAL)
        SELECT DISTINCT
            n.nspname AS "table_schema!",
            c.relname AS "table_name!",
            a.attname AS "column_name!",
            pf.proname AS "function_name!",
            nf.nspname AS "function_schema!",
            pg_catalog.pg_get_function_identity_arguments(pf.oid) AS "function_args!",
            (
                SELECT e.extname
                FROM pg_depend ext_dep
                JOIN pg_extension e ON ext_dep.refobjid = e.oid
                WHERE ext_dep.objid = pf.oid
                AND ext_dep.deptype = 'e'
                LIMIT 1
            ) AS "extension_name?"
        FROM pg_depend d
        JOIN pg_attrdef ad ON d.classid = 'pg_attrdef'::regclass AND d.objid = ad.oid
        JOIN pg_attribute a ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_proc pf ON d.refclassid = 'pg_proc'::regclass AND d.refobjid = pf.oid
        JOIN pg_namespace nf ON pf.pronamespace = nf.oid
        WHERE (a.attgenerated = 's' OR a.atthasdef = true)  -- STORED generated columns OR default expressions
          AND d.deptype = 'n'  -- NORMAL dependency in v15+
          AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND nf.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')

        UNION ALL

        -- PostgreSQL 12-14: dependencies are on the column (AUTO)
        SELECT DISTINCT
            n.nspname AS "table_schema!",
            c.relname AS "table_name!",
            a.attname AS "column_name!",
            pf.proname AS "function_name!",
            nf.nspname AS "function_schema!",
            pg_catalog.pg_get_function_identity_arguments(pf.oid) AS "function_args!",
            (
                SELECT e.extname
                FROM pg_depend ext_dep
                JOIN pg_extension e ON ext_dep.refobjid = e.oid
                WHERE ext_dep.objid = pf.oid
                AND ext_dep.deptype = 'e'
                LIMIT 1
            ) AS "extension_name?"
        FROM pg_depend d
        JOIN pg_attribute a ON d.classid = 'pg_class'::regclass
                             AND d.objid = a.attrelid
                             AND d.objsubid = a.attnum
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_proc pf ON d.refclassid = 'pg_proc'::regclass AND d.refobjid = pf.oid
        JOIN pg_namespace nf ON pf.pronamespace = nf.oid
        WHERE (a.attgenerated = 's' OR a.atthasdef = true)  -- STORED generated columns OR default expressions
          AND d.deptype = 'a'  -- AUTO dependency in pre-v15
          AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND nf.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    let mut function_dep_map: std::collections::BTreeMap<
        (String, String, String),
        Vec<DbObjectId>,
    > = std::collections::BTreeMap::new();
    for row in function_deps {
        function_dep_map
            .entry((row.table_schema, row.table_name, row.column_name))
            .or_default()
            .push(
                // If function is from an extension, depend on the extension instead
                if let Some(ext_name) = row.extension_name {
                    DbObjectId::Extension { name: ext_name }
                } else {
                    DbObjectId::Function {
                        schema: row.function_schema,
                        name: row.function_name,
                        arguments: row.function_args,
                    }
                },
            );
    }

    Ok(function_dep_map)
}

fn initialize_tables(
    all_tables: Vec<(String, String, Option<String>)>,
) -> (
    Vec<Table>,
    std::collections::BTreeMap<(String, String), usize>,
) {
    let mut tables = Vec::new();
    let mut table_index_map = std::collections::BTreeMap::new();

    for (idx, (schema, name, comment)) in all_tables.into_iter().enumerate() {
        table_index_map.insert((schema.clone(), name.clone()), idx);
        let table_deps = vec![DbObjectId::Schema {
            name: schema.clone(),
        }];
        tables.push(Table {
            schema: schema.clone(),
            name,
            columns: Vec::new(),
            primary_key: None,
            comment,
            table_dependencies: table_deps.clone(),
            all_dependencies: table_deps, // Initially same as table_dependencies
        });
    }

    (tables, table_index_map)
}

fn populate_columns(
    tables: &mut [Table],
    rows: Vec<ColumnRow>,
    table_index_map: &std::collections::BTreeMap<(String, String), usize>,
    function_dep_map: std::collections::BTreeMap<(String, String, String), Vec<DbObjectId>>,
    sequence_dep_map: std::collections::BTreeMap<(String, String, String), Vec<DbObjectId>>,
) {
    for ((schema, table), group) in &rows
        .into_iter()
        .chunk_by(|r| (r.table_schema.clone(), r.table_name.clone()))
    {
        let table_idx = match table_index_map.get(&(schema.clone(), table.clone())) {
            Some(&idx) => idx,
            None => continue, // Skip if table not found (shouldn't happen)
        };

        let columns = group
            .map(|r| {
                let mut column_depends_on = Vec::new();

                // type_name is already resolved to the element type for arrays via SQL
                let base_type_name = r.type_name.clone();

                // Track dependencies based on type source
                if r.is_extension_type {
                    // For extension types, depend on the extension itself
                    if let Some(ext_name) = &r.extension_name {
                        column_depends_on.push(DbObjectId::Extension {
                            name: ext_name.clone(),
                        });
                    }
                } else if let (Some(type_schema), Some(ref base_type_name)) =
                    (r.type_schema.clone(), base_type_name.clone())
                    && !is_system_schema(&type_schema)
                {
                    // For user-defined types, depend on the type or domain
                    if r.type_typtype.as_deref() == Some("d") {
                        column_depends_on.push(DbObjectId::Domain {
                            schema: type_schema.clone(),
                            name: base_type_name.clone(),
                        });
                    } else {
                        column_depends_on.push(DbObjectId::Type {
                            schema: type_schema.clone(),
                            name: base_type_name.clone(),
                        });
                    }
                }

                if let Some(funcs) =
                    function_dep_map.get(&(schema.clone(), table.clone(), r.column_name.clone()))
                {
                    column_depends_on.extend(funcs.clone());
                }

                if let Some(seqs) =
                    sequence_dep_map.get(&(schema.clone(), table.clone(), r.column_name.clone()))
                {
                    column_depends_on.extend(seqs.clone());
                }

                Column {
                    name: r.column_name,
                    data_type: match (&r.type_schema, &base_type_name) {
                        (Some(type_schema), Some(base_type_name))
                            if !is_system_schema(type_schema) && !r.is_extension_type =>
                        {
                            // Only schema-qualify user-defined types, not extension types
                            format!(
                                "{}.{}{}",
                                quote_ident(type_schema),
                                quote_ident(base_type_name),
                                "[]".repeat(r.attndims as usize)
                            )
                        }
                        _ => r.data_type,
                    },
                    not_null: r.not_null,
                    generated: match r.attgenerated.as_deref() {
                        Some("s") => r.column_expr.clone(),
                        _ => None,
                    },
                    default: if r.attgenerated.as_deref() == Some("s") {
                        None
                    } else {
                        r.column_expr.clone()
                    },
                    comment: r.column_comment.clone(),
                    depends_on: column_depends_on,
                }
            })
            .collect::<Vec<_>>();

        tables[table_idx].columns = columns;

        tables[table_idx].update_all_dependencies();
    }
}

async fn populate_primary_keys(
    tables: &mut [Table],
    table_index_map: &std::collections::BTreeMap<(String, String), usize>,
    conn: &mut PgConnection,
) -> Result<()> {
    let pk_constraints = sqlx::query!(
        r#"
        SELECT
            c.conname AS constraint_name,
            n.nspname AS schema_name,
            cl.relname AS table_name,
            array_agg(a.attname ORDER BY array_position(c.conkey, a.attnum)) AS pk_columns,
            d.description AS "constraint_comment?"
        FROM pg_constraint c
        JOIN pg_class cl ON c.conrelid = cl.oid
        JOIN pg_namespace n ON cl.relnamespace = n.oid
        JOIN pg_attribute a ON
            a.attrelid = c.conrelid AND
            a.attnum = ANY(c.conkey)
        LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0
        WHERE
            c.contype = 'p' AND
            n.nspname NOT IN ('pg_catalog', 'information_schema')
        GROUP BY c.conname, n.nspname, cl.relname, d.description
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    for pk in pk_constraints {
        let table_idx = match table_index_map.get(&(pk.schema_name.clone(), pk.table_name.clone()))
        {
            Some(&idx) => idx,
            None => continue, // Skip if table not found (shouldn't happen)
        };

        let pk_columns = match pk.pk_columns {
            Some(columns) => columns,
            None => continue,
        };

        tables[table_idx].primary_key = Some(PrimaryKey {
            name: pk.constraint_name,
            columns: pk_columns,
            comment: pk.constraint_comment,
        });
    }

    Ok(())
}

pub async fn fetch(conn: &mut PgConnection) -> Result<Vec<Table>> {
    info!("Fetching tables...");
    let all_tables = fetch_all_tables(&mut *conn).await?;
    info!("Fetching table function dependencies...");
    let function_dep_map = fetch_function_dependencies(&mut *conn).await?;
    info!("Fetching table sequence dependencies...");
    let sequence_dep_map = fetch_sequence_dependencies(&mut *conn).await?;
    info!("Fetching table columns...");
    let column_rows = fetch_table_columns(&mut *conn).await?;

    let (mut tables, table_index_map) = initialize_tables(all_tables);
    populate_columns(
        &mut tables,
        column_rows,
        &table_index_map,
        function_dep_map,
        sequence_dep_map,
    );
    info!("Fetching primary keys...");
    populate_primary_keys(&mut tables, &table_index_map, &mut *conn).await?;

    Ok(tables)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::operations::{ColumnAction, MigrationStep, TableOperation};
    use crate::diff::tables::diff;

    fn make_test_table(
        schema: &str,
        name: &str,
        columns: Vec<(&str, &str, bool)>,
        pk: Option<(&str, Vec<&str>)>,
    ) -> Table {
        let columns = columns
            .into_iter()
            .map(|(name, data_type, not_null)| Column {
                name: name.to_string(),
                data_type: data_type.to_string(),
                default: None,
                generated: None,
                comment: None,
                depends_on: vec![],
                not_null,
            })
            .collect();

        let primary_key = pk.map(|(name, cols)| PrimaryKey {
            name: name.to_string(),
            columns: cols.into_iter().map(|s| s.to_string()).collect(),
            comment: None,
        });

        Table::new(
            schema.to_string(),
            name.to_string(),
            columns,
            primary_key,
            None,
            vec![DbObjectId::Schema {
                name: schema.to_string(),
            }],
        )
    }

    #[test]
    fn test_create_table_with_primary_key() {
        let table = make_test_table(
            "public",
            "users",
            vec![
                ("id", "serial", true),
                ("name", "text", true),
                ("email", "text", false),
            ],
            Some(("users_pkey", vec!["id"])),
        );

        let steps = diff(None, Some(&table));

        assert_eq!(steps.len(), 1);
        match &steps[0] {
            MigrationStep::Table(TableOperation::Create {
                schema,
                name: table_name,
                columns,
                primary_key,
            }) => {
                assert_eq!(schema, "public");
                assert_eq!(table_name, "users");
                assert_eq!(columns.len(), 3);

                assert!(primary_key.is_some());
                let pk = primary_key.as_ref().unwrap();
                assert_eq!(pk.name, "users_pkey");
                assert_eq!(pk.columns, vec!["id"]);
            }
            _ => panic!("Expected CreateTable step"),
        }
    }

    #[test]
    fn test_create_table_without_primary_key() {
        let table = make_test_table(
            "public",
            "logs",
            vec![("timestamp", "timestamp", true), ("message", "text", false)],
            None,
        );

        let steps = diff(None, Some(&table));

        assert_eq!(steps.len(), 1);
        match &steps[0] {
            MigrationStep::Table(TableOperation::Create {
                schema,
                name: table_name,
                columns,
                primary_key,
            }) => {
                assert_eq!(schema, "public");
                assert_eq!(table_name, "logs");
                assert_eq!(columns.len(), 2);
                assert!(primary_key.is_none());
            }
            _ => panic!("Expected CreateTable step"),
        }
    }

    #[test]
    fn test_add_primary_key() {
        let old_table = make_test_table(
            "public",
            "users",
            vec![("id", "serial", true), ("name", "text", true)],
            None,
        );

        let new_table = make_test_table(
            "public",
            "users",
            vec![("id", "serial", true), ("name", "text", true)],
            Some(("users_pkey", vec!["id"])),
        );

        let steps = diff(Some(&old_table), Some(&new_table));

        assert_eq!(steps.len(), 1);
        match &steps[0] {
            MigrationStep::Table(TableOperation::Alter {
                schema,
                name,
                actions,
            }) => {
                assert_eq!(schema, "public");
                assert_eq!(name, "users");
                assert_eq!(actions.len(), 1);

                match &actions[0] {
                    ColumnAction::AddPrimaryKey { constraint } => {
                        assert_eq!(constraint.name, "users_pkey");
                        assert_eq!(constraint.columns, vec!["id"]);
                    }
                    _ => panic!("Expected AddPrimaryKey action"),
                }
            }
            _ => panic!("Expected AlterTable step"),
        }
    }

    #[test]
    fn test_drop_primary_key() {
        let old_table = make_test_table(
            "public",
            "users",
            vec![("id", "serial", true), ("name", "text", true)],
            Some(("users_pkey", vec!["id"])),
        );

        let new_table = make_test_table(
            "public",
            "users",
            vec![("id", "serial", true), ("name", "text", true)],
            None,
        );

        let steps = diff(Some(&old_table), Some(&new_table));

        assert_eq!(steps.len(), 1);
        match &steps[0] {
            MigrationStep::Table(TableOperation::Alter {
                schema,
                name,
                actions,
            }) => {
                assert_eq!(schema, "public");
                assert_eq!(name, "users");
                assert_eq!(actions.len(), 1);

                match &actions[0] {
                    ColumnAction::DropPrimaryKey { name } => {
                        assert_eq!(name, "users_pkey");
                    }
                    _ => panic!("Expected DropPrimaryKey action"),
                }
            }
            _ => panic!("Expected AlterTable step"),
        }
    }

    #[test]
    fn test_change_primary_key() {
        let old_table = make_test_table(
            "public",
            "users",
            vec![
                ("id", "serial", true),
                ("name", "text", true),
                ("email", "text", true),
            ],
            Some(("users_pkey", vec!["id"])),
        );

        let new_table = make_test_table(
            "public",
            "users",
            vec![
                ("id", "serial", true),
                ("name", "text", true),
                ("email", "text", true),
            ],
            Some(("users_email_pkey", vec!["email"])),
        );

        let steps = diff(Some(&old_table), Some(&new_table));

        assert_eq!(steps.len(), 1);
        match &steps[0] {
            MigrationStep::Table(TableOperation::Alter {
                schema,
                name,
                actions,
            }) => {
                assert_eq!(schema, "public");
                assert_eq!(name, "users");
                assert_eq!(actions.len(), 2);

                match &actions[0] {
                    ColumnAction::DropPrimaryKey { name } => {
                        assert_eq!(name, "users_pkey");
                    }
                    _ => panic!("Expected DropPrimaryKey action first"),
                }

                match &actions[1] {
                    ColumnAction::AddPrimaryKey { constraint } => {
                        assert_eq!(constraint.name, "users_email_pkey");
                        assert_eq!(constraint.columns, vec!["email"]);
                    }
                    _ => panic!("Expected AddPrimaryKey action second"),
                }
            }
            _ => panic!("Expected AlterTable step"),
        }
    }

    #[test]
    fn test_compound_primary_key() {
        let table = make_test_table(
            "public",
            "order_items",
            vec![
                ("order_id", "integer", true),
                ("product_id", "integer", true),
                ("quantity", "integer", true),
            ],
            Some(("order_items_pkey", vec!["order_id", "product_id"])),
        );

        let steps = diff(None, Some(&table));

        assert_eq!(steps.len(), 1);
        match &steps[0] {
            MigrationStep::Table(TableOperation::Create {
                schema,
                name: table_name,
                columns: _,
                primary_key,
            }) => {
                assert_eq!(schema, "public");
                assert_eq!(table_name, "order_items");

                assert!(primary_key.is_some());
                let pk = primary_key.as_ref().unwrap();
                assert_eq!(pk.name, "order_items_pkey");
                assert_eq!(pk.columns, vec!["order_id", "product_id"]);
            }
            _ => panic!("Expected CreateTable step"),
        }
    }
}
