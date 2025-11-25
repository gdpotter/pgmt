//! Shared SQL rendering for CREATE TABLE statements
//!
//! This module provides consistent table rendering across both schema generation
//! and migration operations to ensure identical SQL output.

use crate::catalog::table::Table;
use crate::render::quote_ident;

/// Render a complete CREATE TABLE statement for the given table.
///
/// This function handles all PostgreSQL table features:
/// - Column definitions with data types
/// - NOT NULL constraints
/// - DEFAULT values
/// - Generated columns (GENERATED ALWAYS AS ... STORED)
/// - Primary key constraints (single and compound)
/// - Proper SQL formatting and identifier quoting
pub fn render_create_table(table: &Table) -> String {
    let mut sql = String::new();

    // CREATE TABLE schema.name
    sql.push_str("CREATE TABLE ");
    sql.push_str(&format!(
        "{}.{}",
        quote_ident(&table.schema),
        quote_ident(&table.name)
    ));
    sql.push_str(" (\n");

    // Column definitions
    let mut column_definitions = Vec::new();

    for column in &table.columns {
        let mut col_def = String::new();

        // Column name and data type
        col_def.push_str(&format!(
            "    {} {}",
            quote_ident(&column.name),
            column.data_type
        ));

        // Generated column expression (must come before default and not null)
        if let Some(ref generated_expr) = column.generated {
            col_def.push_str(&format!(" GENERATED ALWAYS AS ({}) STORED", generated_expr));
        }

        // Default value
        if let Some(ref default) = column.default {
            col_def.push_str(&format!(" DEFAULT {}", default));
        }

        // NOT NULL constraint
        if column.not_null {
            col_def.push_str(" NOT NULL");
        }

        column_definitions.push(col_def);
    }

    // Add primary key constraint if present
    if let Some(ref pk) = table.primary_key {
        let pk_columns = pk
            .columns
            .iter()
            .map(|col| quote_ident(col))
            .collect::<Vec<_>>()
            .join(", ");

        let pk_def = format!(
            "    CONSTRAINT {} PRIMARY KEY ({})",
            quote_ident(&pk.name),
            pk_columns
        );
        column_definitions.push(pk_def);
    }

    // Join all definitions
    sql.push_str(&column_definitions.join(",\n"));
    sql.push_str("\n);");

    sql
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::table::{Column, PrimaryKey};

    #[test]
    fn test_render_basic_table() {
        let table = Table::new(
            "public".to_string(),
            "users".to_string(),
            vec![
                Column {
                    name: "id".to_string(),
                    data_type: "integer".to_string(),
                    default: None,
                    not_null: true,
                    generated: None,
                    comment: None,
                    depends_on: vec![],
                },
                Column {
                    name: "email".to_string(),
                    data_type: "text".to_string(),
                    default: None,
                    not_null: true,
                    generated: None,
                    comment: None,
                    depends_on: vec![],
                },
            ],
            None,
            None,
            vec![],
        );

        let sql = render_create_table(&table);
        assert_eq!(
            sql,
            "CREATE TABLE \"public\".\"users\" (\n    \"id\" integer NOT NULL,\n    \"email\" text NOT NULL\n);"
        );
    }

    #[test]
    fn test_render_table_with_primary_key() {
        let table = Table::new(
            "public".to_string(),
            "users".to_string(),
            vec![
                Column {
                    name: "id".to_string(),
                    data_type: "integer".to_string(),
                    default: None,
                    not_null: true,
                    generated: None,
                    comment: None,
                    depends_on: vec![],
                },
                Column {
                    name: "email".to_string(),
                    data_type: "text".to_string(),
                    default: None,
                    not_null: true,
                    generated: None,
                    comment: None,
                    depends_on: vec![],
                },
            ],
            Some(PrimaryKey {
                name: "users_pkey".to_string(),
                columns: vec!["id".to_string()],
            }),
            None,
            vec![],
        );

        let sql = render_create_table(&table);
        assert_eq!(
            sql,
            "CREATE TABLE \"public\".\"users\" (\n    \"id\" integer NOT NULL,\n    \"email\" text NOT NULL,\n    CONSTRAINT \"users_pkey\" PRIMARY KEY (\"id\")\n);"
        );
    }

    #[test]
    fn test_render_table_with_compound_primary_key() {
        let table = Table::new(
            "public".to_string(),
            "user_roles".to_string(),
            vec![
                Column {
                    name: "user_id".to_string(),
                    data_type: "integer".to_string(),
                    default: None,
                    not_null: true,
                    generated: None,
                    comment: None,
                    depends_on: vec![],
                },
                Column {
                    name: "role_id".to_string(),
                    data_type: "integer".to_string(),
                    default: None,
                    not_null: true,
                    generated: None,
                    comment: None,
                    depends_on: vec![],
                },
            ],
            Some(PrimaryKey {
                name: "user_roles_pkey".to_string(),
                columns: vec!["user_id".to_string(), "role_id".to_string()],
            }),
            None,
            vec![],
        );

        let sql = render_create_table(&table);
        assert_eq!(
            sql,
            "CREATE TABLE \"public\".\"user_roles\" (\n    \"user_id\" integer NOT NULL,\n    \"role_id\" integer NOT NULL,\n    CONSTRAINT \"user_roles_pkey\" PRIMARY KEY (\"user_id\", \"role_id\")\n);"
        );
    }

    #[test]
    fn test_render_table_with_defaults_and_nullability() {
        let table = Table::new(
            "public".to_string(),
            "posts".to_string(),
            vec![
                Column {
                    name: "id".to_string(),
                    data_type: "integer".to_string(),
                    default: Some("nextval('posts_id_seq'::regclass)".to_string()),
                    not_null: true,
                    generated: None,
                    comment: None,
                    depends_on: vec![],
                },
                Column {
                    name: "title".to_string(),
                    data_type: "text".to_string(),
                    default: None,
                    not_null: true,
                    generated: None,
                    comment: None,
                    depends_on: vec![],
                },
                Column {
                    name: "content".to_string(),
                    data_type: "text".to_string(),
                    default: None,
                    not_null: false,
                    generated: None,
                    comment: None,
                    depends_on: vec![],
                },
                Column {
                    name: "created_at".to_string(),
                    data_type: "timestamp with time zone".to_string(),
                    default: Some("CURRENT_TIMESTAMP".to_string()),
                    not_null: true,
                    generated: None,
                    comment: None,
                    depends_on: vec![],
                },
            ],
            Some(PrimaryKey {
                name: "posts_pkey".to_string(),
                columns: vec!["id".to_string()],
            }),
            None,
            vec![],
        );

        let sql = render_create_table(&table);
        assert_eq!(
            sql,
            "CREATE TABLE \"public\".\"posts\" (\n    \"id\" integer DEFAULT nextval('posts_id_seq'::regclass) NOT NULL,\n    \"title\" text NOT NULL,\n    \"content\" text,\n    \"created_at\" timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,\n    CONSTRAINT \"posts_pkey\" PRIMARY KEY (\"id\")\n);"
        );
    }

    #[test]
    fn test_render_table_with_generated_column() {
        let table = Table::new(
            "public".to_string(),
            "users".to_string(),
            vec![
                Column {
                    name: "first_name".to_string(),
                    data_type: "text".to_string(),
                    default: None,
                    not_null: true,
                    generated: None,
                    comment: None,
                    depends_on: vec![],
                },
                Column {
                    name: "last_name".to_string(),
                    data_type: "text".to_string(),
                    default: None,
                    not_null: true,
                    generated: None,
                    comment: None,
                    depends_on: vec![],
                },
                Column {
                    name: "full_name".to_string(),
                    data_type: "text".to_string(),
                    default: None,
                    not_null: false,
                    generated: Some("first_name || ' ' || last_name".to_string()),
                    comment: None,
                    depends_on: vec![],
                },
            ],
            None,
            None,
            vec![],
        );

        let sql = render_create_table(&table);
        assert_eq!(
            sql,
            "CREATE TABLE \"public\".\"users\" (\n    \"first_name\" text NOT NULL,\n    \"last_name\" text NOT NULL,\n    \"full_name\" text GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED\n);"
        );
    }

    #[test]
    fn test_render_table_minimal() {
        let table = Table::new(
            "app".to_string(),
            "simple_table".to_string(),
            vec![Column {
                name: "data".to_string(),
                data_type: "jsonb".to_string(),
                default: None,
                not_null: false,
                generated: None,
                comment: None,
                depends_on: vec![],
            }],
            None,
            None,
            vec![],
        );

        let sql = render_create_table(&table);
        assert_eq!(
            sql,
            "CREATE TABLE \"app\".\"simple_table\" (\n    \"data\" jsonb\n);"
        );
    }
}
