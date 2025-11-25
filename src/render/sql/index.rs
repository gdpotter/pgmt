//! Shared SQL rendering for CREATE INDEX statements
//!
//! This module provides consistent index rendering across both schema generation
//! and migration operations to ensure identical SQL output.

use crate::catalog::index::Index;
use crate::render::quote_ident;

/// Render a complete CREATE INDEX statement for the given index.
///
/// This function handles all PostgreSQL index features:
/// - Index types (USING btree/gist/gin/etc.)
/// - Operator classes (e.g., gist_trgm_ops)
/// - INCLUDE columns for covering indexes
/// - WHERE predicates for partial indexes
/// - WITH storage parameters
/// - TABLESPACE specifications
/// - Collations and ordering
pub fn render_create_index(index: &Index) -> String {
    let mut sql = String::new();

    // CREATE [UNIQUE] INDEX [CONCURRENTLY] name
    sql.push_str("CREATE ");
    if index.is_unique {
        sql.push_str("UNIQUE ");
    }
    sql.push_str("INDEX ");
    sql.push_str(&quote_ident(&index.name));

    // ON table_name
    sql.push_str(" ON ");
    sql.push_str(&format!(
        "{}.{}",
        quote_ident(&index.table_schema),
        quote_ident(&index.table_name)
    ));

    // USING index_type
    sql.push_str(" USING ");
    sql.push_str(&index.index_type.to_string());

    // Column list
    sql.push_str(" (");
    let column_specs: Vec<String> = index
        .columns
        .iter()
        .map(|col| {
            let mut spec = col.expression.clone();

            // Add collation if specified
            if let Some(ref collation) = col.collation {
                spec.push_str(&format!(" COLLATE {}", collation));
            }

            // Add operator class if specified
            if let Some(ref opclass) = col.opclass {
                spec.push_str(&format!(" {}", opclass));
            }

            // Add ordering for btree indexes
            if let Some(ref ordering) = col.ordering
                && ordering != "ASC"
            {
                // ASC is default, don't need to specify
                spec.push_str(&format!(" {}", ordering));
            }

            // Add nulls ordering for btree indexes
            if let Some(ref nulls_ordering) = col.nulls_ordering
                && nulls_ordering != "NULLS LAST"
            {
                // NULLS LAST is default for ASC
                spec.push_str(&format!(" {}", nulls_ordering));
            }

            spec
        })
        .collect();
    sql.push_str(&column_specs.join(", "));
    sql.push(')');

    // INCLUDE columns for covering indexes
    if !index.include_columns.is_empty() {
        sql.push_str(" INCLUDE (");
        let include_specs: Vec<String> = index
            .include_columns
            .iter()
            .map(|col| quote_ident(col))
            .collect();
        sql.push_str(&include_specs.join(", "));
        sql.push(')');
    }

    // WITH storage parameters
    if !index.storage_parameters.is_empty() {
        sql.push_str(" WITH (");
        let param_specs: Vec<String> = index
            .storage_parameters
            .iter()
            .map(|(key, value)| format!("{} = {}", key, value))
            .collect();
        sql.push_str(&param_specs.join(", "));
        sql.push(')');
    }

    // TABLESPACE
    if let Some(ref tablespace) = index.tablespace {
        sql.push_str(&format!(" TABLESPACE {}", quote_ident(tablespace)));
    }

    // WHERE predicate for partial indexes
    if let Some(ref predicate) = index.predicate {
        sql.push_str(&format!(" WHERE {}", predicate));
    }

    sql.push(';');

    sql
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::index::{IndexColumn, IndexType};

    #[test]
    fn test_render_basic_btree_index() {
        let index = Index {
            schema: "public".to_string(),
            name: "users_email_idx".to_string(),
            table_schema: "public".to_string(),
            table_name: "users".to_string(),
            index_type: IndexType::Btree,
            is_unique: false,
            is_clustered: false,
            is_valid: true,
            columns: vec![IndexColumn {
                expression: "email".to_string(),
                collation: None,
                opclass: None,
                ordering: Some("ASC".to_string()),
                nulls_ordering: Some("NULLS LAST".to_string()),
            }],
            include_columns: vec![],
            predicate: None,
            tablespace: None,
            storage_parameters: vec![],
            comment: None,
            depends_on: vec![],
        };

        let sql = render_create_index(&index);
        assert_eq!(
            sql,
            "CREATE INDEX \"users_email_idx\" ON \"public\".\"users\" USING btree (email);"
        );
    }

    #[test]
    fn test_render_gist_index_with_operator_class() {
        let index = Index {
            schema: "public".to_string(),
            name: "customers_search_idx".to_string(),
            table_schema: "public".to_string(),
            table_name: "customers".to_string(),
            index_type: IndexType::Gist,
            is_unique: false,
            is_clustered: false,
            is_valid: true,
            columns: vec![IndexColumn {
                expression: "(first_name || ' ' || last_name || ' ' || email_address)".to_string(),
                collation: None,
                opclass: Some("gist_trgm_ops".to_string()),
                ordering: None,
                nulls_ordering: None,
            }],
            include_columns: vec![],
            predicate: None,
            tablespace: None,
            storage_parameters: vec![],
            comment: None,
            depends_on: vec![],
        };

        let sql = render_create_index(&index);
        assert_eq!(
            sql,
            "CREATE INDEX \"customers_search_idx\" ON \"public\".\"customers\" USING gist ((first_name || ' ' || last_name || ' ' || email_address) gist_trgm_ops);"
        );
    }

    #[test]
    fn test_render_unique_index() {
        let index = Index {
            schema: "public".to_string(),
            name: "users_email_unique".to_string(),
            table_schema: "public".to_string(),
            table_name: "users".to_string(),
            index_type: IndexType::Btree,
            is_unique: true,
            is_clustered: false,
            is_valid: true,
            columns: vec![IndexColumn {
                expression: "email".to_string(),
                collation: None,
                opclass: None,
                ordering: None,
                nulls_ordering: None,
            }],
            include_columns: vec![],
            predicate: None,
            tablespace: None,
            storage_parameters: vec![],
            comment: None,
            depends_on: vec![],
        };

        let sql = render_create_index(&index);
        assert_eq!(
            sql,
            "CREATE UNIQUE INDEX \"users_email_unique\" ON \"public\".\"users\" USING btree (email);"
        );
    }

    #[test]
    fn test_render_partial_index() {
        let index = Index {
            schema: "public".to_string(),
            name: "users_active_idx".to_string(),
            table_schema: "public".to_string(),
            table_name: "users".to_string(),
            index_type: IndexType::Btree,
            is_unique: false,
            is_clustered: false,
            is_valid: true,
            columns: vec![IndexColumn {
                expression: "created_at".to_string(),
                collation: None,
                opclass: None,
                ordering: None,
                nulls_ordering: None,
            }],
            include_columns: vec![],
            predicate: Some("active = true".to_string()),
            tablespace: None,
            storage_parameters: vec![],
            comment: None,
            depends_on: vec![],
        };

        let sql = render_create_index(&index);
        assert_eq!(
            sql,
            "CREATE INDEX \"users_active_idx\" ON \"public\".\"users\" USING btree (created_at) WHERE active = true;"
        );
    }

    #[test]
    fn test_render_covering_index() {
        let index = Index {
            schema: "public".to_string(),
            name: "users_covering_idx".to_string(),
            table_schema: "public".to_string(),
            table_name: "users".to_string(),
            index_type: IndexType::Btree,
            is_unique: false,
            is_clustered: false,
            is_valid: true,
            columns: vec![IndexColumn {
                expression: "email".to_string(),
                collation: None,
                opclass: None,
                ordering: None,
                nulls_ordering: None,
            }],
            include_columns: vec!["first_name".to_string(), "last_name".to_string()],
            predicate: None,
            tablespace: None,
            storage_parameters: vec![],
            comment: None,
            depends_on: vec![],
        };

        let sql = render_create_index(&index);
        assert_eq!(
            sql,
            "CREATE INDEX \"users_covering_idx\" ON \"public\".\"users\" USING btree (email) INCLUDE (\"first_name\", \"last_name\");"
        );
    }
}
