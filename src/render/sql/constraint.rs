//! Shared SQL rendering for constraint statements
//!
//! This module provides consistent constraint rendering across both schema generation
//! and migration operations to ensure identical SQL output.

use crate::catalog::constraint::{Constraint, ConstraintType};
use crate::render::quote_ident;

/// Render a complete ALTER TABLE ADD CONSTRAINT statement for the given constraint.
///
/// This function handles all PostgreSQL constraint types:
/// - UNIQUE constraints
/// - FOREIGN KEY constraints with full options (ON DELETE/UPDATE, DEFERRABLE)
/// - CHECK constraints (with proper handling of pg_get_constraintdef() output)
/// - EXCLUSION constraints with index methods and operators
/// - Proper SQL formatting and identifier quoting
pub fn render_create_constraint(constraint: &Constraint) -> String {
    let table_name = format!(
        "{}.{}",
        quote_ident(&constraint.schema),
        quote_ident(&constraint.table)
    );

    let constraint_def = match &constraint.constraint_type {
        ConstraintType::Unique { columns } => {
            let column_list = columns
                .iter()
                .map(|c| quote_ident(c))
                .collect::<Vec<_>>()
                .join(", ");
            format!("UNIQUE ({})", column_list)
        }
        ConstraintType::ForeignKey {
            columns,
            referenced_schema,
            referenced_table,
            referenced_columns,
            on_delete,
            on_update,
            deferrable,
            initially_deferred,
        } => {
            let column_list = columns
                .iter()
                .map(|c| quote_ident(c))
                .collect::<Vec<_>>()
                .join(", ");
            let ref_table = format!(
                "{}.{}",
                quote_ident(referenced_schema),
                quote_ident(referenced_table)
            );
            let ref_column_list = referenced_columns
                .iter()
                .map(|c| quote_ident(c))
                .collect::<Vec<_>>()
                .join(", ");

            let mut fk_def = format!(
                "FOREIGN KEY ({}) REFERENCES {} ({})",
                column_list, ref_table, ref_column_list
            );

            if let Some(on_delete) = on_delete {
                fk_def.push_str(&format!(" ON DELETE {}", on_delete));
            }
            if let Some(on_update) = on_update {
                fk_def.push_str(&format!(" ON UPDATE {}", on_update));
            }
            if *deferrable {
                fk_def.push_str(" DEFERRABLE");
                if *initially_deferred {
                    fk_def.push_str(" INITIALLY DEFERRED");
                }
            }

            fk_def
        }
        ConstraintType::Check { expression } => {
            // PostgreSQL's pg_get_constraintdef() returns the complete constraint definition
            // including "CHECK (expression)". Since our template uses "ADD CONSTRAINT name {def}",
            // and CHECK constraints expect "CHECK (expression)", we use it directly.
            // However, to handle cases where the expression might already include CHECK,
            // we ensure we don't double the CHECK keyword.
            if expression.trim_start().starts_with("CHECK") {
                // Expression already includes CHECK keyword, use as-is
                expression.clone()
            } else {
                // Expression is just the condition, wrap in CHECK
                format!("CHECK ({})", expression)
            }
        }
        ConstraintType::Exclusion {
            elements,
            operator_classes: _,
            operators,
            index_method,
            predicate,
        } => {
            let mut exclusion_def = format!("EXCLUDE USING {} (", index_method);

            // Build element WITH operator pairs
            let element_ops: Vec<String> = elements
                .iter()
                .zip(operators.iter())
                .map(|(element, operator)| format!("{} WITH {}", element, operator))
                .collect();

            exclusion_def.push_str(&element_ops.join(", "));
            exclusion_def.push(')');

            if let Some(pred) = predicate {
                exclusion_def.push_str(&format!(" WHERE {}", pred));
            }

            exclusion_def
        }
    };

    format!(
        "ALTER TABLE {} ADD CONSTRAINT {} {};",
        table_name,
        quote_ident(&constraint.name),
        constraint_def
    )
}

/// Render a complete ALTER TABLE DROP CONSTRAINT statement for the given constraint.
pub fn render_drop_constraint(schema: &str, table: &str, constraint_name: &str) -> String {
    let table_name = format!("{}.{}", quote_ident(schema), quote_ident(table));
    format!(
        "ALTER TABLE {} DROP CONSTRAINT {};",
        table_name,
        quote_ident(constraint_name)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::constraint::ConstraintType;

    #[test]
    fn test_render_unique_constraint() {
        let constraint = Constraint {
            schema: "public".to_string(),
            table: "users".to_string(),
            name: "users_email_unique".to_string(),
            constraint_type: ConstraintType::Unique {
                columns: vec!["email".to_string()],
            },
            comment: None,
            depends_on: vec![],
        };

        let sql = render_create_constraint(&constraint);
        assert_eq!(
            sql,
            "ALTER TABLE \"public\".\"users\" ADD CONSTRAINT \"users_email_unique\" UNIQUE (\"email\");"
        );
    }

    #[test]
    fn test_render_unique_constraint_multiple_columns() {
        let constraint = Constraint {
            schema: "public".to_string(),
            table: "user_roles".to_string(),
            name: "user_roles_unique".to_string(),
            constraint_type: ConstraintType::Unique {
                columns: vec!["user_id".to_string(), "role_id".to_string()],
            },
            comment: None,
            depends_on: vec![],
        };

        let sql = render_create_constraint(&constraint);
        assert_eq!(
            sql,
            "ALTER TABLE \"public\".\"user_roles\" ADD CONSTRAINT \"user_roles_unique\" UNIQUE (\"user_id\", \"role_id\");"
        );
    }

    #[test]
    fn test_render_foreign_key_basic() {
        let constraint = Constraint {
            schema: "public".to_string(),
            table: "posts".to_string(),
            name: "posts_user_id_fkey".to_string(),
            constraint_type: ConstraintType::ForeignKey {
                columns: vec!["user_id".to_string()],
                referenced_schema: "public".to_string(),
                referenced_table: "users".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: None,
                on_update: None,
                deferrable: false,
                initially_deferred: false,
            },
            comment: None,
            depends_on: vec![],
        };

        let sql = render_create_constraint(&constraint);
        assert_eq!(
            sql,
            "ALTER TABLE \"public\".\"posts\" ADD CONSTRAINT \"posts_user_id_fkey\" FOREIGN KEY (\"user_id\") REFERENCES \"public\".\"users\" (\"id\");"
        );
    }

    #[test]
    fn test_render_foreign_key_with_actions() {
        let constraint = Constraint {
            schema: "public".to_string(),
            table: "posts".to_string(),
            name: "posts_user_id_fkey".to_string(),
            constraint_type: ConstraintType::ForeignKey {
                columns: vec!["user_id".to_string()],
                referenced_schema: "public".to_string(),
                referenced_table: "users".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: Some("CASCADE".to_string()),
                on_update: Some("RESTRICT".to_string()),
                deferrable: true,
                initially_deferred: true,
            },
            comment: None,
            depends_on: vec![],
        };

        let sql = render_create_constraint(&constraint);
        assert_eq!(
            sql,
            "ALTER TABLE \"public\".\"posts\" ADD CONSTRAINT \"posts_user_id_fkey\" FOREIGN KEY (\"user_id\") REFERENCES \"public\".\"users\" (\"id\") ON DELETE CASCADE ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED;"
        );
    }

    #[test]
    fn test_render_check_constraint_simple_expression() {
        let constraint = Constraint {
            schema: "public".to_string(),
            table: "users".to_string(),
            name: "users_age_check".to_string(),
            constraint_type: ConstraintType::Check {
                expression: "age >= 0".to_string(),
            },
            comment: None,
            depends_on: vec![],
        };

        let sql = render_create_constraint(&constraint);
        assert_eq!(
            sql,
            "ALTER TABLE \"public\".\"users\" ADD CONSTRAINT \"users_age_check\" CHECK (age >= 0);"
        );
    }

    #[test]
    fn test_render_check_constraint_with_check_keyword() {
        let constraint = Constraint {
            schema: "public".to_string(),
            table: "users".to_string(),
            name: "users_age_check".to_string(),
            constraint_type: ConstraintType::Check {
                expression: "CHECK (age >= 0 AND age <= 150)".to_string(),
            },
            comment: None,
            depends_on: vec![],
        };

        let sql = render_create_constraint(&constraint);
        assert_eq!(
            sql,
            "ALTER TABLE \"public\".\"users\" ADD CONSTRAINT \"users_age_check\" CHECK (age >= 0 AND age <= 150);"
        );
    }

    #[test]
    fn test_render_exclusion_constraint() {
        let constraint = Constraint {
            schema: "public".to_string(),
            table: "reservations".to_string(),
            name: "reservations_time_overlap_excl".to_string(),
            constraint_type: ConstraintType::Exclusion {
                elements: vec![
                    "room_id".to_string(),
                    "tsrange(start_time, end_time)".to_string(),
                ],
                operator_classes: vec!["int4_ops".to_string(), "range_ops".to_string()],
                operators: vec!["=".to_string(), "&&".to_string()],
                index_method: "gist".to_string(),
                predicate: None,
            },
            comment: None,
            depends_on: vec![],
        };

        let sql = render_create_constraint(&constraint);
        assert_eq!(
            sql,
            "ALTER TABLE \"public\".\"reservations\" ADD CONSTRAINT \"reservations_time_overlap_excl\" EXCLUDE USING gist (room_id WITH =, tsrange(start_time, end_time) WITH &&);"
        );
    }

    #[test]
    fn test_render_exclusion_constraint_with_predicate() {
        let constraint = Constraint {
            schema: "public".to_string(),
            table: "bookings".to_string(),
            name: "active_bookings_excl".to_string(),
            constraint_type: ConstraintType::Exclusion {
                elements: vec![
                    "resource_id".to_string(),
                    "daterange(start_date, end_date)".to_string(),
                ],
                operator_classes: vec!["int4_ops".to_string(), "range_ops".to_string()],
                operators: vec!["=".to_string(), "&&".to_string()],
                index_method: "gist".to_string(),
                predicate: Some("status = 'active'".to_string()),
            },
            comment: None,
            depends_on: vec![],
        };

        let sql = render_create_constraint(&constraint);
        assert_eq!(
            sql,
            "ALTER TABLE \"public\".\"bookings\" ADD CONSTRAINT \"active_bookings_excl\" EXCLUDE USING gist (resource_id WITH =, daterange(start_date, end_date) WITH &&) WHERE status = 'active';"
        );
    }

    #[test]
    fn test_render_drop_constraint() {
        let sql = render_drop_constraint("public", "users", "users_age_check");
        assert_eq!(
            sql,
            "ALTER TABLE \"public\".\"users\" DROP CONSTRAINT \"users_age_check\";"
        );
    }
}
