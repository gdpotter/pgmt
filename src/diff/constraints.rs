//! Diff constraints between database states

use crate::catalog::constraint::{Constraint, ConstraintType};
use crate::diff::comment_utils;
use crate::diff::operations::{ConstraintIdentifier, ConstraintOperation, MigrationStep};

/// Diff a single constraint
pub fn diff(old: Option<&Constraint>, new: Option<&Constraint>) -> Vec<MigrationStep> {
    match (old, new) {
        // CREATE new constraint
        (None, Some(n)) => {
            let mut steps = vec![MigrationStep::Constraint(ConstraintOperation::Create(
                n.clone(),
            ))];

            // Add constraint comment if present
            if let Some(comment_op) = comment_utils::handle_comment_creation(
                &n.comment,
                ConstraintIdentifier::from_constraint(n),
            ) {
                steps.push(MigrationStep::Constraint(ConstraintOperation::Comment(
                    comment_op,
                )));
            }

            steps
        }
        // DROP removed constraint
        (Some(o), None) => {
            vec![MigrationStep::Constraint(ConstraintOperation::Drop(
                ConstraintIdentifier::from_constraint(o),
            ))]
        }
        (Some(o), Some(n)) => {
            let mut steps = Vec::new();

            // Check if constraint definition has changed
            if constraints_differ(o, n) {
                // Drop and recreate the constraint
                steps.push(MigrationStep::Constraint(ConstraintOperation::Drop(
                    ConstraintIdentifier::from_constraint(o),
                )));
                steps.push(MigrationStep::Constraint(ConstraintOperation::Create(
                    n.clone(),
                )));
            } else {
                // Check for comment changes only
                let comment_ops = comment_utils::handle_comment_diff(Some(o), Some(n), || {
                    ConstraintIdentifier::from_constraint(n)
                });
                for comment_op in comment_ops {
                    steps.push(MigrationStep::Constraint(ConstraintOperation::Comment(
                        comment_op,
                    )));
                }
            }

            steps
        }
        (None, None) => vec![],
    }
}

fn constraints_differ(old: &Constraint, new: &Constraint) -> bool {
    // Compare constraint types - if they're different, we need to recreate
    constraint_types_differ(&old.constraint_type, &new.constraint_type)
}

fn constraint_types_differ(old: &ConstraintType, new: &ConstraintType) -> bool {
    match (old, new) {
        (
            ConstraintType::Unique { columns: old_cols },
            ConstraintType::Unique { columns: new_cols },
        ) => old_cols != new_cols,
        (
            ConstraintType::ForeignKey {
                columns: old_cols,
                referenced_schema: old_ref_schema,
                referenced_table: old_ref_table,
                referenced_columns: old_ref_cols,
                on_delete: old_on_delete,
                on_update: old_on_update,
                deferrable: old_deferrable,
                initially_deferred: old_initially_deferred,
            },
            ConstraintType::ForeignKey {
                columns: new_cols,
                referenced_schema: new_ref_schema,
                referenced_table: new_ref_table,
                referenced_columns: new_ref_cols,
                on_delete: new_on_delete,
                on_update: new_on_update,
                deferrable: new_deferrable,
                initially_deferred: new_initially_deferred,
            },
        ) => {
            old_cols != new_cols
                || old_ref_schema != new_ref_schema
                || old_ref_table != new_ref_table
                || old_ref_cols != new_ref_cols
                || old_on_delete != new_on_delete
                || old_on_update != new_on_update
                || old_deferrable != new_deferrable
                || old_initially_deferred != new_initially_deferred
        }
        (
            ConstraintType::Check {
                expression: old_expr,
            },
            ConstraintType::Check {
                expression: new_expr,
            },
        ) => old_expr != new_expr,
        (
            ConstraintType::Exclusion {
                elements: old_elements,
                operator_classes: old_opcnames,
                operators: old_operators,
                index_method: old_method,
                predicate: old_predicate,
            },
            ConstraintType::Exclusion {
                elements: new_elements,
                operator_classes: new_opcnames,
                operators: new_operators,
                index_method: new_method,
                predicate: new_predicate,
            },
        ) => {
            old_elements != new_elements
                || old_opcnames != new_opcnames
                || old_operators != new_operators
                || old_method != new_method
                || old_predicate != new_predicate
        }
        // Different constraint types always differ
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::id::DbObjectId;

    #[test]
    fn test_foreign_key_diff() {
        let old_fk = Constraint {
            schema: "public".to_string(),
            table: "orders".to_string(),
            name: "orders_user_id_fkey".to_string(),
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
            depends_on: vec![
                DbObjectId::Table {
                    schema: "public".to_string(),
                    name: "orders".to_string(),
                },
                DbObjectId::Table {
                    schema: "public".to_string(),
                    name: "users".to_string(),
                },
            ],
        };

        let new_fk = Constraint {
            schema: "public".to_string(),
            table: "orders".to_string(),
            name: "orders_user_id_fkey".to_string(),
            constraint_type: ConstraintType::ForeignKey {
                columns: vec!["user_id".to_string()],
                referenced_schema: "public".to_string(),
                referenced_table: "users".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: Some("CASCADE".to_string()),
                on_update: None,
                deferrable: false,
                initially_deferred: false,
            },
            comment: None,
            depends_on: vec![
                DbObjectId::Table {
                    schema: "public".to_string(),
                    name: "orders".to_string(),
                },
                DbObjectId::Table {
                    schema: "public".to_string(),
                    name: "users".to_string(),
                },
            ],
        };

        let steps = diff(Some(&old_fk), Some(&new_fk));
        assert_eq!(steps.len(), 2); // Drop + Create for CASCADE change
    }
}
