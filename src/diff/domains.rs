//! Domain diff logic for schema migrations

use crate::catalog::domain::Domain;
use crate::diff::comment_utils;
use crate::diff::operations::{DomainIdentifier, DomainOperation, MigrationStep};

/// Build the CREATE DOMAIN definition string
fn build_domain_definition(domain: &Domain) -> String {
    let mut parts = vec![format!("AS {}", domain.base_type)];

    if let Some(default) = &domain.default {
        parts.push(format!("DEFAULT {}", default));
    }

    if domain.not_null {
        parts.push("NOT NULL".to_string());
    }

    if let Some(collation) = &domain.collation {
        parts.push(format!("COLLATE \"{}\"", collation));
    }

    for constraint in &domain.check_constraints {
        // pg_get_constraintdef returns the full CHECK clause
        parts.push(format!(
            "CONSTRAINT {} {}",
            constraint.name, constraint.expression
        ));
    }

    parts.join(" ")
}

/// Diff a single domain
pub fn diff(old: Option<&Domain>, new: Option<&Domain>) -> Vec<MigrationStep> {
    match (old, new) {
        // CREATE new domain
        (None, Some(n)) => {
            let mut steps = vec![MigrationStep::Domain(DomainOperation::Create {
                schema: n.schema.clone(),
                name: n.name.clone(),
                definition: build_domain_definition(n),
            })];

            // Add domain comment if present
            if let Some(comment_op) = comment_utils::handle_comment_creation(
                &n.comment,
                DomainIdentifier {
                    schema: n.schema.clone(),
                    name: n.name.clone(),
                },
            ) {
                steps.push(MigrationStep::Domain(DomainOperation::Comment(comment_op)));
            }

            steps
        }

        // DROP removed domain
        (Some(o), None) => {
            vec![MigrationStep::Domain(DomainOperation::Drop {
                schema: o.schema.clone(),
                name: o.name.clone(),
            })]
        }

        // ALTER existing domain
        (Some(o), Some(n)) => {
            let mut steps = Vec::new();

            // Check if base type or collation changed - requires drop/recreate
            if o.base_type != n.base_type || o.collation != n.collation {
                // Drop and recreate
                steps.push(MigrationStep::Domain(DomainOperation::Drop {
                    schema: o.schema.clone(),
                    name: o.name.clone(),
                }));
                steps.push(MigrationStep::Domain(DomainOperation::Create {
                    schema: n.schema.clone(),
                    name: n.name.clone(),
                    definition: build_domain_definition(n),
                }));

                // Add domain comment if present
                if let Some(comment_op) = comment_utils::handle_comment_creation(
                    &n.comment,
                    DomainIdentifier {
                        schema: n.schema.clone(),
                        name: n.name.clone(),
                    },
                ) {
                    steps.push(MigrationStep::Domain(DomainOperation::Comment(comment_op)));
                }

                return steps;
            }

            // Handle NOT NULL changes
            if o.not_null != n.not_null {
                if n.not_null {
                    steps.push(MigrationStep::Domain(DomainOperation::AlterSetNotNull {
                        schema: n.schema.clone(),
                        name: n.name.clone(),
                    }));
                } else {
                    steps.push(MigrationStep::Domain(DomainOperation::AlterDropNotNull {
                        schema: n.schema.clone(),
                        name: n.name.clone(),
                    }));
                }
            }

            // Handle DEFAULT changes
            match (&o.default, &n.default) {
                (None, Some(new_default)) => {
                    steps.push(MigrationStep::Domain(DomainOperation::AlterSetDefault {
                        schema: n.schema.clone(),
                        name: n.name.clone(),
                        default: new_default.clone(),
                    }));
                }
                (Some(_), None) => {
                    steps.push(MigrationStep::Domain(DomainOperation::AlterDropDefault {
                        schema: n.schema.clone(),
                        name: n.name.clone(),
                    }));
                }
                (Some(old_default), Some(new_default)) if old_default != new_default => {
                    steps.push(MigrationStep::Domain(DomainOperation::AlterSetDefault {
                        schema: n.schema.clone(),
                        name: n.name.clone(),
                        default: new_default.clone(),
                    }));
                }
                _ => {}
            }

            // Handle CHECK constraint changes
            // Build maps of constraints by name for comparison
            let old_constraints: std::collections::HashMap<&str, &str> = o
                .check_constraints
                .iter()
                .map(|c| (c.name.as_str(), c.expression.as_str()))
                .collect();
            let new_constraints: std::collections::HashMap<&str, &str> = n
                .check_constraints
                .iter()
                .map(|c| (c.name.as_str(), c.expression.as_str()))
                .collect();

            // Drop constraints that no longer exist or have changed expression
            for (name, old_expr) in &old_constraints {
                match new_constraints.get(name) {
                    None => {
                        // Constraint was removed
                        steps.push(MigrationStep::Domain(DomainOperation::DropConstraint {
                            schema: n.schema.clone(),
                            name: n.name.clone(),
                            constraint_name: name.to_string(),
                        }));
                    }
                    Some(new_expr) if old_expr != new_expr => {
                        // Constraint expression changed - drop and re-add
                        steps.push(MigrationStep::Domain(DomainOperation::DropConstraint {
                            schema: n.schema.clone(),
                            name: n.name.clone(),
                            constraint_name: name.to_string(),
                        }));
                    }
                    _ => {}
                }
            }

            // Add new constraints or re-add changed constraints
            for constraint in &n.check_constraints {
                let name = constraint.name.as_str();
                match old_constraints.get(name) {
                    None => {
                        // New constraint
                        steps.push(MigrationStep::Domain(DomainOperation::AddConstraint {
                            schema: n.schema.clone(),
                            name: n.name.clone(),
                            constraint_name: constraint.name.clone(),
                            expression: constraint.expression.clone(),
                        }));
                    }
                    Some(old_expr) if *old_expr != constraint.expression.as_str() => {
                        // Changed constraint - re-add after drop
                        steps.push(MigrationStep::Domain(DomainOperation::AddConstraint {
                            schema: n.schema.clone(),
                            name: n.name.clone(),
                            constraint_name: constraint.name.clone(),
                            expression: constraint.expression.clone(),
                        }));
                    }
                    _ => {}
                }
            }

            // Handle comment changes
            let comment_ops =
                comment_utils::handle_comment_diff(Some(o), Some(n), || DomainIdentifier {
                    schema: n.schema.clone(),
                    name: n.name.clone(),
                });
            for comment_op in comment_ops {
                steps.push(MigrationStep::Domain(DomainOperation::Comment(comment_op)));
            }

            steps
        }

        (None, None) => Vec::new(),
    }
}
