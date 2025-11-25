use crate::catalog::custom_type::{CustomType, TypeKind};
use crate::diff::comment_utils;
use crate::diff::operations::{MigrationStep, TypeIdentifier, TypeOperation};

/// Diff a single custom type
pub fn diff(old: Option<&CustomType>, new: Option<&CustomType>) -> Vec<MigrationStep> {
    match (old, new) {
        // CREATE new type
        (None, Some(n)) => {
            match n.kind {
                TypeKind::Enum => {
                    let values: Vec<String> = n
                        .enum_values
                        .iter()
                        .map(|v| format!("'{}'", v.name))
                        .collect();

                    let mut steps = vec![MigrationStep::Type(TypeOperation::Create {
                        schema: n.schema.clone(),
                        name: n.name.clone(),
                        kind: "ENUM".to_string(),
                        definition: format!("({})", values.join(", ")),
                    })];

                    // Add type comment if present
                    if let Some(comment_op) = comment_utils::handle_comment_creation(
                        &n.comment,
                        TypeIdentifier {
                            schema: n.schema.clone(),
                            name: n.name.clone(),
                        },
                    ) {
                        steps.push(MigrationStep::Type(TypeOperation::Comment(comment_op)));
                    }

                    steps
                }
                TypeKind::Domain => {
                    let base_type = n.base_type.clone().unwrap_or_else(|| "text".to_string());

                    let mut steps = vec![MigrationStep::Type(TypeOperation::Create {
                        schema: n.schema.clone(),
                        name: n.name.clone(),
                        kind: "DOMAIN".to_string(),
                        definition: format!("AS {}", base_type),
                    })];

                    // Add type comment if present
                    if let Some(comment_op) = comment_utils::handle_comment_creation(
                        &n.comment,
                        TypeIdentifier {
                            schema: n.schema.clone(),
                            name: n.name.clone(),
                        },
                    ) {
                        steps.push(MigrationStep::Type(TypeOperation::Comment(comment_op)));
                    }

                    steps
                }
                TypeKind::Composite => {
                    let attributes: Vec<String> = n
                        .composite_attributes
                        .iter()
                        .map(|attr| format!("{} {}", attr.name, attr.type_name))
                        .collect();

                    let mut steps = vec![MigrationStep::Type(TypeOperation::Create {
                        schema: n.schema.clone(),
                        name: n.name.clone(),
                        kind: "COMPOSITE".to_string(),
                        definition: format!("({})", attributes.join(", ")),
                    })];

                    // Add type comment if present
                    if let Some(comment_op) = comment_utils::handle_comment_creation(
                        &n.comment,
                        TypeIdentifier {
                            schema: n.schema.clone(),
                            name: n.name.clone(),
                        },
                    ) {
                        steps.push(MigrationStep::Type(TypeOperation::Comment(comment_op)));
                    }

                    steps
                }
                TypeKind::Range => {
                    // Range types generally require more complex handling
                    let mut steps = vec![MigrationStep::Type(TypeOperation::Create {
                        schema: n.schema.clone(),
                        name: n.name.clone(),
                        kind: "RANGE".to_string(),
                        definition: "".to_string(), // Would need more info from the catalog
                    })];

                    // Add type comment if present
                    if let Some(comment_op) = comment_utils::handle_comment_creation(
                        &n.comment,
                        TypeIdentifier {
                            schema: n.schema.clone(),
                            name: n.name.clone(),
                        },
                    ) {
                        steps.push(MigrationStep::Type(TypeOperation::Comment(comment_op)));
                    }

                    steps
                }
                TypeKind::Other(ref t) => {
                    let mut steps = vec![MigrationStep::Type(TypeOperation::Create {
                        schema: n.schema.clone(),
                        name: n.name.clone(),
                        kind: format!("TYPE ({})", t),
                        definition: "".to_string(),
                    })];

                    // Add type comment if present
                    if let Some(comment_op) = comment_utils::handle_comment_creation(
                        &n.comment,
                        TypeIdentifier {
                            schema: n.schema.clone(),
                            name: n.name.clone(),
                        },
                    ) {
                        steps.push(MigrationStep::Type(TypeOperation::Comment(comment_op)));
                    }

                    steps
                }
            }
        }
        // DROP removed type
        (Some(o), None) => {
            vec![MigrationStep::Type(TypeOperation::Drop {
                schema: o.schema.clone(),
                name: o.name.clone(),
            })]
        }
        // ALTER existing type
        (Some(o), Some(n)) => {
            if o.kind != n.kind {
                // Type kind changed (very unusual) - need to drop and recreate
                return vec![
                    MigrationStep::Type(TypeOperation::Drop {
                        schema: o.schema.clone(),
                        name: o.name.clone(),
                    }),
                    diff(None, Some(n))[0].clone(),
                ];
            }

            match n.kind {
                TypeKind::Enum => {
                    // For enums, Postgres allows adding values but not removing them
                    // Let's check if values were added, removed, or reordered
                    let old_values: Vec<&String> = o.enum_values.iter().map(|v| &v.name).collect();
                    let new_values: Vec<&String> = n.enum_values.iter().map(|v| &v.name).collect();

                    if old_values == new_values {
                        // No enum value changes, check for comment changes
                        let comment_ops =
                            comment_utils::handle_comment_diff(Some(o), Some(n), || {
                                TypeIdentifier {
                                    schema: n.schema.clone(),
                                    name: n.name.clone(),
                                }
                            });
                        let mut steps = Vec::new();
                        for comment_op in comment_ops {
                            steps.push(MigrationStep::Type(TypeOperation::Comment(comment_op)));
                        }
                        steps
                    } else if old_values.iter().all(|v| new_values.contains(v)) {
                        // Only added values - generate a single ALTER TYPE statement with all new values
                        let added_values: Vec<String> = n
                            .enum_values
                            .iter()
                            .filter(|v| !old_values.contains(&&v.name))
                            .map(|v| v.name.clone())
                            .collect();

                        if added_values.is_empty() {
                            // No new values, but order changed, requires drop and recreate
                            return vec![
                                MigrationStep::Type(TypeOperation::Drop {
                                    schema: o.schema.clone(),
                                    name: o.name.clone(),
                                }),
                                diff(None, Some(n))[0].clone(),
                            ];
                        }

                        // If there are existing values, find the last one to add our new values after
                        let after_clause = if !old_values.is_empty() {
                            // Find the last enum value from the old list according to sort order
                            let last_enum_value = o
                                .enum_values
                                .iter()
                                .max_by(|a, b| {
                                    a.sort_order
                                        .partial_cmp(&b.sort_order)
                                        .unwrap_or(std::cmp::Ordering::Equal)
                                })
                                .map(|v| &v.name)
                                .unwrap_or(old_values[0]); // Fallback to first value if we can't determine sort order

                            format!(" AFTER '{}'", last_enum_value)
                        } else {
                            "".to_string()
                        };

                        // Generate separate ALTER TYPE statements for each new value
                        // PostgreSQL doesn't support adding multiple values in one statement
                        let mut steps = Vec::new();

                        for (i, value) in added_values.iter().enumerate() {
                            // For the first value, use the after_clause from the existing values
                            // For subsequent values, add after the previous new value
                            let after = if i == 0 {
                                after_clause.clone()
                            } else {
                                format!(" AFTER '{}'", added_values[i - 1])
                            };

                            steps.push(MigrationStep::Type(TypeOperation::Alter {
                                schema: n.schema.clone(),
                                name: n.name.clone(),
                                action: "ADD VALUE".to_string(),
                                definition: format!("'{}'{}", value, after),
                            }));
                        }

                        // Handle comment changes after adding enum values
                        let comment_ops =
                            comment_utils::handle_comment_diff(Some(o), Some(n), || {
                                TypeIdentifier {
                                    schema: n.schema.clone(),
                                    name: n.name.clone(),
                                }
                            });
                        for comment_op in comment_ops {
                            steps.push(MigrationStep::Type(TypeOperation::Comment(comment_op)));
                        }

                        steps
                    } else {
                        // Values were removed or both added and removed - requires drop and recreate
                        vec![
                            MigrationStep::Type(TypeOperation::Drop {
                                schema: o.schema.clone(),
                                name: o.name.clone(),
                            }),
                            diff(None, Some(n))[0].clone(),
                        ]
                    }
                }
                TypeKind::Domain => {
                    let old_base = o.base_type.clone().unwrap_or_else(|| "text".to_string());
                    let new_base = n.base_type.clone().unwrap_or_else(|| "text".to_string());

                    if old_base != new_base {
                        // Base type changed - requires drop and recreate
                        return vec![
                            MigrationStep::Type(TypeOperation::Drop {
                                schema: o.schema.clone(),
                                name: o.name.clone(),
                            }),
                            diff(None, Some(n))[0].clone(),
                        ];
                    }

                    // No domain base type changes, check for comment changes
                    let comment_ops =
                        comment_utils::handle_comment_diff(Some(o), Some(n), || TypeIdentifier {
                            schema: n.schema.clone(),
                            name: n.name.clone(),
                        });
                    let mut steps = Vec::new();
                    for comment_op in comment_ops {
                        steps.push(MigrationStep::Type(TypeOperation::Comment(comment_op)));
                    }
                    steps
                }
                TypeKind::Composite => {
                    // For composite types, we'll check if attributes changed
                    let old_attrs: Vec<(&String, &String)> = o
                        .composite_attributes
                        .iter()
                        .map(|attr| (&attr.name, &attr.type_name))
                        .collect();
                    let new_attrs: Vec<(&String, &String)> = n
                        .composite_attributes
                        .iter()
                        .map(|attr| (&attr.name, &attr.type_name))
                        .collect();

                    if old_attrs != new_attrs {
                        // Attributes changed - requires drop and recreate
                        return vec![
                            MigrationStep::Type(TypeOperation::Drop {
                                schema: o.schema.clone(),
                                name: o.name.clone(),
                            }),
                            diff(None, Some(n))[0].clone(),
                        ];
                    }

                    // No composite attribute changes, check for comment changes
                    let comment_ops =
                        comment_utils::handle_comment_diff(Some(o), Some(n), || TypeIdentifier {
                            schema: n.schema.clone(),
                            name: n.name.clone(),
                        });
                    let mut steps = Vec::new();
                    for comment_op in comment_ops {
                        steps.push(MigrationStep::Type(TypeOperation::Comment(comment_op)));
                    }
                    steps
                }
                _ => {
                    // For other types, generally require a drop and recreate if changed
                    // Check for comment changes only
                    let comment_ops =
                        comment_utils::handle_comment_diff(Some(o), Some(n), || TypeIdentifier {
                            schema: n.schema.clone(),
                            name: n.name.clone(),
                        });
                    let mut steps = Vec::new();
                    for comment_op in comment_ops {
                        steps.push(MigrationStep::Type(TypeOperation::Comment(comment_op)));
                    }
                    steps
                }
            }
        }
        (None, None) => {
            Vec::new() // Impossible case
        }
    }
}
