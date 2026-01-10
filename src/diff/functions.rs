use crate::catalog::function::{Function, FunctionKind, FunctionParam};
use crate::diff::comment_utils;
use crate::diff::operations::{FunctionIdentifier, FunctionOperation, MigrationStep};

/// Check if two functions have the same signature
fn same_signature(old: &Function, new: &Function) -> bool {
    // Same name and schema already checked by the diff_list function

    // Check if parameter types and modes match
    if old.parameters.len() != new.parameters.len() {
        return false;
    }

    for (o, n) in old.parameters.iter().zip(new.parameters.iter()) {
        // For function signatures, parameter names don't matter, only types and modes
        if o.data_type != n.data_type || o.mode != n.mode {
            return false;
        }
    }

    // Check if return type matches
    if old.return_type != new.return_type {
        return false;
    }

    // Same signature
    true
}

/// Generate a parameter list string for use in function/procedure creation
pub fn format_parameter_list(params: &[FunctionParam]) -> String {
    let param_strs: Vec<String> = params
        .iter()
        .map(|p| {
            let mode_str = match &p.mode {
                Some(mode) => format!("{} ", mode),
                None => "".to_string(),
            };

            let name_str = match &p.name {
                Some(name) => format!("{} ", name),
                None => "".to_string(),
            };

            format!("{}{}{}", mode_str, name_str, p.data_type)
        })
        .collect();

    param_strs.join(", ")
}

/// Generate a return type clause for functions
pub fn format_return_clause(func: &Function) -> String {
    match &func.return_type {
        Some(rt) => format!(" RETURNS {}", rt),
        None => "".to_string(),
    }
}

/// Format function/procedure attributes for creation
pub fn format_attributes(func: &Function) -> String {
    let mut attrs = Vec::new();

    // Language
    attrs.push(format!("LANGUAGE {}", func.language));

    // Volatility (only for functions, not procedures)
    if func.kind == FunctionKind::Function {
        attrs.push(func.volatility.clone());
    }

    // Strictness (only for functions)
    if func.kind == FunctionKind::Function && func.is_strict {
        attrs.push("STRICT".to_string());
    }

    // Security
    attrs.push(format!("SECURITY {}", func.security_type));

    attrs.join(" ")
}

/// Diff a single function
pub fn diff(old: Option<&Function>, new: Option<&Function>) -> Vec<MigrationStep> {
    match (old, new) {
        // CREATE new function
        (None, Some(n)) => {
            let kind_str = match n.kind {
                FunctionKind::Function => "FUNCTION",
                FunctionKind::Procedure => "PROCEDURE",
                FunctionKind::Aggregate => "AGGREGATE FUNCTION",
            };

            let params = format_parameter_list(&n.parameters);
            let returns = format_return_clause(n);
            let attributes = format_attributes(n);

            let mut steps = vec![MigrationStep::Function(FunctionOperation::Create {
                schema: n.schema.clone(),
                name: n.name.clone(),
                arguments: n.arguments.clone(),
                kind: kind_str.to_string(),
                parameters: params,
                returns,
                attributes,
                definition: n.definition.clone(),
            })];

            // Add function comment if present
            if let Some(comment_op) = comment_utils::handle_comment_creation(
                &n.comment,
                FunctionIdentifier {
                    schema: n.schema.clone(),
                    name: n.name.clone(),
                    arguments: n.arguments.clone(),
                },
            ) {
                steps.push(MigrationStep::Function(FunctionOperation::Comment(
                    comment_op,
                )));
            }

            steps
        }

        // DROP removed function
        (Some(o), None) => {
            let kind_str = match o.kind {
                FunctionKind::Function => "FUNCTION",
                FunctionKind::Procedure => "PROCEDURE",
                FunctionKind::Aggregate => "AGGREGATE FUNCTION",
            };

            let param_types: Vec<String> =
                o.parameters.iter().map(|p| p.data_type.clone()).collect();

            vec![MigrationStep::Function(FunctionOperation::Drop {
                schema: o.schema.clone(),
                name: o.name.clone(),
                arguments: o.arguments.clone(),
                kind: kind_str.to_string(),
                parameter_types: param_types.join(", "),
            })]
        }

        // REPLACE existing function (CREATE OR REPLACE)
        (Some(o), Some(n)) => {
            // Check if the signatures match
            if !same_signature(o, n) {
                // If signatures don't match, we need to drop and recreate
                let mut steps = Vec::new();
                steps.extend(diff(Some(o), None)); // Drop the old function
                steps.extend(diff(None, Some(n))); // Create the new function
                return steps;
            }

            // For matching signatures, check if the implementation or attributes changed
            let o_attributes = format_attributes(o);
            let n_attributes = format_attributes(n);

            // If anything changed, do a CREATE OR REPLACE
            if o.definition != n.definition || o_attributes != n_attributes {
                let kind_str = match n.kind {
                    FunctionKind::Function => "FUNCTION",
                    FunctionKind::Procedure => "PROCEDURE",
                    FunctionKind::Aggregate => "AGGREGATE FUNCTION",
                };

                let params = format_parameter_list(&n.parameters);
                let returns = format_return_clause(n);
                let attributes = n_attributes;

                let mut steps = vec![MigrationStep::Function(FunctionOperation::Replace {
                    schema: n.schema.clone(),
                    name: n.name.clone(),
                    arguments: n.arguments.clone(),
                    kind: kind_str.to_string(),
                    parameters: params,
                    returns,
                    attributes,
                    definition: n.definition.clone(),
                })];

                // Handle comment changes for replaced functions
                let comment_ops =
                    comment_utils::handle_comment_diff(Some(o), Some(n), || FunctionIdentifier {
                        schema: n.schema.clone(),
                        name: n.name.clone(),
                        arguments: n.arguments.clone(),
                    });
                for comment_op in comment_ops {
                    steps.push(MigrationStep::Function(FunctionOperation::Comment(
                        comment_op,
                    )));
                }

                steps
            } else {
                // No function definition/attributes changes, check for comment changes
                let comment_ops =
                    comment_utils::handle_comment_diff(Some(o), Some(n), || FunctionIdentifier {
                        schema: n.schema.clone(),
                        name: n.name.clone(),
                        arguments: n.arguments.clone(),
                    });
                let mut steps = Vec::new();
                for comment_op in comment_ops {
                    steps.push(MigrationStep::Function(FunctionOperation::Comment(
                        comment_op,
                    )));
                }
                steps
            }
        }

        (None, None) => {
            Vec::new() // Impossible case
        }
    }
}
