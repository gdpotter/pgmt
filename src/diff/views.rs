use crate::catalog::view::View;
use crate::diff::comment_utils;
use crate::diff::operations::{MigrationStep, ViewIdentifier, ViewOperation, ViewOption};

/// Diff a single view
pub fn diff(old: Option<&View>, new: Option<&View>) -> Vec<MigrationStep> {
    match (old, new) {
        // CREATE new view
        (None, Some(n)) => {
            let mut steps = vec![MigrationStep::View(ViewOperation::Create {
                schema: n.schema.clone(),
                name: n.name.clone(),
                definition: n.definition.clone(),
                security_invoker: n.security_invoker,
                security_barrier: n.security_barrier,
            })];

            // Add view comment if present
            if let Some(comment_op) = comment_utils::handle_comment_creation(
                &n.comment,
                ViewIdentifier {
                    schema: n.schema.clone(),
                    name: n.name.clone(),
                },
            ) {
                steps.push(MigrationStep::View(ViewOperation::Comment(comment_op)));
            }

            steps
        }
        // DROP removed view
        (Some(o), None) => {
            vec![MigrationStep::View(ViewOperation::Drop {
                schema: o.schema.clone(),
                name: o.name.clone(),
            })]
        }
        (Some(o), Some(n)) => {
            let mut steps = Vec::new();

            if o.columns != n.columns {
                steps.extend(vec![
                    MigrationStep::View(ViewOperation::Drop {
                        schema: o.schema.clone(),
                        name: o.name.clone(),
                    }),
                    MigrationStep::View(ViewOperation::Create {
                        schema: n.schema.clone(),
                        name: n.name.clone(),
                        definition: n.definition.clone(),
                        security_invoker: n.security_invoker,
                        security_barrier: n.security_barrier,
                    }),
                ]);

                // Add view comment if present after recreating
                if let Some(comment_op) = comment_utils::handle_comment_creation(
                    &n.comment,
                    ViewIdentifier {
                        schema: n.schema.clone(),
                        name: n.name.clone(),
                    },
                ) {
                    steps.push(MigrationStep::View(ViewOperation::Comment(comment_op)));
                }
            } else if o.definition != n.definition {
                steps.push(MigrationStep::View(ViewOperation::Replace {
                    schema: n.schema.clone(),
                    name: n.name.clone(),
                    definition: n.definition.clone(),
                }));

                // Handle security option changes for replaced views
                if o.security_invoker != n.security_invoker {
                    steps.push(MigrationStep::View(ViewOperation::SetOption {
                        schema: n.schema.clone(),
                        name: n.name.clone(),
                        option: ViewOption::SecurityInvoker,
                        enabled: n.security_invoker,
                    }));
                }
                if o.security_barrier != n.security_barrier {
                    steps.push(MigrationStep::View(ViewOperation::SetOption {
                        schema: n.schema.clone(),
                        name: n.name.clone(),
                        option: ViewOption::SecurityBarrier,
                        enabled: n.security_barrier,
                    }));
                }

                // Handle comment changes for replaced views
                let comment_ops =
                    comment_utils::handle_comment_diff(Some(o), Some(n), || ViewIdentifier {
                        schema: n.schema.clone(),
                        name: n.name.clone(),
                    });
                for comment_op in comment_ops {
                    steps.push(MigrationStep::View(ViewOperation::Comment(comment_op)));
                }
            } else {
                // Check for security option changes even when definition is unchanged
                if o.security_invoker != n.security_invoker {
                    steps.push(MigrationStep::View(ViewOperation::SetOption {
                        schema: n.schema.clone(),
                        name: n.name.clone(),
                        option: ViewOption::SecurityInvoker,
                        enabled: n.security_invoker,
                    }));
                }
                if o.security_barrier != n.security_barrier {
                    steps.push(MigrationStep::View(ViewOperation::SetOption {
                        schema: n.schema.clone(),
                        name: n.name.clone(),
                        option: ViewOption::SecurityBarrier,
                        enabled: n.security_barrier,
                    }));
                }

                // Handle comment changes
                let comment_ops =
                    comment_utils::handle_comment_diff(Some(o), Some(n), || ViewIdentifier {
                        schema: n.schema.clone(),
                        name: n.name.clone(),
                    });
                for comment_op in comment_ops {
                    steps.push(MigrationStep::View(ViewOperation::Comment(comment_op)));
                }
            }

            steps
        }
        (None, None) => {
            Vec::new() // Impossible
        }
    }
}
