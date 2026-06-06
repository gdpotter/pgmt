use crate::catalog::target::AttrTarget;
use crate::catalog::view::{View, ViewColumn};
use crate::diff::comment_utils;
use crate::diff::operations::{CommentOperation, MigrationStep, ViewOperation, ViewOption};

/// Emit SET steps for all non-empty column comments on a (newly created or recreated) view.
fn emit_initial_column_comments(view: &View) -> Vec<MigrationStep> {
    view.columns
        .iter()
        .filter_map(|col| {
            col.comment.as_ref().map(|c| {
                MigrationStep::View(ViewOperation::ColumnComment(CommentOperation::Set {
                    target: AttrTarget::column(view.id(), col.name.clone()),
                    comment: c.clone(),
                }))
            })
        })
        .collect()
}

/// Diff per-column comments between two views (assumes columns are otherwise identical).
fn diff_column_comments(old: &View, new: &View) -> Vec<MigrationStep> {
    let mut steps = Vec::new();
    let by_name_old: std::collections::HashMap<&str, &ViewColumn> =
        old.columns.iter().map(|c| (c.name.as_str(), c)).collect();

    for new_col in &new.columns {
        let Some(old_col) = by_name_old.get(new_col.name.as_str()) else {
            continue;
        };
        let target = || AttrTarget::column(new.id(), new_col.name.clone());
        match (&old_col.comment, &new_col.comment) {
            (None, Some(c)) => {
                steps.push(MigrationStep::View(ViewOperation::ColumnComment(
                    CommentOperation::Set {
                        target: target(),
                        comment: c.clone(),
                    },
                )));
            }
            (Some(old_c), Some(new_c)) if old_c != new_c => {
                steps.push(MigrationStep::View(ViewOperation::ColumnComment(
                    CommentOperation::Set {
                        target: target(),
                        comment: new_c.clone(),
                    },
                )));
            }
            (Some(_), None) => {
                steps.push(MigrationStep::View(ViewOperation::ColumnComment(
                    CommentOperation::Drop { target: target() },
                )));
            }
            _ => {}
        }
    }

    steps
}

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
            if let Some(comment_op) =
                comment_utils::handle_comment_creation(&n.comment, AttrTarget::object(n.id()))
            {
                steps.push(MigrationStep::View(ViewOperation::Comment(comment_op)));
            }

            steps.extend(emit_initial_column_comments(n));

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

            // Compare column structure ignoring comments — comments are handled separately
            // so a comment-only column change doesn't force a drop/recreate.
            let structural_columns_changed = o.columns.len() != n.columns.len()
                || o.columns
                    .iter()
                    .zip(n.columns.iter())
                    .any(|(a, b)| a.name != b.name || a.type_ != b.type_);

            if structural_columns_changed {
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
                if let Some(comment_op) =
                    comment_utils::handle_comment_creation(&n.comment, AttrTarget::object(n.id()))
                {
                    steps.push(MigrationStep::View(ViewOperation::Comment(comment_op)));
                }

                // Column comments are dropped along with the view; re-emit any present on the new view.
                steps.extend(emit_initial_column_comments(n));
            } else if o.definition != n.definition {
                steps.push(MigrationStep::View(ViewOperation::Replace {
                    schema: n.schema.clone(),
                    name: n.name.clone(),
                    definition: n.definition.clone(),
                    security_invoker: n.security_invoker,
                    security_barrier: n.security_barrier,
                }));

                // Handle comment changes for replaced views
                let comment_ops = comment_utils::handle_comment_diff(Some(o), Some(n), || {
                    AttrTarget::object(n.id())
                });
                for comment_op in comment_ops {
                    steps.push(MigrationStep::View(ViewOperation::Comment(comment_op)));
                }

                steps.extend(diff_column_comments(o, n));
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
                let comment_ops = comment_utils::handle_comment_diff(Some(o), Some(n), || {
                    AttrTarget::object(n.id())
                });
                for comment_op in comment_ops {
                    steps.push(MigrationStep::View(ViewOperation::Comment(comment_op)));
                }

                steps.extend(diff_column_comments(o, n));
            }

            steps
        }
        (None, None) => {
            Vec::new() // Impossible
        }
    }
}
