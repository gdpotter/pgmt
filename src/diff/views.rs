use crate::catalog::view::View;
use crate::diff::operations::{MigrationStep, ViewOperation, ViewOption};

/// Diff a single view's structure. Comments (on the view and its columns) are
/// handled centrally by [`crate::diff::comments`], not here.
pub fn diff(old: Option<&View>, new: Option<&View>) -> Vec<MigrationStep> {
    match (old, new) {
        // CREATE new view
        (None, Some(n)) => vec![MigrationStep::View(ViewOperation::Create {
            schema: n.schema.clone(),
            name: n.name.clone(),
            definition: n.definition.clone(),
            security_invoker: n.security_invoker,
            security_barrier: n.security_barrier,
        })],
        // DROP removed view
        (Some(o), None) => vec![MigrationStep::View(ViewOperation::Drop {
            schema: o.schema.clone(),
            name: o.name.clone(),
        })],
        (Some(o), Some(n)) => {
            // Compare column structure. A column set/type change forces a
            // DROP+CREATE; comments are diffed separately, so a comment-only
            // change never lands here.
            let structural_columns_changed = o.columns.len() != n.columns.len()
                || o.columns
                    .iter()
                    .zip(n.columns.iter())
                    .any(|(a, b)| a.name != b.name || a.type_ != b.type_);

            if structural_columns_changed {
                vec![
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
                ]
            } else if o.definition != n.definition {
                vec![MigrationStep::View(ViewOperation::Replace {
                    schema: n.schema.clone(),
                    name: n.name.clone(),
                    definition: n.definition.clone(),
                    security_invoker: n.security_invoker,
                    security_barrier: n.security_barrier,
                })]
            } else {
                // Definition unchanged — only security options can differ.
                let mut steps = Vec::new();
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
                steps
            }
        }
        (None, None) => Vec::new(),
    }
}
