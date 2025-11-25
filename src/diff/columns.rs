use crate::catalog::table::Column;
use crate::diff::operations::ColumnAction;

/// Diff a single column
pub fn diff(old: Option<&Column>, new: Option<&Column>) -> Vec<ColumnAction> {
    match (old, new) {
        // 1) brand-new column
        (None, Some(n)) => {
            vec![ColumnAction::Add { column: n.clone() }]
        }
        // 2) dropped column
        (Some(o), None) => {
            vec![ColumnAction::Drop {
                name: o.name.clone(),
            }]
        }
        // 3) existed in both →  type, default, or not null changed?
        (Some(o), Some(n)) => {
            let mut changes = Vec::new();
            if o.data_type != n.data_type {
                changes.push(ColumnAction::AlterType {
                    name: n.name.clone(),
                    new_type: n.data_type.clone(),
                });
            }

            if o.generated != n.generated {
                match (&o.generated, &n.generated) {
                    (Some(_), None) => {
                        changes.push(ColumnAction::DropGenerated {
                            name: n.name.clone(),
                        });
                    }
                    (None, Some(_)) => {
                        changes.push(ColumnAction::Drop {
                            name: n.name.clone(),
                        });
                        changes.extend(diff(None, Some(n)));
                    }
                    (Some(old_expr), Some(new_expr)) if old_expr != new_expr => {
                        changes.push(ColumnAction::Drop {
                            name: n.name.clone(),
                        });
                        changes.extend(diff(None, Some(n)));
                    }
                    _ => {}
                }
            }

            // Check for changes in DEFAULT
            match (&o.default, &n.default) {
                (Some(_), None) => {
                    changes.push(ColumnAction::DropDefault {
                        name: n.name.clone(),
                    });
                }
                (None, Some(d)) => {
                    changes.push(ColumnAction::SetDefault {
                        name: n.name.clone(),
                        default: d.clone(),
                    });
                }
                _ => {}
            }

            // Check for changes in NOT NULL constraint
            match (o.not_null, n.not_null) {
                (false, true) => {
                    changes.push(ColumnAction::SetNotNull {
                        name: n.name.clone(),
                    });
                }
                (true, false) => {
                    changes.push(ColumnAction::DropNotNull {
                        name: n.name.clone(),
                    });
                }
                _ => {}
            }

            // Note: Column comments are handled separately in the table diff
            // since they require schema/table context for the CommentOperation

            changes
        }
        // (None, None) impossible
        _ => Vec::new(),
    }
}
