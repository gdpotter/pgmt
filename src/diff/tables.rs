use crate::catalog::id::DbObjectId;
use crate::catalog::table::Table;
use crate::catalog::target::AttrTarget;
use crate::diff::operations::{
    ColumnAction, CommentOperation, ConstraintOperation, MigrationStep, TableOperation,
};
use crate::diff::{columns, diff_list};

pub fn diff(old: Option<&Table>, new: Option<&Table>) -> Vec<MigrationStep> {
    match (old, new) {
        (None, Some(n)) => {
            let mut steps = vec![MigrationStep::Table(TableOperation::Create {
                schema: n.schema.clone(),
                name: n.name.clone(),
                columns: n.columns.clone(),
                primary_key: n.primary_key.clone(),
            })];

            if let Some(comment) = &n.comment {
                steps.push(MigrationStep::Table(TableOperation::Comment(
                    CommentOperation::Set {
                        target: AttrTarget::object(n.id()),
                        comment: comment.clone(),
                    },
                )));
            }

            for col in &n.columns {
                if let Some(comment) = &col.comment {
                    steps.push(MigrationStep::Table(TableOperation::Alter {
                        schema: n.schema.clone(),
                        name: n.name.clone(),
                        actions: vec![ColumnAction::Comment(CommentOperation::Set {
                            target: AttrTarget::column(n.id(), col.name.clone()),
                            comment: comment.clone(),
                        })],
                    }));
                }
            }

            // Add primary key comment if present
            if let Some(pk) = &n.primary_key
                && let Some(comment) = &pk.comment
            {
                steps.push(MigrationStep::Constraint(ConstraintOperation::Comment(
                    CommentOperation::Set {
                        target: AttrTarget::object(DbObjectId::Constraint {
                            schema: n.schema.clone(),
                            table: n.name.clone(),
                            name: pk.name.clone(),
                        }),
                        comment: comment.clone(),
                    },
                )));
            }

            // Add RLS settings if enabled
            if n.rls_enabled {
                steps.push(MigrationStep::Table(TableOperation::Alter {
                    schema: n.schema.clone(),
                    name: n.name.clone(),
                    actions: vec![ColumnAction::EnableRls],
                }));
            }

            if n.rls_forced {
                steps.push(MigrationStep::Table(TableOperation::Alter {
                    schema: n.schema.clone(),
                    name: n.name.clone(),
                    actions: vec![ColumnAction::ForceRls],
                }));
            }

            steps
        }
        (Some(o), None) => {
            vec![MigrationStep::Table(TableOperation::Drop {
                schema: o.schema.clone(),
                name: o.name.clone(),
            })]
        }
        (Some(o), Some(n)) => {
            // The old PK must drop before any column action: PostgreSQL silently
            // drops a constraint when one of its columns is dropped, so an explicit
            // DROP CONSTRAINT emitted afterwards fails. The new PK goes after the
            // column actions so it can reference freshly added columns.
            let (drop_pk, add_pk) = match (&o.primary_key, &n.primary_key) {
                (None, None) => (None, None),
                (Some(o_pk), Some(n_pk)) if o_pk == n_pk => (None, None),
                (None, Some(pk)) => (None, Some(pk.clone())),
                (Some(pk), None) => (Some(pk.name.clone()), None),
                (Some(o_pk), Some(n_pk)) => {
                    let structure_same = o_pk.name == n_pk.name && o_pk.columns == n_pk.columns;
                    if structure_same && o_pk.comment != n_pk.comment {
                        // Only comment changed - handle separately below
                        (None, None)
                    } else {
                        (Some(o_pk.name.clone()), Some(n_pk.clone()))
                    }
                }
            };

            let mut actions: Vec<ColumnAction> = Vec::new();

            if let Some(name) = drop_pk {
                actions.push(ColumnAction::DropPrimaryKey { name });
            }

            actions.extend(diff_list(
                &o.columns,
                &n.columns,
                |c| c.name.clone(),
                columns::diff,
            ));

            if let Some(constraint) = add_pk {
                actions.push(ColumnAction::AddPrimaryKey { constraint });
            }

            // Check RLS settings changes
            if o.rls_enabled != n.rls_enabled {
                if n.rls_enabled {
                    actions.push(ColumnAction::EnableRls);
                } else {
                    actions.push(ColumnAction::DisableRls);
                }
            }

            if o.rls_forced != n.rls_forced {
                if n.rls_forced {
                    actions.push(ColumnAction::ForceRls);
                } else {
                    actions.push(ColumnAction::NoForceRls);
                }
            }

            let mut steps = Vec::new();

            if !actions.is_empty() {
                steps.push(MigrationStep::Table(TableOperation::Alter {
                    schema: n.schema.clone(),
                    name: n.name.clone(),
                    actions,
                }));
            }

            match (&o.comment, &n.comment) {
                (None, Some(comment)) => {
                    steps.push(MigrationStep::Table(TableOperation::Comment(
                        CommentOperation::Set {
                            target: AttrTarget::object(n.id()),
                            comment: comment.clone(),
                        },
                    )));
                }
                (Some(_), None) => {
                    steps.push(MigrationStep::Table(TableOperation::Comment(
                        CommentOperation::Drop {
                            target: AttrTarget::object(n.id()),
                        },
                    )));
                }
                (Some(old_comment), Some(new_comment)) if old_comment != new_comment => {
                    steps.push(MigrationStep::Table(TableOperation::Comment(
                        CommentOperation::Set {
                            target: AttrTarget::object(n.id()),
                            comment: new_comment.clone(),
                        },
                    )));
                }
                _ => {}
            }

            // Handle primary key comment changes
            match (&o.primary_key, &n.primary_key) {
                (Some(o_pk), Some(n_pk))
                    if o_pk.name == n_pk.name
                        && o_pk.columns == n_pk.columns
                        && o_pk.comment != n_pk.comment =>
                {
                    // Primary key structure is the same but comment changed
                    let identifier = AttrTarget::object(DbObjectId::Constraint {
                        schema: n.schema.clone(),
                        table: n.name.clone(),
                        name: n_pk.name.clone(),
                    });

                    match (&o_pk.comment, &n_pk.comment) {
                        (None, Some(comment)) => {
                            steps.push(MigrationStep::Constraint(ConstraintOperation::Comment(
                                CommentOperation::Set {
                                    target: identifier,
                                    comment: comment.clone(),
                                },
                            )));
                        }
                        (Some(_), None) => {
                            steps.push(MigrationStep::Constraint(ConstraintOperation::Comment(
                                CommentOperation::Drop { target: identifier },
                            )));
                        }
                        (Some(_), Some(comment)) => {
                            steps.push(MigrationStep::Constraint(ConstraintOperation::Comment(
                                CommentOperation::Set {
                                    target: identifier,
                                    comment: comment.clone(),
                                },
                            )));
                        }
                        _ => {}
                    }
                }
                _ => {}
            }

            for (old_col, new_col) in o.columns.iter().zip(n.columns.iter()) {
                if old_col.name == new_col.name {
                    match (&old_col.comment, &new_col.comment) {
                        (None, Some(comment)) => {
                            steps.push(MigrationStep::Table(TableOperation::Alter {
                                schema: n.schema.clone(),
                                name: n.name.clone(),
                                actions: vec![ColumnAction::Comment(CommentOperation::Set {
                                    target: AttrTarget::column(n.id(), new_col.name.clone()),
                                    comment: comment.clone(),
                                })],
                            }));
                        }
                        (Some(_), None) => {
                            steps.push(MigrationStep::Table(TableOperation::Alter {
                                schema: n.schema.clone(),
                                name: n.name.clone(),
                                actions: vec![ColumnAction::Comment(CommentOperation::Drop {
                                    target: AttrTarget::column(n.id(), new_col.name.clone()),
                                })],
                            }));
                        }
                        (Some(old_comment), Some(new_comment)) if old_comment != new_comment => {
                            steps.push(MigrationStep::Table(TableOperation::Alter {
                                schema: n.schema.clone(),
                                name: n.name.clone(),
                                actions: vec![ColumnAction::Comment(CommentOperation::Set {
                                    target: AttrTarget::column(n.id(), new_col.name.clone()),
                                    comment: new_comment.clone(),
                                })],
                            }));
                        }
                        _ => {}
                    }
                }
            }

            steps
        }
        _ => Vec::new(),
    }
}
