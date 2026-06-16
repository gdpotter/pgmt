use crate::catalog::table::Table;
use crate::diff::operations::{ColumnAction, MigrationStep, TableOperation};
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
                        // Only the comment changed — keep the PK; its comment is
                        // handled centrally by `crate::diff::comments`.
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

            steps
        }
        _ => Vec::new(),
    }
}
