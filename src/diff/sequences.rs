use crate::catalog::sequence::Sequence;
use crate::diff::comment_utils;
use crate::diff::operations::{MigrationStep, SequenceIdentifier, SequenceOperation};

/// Generate migration steps for sequence differences
pub fn diff(old: Option<&Sequence>, new: Option<&Sequence>) -> Vec<MigrationStep> {
    match (old, new) {
        (None, Some(new_seq)) => {
            // Create new sequence (primary phase) - always without ownership initially
            let mut steps = vec![MigrationStep::Sequence(SequenceOperation::Create {
                schema: new_seq.schema.clone(),
                name: new_seq.name.clone(),
                data_type: new_seq.data_type.clone(),
                start_value: new_seq.start_value,
                min_value: new_seq.min_value,
                max_value: new_seq.max_value,
                increment: new_seq.increment,
                cycle: new_seq.cycle,
            })];

            // If this sequence is owned by a column, add ownership step (relationship phase)
            // This will be handled in phase 2 after tables are created
            if let Some(owned_by) = &new_seq.owned_by {
                steps.push(MigrationStep::Sequence(SequenceOperation::AlterOwnership {
                    schema: new_seq.schema.clone(),
                    name: new_seq.name.clone(),
                    owned_by: owned_by.clone(),
                }));
            }

            // Add sequence comment if present
            if let Some(comment_op) = comment_utils::handle_comment_creation(
                &new_seq.comment,
                SequenceIdentifier {
                    schema: new_seq.schema.clone(),
                    name: new_seq.name.clone(),
                },
            ) {
                steps.push(MigrationStep::Sequence(SequenceOperation::Comment(
                    comment_op,
                )));
            }

            steps
        }

        (Some(old_seq), None) => {
            // Drop sequence
            vec![MigrationStep::Sequence(SequenceOperation::Drop {
                schema: old_seq.schema.clone(),
                name: old_seq.name.clone(),
            })]
        }

        (Some(old_seq), Some(new_seq)) => {
            let mut steps = Vec::new();

            // Check for ownership changes
            if old_seq.owned_by != new_seq.owned_by {
                let owned_by = new_seq.owned_by.as_deref().unwrap_or("NONE");
                steps.push(MigrationStep::Sequence(SequenceOperation::AlterOwnership {
                    schema: new_seq.schema.clone(),
                    name: new_seq.name.clone(),
                    owned_by: owned_by.to_string(),
                }));
            }

            // Note: Other sequence properties (start_value, min_value, etc.) are typically
            // not changed after creation, but could be added here if needed

            // Handle comment changes
            let comment_ops =
                comment_utils::handle_comment_diff(Some(old_seq), Some(new_seq), || {
                    SequenceIdentifier {
                        schema: new_seq.schema.clone(),
                        name: new_seq.name.clone(),
                    }
                });
            for comment_op in comment_ops {
                steps.push(MigrationStep::Sequence(SequenceOperation::Comment(
                    comment_op,
                )));
            }

            steps
        }

        (None, None) => vec![],
    }
}
