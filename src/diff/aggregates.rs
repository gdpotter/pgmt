use crate::catalog::aggregate::Aggregate;
use crate::diff::comment_utils;
use crate::diff::operations::{AggregateIdentifier, AggregateOperation, MigrationStep};

/// Diff a single aggregate function
pub fn diff(old: Option<&Aggregate>, new: Option<&Aggregate>) -> Vec<MigrationStep> {
    match (old, new) {
        // CREATE new aggregate
        (None, Some(new_aggregate)) => {
            vec![MigrationStep::Aggregate(AggregateOperation::Create {
                aggregate: Box::new(new_aggregate.clone()),
            })]
        }

        // DROP old aggregate
        (Some(old_aggregate), None) => {
            let identifier = AggregateIdentifier::new(
                old_aggregate.schema.clone(),
                old_aggregate.name.clone(),
                old_aggregate.arguments.clone(),
            );
            vec![MigrationStep::Aggregate(AggregateOperation::Drop {
                identifier,
            })]
        }

        // REPLACE or comment-only changes
        (Some(old_aggregate), Some(new_aggregate)) => {
            let mut steps = Vec::new();

            if aggregates_differ_structurally(old_aggregate, new_aggregate) {
                // Structural changes require replacement
                steps.push(MigrationStep::Aggregate(AggregateOperation::Replace {
                    old_aggregate: Box::new(old_aggregate.clone()),
                    new_aggregate: Box::new(new_aggregate.clone()),
                }));
            } else {
                // Only comments might have changed
                let comment_ops = comment_utils::handle_comment_diff(
                    Some(old_aggregate),
                    Some(new_aggregate),
                    || AggregateIdentifier::from_aggregate(new_aggregate),
                );
                for comment_op in comment_ops {
                    steps.push(MigrationStep::Aggregate(AggregateOperation::Comment(
                        comment_op,
                    )));
                }
            }

            steps
        }

        // No change
        (None, None) => vec![],
    }
}

/// Check if two aggregates differ in their structural properties
/// (everything except comments)
fn aggregates_differ_structurally(old: &Aggregate, new: &Aggregate) -> bool {
    // Compare the complete aggregate definitions - this handles all structural changes
    old.definition != new.definition
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::id::DbObjectId;

    fn create_test_aggregate(name: &str) -> Aggregate {
        Aggregate {
            schema: "public".to_string(),
            name: name.to_string(),
            arguments: "text".to_string(),
            state_type: "text".to_string(),
            state_type_schema: "pg_catalog".to_string(),
            state_type_formatted: "text".to_string(),
            state_func: "group_concat_state".to_string(),
            state_func_schema: "public".to_string(),
            final_func: None,
            final_func_schema: None,
            combine_func: None,
            combine_func_schema: None,
            initial_value: Some("".to_string()),
            definition: format!(
                "CREATE AGGREGATE public.{}(text) (\n    SFUNC = public.group_concat_state,\n    STYPE = text,\n    INITCOND = ''\n)",
                name
            ),
            comment: None,
            depends_on: vec![
                DbObjectId::Schema {
                    name: "public".to_string(),
                },
                DbObjectId::Function {
                    schema: "public".to_string(),
                    name: "group_concat_state".to_string(),
                    arguments: "text, text".to_string(),
                },
            ],
        }
    }

    #[test]
    fn test_diff_no_changes() {
        let aggregate = create_test_aggregate("group_concat");
        let steps = diff(Some(&aggregate), Some(&aggregate));
        assert!(steps.is_empty());
    }

    #[test]
    fn test_diff_create_aggregate() {
        let new_aggregate = create_test_aggregate("new_agg");
        let steps = diff(None, Some(&new_aggregate));
        assert_eq!(steps.len(), 1);

        match &steps[0] {
            MigrationStep::Aggregate(AggregateOperation::Create { aggregate }) => {
                assert_eq!(aggregate.name, "new_agg");
            }
            _ => panic!("Expected AggregateOperation::Create"),
        }
    }

    #[test]
    fn test_diff_drop_aggregate() {
        let old_aggregate = create_test_aggregate("old_agg");
        let steps = diff(Some(&old_aggregate), None);
        assert_eq!(steps.len(), 1);

        match &steps[0] {
            MigrationStep::Aggregate(AggregateOperation::Drop { identifier }) => {
                assert_eq!(identifier.name, "old_agg");
                assert_eq!(identifier.schema, "public");
                assert_eq!(identifier.arguments, "text");
            }
            _ => panic!("Expected AggregateOperation::Drop"),
        }
    }

    #[test]
    fn test_diff_replace_aggregate() {
        let old_aggregate = create_test_aggregate("test_agg");
        let mut new_aggregate = create_test_aggregate("test_agg");
        new_aggregate.initial_value = Some("N/A".to_string());
        new_aggregate.definition = "CREATE AGGREGATE public.test_agg(text) (\n    SFUNC = public.group_concat_state,\n    STYPE = text,\n    INITCOND = 'N/A'\n)".to_string();

        let steps = diff(Some(&old_aggregate), Some(&new_aggregate));
        assert_eq!(steps.len(), 1);

        match &steps[0] {
            MigrationStep::Aggregate(AggregateOperation::Replace {
                old_aggregate,
                new_aggregate,
            }) => {
                // Verify definitions are different (that's what triggers the replace)
                assert_ne!(old_aggregate.definition, new_aggregate.definition);
            }
            _ => panic!("Expected AggregateOperation::Replace"),
        }
    }

    #[test]
    fn test_diff_comment_change_only() {
        let old_aggregate = create_test_aggregate("test_agg");
        let mut new_aggregate = create_test_aggregate("test_agg");
        new_aggregate.comment = Some("New comment".to_string());

        let steps = diff(Some(&old_aggregate), Some(&new_aggregate));
        assert_eq!(steps.len(), 1);

        match &steps[0] {
            MigrationStep::Aggregate(AggregateOperation::Comment(_)) => {
                // Expected comment operation
            }
            _ => panic!("Expected AggregateOperation::Comment"),
        }
    }

    #[test]
    fn test_aggregates_differ_structurally() {
        let agg1 = create_test_aggregate("test");
        let agg2 = create_test_aggregate("test");

        // Same aggregates (same definition)
        assert!(!aggregates_differ_structurally(&agg1, &agg2));

        // Different definition (structural change)
        let mut agg3 = create_test_aggregate("test");
        agg3.definition = "CREATE AGGREGATE public.test(text) (\n    SFUNC = public.other_func,\n    STYPE = text\n)".to_string();
        assert!(aggregates_differ_structurally(&agg1, &agg3));
    }
}
