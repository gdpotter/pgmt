use crate::catalog::triggers::Trigger;
use crate::diff::comment_utils;
use crate::diff::operations::{MigrationStep, TriggerIdentifier, TriggerOperation};

/// Diff a single trigger
pub fn diff(old: Option<&Trigger>, new: Option<&Trigger>) -> Vec<MigrationStep> {
    match (old, new) {
        // CREATE new trigger
        (None, Some(new_trigger)) => {
            vec![MigrationStep::Trigger(TriggerOperation::Create {
                trigger: Box::new(new_trigger.clone()),
            })]
        }

        // DROP old trigger
        (Some(old_trigger), None) => {
            let identifier = TriggerIdentifier::new(
                old_trigger.schema.clone(),
                old_trigger.table_name.clone(),
                old_trigger.name.clone(),
            );
            vec![MigrationStep::Trigger(TriggerOperation::Drop {
                identifier,
            })]
        }

        // REPLACE or comment-only changes
        (Some(old_trigger), Some(new_trigger)) => {
            let mut steps = Vec::new();

            if triggers_differ_structurally(old_trigger, new_trigger) {
                // Structural changes require replacement
                steps.push(MigrationStep::Trigger(TriggerOperation::Replace {
                    old_trigger: Box::new(old_trigger.clone()),
                    new_trigger: Box::new(new_trigger.clone()),
                }));
            } else {
                // Only comments might have changed
                let comment_ops = comment_utils::handle_comment_diff(
                    Some(old_trigger),
                    Some(new_trigger),
                    || TriggerIdentifier::from_trigger(new_trigger),
                );
                for comment_op in comment_ops {
                    steps.push(MigrationStep::Trigger(TriggerOperation::Comment(
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

/// Check if two triggers differ in their structural properties
/// (everything except comments)
fn triggers_differ_structurally(old: &Trigger, new: &Trigger) -> bool {
    // Compare the complete trigger definitions - this handles all structural changes
    // including timing, events, WHEN conditions, function calls, etc.
    old.definition != new.definition
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::id::DbObjectId;

    fn create_test_trigger(name: &str) -> Trigger {
        Trigger {
            schema: "public".to_string(),
            table_name: "users".to_string(),
            name: name.to_string(),
            function_schema: "public".to_string(),
            function_name: "set_updated_at".to_string(),
            function_args: "".to_string(),
            comment: None,
            depends_on: vec![
                DbObjectId::Table {
                    schema: "public".to_string(),
                    name: "users".to_string(),
                },
                DbObjectId::Function {
                    schema: "public".to_string(),
                    name: "set_updated_at".to_string(),
                    arguments: "".to_string(),
                },
            ],
            definition: format!(
                "CREATE TRIGGER {} BEFORE UPDATE ON public.users FOR EACH ROW EXECUTE FUNCTION public.set_updated_at()",
                name
            ),
        }
    }

    #[test]
    fn test_diff_no_changes() {
        let trigger = create_test_trigger("trigger1");
        let steps = diff(Some(&trigger), Some(&trigger));
        assert!(steps.is_empty());
    }

    #[test]
    fn test_diff_create_trigger() {
        let new_trigger = create_test_trigger("new_trigger");
        let steps = diff(None, Some(&new_trigger));
        assert_eq!(steps.len(), 1);

        match &steps[0] {
            MigrationStep::Trigger(TriggerOperation::Create { trigger }) => {
                assert_eq!(trigger.name, "new_trigger");
            }
            _ => panic!("Expected TriggerOperation::Create"),
        }
    }

    #[test]
    fn test_diff_drop_trigger() {
        let old_trigger = create_test_trigger("old_trigger");
        let steps = diff(Some(&old_trigger), None);
        assert_eq!(steps.len(), 1);

        match &steps[0] {
            MigrationStep::Trigger(TriggerOperation::Drop { identifier }) => {
                assert_eq!(identifier.name, "old_trigger");
                assert_eq!(identifier.schema, "public");
                assert_eq!(identifier.table, "users");
            }
            _ => panic!("Expected TriggerOperation::Drop"),
        }
    }

    #[test]
    fn test_diff_replace_trigger() {
        let old_trigger = create_test_trigger("test_trigger");
        let mut new_trigger = create_test_trigger("test_trigger");
        new_trigger.definition = "CREATE TRIGGER test_trigger AFTER UPDATE ON public.users FOR EACH ROW EXECUTE FUNCTION public.set_updated_at()".to_string();

        let steps = diff(Some(&old_trigger), Some(&new_trigger));
        assert_eq!(steps.len(), 1);

        match &steps[0] {
            MigrationStep::Trigger(TriggerOperation::Replace {
                old_trigger,
                new_trigger,
            }) => {
                // Verify definitions are different (that's what triggers the replace)
                assert_ne!(old_trigger.definition, new_trigger.definition);
                assert!(old_trigger.definition.contains("BEFORE"));
                assert!(new_trigger.definition.contains("AFTER"));
            }
            _ => panic!("Expected TriggerOperation::Replace"),
        }
    }

    #[test]
    fn test_diff_comment_change_only() {
        let old_trigger = create_test_trigger("test_trigger");
        let mut new_trigger = create_test_trigger("test_trigger");
        new_trigger.comment = Some("New comment".to_string());

        let steps = diff(Some(&old_trigger), Some(&new_trigger));
        assert_eq!(steps.len(), 1);

        match &steps[0] {
            MigrationStep::Trigger(TriggerOperation::Comment(_)) => {
                // Expected comment operation
            }
            _ => panic!("Expected TriggerOperation::Comment"),
        }
    }

    #[test]
    fn test_triggers_differ_structurally() {
        let trigger1 = create_test_trigger("test");
        let trigger2 = create_test_trigger("test");

        // Same triggers (same definition)
        assert!(!triggers_differ_structurally(&trigger1, &trigger2));

        // Different definition (structural change)
        let mut trigger3 = create_test_trigger("test");
        trigger3.definition = "CREATE TRIGGER test AFTER INSERT ON public.users FOR EACH ROW EXECUTE FUNCTION public.set_updated_at()".to_string();
        assert!(triggers_differ_structurally(&trigger1, &trigger3));

        // Same definition should be equal (definition is authoritative)
        let trigger4 = create_test_trigger("test");
        assert!(!triggers_differ_structurally(&trigger1, &trigger4));
    }
}
