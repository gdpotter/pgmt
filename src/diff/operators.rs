use crate::catalog::operator::Operator;
use crate::catalog::target::AttrTarget;
use crate::diff::comment_utils;
use crate::diff::operations::{MigrationStep, OperatorIdentifier, OperatorOperation};

/// Diff a single operator.
pub fn diff(old: Option<&Operator>, new: Option<&Operator>) -> Vec<MigrationStep> {
    match (old, new) {
        // CREATE new operator
        (None, Some(new_operator)) => {
            let mut steps = vec![MigrationStep::Operator(OperatorOperation::Create {
                operator: Box::new(new_operator.clone()),
            })];

            // A CREATE OPERATOR statement cannot embed a comment, so emit it as a
            // follow-up step.
            if let Some(comment_op) = comment_utils::handle_comment_creation(
                &new_operator.comment,
                AttrTarget::object(new_operator.id()),
            ) {
                steps.push(MigrationStep::Operator(OperatorOperation::Comment(
                    comment_op,
                )));
            }

            steps
        }

        // DROP removed operator
        (Some(old_operator), None) => {
            vec![MigrationStep::Operator(OperatorOperation::Drop {
                identifier: OperatorIdentifier::from_operator(old_operator),
            })]
        }

        // REPLACE or comment-only changes
        (Some(old_operator), Some(new_operator)) => {
            let mut steps = Vec::new();

            if operators_differ_structurally(old_operator, new_operator) {
                steps.push(MigrationStep::Operator(OperatorOperation::Replace {
                    old_operator: Box::new(old_operator.clone()),
                    new_operator: Box::new(new_operator.clone()),
                }));
            } else {
                // Only the comment might have changed.
                let comment_ops = comment_utils::handle_comment_diff(
                    Some(old_operator),
                    Some(new_operator),
                    || AttrTarget::object(new_operator.id()),
                );
                for comment_op in comment_ops {
                    steps.push(MigrationStep::Operator(OperatorOperation::Comment(
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

/// Two operators differ structurally if anything other than the comment changed.
/// The reconstructed `definition` captures every structural property.
fn operators_differ_structurally(old: &Operator, new: &Operator) -> bool {
    old.definition != new.definition
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::id::DbObjectId;

    fn test_operator(name: &str) -> Operator {
        Operator {
            schema: "public".to_string(),
            name: name.to_string(),
            arguments: "integer, integer".to_string(),
            definition: format!(
                "CREATE OPERATOR public.{} (\n    FUNCTION = public.my_eq,\n    LEFTARG = integer,\n    RIGHTARG = integer\n)",
                name
            ),
            comment: None,
            depends_on: vec![
                DbObjectId::Schema {
                    name: "public".to_string(),
                },
                DbObjectId::Function {
                    schema: "public".to_string(),
                    name: "my_eq".to_string(),
                    arguments: "integer, integer".to_string(),
                },
            ],
        }
    }

    #[test]
    fn test_diff_no_changes() {
        let op = test_operator("===");
        assert!(diff(Some(&op), Some(&op)).is_empty());
    }

    #[test]
    fn test_diff_create() {
        let op = test_operator("===");
        let steps = diff(None, Some(&op));
        assert_eq!(steps.len(), 1);
        assert!(matches!(
            &steps[0],
            MigrationStep::Operator(OperatorOperation::Create { .. })
        ));
    }

    #[test]
    fn test_diff_create_with_comment() {
        let mut op = test_operator("===");
        op.comment = Some("custom equality".to_string());
        let steps = diff(None, Some(&op));
        assert_eq!(steps.len(), 2);
        assert!(matches!(
            &steps[0],
            MigrationStep::Operator(OperatorOperation::Create { .. })
        ));
        assert!(matches!(
            &steps[1],
            MigrationStep::Operator(OperatorOperation::Comment(_))
        ));
    }

    #[test]
    fn test_diff_drop() {
        let op = test_operator("===");
        let steps = diff(Some(&op), None);
        assert_eq!(steps.len(), 1);
        match &steps[0] {
            MigrationStep::Operator(OperatorOperation::Drop { identifier }) => {
                assert_eq!(identifier.name, "===");
                assert_eq!(identifier.arguments, "integer, integer");
            }
            _ => panic!("Expected OperatorOperation::Drop"),
        }
    }

    #[test]
    fn test_diff_replace_on_structural_change() {
        let old = test_operator("===");
        let mut new = test_operator("===");
        new.definition.push_str(",\n    HASHES");
        let steps = diff(Some(&old), Some(&new));
        assert_eq!(steps.len(), 1);
        assert!(matches!(
            &steps[0],
            MigrationStep::Operator(OperatorOperation::Replace { .. })
        ));
    }

    #[test]
    fn test_diff_comment_only_change() {
        let old = test_operator("===");
        let mut new = test_operator("===");
        new.comment = Some("now documented".to_string());
        let steps = diff(Some(&old), Some(&new));
        assert_eq!(steps.len(), 1);
        assert!(matches!(
            &steps[0],
            MigrationStep::Operator(OperatorOperation::Comment(_))
        ));
    }
}
