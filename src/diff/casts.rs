use crate::catalog::cast::Cast;
use crate::catalog::target::AttrTarget;
use crate::diff::comment_utils;
use crate::diff::operations::{CastIdentifier, CastOperation, MigrationStep};

/// Diff a single cast.
pub fn diff(old: Option<&Cast>, new: Option<&Cast>) -> Vec<MigrationStep> {
    match (old, new) {
        // CREATE new cast
        (None, Some(new_cast)) => {
            let mut steps = vec![MigrationStep::Cast(CastOperation::Create {
                cast: Box::new(new_cast.clone()),
            })];

            // A CREATE CAST statement cannot embed a comment, so emit it separately.
            if let Some(comment_op) = comment_utils::handle_comment_creation(
                &new_cast.comment,
                AttrTarget::object(new_cast.id()),
            ) {
                steps.push(MigrationStep::Cast(CastOperation::Comment(comment_op)));
            }

            steps
        }

        // DROP removed cast
        (Some(old_cast), None) => {
            vec![MigrationStep::Cast(CastOperation::Drop {
                identifier: CastIdentifier::from_cast(old_cast),
            })]
        }

        // REPLACE or comment-only changes
        (Some(old_cast), Some(new_cast)) => {
            let mut steps = Vec::new();

            if casts_differ_structurally(old_cast, new_cast) {
                steps.push(MigrationStep::Cast(CastOperation::Replace {
                    old_cast: Box::new(old_cast.clone()),
                    new_cast: Box::new(new_cast.clone()),
                }));
            } else {
                let comment_ops =
                    comment_utils::handle_comment_diff(Some(old_cast), Some(new_cast), || {
                        AttrTarget::object(new_cast.id())
                    });
                for comment_op in comment_ops {
                    steps.push(MigrationStep::Cast(CastOperation::Comment(comment_op)));
                }
            }

            steps
        }

        // No change
        (None, None) => vec![],
    }
}

/// Two casts differ structurally if anything other than the comment changed.
/// The reconstructed `definition` captures method, context, and function.
fn casts_differ_structurally(old: &Cast, new: &Cast) -> bool {
    old.definition != new.definition
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::id::DbObjectId;

    fn test_cast(target: &str) -> Cast {
        Cast {
            source: "celsius".to_string(),
            target: target.to_string(),
            definition: format!(
                "CREATE CAST (celsius AS {}) WITH FUNCTION public.c_to_x(celsius)",
                target
            ),
            comment: None,
            depends_on: vec![DbObjectId::Type {
                schema: "public".to_string(),
                name: "celsius".to_string(),
            }],
        }
    }

    #[test]
    fn test_diff_no_changes() {
        let c = test_cast("fahrenheit");
        assert!(diff(Some(&c), Some(&c)).is_empty());
    }

    #[test]
    fn test_diff_create() {
        let c = test_cast("fahrenheit");
        let steps = diff(None, Some(&c));
        assert_eq!(steps.len(), 1);
        assert!(matches!(
            &steps[0],
            MigrationStep::Cast(CastOperation::Create { .. })
        ));
    }

    #[test]
    fn test_diff_create_with_comment() {
        let mut c = test_cast("fahrenheit");
        c.comment = Some("temperature conversion".to_string());
        let steps = diff(None, Some(&c));
        assert_eq!(steps.len(), 2);
        assert!(matches!(
            &steps[1],
            MigrationStep::Cast(CastOperation::Comment(_))
        ));
    }

    #[test]
    fn test_diff_drop() {
        let c = test_cast("fahrenheit");
        let steps = diff(Some(&c), None);
        assert_eq!(steps.len(), 1);
        match &steps[0] {
            MigrationStep::Cast(CastOperation::Drop { identifier }) => {
                assert_eq!(identifier.source, "celsius");
                assert_eq!(identifier.target, "fahrenheit");
            }
            _ => panic!("expected CastOperation::Drop"),
        }
    }

    #[test]
    fn test_diff_replace_on_structural_change() {
        let old = test_cast("fahrenheit");
        let mut new = test_cast("fahrenheit");
        new.definition.push_str(" AS ASSIGNMENT");
        let steps = diff(Some(&old), Some(&new));
        assert_eq!(steps.len(), 1);
        assert!(matches!(
            &steps[0],
            MigrationStep::Cast(CastOperation::Replace { .. })
        ));
    }

    #[test]
    fn test_diff_comment_only_change() {
        let old = test_cast("fahrenheit");
        let mut new = test_cast("fahrenheit");
        new.comment = Some("now documented".to_string());
        let steps = diff(Some(&old), Some(&new));
        assert_eq!(steps.len(), 1);
        assert!(matches!(
            &steps[0],
            MigrationStep::Cast(CastOperation::Comment(_))
        ));
    }
}
