use crate::catalog::policy::Policy;
use crate::diff::comment_utils;
use crate::diff::operations::{MigrationStep, PolicyIdentifier, PolicyOperation};

/// Diff a single policy
pub fn diff(old: Option<&Policy>, new: Option<&Policy>) -> Vec<MigrationStep> {
    match (old, new) {
        // CREATE new policy
        (None, Some(new_policy)) => {
            vec![MigrationStep::Policy(PolicyOperation::Create {
                policy: Box::new(new_policy.clone()),
            })]
        }

        // DROP old policy
        (Some(old_policy), None) => {
            let identifier = PolicyIdentifier::new(
                old_policy.schema.clone(),
                old_policy.table_name.clone(),
                old_policy.name.clone(),
            );
            vec![MigrationStep::Policy(PolicyOperation::Drop { identifier })]
        }

        // REPLACE, ALTER, or comment-only changes
        (Some(old_policy), Some(new_policy)) => {
            let mut steps = Vec::new();

            // Check if we need REPLACE (command or permissive changed)
            if old_policy.command != new_policy.command
                || old_policy.permissive != new_policy.permissive
            {
                // Command type or permissive/restrictive changed - requires DROP + CREATE
                steps.push(MigrationStep::Policy(PolicyOperation::Replace {
                    old_policy: Box::new(old_policy.clone()),
                    new_policy: Box::new(new_policy.clone()),
                }));
            } else if policies_differ_in_alterable_fields(old_policy, new_policy) {
                // Only roles, USING, or WITH CHECK changed - can use ALTER
                let identifier = PolicyIdentifier::from_policy(new_policy);

                let new_roles = if old_policy.roles != new_policy.roles {
                    Some(new_policy.roles.clone())
                } else {
                    None
                };

                let new_using = if old_policy.using_expr != new_policy.using_expr {
                    Some(new_policy.using_expr.clone())
                } else {
                    None
                };

                let new_with_check = if old_policy.with_check_expr != new_policy.with_check_expr {
                    Some(new_policy.with_check_expr.clone())
                } else {
                    None
                };

                steps.push(MigrationStep::Policy(PolicyOperation::Alter {
                    identifier,
                    new_roles,
                    new_using,
                    new_with_check,
                }));
            } else {
                // Only comments might have changed
                let comment_ops =
                    comment_utils::handle_comment_diff(Some(old_policy), Some(new_policy), || {
                        PolicyIdentifier::from_policy(new_policy)
                    });
                for comment_op in comment_ops {
                    steps.push(MigrationStep::Policy(PolicyOperation::Comment(comment_op)));
                }
            }

            steps
        }

        // No change
        (None, None) => vec![],
    }
}

/// Check if policies differ in fields that can be altered
/// (roles, using_expr, with_check_expr - but NOT command or permissive)
fn policies_differ_in_alterable_fields(old: &Policy, new: &Policy) -> bool {
    old.roles != new.roles
        || old.using_expr != new.using_expr
        || old.with_check_expr != new.with_check_expr
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::id::DbObjectId;
    use crate::catalog::policy::PolicyCommand;

    fn create_test_policy(name: &str) -> Policy {
        Policy {
            schema: "public".to_string(),
            table_name: "users".to_string(),
            name: name.to_string(),
            command: PolicyCommand::All,
            permissive: true,
            roles: vec![],
            using_expr: Some("true".to_string()),
            with_check_expr: None,
            comment: None,
            depends_on: vec![DbObjectId::Table {
                schema: "public".to_string(),
                name: "users".to_string(),
            }],
        }
    }

    #[test]
    fn test_diff_no_changes() {
        let policy = create_test_policy("policy1");
        let steps = diff(Some(&policy), Some(&policy));
        assert!(steps.is_empty());
    }

    #[test]
    fn test_diff_create_policy() {
        let new_policy = create_test_policy("new_policy");
        let steps = diff(None, Some(&new_policy));
        assert_eq!(steps.len(), 1);

        match &steps[0] {
            MigrationStep::Policy(PolicyOperation::Create { policy }) => {
                assert_eq!(policy.name, "new_policy");
            }
            _ => panic!("Expected PolicyOperation::Create"),
        }
    }

    #[test]
    fn test_diff_drop_policy() {
        let old_policy = create_test_policy("old_policy");
        let steps = diff(Some(&old_policy), None);
        assert_eq!(steps.len(), 1);

        match &steps[0] {
            MigrationStep::Policy(PolicyOperation::Drop { identifier }) => {
                assert_eq!(identifier.name, "old_policy");
                assert_eq!(identifier.schema, "public");
                assert_eq!(identifier.table, "users");
            }
            _ => panic!("Expected PolicyOperation::Drop"),
        }
    }

    #[test]
    fn test_diff_replace_policy_command_change() {
        let old_policy = create_test_policy("test_policy");
        let mut new_policy = create_test_policy("test_policy");
        new_policy.command = PolicyCommand::Select;

        let steps = diff(Some(&old_policy), Some(&new_policy));
        assert_eq!(steps.len(), 1);

        match &steps[0] {
            MigrationStep::Policy(PolicyOperation::Replace {
                old_policy,
                new_policy,
            }) => {
                assert_eq!(old_policy.command, PolicyCommand::All);
                assert_eq!(new_policy.command, PolicyCommand::Select);
            }
            _ => panic!("Expected PolicyOperation::Replace"),
        }
    }

    #[test]
    fn test_diff_replace_policy_permissive_change() {
        let old_policy = create_test_policy("test_policy");
        let mut new_policy = create_test_policy("test_policy");
        new_policy.permissive = false; // Change to RESTRICTIVE

        let steps = diff(Some(&old_policy), Some(&new_policy));
        assert_eq!(steps.len(), 1);

        match &steps[0] {
            MigrationStep::Policy(PolicyOperation::Replace { .. }) => {
                // Expected
            }
            _ => panic!("Expected PolicyOperation::Replace"),
        }
    }

    #[test]
    fn test_diff_alter_policy_roles() {
        let old_policy = create_test_policy("test_policy");
        let mut new_policy = create_test_policy("test_policy");
        new_policy.roles = vec!["authenticated".to_string()];

        let steps = diff(Some(&old_policy), Some(&new_policy));
        assert_eq!(steps.len(), 1);

        match &steps[0] {
            MigrationStep::Policy(PolicyOperation::Alter {
                identifier,
                new_roles,
                new_using,
                new_with_check,
            }) => {
                assert_eq!(identifier.name, "test_policy");
                assert!(new_roles.is_some());
                assert_eq!(new_roles.as_ref().unwrap().len(), 1);
                assert!(new_using.is_none());
                assert!(new_with_check.is_none());
            }
            _ => panic!("Expected PolicyOperation::Alter"),
        }
    }

    #[test]
    fn test_diff_alter_policy_using() {
        let old_policy = create_test_policy("test_policy");
        let mut new_policy = create_test_policy("test_policy");
        new_policy.using_expr = Some("user_id = current_user_id()".to_string());

        let steps = diff(Some(&old_policy), Some(&new_policy));
        assert_eq!(steps.len(), 1);

        match &steps[0] {
            MigrationStep::Policy(PolicyOperation::Alter {
                new_roles,
                new_using,
                new_with_check,
                ..
            }) => {
                assert!(new_roles.is_none());
                assert!(new_using.is_some());
                assert_eq!(
                    new_using.as_ref().unwrap().as_ref().unwrap(),
                    "user_id = current_user_id()"
                );
                assert!(new_with_check.is_none());
            }
            _ => panic!("Expected PolicyOperation::Alter"),
        }
    }

    #[test]
    fn test_diff_alter_policy_with_check() {
        let old_policy = create_test_policy("test_policy");
        let mut new_policy = create_test_policy("test_policy");
        new_policy.with_check_expr = Some("status = 'active'".to_string());

        let steps = diff(Some(&old_policy), Some(&new_policy));
        assert_eq!(steps.len(), 1);

        match &steps[0] {
            MigrationStep::Policy(PolicyOperation::Alter {
                new_roles,
                new_using,
                new_with_check,
                ..
            }) => {
                assert!(new_roles.is_none());
                assert!(new_using.is_none());
                assert!(new_with_check.is_some());
                assert_eq!(
                    new_with_check.as_ref().unwrap().as_ref().unwrap(),
                    "status = 'active'"
                );
            }
            _ => panic!("Expected PolicyOperation::Alter"),
        }
    }

    #[test]
    fn test_diff_alter_multiple_fields() {
        let old_policy = create_test_policy("test_policy");
        let mut new_policy = create_test_policy("test_policy");
        new_policy.roles = vec!["authenticated".to_string()];
        new_policy.using_expr = Some("owner_id = current_user_id()".to_string());
        new_policy.with_check_expr = Some("status = 'active'".to_string());

        let steps = diff(Some(&old_policy), Some(&new_policy));
        assert_eq!(steps.len(), 1);

        match &steps[0] {
            MigrationStep::Policy(PolicyOperation::Alter {
                new_roles,
                new_using,
                new_with_check,
                ..
            }) => {
                assert!(new_roles.is_some());
                assert!(new_using.is_some());
                assert!(new_with_check.is_some());
            }
            _ => panic!("Expected PolicyOperation::Alter"),
        }
    }

    #[test]
    fn test_diff_comment_change_only() {
        let old_policy = create_test_policy("test_policy");
        let mut new_policy = create_test_policy("test_policy");
        new_policy.comment = Some("New comment".to_string());

        let steps = diff(Some(&old_policy), Some(&new_policy));
        assert_eq!(steps.len(), 1);

        match &steps[0] {
            MigrationStep::Policy(PolicyOperation::Comment(_)) => {
                // Expected comment operation
            }
            _ => panic!("Expected PolicyOperation::Comment"),
        }
    }

    #[test]
    fn test_policies_differ_in_alterable_fields() {
        let policy1 = create_test_policy("test");
        let policy2 = create_test_policy("test");

        // Same policies
        assert!(!policies_differ_in_alterable_fields(&policy1, &policy2));

        // Different roles
        let mut policy3 = create_test_policy("test");
        policy3.roles = vec!["authenticated".to_string()];
        assert!(policies_differ_in_alterable_fields(&policy1, &policy3));

        // Different using expression
        let mut policy4 = create_test_policy("test");
        policy4.using_expr = Some("user_id = current_user_id()".to_string());
        assert!(policies_differ_in_alterable_fields(&policy1, &policy4));

        // Different with check expression
        let mut policy5 = create_test_policy("test");
        policy5.with_check_expr = Some("status = 'active'".to_string());
        assert!(policies_differ_in_alterable_fields(&policy1, &policy5));

        // Different command (not an alterable field - should return false here)
        let mut policy6 = create_test_policy("test");
        policy6.command = PolicyCommand::Select;
        assert!(!policies_differ_in_alterable_fields(&policy1, &policy6));

        // Different permissive (not an alterable field - should return false here)
        let mut policy7 = create_test_policy("test");
        policy7.permissive = false;
        assert!(!policies_differ_in_alterable_fields(&policy1, &policy7));
    }
}
