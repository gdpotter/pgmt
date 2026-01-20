//! Diff grants between catalogs

use crate::catalog::grant::{Grant, GranteeType, ObjectType};
use crate::diff::operations::{GrantOperation, MigrationStep};
use std::collections::BTreeMap;

/// Check if a grant is to the owner of the object (owner grants are implicit in PostgreSQL)
pub fn is_owner_grant(grant: &Grant) -> bool {
    match &grant.grantee {
        GranteeType::Role(role_name) => role_name == &grant.object_owner,
        GranteeType::Public => false, // PUBLIC grants are never owner grants
    }
}

pub fn diff(old_grant: Option<&Grant>, new_grant: Option<&Grant>) -> Vec<MigrationStep> {
    match (old_grant, new_grant) {
        (None, Some(new)) => {
            // New grant - create GRANT operation, but skip owner grants
            if is_owner_grant(new) {
                vec![] // Skip owner grants
            } else {
                vec![MigrationStep::Grant(GrantOperation::Grant {
                    grant: new.clone(),
                })]
            }
        }
        (Some(old), None) => {
            // Grant removed - create REVOKE operation, but skip owner grants
            if is_owner_grant(old) {
                vec![] // Skip owner grants
            } else {
                vec![MigrationStep::Grant(GrantOperation::Revoke {
                    grant: old.clone(),
                })]
            }
        }
        (Some(old), Some(new)) => {
            // Grant exists in both - compare privileges, but skip owner grants
            if is_owner_grant(old) || is_owner_grant(new) {
                vec![] // Skip owner grants
            } else {
                let mut steps = Vec::new();

                // If they're different (privileges or grant options changed),
                // we need to revoke old and grant new
                if old.privileges != new.privileges
                    || old.with_grant_option != new.with_grant_option
                {
                    steps.push(MigrationStep::Grant(GrantOperation::Revoke {
                        grant: old.clone(),
                    }));
                    steps.push(MigrationStep::Grant(GrantOperation::Grant {
                        grant: new.clone(),
                    }));
                }

                steps
            }
        }
        (None, None) => vec![], // Should not happen in practice
    }
}

/// Compare grants by building maps by grant ID for efficient comparison.
/// Also generates REVOKE statements for default privileges that have been explicitly revoked.
pub fn diff_grants(old_grants: &[Grant], new_grants: &[Grant]) -> Vec<MigrationStep> {
    let mut old_map = BTreeMap::new();
    let mut new_map = BTreeMap::new();

    for grant in old_grants {
        old_map.insert(grant.id(), grant);
    }

    for grant in new_grants {
        new_map.insert(grant.id(), grant);
    }

    let all_ids: std::collections::BTreeSet<_> =
        old_map.keys().chain(new_map.keys()).cloned().collect();

    let mut steps: Vec<MigrationStep> = all_ids
        .into_iter()
        .flat_map(|id| {
            let old = old_map.get(&id).cloned();
            let new = new_map.get(&id).cloned();
            diff(old, new)
        })
        .collect();

    // Generate REVOKE statements for default privileges that have been explicitly revoked.
    // This captures cases like `REVOKE EXECUTE ON FUNCTION foo FROM PUBLIC` where
    // the default PUBLIC EXECUTE privilege was removed.
    // We only generate REVOKEs for objects that newly have explicit ACL (not for objects
    // that already had explicit ACL in the old state).
    steps.extend(generate_revoke_for_new_explicit_acls(
        old_grants, new_grants,
    ));

    steps
}

/// Generate REVOKE statements for default privileges that have been explicitly revoked
/// in the new state but were NOT already revoked in the old state.
///
/// When an object has `is_default_acl = false`, it means the ACL was explicitly set.
/// If a default privilege (like PUBLIC EXECUTE on functions) is not present in the
/// actual grants, we need to generate a REVOKE statement for it.
///
/// However, we only generate the REVOKE if:
/// 1. The object is new (didn't exist in old_grants), OR
/// 2. The object existed but had default ACL in old (is_default_acl = true) and now has
///    explicit ACL in new (is_default_acl = false)
///
/// This prevents generating spurious REVOKE statements when comparing two catalogs
/// that already have the same explicit ACL state.
fn generate_revoke_for_new_explicit_acls(
    old_grants: &[Grant],
    new_grants: &[Grant],
) -> Vec<MigrationStep> {
    let mut steps = Vec::new();

    // Group old grants by object to check prior state
    let mut old_by_object: BTreeMap<String, Vec<&Grant>> = BTreeMap::new();
    for grant in old_grants {
        old_by_object
            .entry(object_key(&grant.object))
            .or_default()
            .push(grant);
    }

    // Group new grants by object
    let mut new_by_object: BTreeMap<String, Vec<&Grant>> = BTreeMap::new();
    for grant in new_grants {
        new_by_object
            .entry(object_key(&grant.object))
            .or_default()
            .push(grant);
    }

    // For each unique object in new_grants, check if it newly has explicit ACL
    for (obj_key, new_object_grants) in &new_by_object {
        // Check if new state has explicit ACL
        let new_has_explicit_acl = new_object_grants.iter().any(|g| !g.is_default_acl);

        if !new_has_explicit_acl {
            continue; // Object uses defaults in new, no REVOKEs needed
        }

        // Check if old state also had explicit ACL for this object
        let old_had_explicit_acl = old_by_object
            .get(obj_key)
            .is_some_and(|old_grants| old_grants.iter().any(|g| !g.is_default_acl));

        if old_had_explicit_acl {
            // Object already had explicit ACL in old state - don't generate REVOKE
            // (the actual grants are compared separately via the normal diff path)
            continue;
        }

        // Object either didn't exist or had default ACL before, now has explicit ACL
        // Generate REVOKEs for missing PUBLIC privileges
        let sample_grant = new_object_grants[0];
        let expected_public_privileges = get_default_public_privileges(&sample_grant.object);

        for privilege in expected_public_privileges {
            // Check if this default PUBLIC grant exists in new state
            let public_grant_exists = new_object_grants.iter().any(|g| {
                matches!(&g.grantee, GranteeType::Public) && g.privileges.contains(&privilege)
            });

            if !public_grant_exists {
                // Default was revoked - generate REVOKE statement
                let revoke_grant = Grant {
                    grantee: GranteeType::Public,
                    object: sample_grant.object.clone(),
                    privileges: vec![privilege],
                    with_grant_option: false,
                    depends_on: vec![sample_grant.object.db_object_id()],
                    object_owner: sample_grant.object_owner.clone(),
                    is_default_acl: false,
                };

                steps.push(MigrationStep::Grant(GrantOperation::Revoke {
                    grant: revoke_grant,
                }));
            }
        }
    }

    steps
}

/// Generate a unique key for an object (for grouping grants)
fn object_key(object: &ObjectType) -> String {
    match object {
        ObjectType::Table { schema, name } => format!("table:{}.{}", schema, name),
        ObjectType::View { schema, name } => format!("view:{}.{}", schema, name),
        ObjectType::Schema { name } => format!("schema:{}", name),
        ObjectType::Function {
            schema,
            name,
            arguments,
        } => format!("function:{}.{}({})", schema, name, arguments),
        ObjectType::Procedure {
            schema,
            name,
            arguments,
        } => format!("procedure:{}.{}({})", schema, name, arguments),
        ObjectType::Aggregate {
            schema,
            name,
            arguments,
        } => format!("aggregate:{}.{}({})", schema, name, arguments),
        ObjectType::Sequence { schema, name } => format!("sequence:{}.{}", schema, name),
        ObjectType::Type { schema, name } => format!("type:{}.{}", schema, name),
        ObjectType::Domain { schema, name } => format!("domain:{}.{}", schema, name),
    }
}

/// Get the default PUBLIC privileges for an object type.
/// These are the privileges that PostgreSQL grants to PUBLIC by default.
fn get_default_public_privileges(object: &ObjectType) -> Vec<String> {
    match object {
        // Functions and procedures: PUBLIC has EXECUTE by default
        ObjectType::Function { .. }
        | ObjectType::Procedure { .. }
        | ObjectType::Aggregate { .. } => {
            vec!["EXECUTE".to_string()]
        }
        // Types and domains: PUBLIC has USAGE by default
        ObjectType::Type { .. } | ObjectType::Domain { .. } => {
            vec!["USAGE".to_string()]
        }
        // Tables, views, sequences, schemas: no PUBLIC defaults (owner only)
        ObjectType::Table { .. }
        | ObjectType::View { .. }
        | ObjectType::Sequence { .. }
        | ObjectType::Schema { .. } => {
            vec![]
        }
    }
}
