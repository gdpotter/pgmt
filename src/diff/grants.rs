//! Diff grants between catalogs

use crate::catalog::grant::{Grant, GranteeType, target_key};
use crate::catalog::id::DbObjectId;
use crate::diff::operations::{GrantOperation, MigrationStep};
use std::collections::{BTreeMap, BTreeSet};

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
                diff_privilege_change(old, new)
            }
        }
        (None, None) => vec![], // Should not happen in practice
    }
}

/// Diff a grant that exists in both states for the same grantee/target.
///
/// When the grant option is unchanged we emit only the delta: a single REVOKE
/// for privileges that were dropped and a single GRANT for privileges that were
/// added (either may be empty). This avoids the churn of revoking and
/// re-granting privileges present in both states — going from `SELECT, INSERT`
/// to `SELECT, INSERT, UPDATE` emits just `GRANT UPDATE`, with no REVOKE.
///
/// When the grant option differs we fall back to a full revoke + re-grant. A
/// granular downgrade (dropping the grant option while keeping the privilege)
/// would require `REVOKE GRANT OPTION FOR ...`, which pgmt does not model, and
/// grant-option changes are rare; a full revoke + re-grant is always correct.
fn diff_privilege_change(old: &Grant, new: &Grant) -> Vec<MigrationStep> {
    if old.with_grant_option != new.with_grant_option {
        return vec![
            MigrationStep::Grant(GrantOperation::Revoke { grant: old.clone() }),
            MigrationStep::Grant(GrantOperation::Grant { grant: new.clone() }),
        ];
    }

    // Grant option unchanged: emit only the privilege delta. BTreeSet keeps the
    // resulting privilege lists in deterministic (alphabetical) order, matching
    // the order produced when fetching grants from the catalog.
    let old_privs: BTreeSet<&str> = old.privileges.iter().map(String::as_str).collect();
    let new_privs: BTreeSet<&str> = new.privileges.iter().map(String::as_str).collect();

    let to_revoke: Vec<String> = old_privs
        .difference(&new_privs)
        .map(|p| p.to_string())
        .collect();
    let to_grant: Vec<String> = new_privs
        .difference(&old_privs)
        .map(|p| p.to_string())
        .collect();

    let mut steps = Vec::new();

    if !to_revoke.is_empty() {
        steps.push(MigrationStep::Grant(GrantOperation::Revoke {
            grant: Grant {
                privileges: to_revoke,
                ..old.clone()
            },
        }));
    }

    if !to_grant.is_empty() {
        steps.push(MigrationStep::Grant(GrantOperation::Grant {
            grant: Grant {
                privileges: to_grant,
                ..new.clone()
            },
        }));
    }

    steps
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

    let all_ids: BTreeSet<_> = old_map.keys().chain(new_map.keys()).cloned().collect();

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
            .entry(target_key(&grant.target))
            .or_default()
            .push(grant);
    }

    // Group new grants by object
    let mut new_by_object: BTreeMap<String, Vec<&Grant>> = BTreeMap::new();
    for grant in new_grants {
        new_by_object
            .entry(target_key(&grant.target))
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
        let expected_public_privileges = get_default_public_privileges(&sample_grant.target.object);

        for privilege in expected_public_privileges {
            // Check if this default PUBLIC grant exists in new state
            let public_grant_exists = new_object_grants.iter().any(|g| {
                matches!(&g.grantee, GranteeType::Public) && g.privileges.contains(&privilege)
            });

            if !public_grant_exists {
                // Default was revoked - generate REVOKE statement
                let revoke_grant = Grant {
                    grantee: GranteeType::Public,
                    target: sample_grant.target.clone(),
                    privileges: vec![privilege],
                    with_grant_option: false,
                    depends_on: vec![sample_grant.target.db_object_id()],
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

/// Get the default PUBLIC privileges for an object kind.
/// These are the privileges that PostgreSQL grants to PUBLIC by default.
fn get_default_public_privileges(object: &DbObjectId) -> Vec<String> {
    match object {
        // Functions, procedures, and aggregates: PUBLIC has EXECUTE by default
        DbObjectId::Function { .. }
        | DbObjectId::Procedure { .. }
        | DbObjectId::Aggregate { .. } => {
            vec!["EXECUTE".to_string()]
        }
        // Types and domains: PUBLIC has USAGE by default
        DbObjectId::Type { .. } | DbObjectId::Domain { .. } => {
            vec!["USAGE".to_string()]
        }
        // Everything else has no PUBLIC defaults (owner only). Columns never have
        // default privileges (attacl is NULL by default).
        _ => {
            vec![]
        }
    }
}
