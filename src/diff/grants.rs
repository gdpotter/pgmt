//! Diff grants between catalogs

use crate::catalog::grant::{Grant, GranteeType};
use crate::diff::operations::{GrantOperation, MigrationStep};
use std::collections::BTreeMap;

/// Check if a grant is to the owner of the object (owner grants are implicit in PostgreSQL)
fn is_owner_grant(grant: &Grant) -> bool {
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

/// Compare grants by building maps by grant ID for efficient comparison
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

    all_ids
        .into_iter()
        .flat_map(|id| {
            let old = old_map.get(&id).cloned();
            let new = new_map.get(&id).cloned();
            diff(old, new)
        })
        .collect()
}
