//! Diff grants between catalogs

use crate::catalog::Catalog;
use crate::catalog::grant::{Grant, GranteeType, target_key};
use crate::catalog::id::DbObjectId;
use crate::catalog::target::AttrTarget;
use crate::diff::operations::{ColumnGrants, GrantOperation, MigrationStep};
use std::collections::{BTreeMap, BTreeSet};

/// Check if a grant is to the owner of the object (owner grants are implicit in PostgreSQL)
pub fn is_owner_grant(grant: &Grant) -> bool {
    match &grant.grantee {
        GranteeType::Role(role_name) => role_name == &grant.object_owner,
        GranteeType::Public => false, // PUBLIC grants are never owner grants
    }
}

/// The object a grant operation applies to — the `GRANT … ON <object>` target,
/// not the grant's own synthetic identity. Used to match grant steps against the
/// set of objects being recreated (column grants resolve to their relation).
pub fn grant_target_object(op: &GrantOperation) -> DbObjectId {
    match op {
        GrantOperation::Grant { grant } | GrantOperation::Revoke { grant } => {
            grant.target.db_object_id()
        }
        GrantOperation::GrantColumns(cg) | GrantOperation::RevokeColumns(cg) => cg.relation.clone(),
    }
}

/// The full desired ACL for an object created from scratch in this migration —
/// brand-new, or recreated via DROP+CREATE. A DROP discards every privilege and
/// the CREATE resets the object to PostgreSQL's default ACL, so a delta computed
/// against the pre-drop object (typically empty) silently loses privileges that
/// were already in effect. We therefore emit the whole desired state:
///   - one GRANT per explicit, non-owner grant on the object (owner grants are
///     implicit in PostgreSQL), and
///   - one REVOKE … FROM PUBLIC for each default PUBLIC privilege the desired
///     state drops (e.g. EXECUTE on a function whose schema revokes it).
///
/// Column grants are included: a column grant's target resolves to its owning
/// relation, so filtering by `id` spans them, and `coalesce_column_grants` folds
/// them into per-relation statements later.
pub fn desired_acl_steps(id: &DbObjectId, new_catalog: &Catalog) -> Vec<MigrationStep> {
    let object_grants: Vec<&Grant> = new_catalog
        .grants
        .iter()
        .filter(|g| &g.target.db_object_id() == id)
        .collect();

    // When the object has no explicit ACL, the freshly CREATEd object already
    // reproduces the exact desired state (PostgreSQL's default ACL) — emitting
    // the default grants would be redundant and would flip the object's ACL from
    // default to explicit. Column ACLs are always explicit (attacl is never the
    // default), so a relation with column grants still qualifies as explicit.
    if !object_grants.iter().any(|g| !g.is_default_acl) {
        return Vec::new();
    }

    let mut steps: Vec<MigrationStep> = object_grants
        .iter()
        .filter(|g| !is_owner_grant(g))
        .map(|g| {
            MigrationStep::Grant(GrantOperation::Grant {
                grant: (*g).clone(),
            })
        })
        .collect();

    steps.extend(revoke_missing_default_public(id, &object_grants));

    steps
}

/// REVOKE steps for the default PUBLIC privileges of `id` that are absent from
/// `object_grants` (the object's grants in the desired catalog). Empty when the
/// object keeps its defaults, or when it has no ACL rows at all (default ACL —
/// the defaults ARE the desired state, so nothing to revoke).
fn revoke_missing_default_public(id: &DbObjectId, object_grants: &[&Grant]) -> Vec<MigrationStep> {
    let Some(sample) = object_grants.first() else {
        return Vec::new();
    };

    get_default_public_privileges(id)
        .into_iter()
        .filter(|privilege| {
            !object_grants.iter().any(|g| {
                matches!(&g.grantee, GranteeType::Public) && g.privileges.contains(privilege)
            })
        })
        .map(|privilege| {
            MigrationStep::Grant(GrantOperation::Revoke {
                grant: Grant {
                    grantee: GranteeType::Public,
                    target: AttrTarget::object(id.clone()),
                    privileges: vec![privilege],
                    with_grant_option: false,
                    depends_on: vec![id.clone()],
                    object_owner: sample.object_owner.clone(),
                    is_default_acl: false,
                },
            })
        })
        .collect()
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

/// Fold per-column GRANT/REVOKE steps on the same relation into a single
/// statement each: many `GRANT INSERT (col) ON t` steps become one
/// `GRANT INSERT (a, b, c) ON t`. Every other step passes through untouched.
///
/// This runs after cascade expansion (so it catches both the normal diff and
/// cascade re-grants) and before ordering. Grouping key is
/// `(GRANT vs REVOKE, grantee, relation, with_grant_option)`; within a group the
/// privileges are collected per column. It is safe with respect to dependency
/// ordering because every column grant on a relation shares the single
/// dependency on that relation, so collapsing them cannot introduce a cycle —
/// the merged op simply inherits that one dependency (see [`ColumnGrants`]).
pub fn coalesce_column_grants(steps: Vec<MigrationStep>) -> Vec<MigrationStep> {
    /// A position in the output: either a pass-through step or a placeholder for
    /// the i-th folded group (filled in once all members are accumulated).
    enum Slot {
        Step(Box<MigrationStep>),
        Group(usize),
    }

    struct Acc {
        grantee: GranteeType,
        relation: DbObjectId,
        with_grant_option: bool,
        is_grant: bool,
        privilege_columns: BTreeMap<String, BTreeSet<String>>,
        /// Lexicographically smallest constituent grant id, for stable ordering.
        rep_id: String,
    }

    // Group identity. Grantee is split into (is_public, role-name) so a role
    // literally named "public" never collides with PUBLIC.
    type Key = (bool, bool, String, DbObjectId, bool);

    let mut slots: Vec<Slot> = Vec::new();
    let mut group_index: BTreeMap<Key, usize> = BTreeMap::new();
    let mut accs: Vec<Acc> = Vec::new();

    for step in steps {
        // Only per-column GRANT/REVOKE steps participate; everything else
        // (including whole-relation and non-column grants) passes through.
        let column_grant = match &step {
            MigrationStep::Grant(GrantOperation::Grant { grant })
                if grant.target.column_name().is_some() =>
            {
                Some((true, grant))
            }
            MigrationStep::Grant(GrantOperation::Revoke { grant })
                if grant.target.column_name().is_some() =>
            {
                Some((false, grant))
            }
            _ => None,
        };

        let Some((is_grant, grant)) = column_grant else {
            slots.push(Slot::Step(Box::new(step)));
            continue;
        };

        let (is_public, role_name) = match &grant.grantee {
            GranteeType::Public => (true, String::new()),
            GranteeType::Role(name) => (false, name.clone()),
        };
        let relation = grant.target.object.clone();
        let column = grant
            .target
            .column_name()
            .expect("filtered to column grants above")
            .to_string();
        let id = grant.id();

        let key: Key = (
            is_grant,
            is_public,
            role_name,
            relation.clone(),
            grant.with_grant_option,
        );

        let idx = if let Some(&i) = group_index.get(&key) {
            i
        } else {
            let i = accs.len();
            accs.push(Acc {
                grantee: grant.grantee.clone(),
                relation,
                with_grant_option: grant.with_grant_option,
                is_grant,
                privilege_columns: BTreeMap::new(),
                rep_id: id.clone(),
            });
            slots.push(Slot::Group(i));
            group_index.insert(key, i);
            i
        };

        let acc = &mut accs[idx];
        for privilege in &grant.privileges {
            acc.privilege_columns
                .entry(privilege.clone())
                .or_default()
                .insert(column.clone());
        }
        if id < acc.rep_id {
            acc.rep_id = id;
        }
    }

    // Build one folded op per group.
    let mut group_ops: Vec<Option<MigrationStep>> = accs
        .into_iter()
        .map(|acc| {
            let cg = ColumnGrants {
                grantee: acc.grantee,
                relation: acc.relation.clone(),
                with_grant_option: acc.with_grant_option,
                privilege_columns: acc.privilege_columns,
                depends_on: vec![acc.relation],
                rep_id: acc.rep_id,
            };
            Some(MigrationStep::Grant(if acc.is_grant {
                GrantOperation::GrantColumns(cg)
            } else {
                GrantOperation::RevokeColumns(cg)
            }))
        })
        .collect();

    slots
        .into_iter()
        .map(|slot| match slot {
            Slot::Step(step) => *step,
            Slot::Group(i) => group_ops[i]
                .take()
                .expect("each group placeholder is emitted exactly once"),
        })
        .collect()
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

        // Object either didn't exist or had default ACL before, now has explicit
        // ACL: REVOKE any default PUBLIC privileges it no longer keeps.
        let object = new_object_grants[0].target.db_object_id();
        steps.extend(revoke_missing_default_public(&object, new_object_grants));
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
