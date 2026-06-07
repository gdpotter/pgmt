//! Grant operations

use super::OperationKind;
use crate::catalog::grant::{Grant, GranteeType};
use crate::catalog::id::DbObjectId;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone)]
pub enum GrantOperation {
    Grant {
        grant: Grant,
    },
    Revoke {
        grant: Grant,
    },
    /// A set of column-level grants on one relation, folded into a single
    /// statement (`GRANT SELECT (a, b), UPDATE (a) ON t TO role`). Produced by
    /// the column-grant coalescing pass; see [`crate::diff::grants::coalesce_column_grants`].
    GrantColumns(ColumnGrants),
    /// The REVOKE counterpart of [`GrantOperation::GrantColumns`].
    RevokeColumns(ColumnGrants),
}

/// Column-level grants for a single `(grantee, relation, grant-option)` tuple,
/// grouped by privilege so they render as one statement.
///
/// Grant identity stays per-column everywhere else in the model (catalog fetch,
/// `Grant::id()`, dependency tracking); this is purely an output-shaping
/// representation built right before ordering.
#[derive(Debug, Clone)]
pub struct ColumnGrants {
    pub grantee: GranteeType,
    /// The owning relation (Table or View). Column grants reference the bare
    /// relation in SQL, with the columns attached to each privilege.
    pub relation: DbObjectId,
    pub with_grant_option: bool,
    /// Privilege -> the columns it applies to, e.g.
    /// `{"SELECT": {"a", "b"}, "UPDATE": {"a"}}`. Sorted map/sets keep the
    /// rendered privilege and column order stable across runs.
    pub privilege_columns: BTreeMap<String, BTreeSet<String>>,
    /// What this grant orders after — always the owning relation.
    pub depends_on: Vec<DbObjectId>,
    /// The id of one constituent per-column grant, used as this operation's
    /// identity for dependency ordering. For `GrantColumns` it names a grant
    /// present in the new catalog; for `RevokeColumns`, one present in the old
    /// catalog — so ordering resolves the relation edge in the correct
    /// direction (create: relation -> grant; drop: revoke -> relation).
    pub rep_id: String,
}

impl ColumnGrants {
    /// The `(schema, name)` of the owning relation. The relation is always a
    /// table or view (the only relations with grantable columns).
    pub fn relation_schema_and_name(&self) -> (String, String) {
        match &self.relation {
            DbObjectId::Table { schema, name } | DbObjectId::View { schema, name } => {
                (schema.clone(), name.clone())
            }
            other => unreachable!("column grant relation must be a table or view, got {other}"),
        }
    }
}

impl GrantOperation {
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            // GRANT creates a new permission
            Self::Grant { .. } | Self::GrantColumns(_) => OperationKind::Create,
            // REVOKE removes a permission - needs to run before the object is dropped
            // so it's classified as Drop for proper ordering
            Self::Revoke { .. } | Self::RevokeColumns(_) => OperationKind::Drop,
        }
    }
}
