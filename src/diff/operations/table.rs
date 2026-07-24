//! Table operations

use super::OperationKind;
use crate::catalog::collation::CollationRef;
use crate::catalog::table::{Column, IdentityKind, PrimaryKey};

#[derive(Debug, Clone)]
pub enum TableOperation {
    Create {
        schema: String,
        name: String,
        columns: Vec<Column>,
        primary_key: Option<PrimaryKey>,
    },
    Drop {
        schema: String,
        name: String,
    },
    Alter {
        schema: String,
        name: String,
        actions: Vec<ColumnAction>,
    },
}

impl TableOperation {
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            Self::Create { .. } => OperationKind::Create,
            Self::Drop { .. } => OperationKind::Drop,
            Self::Alter { .. } => OperationKind::Alter,
        }
    }
}

/// Column-level actions within ALTER TABLE
#[derive(Debug, Clone)]
pub enum ColumnAction {
    Add {
        column: Column,
    },
    Drop {
        name: String,
    },
    SetNotNull {
        name: String,
    },
    DropNotNull {
        name: String,
    },
    SetDefault {
        name: String,
        default: String,
    },
    DropDefault {
        name: String,
    },
    DropGenerated {
        name: String,
    },
    AddIdentity {
        name: String,
        kind: IdentityKind,
    },
    SetIdentityKind {
        name: String,
        kind: IdentityKind,
    },
    DropIdentity {
        name: String,
    },
    /// `ALTER COLUMN ... TYPE ...`. Also carries the column's target collation:
    /// PostgreSQL recomputes a column's collation from the TYPE clause, so
    /// omitting COLLATE resets it to the type's default — which makes this the
    /// one action for type changes, collation changes, and collation removal.
    AlterType {
        name: String,
        new_type: String,
        new_collation: Option<CollationRef>,
    },
    AddPrimaryKey {
        constraint: PrimaryKey,
    },
    DropPrimaryKey {
        name: String,
    },
    EnableRls,
    DisableRls,
    ForceRls,
    NoForceRls,
}
