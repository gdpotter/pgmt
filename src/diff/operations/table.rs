//! Table operations

use super::{CommentOperation, OperationKind};
use crate::catalog::table::{Column, PrimaryKey};

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
    Comment(CommentOperation),
}

impl TableOperation {
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            Self::Create { .. } => OperationKind::Create,
            Self::Drop { .. } => OperationKind::Drop,
            Self::Alter { .. } => OperationKind::Alter,
            Self::Comment(_) => OperationKind::Alter,
        }
    }
}

/// Column-level actions within ALTER TABLE
#[derive(Debug, Clone)]
pub enum ColumnAction {
    Add { column: Column },
    Drop { name: String },
    SetNotNull { name: String },
    DropNotNull { name: String },
    SetDefault { name: String, default: String },
    DropDefault { name: String },
    DropGenerated { name: String },
    AlterType { name: String, new_type: String },
    AddPrimaryKey { constraint: PrimaryKey },
    DropPrimaryKey { name: String },
    EnableRls,
    DisableRls,
    ForceRls,
    NoForceRls,
    Comment(CommentOperation),
}
