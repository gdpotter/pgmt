//! Table operations

use super::{CommentOperation, CommentTarget, OperationKind};
use crate::catalog::id::DbObjectId;
use crate::catalog::table::{Column, PrimaryKey};
use crate::render::quote_ident;

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
    Comment(CommentOperation<TableTarget>),
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

#[derive(Debug, Clone)]
pub struct TableTarget {
    pub schema: String,
    pub table: String,
}

impl CommentTarget for TableTarget {
    const OBJECT_TYPE: &'static str = "TABLE";

    fn identifier(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.table))
    }

    fn db_object_id(&self) -> DbObjectId {
        DbObjectId::Table {
            schema: self.schema.clone(),
            name: self.table.clone(),
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
    Comment(CommentOperation<ColumnIdentifier>),
}

#[derive(Debug, Clone)]
pub struct ColumnIdentifier {
    pub schema: String,
    pub table: String,
    pub name: String,
}

impl CommentTarget for ColumnIdentifier {
    const OBJECT_TYPE: &'static str = "COLUMN";

    fn identifier(&self) -> String {
        format!(
            "{}.{}.{}",
            quote_ident(&self.schema),
            quote_ident(&self.table),
            quote_ident(&self.name)
        )
    }

    fn db_object_id(&self) -> DbObjectId {
        DbObjectId::Table {
            schema: self.schema.clone(),
            name: self.table.clone(),
        }
    }
}
