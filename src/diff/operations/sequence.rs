//! Sequence operations for schema migrations

use super::OperationKind;
use super::comments::{CommentOperation, CommentTarget};
use crate::catalog::id::DbObjectId;
use crate::render::quote_ident;

#[derive(Debug, Clone)]
pub enum SequenceOperation {
    Create {
        schema: String,
        name: String,
        data_type: String,
        start_value: i64,
        min_value: i64,
        max_value: i64,
        increment: i64,
        cycle: bool,
    },
    Drop {
        schema: String,
        name: String,
    },
    AlterOwnership {
        schema: String,
        name: String,
        owned_by: String,
    },
    Comment(CommentOperation<SequenceIdentifier>),
}

impl SequenceOperation {
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            Self::Create { .. } => OperationKind::Create,
            Self::Drop { .. } => OperationKind::Drop,
            Self::AlterOwnership { .. } | Self::Comment(_) => OperationKind::Alter,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SequenceIdentifier {
    pub schema: String,
    pub name: String,
}

impl CommentTarget for SequenceIdentifier {
    const OBJECT_TYPE: &'static str = "SEQUENCE";

    fn identifier(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.name))
    }

    fn db_object_id(&self) -> DbObjectId {
        DbObjectId::Sequence {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }
}
