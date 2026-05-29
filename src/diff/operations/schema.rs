//! Schema operations

use super::{CommentOperation, OperationKind};

#[derive(Debug, Clone)]
pub enum SchemaOperation {
    Create { name: String },
    Drop { name: String },
    Comment(CommentOperation),
}

impl SchemaOperation {
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            Self::Create { .. } => OperationKind::Create,
            Self::Drop { .. } => OperationKind::Drop,
            Self::Comment(_) => OperationKind::Alter,
        }
    }
}
