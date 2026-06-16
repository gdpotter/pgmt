//! Schema operations

use super::OperationKind;

#[derive(Debug, Clone)]
pub enum SchemaOperation {
    Create { name: String },
    Drop { name: String },
}

impl SchemaOperation {
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            Self::Create { .. } => OperationKind::Create,
            Self::Drop { .. } => OperationKind::Drop,
        }
    }
}
