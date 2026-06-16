//! Type operations for schema migrations

use super::OperationKind;

#[derive(Debug, Clone)]
pub enum TypeOperation {
    Create {
        schema: String,
        name: String,
        kind: String,
        definition: String,
    },
    Drop {
        schema: String,
        name: String,
    },
    Alter {
        schema: String,
        name: String,
        action: String,
        definition: String,
    },
}

impl TypeOperation {
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            Self::Create { .. } => OperationKind::Create,
            Self::Drop { .. } => OperationKind::Drop,
            Self::Alter { .. } => OperationKind::Alter,
        }
    }
}
