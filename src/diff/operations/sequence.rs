//! Sequence operations for schema migrations

use super::OperationKind;

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
}

impl SequenceOperation {
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            Self::Create { .. } => OperationKind::Create,
            Self::Drop { .. } => OperationKind::Drop,
            Self::AlterOwnership { .. } => OperationKind::Alter,
        }
    }
}
