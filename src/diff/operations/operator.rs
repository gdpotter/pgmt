use super::OperationKind;
use crate::catalog::operator::Operator;

/// Identifier for a user-defined operator, used by DROP.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorIdentifier {
    pub schema: String,
    pub name: String,
    pub arguments: String,
}

impl OperatorIdentifier {
    pub fn from_operator(operator: &Operator) -> Self {
        Self {
            schema: operator.schema.clone(),
            name: operator.name.clone(),
            arguments: operator.arguments.clone(),
        }
    }
}

/// Operations that can be performed on user-defined operators.
///
/// Operators cannot change their argument types or implementing function in
/// place, so any structural change is a DROP + CREATE (`Replace`). Only the
/// comment can be altered without a recreate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorOperation {
    Create {
        operator: Box<Operator>,
    },
    Drop {
        identifier: OperatorIdentifier,
    },
    Replace {
        old_operator: Box<Operator>,
        new_operator: Box<Operator>,
    },
}

impl OperatorOperation {
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            Self::Create { .. } => OperationKind::Create,
            Self::Drop { .. } => OperationKind::Drop,
            Self::Replace { .. } => OperationKind::Alter,
        }
    }
}
