//! Domain operations for schema migrations

use super::OperationKind;

#[derive(Debug, Clone)]
pub enum DomainOperation {
    Create {
        schema: String,
        name: String,
        definition: String,
    },
    Drop {
        schema: String,
        name: String,
    },
    AlterSetNotNull {
        schema: String,
        name: String,
    },
    AlterDropNotNull {
        schema: String,
        name: String,
    },
    AlterSetDefault {
        schema: String,
        name: String,
        default: String,
    },
    AlterDropDefault {
        schema: String,
        name: String,
    },
    AddConstraint {
        schema: String,
        name: String,
        constraint_name: String,
        expression: String,
    },
    DropConstraint {
        schema: String,
        name: String,
        constraint_name: String,
    },
}

impl DomainOperation {
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            Self::Create { .. } => OperationKind::Create,
            Self::Drop { .. } => OperationKind::Drop,
            Self::AlterSetNotNull { .. }
            | Self::AlterDropNotNull { .. }
            | Self::AlterSetDefault { .. }
            | Self::AlterDropDefault { .. }
            | Self::AddConstraint { .. }
            | Self::DropConstraint { .. } => OperationKind::Alter,
        }
    }
}
