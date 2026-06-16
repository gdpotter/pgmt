use super::OperationKind;
use crate::catalog::triggers::Trigger;

/// Identifier for a trigger
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerIdentifier {
    pub schema: String,
    pub table: String,
    pub name: String,
}

impl TriggerIdentifier {
    pub fn new(schema: String, table: String, name: String) -> Self {
        Self {
            schema,
            table,
            name,
        }
    }

    pub fn from_trigger(trigger: &Trigger) -> Self {
        Self {
            schema: trigger.schema.clone(),
            table: trigger.table_name.clone(),
            name: trigger.name.clone(),
        }
    }
}

/// Operations that can be performed on triggers
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerOperation {
    Create {
        trigger: Box<Trigger>,
    },
    Drop {
        identifier: TriggerIdentifier,
    },
    Replace {
        old_trigger: Box<Trigger>,
        new_trigger: Box<Trigger>,
    },
}

impl TriggerOperation {
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            Self::Create { .. } => OperationKind::Create,
            Self::Drop { .. } => OperationKind::Drop,
            Self::Replace { .. } => OperationKind::Alter,
        }
    }
}
