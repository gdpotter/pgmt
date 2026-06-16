use super::OperationKind;
use crate::catalog::extension::Extension;

/// Identifier for an extension
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionIdentifier {
    pub name: String,
}

impl ExtensionIdentifier {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

/// Operations that can be performed on extensions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtensionOperation {
    Create { extension: Extension },
    Drop { identifier: ExtensionIdentifier },
}

impl ExtensionOperation {
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            Self::Create { .. } => OperationKind::Create,
            Self::Drop { .. } => OperationKind::Drop,
        }
    }
}
