use super::OperationKind;
use crate::catalog::collation::Collation;

/// Operations that can be performed on collations. There is no ALTER: every
/// attribute of a collation (provider, locale, determinism, rules) is fixed at
/// creation, so any change is a drop + recreate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CollationOperation {
    Create { collation: Box<Collation> },
    Drop { schema: String, name: String },
}

impl CollationOperation {
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            Self::Create { .. } => OperationKind::Create,
            Self::Drop { .. } => OperationKind::Drop,
        }
    }
}
