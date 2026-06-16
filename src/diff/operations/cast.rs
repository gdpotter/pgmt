use super::OperationKind;
use crate::catalog::cast::Cast;

/// Identifier for a cast, used by DROP. A cast is keyed by its (source, target)
/// type pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CastIdentifier {
    pub source: String,
    pub target: String,
}

impl CastIdentifier {
    pub fn from_cast(cast: &Cast) -> Self {
        Self {
            source: cast.source.clone(),
            target: cast.target.clone(),
        }
    }
}

/// Operations that can be performed on casts.
///
/// PostgreSQL has no `ALTER CAST`, so any structural change is a DROP + CREATE
/// (`Replace`). Only the comment can be altered without a recreate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CastOperation {
    Create {
        cast: Box<Cast>,
    },
    Drop {
        identifier: CastIdentifier,
    },
    Replace {
        old_cast: Box<Cast>,
        new_cast: Box<Cast>,
    },
}

impl CastOperation {
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            Self::Create { .. } => OperationKind::Create,
            Self::Drop { .. } => OperationKind::Drop,
            Self::Replace { .. } => OperationKind::Alter,
        }
    }
}
