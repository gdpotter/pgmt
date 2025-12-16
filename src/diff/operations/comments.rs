//! Generic comment operations for all object types

use super::OperationKind;
use crate::catalog::id::DbObjectId;

/// Generic comment operation that works for any object type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommentOperation<T> {
    Set { target: T, comment: String },
    Drop { target: T },
}

impl<T> CommentOperation<T> {
    /// Returns the operation kind. Note: This is typically not called directly since
    /// CommentOperation is wrapped by parent operations (e.g., TableOperation::Comment),
    /// but it's provided for consistency with other operation types.
    #[allow(dead_code)]
    pub fn operation_kind(&self) -> OperationKind {
        // Comments are always Alter operations - they modify metadata on existing objects
        match self {
            Self::Set { .. } | Self::Drop { .. } => OperationKind::Alter,
        }
    }
}

/// Trait for objects that can have comments
pub trait CommentTarget {
    const OBJECT_TYPE: &'static str;
    fn identifier(&self) -> String;
    fn db_object_id(&self) -> DbObjectId;
}
