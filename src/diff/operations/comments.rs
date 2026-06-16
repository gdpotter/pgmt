//! Generic comment operations for all object types

use super::OperationKind;
use crate::catalog::target::AttrTarget;

/// A comment operation against any object or sub-object, identified by its
/// [`AttrTarget`]. Rendering (keyword + SQL reference) lives in `render::comment`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommentOperation {
    Set { target: AttrTarget, comment: String },
    Drop { target: AttrTarget },
}

impl CommentOperation {
    /// A comment is metadata on an existing object — always an ALTER for
    /// ordering purposes, whether it is being set or dropped.
    pub fn operation_kind(&self) -> OperationKind {
        OperationKind::Alter
    }

    /// The object (or sub-object) this comment attaches to.
    pub fn target(&self) -> &AttrTarget {
        match self {
            CommentOperation::Set { target, .. } | CommentOperation::Drop { target } => target,
        }
    }
}
