//! Generic comment operations for all object types

use crate::catalog::target::AttrTarget;

/// A comment operation against any object or sub-object, identified by its
/// [`AttrTarget`]. Rendering (keyword + SQL reference) lives in `render::comment`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommentOperation {
    Set { target: AttrTarget, comment: String },
    Drop { target: AttrTarget },
}
