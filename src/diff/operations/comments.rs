//! Generic comment operations for all object types

use crate::catalog::id::DbObjectId;

/// Generic comment operation that works for any object type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommentOperation<T> {
    Set { target: T, comment: String },
    Drop { target: T },
}

/// Trait for objects that can have comments
pub trait CommentTarget {
    const OBJECT_TYPE: &'static str;
    fn identifier(&self) -> String;
    fn db_object_id(&self) -> DbObjectId;
}
