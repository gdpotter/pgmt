//! Generic comment operations for all object types

use super::SqlRenderer;
use crate::catalog::id::DbObjectId;
use crate::render::{RenderedSql, render_comment_sql};

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

impl<T: CommentTarget> SqlRenderer for CommentOperation<T> {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            CommentOperation::Set { target, comment } => {
                vec![render_comment_sql(
                    T::OBJECT_TYPE,
                    &target.identifier(),
                    Some(comment),
                )]
            }
            CommentOperation::Drop { target } => {
                vec![render_comment_sql(
                    T::OBJECT_TYPE,
                    &target.identifier(),
                    None,
                )]
            }
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            CommentOperation::Set { target, .. } | CommentOperation::Drop { target } => {
                DbObjectId::Comment {
                    object_id: Box::new(target.db_object_id()),
                }
            }
        }
    }

    fn is_destructive(&self) -> bool {
        matches!(self, CommentOperation::Drop { .. })
    }
}
