//! Utility functions for handling comment operations in diff modules
//! Eliminates code duplication across different database object types

use crate::catalog::comments::{CommentAction, Commentable, diff_comments};
use crate::diff::operations::{CommentOperation, CommentTarget};

/// Handle comment creation for new objects
/// Returns a CommentOperation if the object has a comment
pub fn handle_comment_creation<T: CommentTarget>(
    object_comment: &Option<String>,
    target: T,
) -> Option<CommentOperation<T>> {
    object_comment
        .as_ref()
        .map(|comment| CommentOperation::Set {
            target,
            comment: comment.clone(),
        })
}

/// Process comment diff actions and convert them to comment operations
/// Returns a vector of comment operations for comment changes
pub fn handle_comment_diff<T: CommentTarget, C: Commentable + Clone>(
    old: Option<&C>,
    new: Option<&C>,
    target_factory: impl Fn() -> T,
) -> Vec<CommentOperation<T>> {
    let comment_actions = diff_comments(old, new);
    comment_actions
        .into_iter()
        .map(|action| match action {
            CommentAction::SetComment { comment } => CommentOperation::Set {
                target: target_factory(),
                comment,
            },
            CommentAction::DropComment => CommentOperation::Drop {
                target: target_factory(),
            },
        })
        .collect()
}
