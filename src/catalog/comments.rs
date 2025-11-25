/// Trait for objects that can have comments
pub trait Commentable {
    /// Get the comment for this object
    fn comment(&self) -> &Option<String>;
}

/// Generate comment diff actions for any commentable object
pub fn diff_comments<T: Commentable + Clone>(
    old: Option<&T>,
    new: Option<&T>,
) -> Vec<CommentAction> {
    match (old, new) {
        (Some(old_obj), Some(new_obj)) => {
            match (old_obj.comment(), new_obj.comment()) {
                (None, Some(comment)) => vec![CommentAction::SetComment {
                    comment: comment.clone(),
                }],
                (Some(_), None) => vec![CommentAction::DropComment],
                (Some(old_comment), Some(new_comment)) if old_comment != new_comment => {
                    vec![CommentAction::SetComment {
                        comment: new_comment.clone(),
                    }]
                }
                _ => vec![], // No change
            }
        }
        _ => vec![], // Object creation/deletion handles comments separately
    }
}

/// Generic comment action for any object type
#[derive(Debug, Clone)]
pub enum CommentAction {
    SetComment { comment: String },
    DropComment,
}
