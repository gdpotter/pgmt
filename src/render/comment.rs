//! SQL rendering for comment operations

use crate::catalog::id::DbObjectId;
use crate::diff::operations::{CommentOperation, CommentTarget};
use crate::render::{RenderedSql, SqlRenderer, render_comment_sql};

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::operations::SchemaTarget;

    #[test]
    fn test_render_set_comment() {
        let target = SchemaTarget {
            name: "app".to_string(),
        };
        let op: CommentOperation<SchemaTarget> = CommentOperation::Set {
            target,
            comment: "Application schema".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert_eq!(
            rendered[0].sql,
            "COMMENT ON SCHEMA \"app\" IS 'Application schema';"
        );
    }

    #[test]
    fn test_render_set_comment_with_quotes() {
        let target = SchemaTarget {
            name: "test".to_string(),
        };
        let op: CommentOperation<SchemaTarget> = CommentOperation::Set {
            target,
            comment: "Schema for 'testing' purposes".to_string(),
        };
        let rendered = op.to_sql();
        // Single quotes in comment should be escaped
        assert!(rendered[0].sql.contains("''testing''"));
    }

    #[test]
    fn test_render_drop_comment() {
        let target = SchemaTarget {
            name: "old_schema".to_string(),
        };
        let op: CommentOperation<SchemaTarget> = CommentOperation::Drop { target };
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert_eq!(rendered[0].sql, "COMMENT ON SCHEMA \"old_schema\" IS NULL;");
    }

    #[test]
    fn test_is_destructive() {
        let target = SchemaTarget {
            name: "test".to_string(),
        };
        let set_op: CommentOperation<SchemaTarget> = CommentOperation::Set {
            target: target.clone(),
            comment: "Test".to_string(),
        };
        let drop_op: CommentOperation<SchemaTarget> = CommentOperation::Drop { target };

        assert!(!set_op.is_destructive());
        assert!(drop_op.is_destructive());
    }

    #[test]
    fn test_db_object_id() {
        let target = SchemaTarget {
            name: "myschema".to_string(),
        };
        let op: CommentOperation<SchemaTarget> = CommentOperation::Set {
            target,
            comment: "Test".to_string(),
        };
        assert_eq!(
            op.db_object_id(),
            DbObjectId::Comment {
                object_id: Box::new(DbObjectId::Schema {
                    name: "myschema".to_string()
                })
            }
        );
    }
}
