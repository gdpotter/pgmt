//! Constraint operations for migrations
use crate::catalog::constraint::Constraint;
use crate::catalog::id::DbObjectId;
use crate::diff::operations::{CommentOperation, CommentTarget, SqlRenderer};
use crate::render::{RenderedSql, quote_ident};

#[derive(Debug, Clone)]
pub struct ConstraintIdentifier {
    pub schema: String,
    pub table: String,
    pub name: String,
}

impl ConstraintIdentifier {
    pub fn from_constraint(constraint: &Constraint) -> Self {
        Self {
            schema: constraint.schema.clone(),
            table: constraint.table.clone(),
            name: constraint.name.clone(),
        }
    }

    pub fn to_db_object_id(&self) -> DbObjectId {
        DbObjectId::Constraint {
            schema: self.schema.clone(),
            table: self.table.clone(),
            name: self.name.clone(),
        }
    }
}

impl CommentTarget for ConstraintIdentifier {
    const OBJECT_TYPE: &'static str = "CONSTRAINT";

    fn identifier(&self) -> String {
        format!(
            "{} ON {}.{}",
            quote_ident(&self.name),
            quote_ident(&self.schema),
            quote_ident(&self.table)
        )
    }

    fn db_object_id(&self) -> DbObjectId {
        self.to_db_object_id()
    }
}

#[derive(Debug, Clone)]
pub enum ConstraintOperation {
    Create(Constraint),
    Drop(ConstraintIdentifier),
    Comment(CommentOperation<ConstraintIdentifier>),
}

impl SqlRenderer for ConstraintOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            ConstraintOperation::Create(constraint) => {
                vec![RenderedSql::new(
                    crate::render::sql::render_create_constraint(constraint),
                )]
            }
            ConstraintOperation::Drop(identifier) => {
                vec![RenderedSql::destructive(
                    crate::render::sql::render_drop_constraint(
                        &identifier.schema,
                        &identifier.table,
                        &identifier.name,
                    ),
                )]
            }
            ConstraintOperation::Comment(comment_op) => comment_op.to_sql(),
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            ConstraintOperation::Create(constraint) => constraint.id(),
            ConstraintOperation::Drop(identifier) => identifier.to_db_object_id(),
            ConstraintOperation::Comment(comment_op) => comment_op.db_object_id(),
        }
    }

    fn is_destructive(&self) -> bool {
        matches!(self, ConstraintOperation::Drop(_))
    }
}
