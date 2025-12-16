//! Constraint operations for migrations
use crate::catalog::constraint::Constraint;
use crate::catalog::id::DbObjectId;
use crate::diff::operations::{CommentOperation, CommentTarget, OperationKind};
use crate::render::quote_ident;

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

impl ConstraintOperation {
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            Self::Create(_) => OperationKind::Create,
            Self::Drop(_) => OperationKind::Drop,
            Self::Comment(_) => OperationKind::Alter,
        }
    }
}
