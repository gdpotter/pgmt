//! Constraint operations for migrations
use crate::catalog::constraint::Constraint;
use crate::catalog::id::DbObjectId;
use crate::diff::operations::OperationKind;

#[derive(Debug, Clone)]
pub struct ConstraintIdentifier {
    pub schema: String,
    pub table_name: String,
    pub name: String,
}

impl ConstraintIdentifier {
    pub fn from_constraint(constraint: &Constraint) -> Self {
        Self {
            schema: constraint.schema.clone(),
            table_name: constraint.table_name.clone(),
            name: constraint.name.clone(),
        }
    }

    pub fn to_db_object_id(&self) -> DbObjectId {
        DbObjectId::Constraint {
            schema: self.schema.clone(),
            table: self.table_name.clone(),
            name: self.name.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ConstraintOperation {
    Create(Constraint),
    Drop(ConstraintIdentifier),
}

impl ConstraintOperation {
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            Self::Create(_) => OperationKind::Create,
            Self::Drop(_) => OperationKind::Drop,
        }
    }
}
