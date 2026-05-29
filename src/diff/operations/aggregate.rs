use super::{CommentOperation, OperationKind};
use crate::catalog::aggregate::Aggregate;

/// Identifier for an aggregate function
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateIdentifier {
    pub schema: String,
    pub name: String,
    pub arguments: String,
}

impl AggregateIdentifier {
    pub fn new(schema: String, name: String, arguments: String) -> Self {
        Self {
            schema,
            name,
            arguments,
        }
    }

    pub fn from_aggregate(aggregate: &Aggregate) -> Self {
        Self {
            schema: aggregate.schema.clone(),
            name: aggregate.name.clone(),
            arguments: aggregate.arguments.clone(),
        }
    }
}

/// Operations that can be performed on aggregate functions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateOperation {
    Create {
        aggregate: Box<Aggregate>,
    },
    Drop {
        identifier: AggregateIdentifier,
    },
    Replace {
        old_aggregate: Box<Aggregate>,
        new_aggregate: Box<Aggregate>,
    },
    Comment(CommentOperation),
}

impl AggregateOperation {
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            Self::Create { .. } => OperationKind::Create,
            Self::Drop { .. } => OperationKind::Drop,
            Self::Replace { .. } | Self::Comment(_) => OperationKind::Alter,
        }
    }
}
