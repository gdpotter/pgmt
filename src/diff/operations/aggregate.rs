use super::{CommentOperation, CommentTarget};
use crate::catalog::aggregate::Aggregate;
use crate::catalog::id::DbObjectId;

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

impl CommentTarget for AggregateIdentifier {
    const OBJECT_TYPE: &'static str = "AGGREGATE";

    fn identifier(&self) -> String {
        format!("\"{}\".\"{}\"({})", self.schema, self.name, self.arguments)
    }

    fn db_object_id(&self) -> DbObjectId {
        DbObjectId::Aggregate {
            schema: self.schema.clone(),
            name: self.name.clone(),
            arguments: self.arguments.clone(),
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
    Comment(CommentOperation<AggregateIdentifier>),
}
