//! Schema operations

use super::{CommentOperation, CommentTarget, SqlRenderer};
use crate::catalog::id::DbObjectId;
use crate::render::{RenderedSql, Safety, quote_ident};

#[derive(Debug, Clone)]
pub enum SchemaOperation {
    Create { name: String },
    Drop { name: String },
    Comment(CommentOperation<SchemaTarget>),
}

#[derive(Debug, Clone)]
pub struct SchemaTarget {
    pub name: String,
}

impl CommentTarget for SchemaTarget {
    const OBJECT_TYPE: &'static str = "SCHEMA";

    fn identifier(&self) -> String {
        quote_ident(&self.name)
    }

    fn db_object_id(&self) -> DbObjectId {
        DbObjectId::Schema {
            name: self.name.clone(),
        }
    }
}

impl SqlRenderer for SchemaOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            SchemaOperation::Create { name } => vec![RenderedSql {
                sql: format!("CREATE SCHEMA {};", quote_ident(name)),
                safety: Safety::Safe,
            }],
            SchemaOperation::Drop { name } => vec![RenderedSql {
                sql: format!("DROP SCHEMA {};", quote_ident(name)),
                safety: Safety::Destructive,
            }],
            SchemaOperation::Comment(op) => op.to_sql(),
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            SchemaOperation::Create { name } | SchemaOperation::Drop { name } => {
                DbObjectId::Schema { name: name.clone() }
            }
            SchemaOperation::Comment(op) => op.db_object_id(),
        }
    }

    fn is_destructive(&self) -> bool {
        matches!(self, SchemaOperation::Drop { .. })
    }
}
