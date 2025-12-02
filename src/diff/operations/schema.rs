//! Schema operations

use super::{CommentOperation, CommentTarget};
use crate::catalog::id::DbObjectId;
use crate::render::quote_ident;

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
