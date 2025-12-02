//! Domain operations for schema migrations

use super::comments::{CommentOperation, CommentTarget};
use crate::catalog::id::DbObjectId;
use crate::render::quote_ident;

#[derive(Debug, Clone)]
pub enum DomainOperation {
    Create {
        schema: String,
        name: String,
        definition: String,
    },
    Drop {
        schema: String,
        name: String,
    },
    AlterSetNotNull {
        schema: String,
        name: String,
    },
    AlterDropNotNull {
        schema: String,
        name: String,
    },
    AlterSetDefault {
        schema: String,
        name: String,
        default: String,
    },
    AlterDropDefault {
        schema: String,
        name: String,
    },
    AddConstraint {
        schema: String,
        name: String,
        constraint_name: String,
        expression: String,
    },
    DropConstraint {
        schema: String,
        name: String,
        constraint_name: String,
    },
    Comment(CommentOperation<DomainIdentifier>),
}

#[derive(Debug, Clone)]
pub struct DomainIdentifier {
    pub schema: String,
    pub name: String,
}

impl CommentTarget for DomainIdentifier {
    const OBJECT_TYPE: &'static str = "DOMAIN";

    fn identifier(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.name))
    }

    fn db_object_id(&self) -> DbObjectId {
        DbObjectId::Domain {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }
}
