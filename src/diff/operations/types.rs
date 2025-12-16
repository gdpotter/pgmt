//! Type operations for schema migrations

use super::OperationKind;
use super::comments::{CommentOperation, CommentTarget};
use crate::catalog::id::DbObjectId;
use crate::render::quote_ident;

#[derive(Debug, Clone)]
pub enum TypeOperation {
    Create {
        schema: String,
        name: String,
        kind: String,
        definition: String,
    },
    Drop {
        schema: String,
        name: String,
    },
    Alter {
        schema: String,
        name: String,
        action: String,
        definition: String,
    },
    Comment(CommentOperation<TypeIdentifier>),
}

impl TypeOperation {
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            Self::Create { .. } => OperationKind::Create,
            Self::Drop { .. } => OperationKind::Drop,
            Self::Alter { .. } => OperationKind::Alter,
            Self::Comment(_) => OperationKind::Alter,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TypeIdentifier {
    pub schema: String,
    pub name: String,
}

impl CommentTarget for TypeIdentifier {
    const OBJECT_TYPE: &'static str = "TYPE";

    fn identifier(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.name))
    }

    fn db_object_id(&self) -> DbObjectId {
        DbObjectId::Type {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }
}
