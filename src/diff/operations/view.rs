//! View operations for schema migrations

use super::OperationKind;
use super::comments::{CommentOperation, CommentTarget};
use crate::catalog::id::DbObjectId;
use crate::render::quote_ident;

#[derive(Debug, Clone)]
pub enum ViewOperation {
    Create {
        schema: String,
        name: String,
        definition: String,
    },
    Drop {
        schema: String,
        name: String,
    },
    Replace {
        schema: String,
        name: String,
        definition: String,
    },
    Comment(CommentOperation<ViewIdentifier>),
}

impl ViewOperation {
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            Self::Create { .. } => OperationKind::Create,
            Self::Drop { .. } => OperationKind::Drop,
            Self::Replace { .. } => OperationKind::Alter,
            Self::Comment(_) => OperationKind::Alter,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ViewIdentifier {
    pub schema: String,
    pub name: String,
}

impl CommentTarget for ViewIdentifier {
    const OBJECT_TYPE: &'static str = "VIEW";

    fn identifier(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.name))
    }

    fn db_object_id(&self) -> DbObjectId {
        DbObjectId::View {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }
}
