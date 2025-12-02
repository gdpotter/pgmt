use super::comments::{CommentOperation, CommentTarget};
use crate::catalog::id::DbObjectId;
use crate::render::quote_ident;

#[derive(Debug, Clone)]
pub enum FunctionOperation {
    Create {
        schema: String,
        name: String,
        arguments: String,
        #[allow(dead_code)]
        kind: String,
        #[allow(dead_code)]
        parameters: String,
        #[allow(dead_code)]
        returns: String,
        #[allow(dead_code)]
        attributes: String,
        definition: String,
    },
    Replace {
        schema: String,
        name: String,
        arguments: String,
        #[allow(dead_code)]
        kind: String,
        #[allow(dead_code)]
        parameters: String,
        #[allow(dead_code)]
        returns: String,
        #[allow(dead_code)]
        attributes: String,
        definition: String,
    },
    Drop {
        schema: String,
        name: String,
        arguments: String,
        kind: String,
        parameter_types: String,
    },
    Comment(CommentOperation<FunctionIdentifier>),
}

#[derive(Debug, Clone)]
pub struct FunctionIdentifier {
    pub schema: String,
    pub name: String,
    pub arguments: String,
}

impl CommentTarget for FunctionIdentifier {
    const OBJECT_TYPE: &'static str = "FUNCTION";

    fn identifier(&self) -> String {
        format!(
            "{}.{}({})",
            quote_ident(&self.schema),
            quote_ident(&self.name),
            self.arguments
        )
    }

    fn db_object_id(&self) -> DbObjectId {
        DbObjectId::Function {
            schema: self.schema.clone(),
            name: self.name.clone(),
            arguments: self.arguments.clone(),
        }
    }
}
