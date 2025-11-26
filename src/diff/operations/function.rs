use super::SqlRenderer;
use super::comments::{CommentOperation, CommentTarget};
use crate::catalog::id::DbObjectId;
use crate::render::{RenderedSql, Safety, quote_ident};

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

impl SqlRenderer for FunctionOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            FunctionOperation::Create { definition, .. } => {
                // definition contains the complete CREATE OR REPLACE FUNCTION statement from pg_get_functiondef()
                vec![RenderedSql {
                    sql: if definition.trim_end().ends_with(';') {
                        definition.clone()
                    } else {
                        format!("{};", definition.trim_end())
                    },
                    safety: Safety::Safe,
                }]
            }
            FunctionOperation::Replace { definition, .. } => {
                // definition contains the complete CREATE OR REPLACE FUNCTION statement from pg_get_functiondef()
                vec![RenderedSql {
                    sql: if definition.trim_end().ends_with(';') {
                        definition.clone()
                    } else {
                        format!("{};", definition.trim_end())
                    },
                    safety: Safety::Safe,
                }]
            }
            FunctionOperation::Drop {
                schema,
                name,
                arguments: _,
                kind,
                parameter_types,
            } => vec![RenderedSql {
                sql: format!(
                    "DROP {} {}.{}({});",
                    kind,
                    quote_ident(schema),
                    quote_ident(name),
                    parameter_types
                ),
                safety: Safety::Destructive,
            }],
            FunctionOperation::Comment(op) => op.to_sql(),
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            FunctionOperation::Create {
                schema,
                name,
                arguments,
                ..
            }
            | FunctionOperation::Replace {
                schema,
                name,
                arguments,
                ..
            }
            | FunctionOperation::Drop {
                schema,
                name,
                arguments,
                ..
            } => DbObjectId::Function {
                schema: schema.clone(),
                name: name.clone(),
                arguments: arguments.clone(),
            },
            FunctionOperation::Comment(op) => op.db_object_id(),
        }
    }

    fn is_destructive(&self) -> bool {
        matches!(self, FunctionOperation::Drop { .. })
    }
}
