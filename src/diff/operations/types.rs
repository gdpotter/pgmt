//! Type operations for schema migrations

use super::SqlRenderer;
use super::comments::{CommentOperation, CommentTarget};
use crate::catalog::id::DbObjectId;
use crate::render::{RenderedSql, Safety, quote_ident};

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

impl SqlRenderer for TypeOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            TypeOperation::Create {
                schema,
                name,
                kind,
                definition,
            } => vec![RenderedSql {
                sql: match kind.as_str() {
                    "ENUM" => format!(
                        "CREATE TYPE {}.{} AS ENUM {};",
                        quote_ident(schema),
                        quote_ident(name),
                        definition
                    ),
                    "COMPOSITE" => format!(
                        "CREATE TYPE {}.{} AS {};",
                        quote_ident(schema),
                        quote_ident(name),
                        definition
                    ),
                    "RANGE" => format!(
                        "CREATE TYPE {}.{} AS RANGE {}",
                        quote_ident(schema),
                        quote_ident(name),
                        definition
                    ),
                    _ => format!(
                        "CREATE TYPE {}.{} AS {} {}",
                        quote_ident(schema),
                        quote_ident(name),
                        kind,
                        definition
                    ),
                },
                safety: Safety::Safe,
            }],
            TypeOperation::Drop { schema, name } => vec![RenderedSql {
                sql: format!("DROP TYPE {}.{};", quote_ident(schema), quote_ident(name)),
                safety: Safety::Destructive,
            }],
            TypeOperation::Alter {
                schema,
                name,
                action,
                definition,
            } => vec![RenderedSql {
                sql: format!(
                    "ALTER TYPE {}.{} {} {};",
                    quote_ident(schema),
                    quote_ident(name),
                    action,
                    definition
                ),
                safety: Safety::Safe,
            }],
            TypeOperation::Comment(op) => op.to_sql(),
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            TypeOperation::Create { schema, name, .. }
            | TypeOperation::Drop { schema, name }
            | TypeOperation::Alter { schema, name, .. } => DbObjectId::Type {
                schema: schema.clone(),
                name: name.clone(),
            },
            TypeOperation::Comment(op) => op.db_object_id(),
        }
    }

    fn is_destructive(&self) -> bool {
        matches!(self, TypeOperation::Drop { .. })
    }
}
