//! Sequence operations for schema migrations

use super::SqlRenderer;
use super::comments::{CommentOperation, CommentTarget};
use crate::catalog::id::DbObjectId;
use crate::render::{RenderedSql, Safety, quote_ident};

#[derive(Debug, Clone)]
pub enum SequenceOperation {
    Create {
        schema: String,
        name: String,
        data_type: String,
        start_value: i64,
        min_value: i64,
        max_value: i64,
        increment: i64,
        cycle: bool,
    },
    Drop {
        schema: String,
        name: String,
    },
    AlterOwnership {
        schema: String,
        name: String,
        owned_by: String,
    },
    Comment(CommentOperation<SequenceIdentifier>),
}

#[derive(Debug, Clone)]
pub struct SequenceIdentifier {
    pub schema: String,
    pub name: String,
}

impl CommentTarget for SequenceIdentifier {
    const OBJECT_TYPE: &'static str = "SEQUENCE";

    fn identifier(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.name))
    }

    fn db_object_id(&self) -> DbObjectId {
        DbObjectId::Sequence {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }
}

impl SqlRenderer for SequenceOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            SequenceOperation::Create {
                schema,
                name,
                data_type,
                start_value,
                min_value,
                max_value,
                increment,
                cycle,
            } => vec![RenderedSql {
                sql: format!(
                    "CREATE SEQUENCE {}.{} AS {} START {} MINVALUE {} MAXVALUE {} INCREMENT {}{};",
                    quote_ident(schema),
                    quote_ident(name),
                    data_type,
                    start_value,
                    min_value,
                    max_value,
                    increment,
                    if *cycle { " CYCLE" } else { " NO CYCLE" }
                ),
                safety: Safety::Safe,
            }],
            SequenceOperation::Drop { schema, name } => vec![RenderedSql {
                sql: format!(
                    "DROP SEQUENCE {}.{};",
                    quote_ident(schema),
                    quote_ident(name)
                ),
                safety: Safety::Destructive,
            }],
            SequenceOperation::AlterOwnership {
                schema,
                name,
                owned_by,
            } => vec![RenderedSql {
                sql: if owned_by == "NONE" {
                    format!(
                        "ALTER SEQUENCE {}.{} OWNED BY NONE;",
                        quote_ident(schema),
                        quote_ident(name)
                    )
                } else {
                    let parts: Vec<&str> = owned_by.split('.').collect();
                    if parts.len() == 3 {
                        format!(
                            "ALTER SEQUENCE {}.{} OWNED BY {}.{}.{};",
                            quote_ident(schema),
                            quote_ident(name),
                            quote_ident(parts[0]),
                            quote_ident(parts[1]),
                            quote_ident(parts[2])
                        )
                    } else {
                        format!(
                            "ALTER SEQUENCE {}.{} OWNED BY {};",
                            quote_ident(schema),
                            quote_ident(name),
                            owned_by
                        )
                    }
                },
                safety: Safety::Safe,
            }],
            SequenceOperation::Comment(op) => op.to_sql(),
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            SequenceOperation::Create { schema, name, .. }
            | SequenceOperation::Drop { schema, name }
            | SequenceOperation::AlterOwnership { schema, name, .. } => DbObjectId::Sequence {
                schema: schema.clone(),
                name: name.clone(),
            },
            SequenceOperation::Comment(op) => op.db_object_id(),
        }
    }

    fn is_destructive(&self) -> bool {
        matches!(self, SequenceOperation::Drop { .. })
    }
}
