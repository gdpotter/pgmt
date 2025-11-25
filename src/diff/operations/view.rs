//! View operations for schema migrations

use super::SqlRenderer;
use super::comments::{CommentOperation, CommentTarget};
use crate::catalog::id::DbObjectId;
use crate::render::{RenderedSql, Safety, quote_ident};

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

impl SqlRenderer for ViewOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            ViewOperation::Create {
                schema,
                name,
                definition,
            } => vec![RenderedSql {
                sql: format!(
                    "CREATE VIEW {}.{} AS\n{};",
                    quote_ident(schema),
                    quote_ident(name),
                    definition.trim_end_matches(';'),
                ),
                safety: Safety::Safe,
            }],
            ViewOperation::Drop { schema, name } => vec![RenderedSql {
                sql: format!("DROP VIEW {}.{};", quote_ident(schema), quote_ident(name)),
                safety: Safety::Destructive,
            }],
            ViewOperation::Replace {
                schema,
                name,
                definition,
            } => vec![RenderedSql {
                sql: format!(
                    "CREATE OR REPLACE VIEW {}.{} AS\n{};",
                    quote_ident(schema),
                    quote_ident(name),
                    definition.trim_end_matches(';'),
                ),
                safety: Safety::Safe,
            }],
            ViewOperation::Comment(op) => op.to_sql(),
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            ViewOperation::Create { schema, name, .. }
            | ViewOperation::Drop { schema, name }
            | ViewOperation::Replace { schema, name, .. } => DbObjectId::View {
                schema: schema.clone(),
                name: name.clone(),
            },
            ViewOperation::Comment(op) => op.db_object_id(),
        }
    }

    fn is_destructive(&self) -> bool {
        matches!(self, ViewOperation::Drop { .. })
    }
}
