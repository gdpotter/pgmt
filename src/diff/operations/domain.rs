//! Domain operations for schema migrations

use super::SqlRenderer;
use super::comments::{CommentOperation, CommentTarget};
use crate::catalog::id::DbObjectId;
use crate::render::{RenderedSql, Safety, quote_ident};

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

impl SqlRenderer for DomainOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            DomainOperation::Create {
                schema,
                name,
                definition,
            } => vec![RenderedSql {
                sql: format!(
                    "CREATE DOMAIN {}.{} {};",
                    quote_ident(schema),
                    quote_ident(name),
                    definition
                ),
                safety: Safety::Safe,
            }],
            DomainOperation::Drop { schema, name } => vec![RenderedSql {
                sql: format!("DROP DOMAIN {}.{};", quote_ident(schema), quote_ident(name)),
                safety: Safety::Destructive,
            }],
            DomainOperation::AlterSetNotNull { schema, name } => vec![RenderedSql {
                sql: format!(
                    "ALTER DOMAIN {}.{} SET NOT NULL;",
                    quote_ident(schema),
                    quote_ident(name)
                ),
                safety: Safety::Safe,
            }],
            DomainOperation::AlterDropNotNull { schema, name } => vec![RenderedSql {
                sql: format!(
                    "ALTER DOMAIN {}.{} DROP NOT NULL;",
                    quote_ident(schema),
                    quote_ident(name)
                ),
                safety: Safety::Safe,
            }],
            DomainOperation::AlterSetDefault {
                schema,
                name,
                default,
            } => vec![RenderedSql {
                sql: format!(
                    "ALTER DOMAIN {}.{} SET DEFAULT {};",
                    quote_ident(schema),
                    quote_ident(name),
                    default
                ),
                safety: Safety::Safe,
            }],
            DomainOperation::AlterDropDefault { schema, name } => vec![RenderedSql {
                sql: format!(
                    "ALTER DOMAIN {}.{} DROP DEFAULT;",
                    quote_ident(schema),
                    quote_ident(name)
                ),
                safety: Safety::Safe,
            }],
            DomainOperation::AddConstraint {
                schema,
                name,
                constraint_name,
                expression,
            } => vec![RenderedSql {
                sql: format!(
                    "ALTER DOMAIN {}.{} ADD CONSTRAINT {} {};",
                    quote_ident(schema),
                    quote_ident(name),
                    quote_ident(constraint_name),
                    expression
                ),
                safety: Safety::Safe,
            }],
            DomainOperation::DropConstraint {
                schema,
                name,
                constraint_name,
            } => vec![RenderedSql {
                sql: format!(
                    "ALTER DOMAIN {}.{} DROP CONSTRAINT {};",
                    quote_ident(schema),
                    quote_ident(name),
                    quote_ident(constraint_name)
                ),
                safety: Safety::Safe,
            }],
            DomainOperation::Comment(op) => op.to_sql(),
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            DomainOperation::Create { schema, name, .. }
            | DomainOperation::Drop { schema, name }
            | DomainOperation::AlterSetNotNull { schema, name }
            | DomainOperation::AlterDropNotNull { schema, name }
            | DomainOperation::AlterSetDefault { schema, name, .. }
            | DomainOperation::AlterDropDefault { schema, name }
            | DomainOperation::AddConstraint { schema, name, .. }
            | DomainOperation::DropConstraint { schema, name, .. } => DbObjectId::Domain {
                schema: schema.clone(),
                name: name.clone(),
            },
            DomainOperation::Comment(op) => op.db_object_id(),
        }
    }

    fn is_destructive(&self) -> bool {
        matches!(self, DomainOperation::Drop { .. })
    }
}
