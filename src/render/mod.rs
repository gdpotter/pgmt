pub mod aggregate;
pub mod comment;
pub mod constraint;
pub mod domain;
pub mod extension;
pub mod function;
pub mod grant;
pub mod index;
pub mod schema;
pub mod sequence;
pub mod sql;
pub mod table;
pub mod trigger;
pub mod types;
pub mod view;

use crate::catalog::id::DbObjectId;
use crate::diff::operations::MigrationStep;

/// Trait for rendering SQL from operations
pub trait SqlRenderer {
    fn to_sql(&self) -> Vec<RenderedSql>;
    fn db_object_id(&self) -> DbObjectId;
    fn is_destructive(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Safety {
    Safe,
    Destructive,
}

#[derive(Debug, Clone)]
pub struct RenderedSql {
    pub safety: Safety,
    pub sql: String,
}

impl RenderedSql {
    pub fn new(sql: String) -> Self {
        Self {
            sql,
            safety: Safety::Safe,
        }
    }

    pub fn destructive(sql: String) -> Self {
        Self {
            sql,
            safety: Safety::Destructive,
        }
    }
}

pub fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

pub fn escape_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Generic helper for rendering comment SQL
pub fn render_comment_sql(
    object_type: &str,
    identifier: &str,
    comment: Option<&str>,
) -> RenderedSql {
    let sql = match comment {
        Some(comment_text) => format!(
            "COMMENT ON {} {} IS {};",
            object_type,
            identifier,
            escape_string(comment_text)
        ),
        None => format!("COMMENT ON {} {} IS NULL;", object_type, identifier),
    };

    RenderedSql {
        sql,
        safety: Safety::Safe,
    }
}

impl SqlRenderer for MigrationStep {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            MigrationStep::Schema(op) => op.to_sql(),
            MigrationStep::Table(op) => op.to_sql(),
            MigrationStep::View(op) => op.to_sql(),
            MigrationStep::Type(op) => op.to_sql(),
            MigrationStep::Domain(op) => op.to_sql(),
            MigrationStep::Sequence(op) => op.to_sql(),
            MigrationStep::Function(op) => op.to_sql(),
            MigrationStep::Aggregate(op) => op.to_sql(),
            MigrationStep::Index(op) => op.to_sql(),
            MigrationStep::Constraint(op) => op.to_sql(),
            MigrationStep::Trigger(op) => op.to_sql(),
            MigrationStep::Extension(op) => op.to_sql(),
            MigrationStep::Grant(op) => op.to_sql(),
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            MigrationStep::Schema(op) => op.db_object_id(),
            MigrationStep::Table(op) => op.db_object_id(),
            MigrationStep::View(op) => op.db_object_id(),
            MigrationStep::Type(op) => op.db_object_id(),
            MigrationStep::Domain(op) => op.db_object_id(),
            MigrationStep::Sequence(op) => op.db_object_id(),
            MigrationStep::Function(op) => op.db_object_id(),
            MigrationStep::Aggregate(op) => op.db_object_id(),
            MigrationStep::Index(op) => op.db_object_id(),
            MigrationStep::Constraint(op) => op.db_object_id(),
            MigrationStep::Trigger(op) => op.db_object_id(),
            MigrationStep::Extension(op) => op.db_object_id(),
            MigrationStep::Grant(op) => op.db_object_id(),
        }
    }

    fn is_destructive(&self) -> bool {
        match self {
            MigrationStep::Schema(op) => op.is_destructive(),
            MigrationStep::Table(op) => op.is_destructive(),
            MigrationStep::View(op) => op.is_destructive(),
            MigrationStep::Type(op) => op.is_destructive(),
            MigrationStep::Domain(op) => op.is_destructive(),
            MigrationStep::Sequence(op) => op.is_destructive(),
            MigrationStep::Function(op) => op.is_destructive(),
            MigrationStep::Aggregate(op) => op.is_destructive(),
            MigrationStep::Index(op) => op.is_destructive(),
            MigrationStep::Constraint(op) => op.is_destructive(),
            MigrationStep::Trigger(op) => op.is_destructive(),
            MigrationStep::Extension(op) => op.is_destructive(),
            MigrationStep::Grant(op) => op.is_destructive(),
        }
    }
}
