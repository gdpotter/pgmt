pub mod aggregate;
pub mod cast;
pub mod comment;
pub mod constraint;
pub mod domain;
pub mod extension;
pub mod function;
pub mod grant;
pub mod index;
pub mod operator;
pub mod policy;
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
}

pub fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

pub fn escape_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
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
            MigrationStep::Operator(op) => op.to_sql(),
            MigrationStep::Cast(op) => op.to_sql(),
            MigrationStep::Index(op) => op.to_sql(),
            MigrationStep::Constraint(op) => op.to_sql(),
            MigrationStep::Trigger(op) => op.to_sql(),
            MigrationStep::Policy(op) => op.to_sql(),
            MigrationStep::Extension(op) => op.to_sql(),
            MigrationStep::Grant(op) => op.to_sql(),
            MigrationStep::Comment(op) => op.to_sql(),
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
            MigrationStep::Operator(op) => op.db_object_id(),
            MigrationStep::Cast(op) => op.db_object_id(),
            MigrationStep::Index(op) => op.db_object_id(),
            MigrationStep::Constraint(op) => op.db_object_id(),
            MigrationStep::Trigger(op) => op.db_object_id(),
            MigrationStep::Policy(op) => op.db_object_id(),
            MigrationStep::Extension(op) => op.db_object_id(),
            MigrationStep::Grant(op) => op.db_object_id(),
            MigrationStep::Comment(op) => op.db_object_id(),
        }
    }
}
