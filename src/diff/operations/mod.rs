//! Hierarchical and scalable migration operations
//!
//! This module provides a more maintainable approach to migration steps
//! using hierarchical enums and trait-based rendering.

use crate::catalog::id::DbObjectId;
use crate::render::RenderedSql;

pub use comments::*;
pub use constraint::*;
pub use extension::*;
pub use function::*;
pub use grant::*;
pub use index::*;
pub use schema::*;
pub use sequence::*;
pub use table::*;
pub use trigger::*;
pub use types::*;
pub use view::*;

pub mod comments;
pub mod constraint;
pub mod extension;
pub mod function;
pub mod grant;
pub mod index;
pub mod schema;
pub mod sequence;
pub mod table;
pub mod trigger;
pub mod types;
pub mod view;

/// Main migration step - hierarchical structure for scalability
#[derive(Debug, Clone)]
pub enum MigrationStep {
    Schema(SchemaOperation),
    Table(TableOperation),
    View(ViewOperation),
    Type(TypeOperation),
    Sequence(SequenceOperation),
    Function(FunctionOperation),
    Index(IndexOperation),
    Constraint(ConstraintOperation),
    Trigger(TriggerOperation),
    Extension(ExtensionOperation),
    Grant(GrantOperation),
}

/// Trait for rendering SQL from operations
pub trait SqlRenderer {
    fn to_sql(&self) -> Vec<RenderedSql>;
    fn db_object_id(&self) -> DbObjectId;
    fn is_destructive(&self) -> bool {
        false
    }
}

impl SqlRenderer for MigrationStep {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            MigrationStep::Schema(op) => op.to_sql(),
            MigrationStep::Table(op) => op.to_sql(),
            MigrationStep::View(op) => op.to_sql(),
            MigrationStep::Type(op) => op.to_sql(),
            MigrationStep::Sequence(op) => op.to_sql(),
            MigrationStep::Function(op) => op.to_sql(),
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
            MigrationStep::Sequence(op) => op.db_object_id(),
            MigrationStep::Function(op) => op.db_object_id(),
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
            MigrationStep::Sequence(op) => op.is_destructive(),
            MigrationStep::Function(op) => op.is_destructive(),
            MigrationStep::Index(op) => op.is_destructive(),
            MigrationStep::Constraint(op) => op.is_destructive(),
            MigrationStep::Trigger(op) => op.is_destructive(),
            MigrationStep::Extension(op) => op.is_destructive(),
            MigrationStep::Grant(op) => op.is_destructive(),
        }
    }
}

impl MigrationStep {
    /// Returns the database object ID for this migration step
    pub fn id(&self) -> DbObjectId {
        self.db_object_id()
    }

    /// Returns true if this step is a destructive operation (drop)
    pub fn is_drop(&self) -> bool {
        self.is_destructive()
    }

    /// Returns true if this step is a create operation
    pub fn is_create(&self) -> bool {
        matches!(
            self,
            MigrationStep::Schema(SchemaOperation::Create { .. })
                | MigrationStep::Extension(ExtensionOperation::Create { .. })
                | MigrationStep::Table(TableOperation::Create { .. })
                | MigrationStep::View(ViewOperation::Create { .. })
                | MigrationStep::Type(TypeOperation::Create { .. })
                | MigrationStep::Sequence(SequenceOperation::Create { .. })
                | MigrationStep::Function(FunctionOperation::Create { .. })
                | MigrationStep::Index(IndexOperation::Create { .. })
                | MigrationStep::Constraint(ConstraintOperation::Create(_))
                | MigrationStep::Trigger(TriggerOperation::Create { .. })
                | MigrationStep::Grant(GrantOperation::Grant { .. })
        )
    }

    /// Returns true if this step is a "relationship" step that creates circular dependencies
    /// These steps should be executed in a second phase after all primary object creation
    pub fn is_relationship(&self) -> bool {
        match self {
            MigrationStep::Sequence(SequenceOperation::AlterOwnership { .. }) => true,
            MigrationStep::Constraint(ConstraintOperation::Create(constraint)) => {
                matches!(
                    constraint.constraint_type,
                    crate::catalog::constraint::ConstraintType::ForeignKey { .. }
                )
            }
            _ => false,
        }
    }
}
