//! Hierarchical and scalable migration operations
//!
//! This module provides a more maintainable approach to migration steps
//! using hierarchical enums and trait-based rendering.

use crate::catalog::id::DbObjectId;
use crate::render::Safety;

// Re-export SqlRenderer from render module
pub use crate::render::SqlRenderer;

/// The kind of operation being performed, used for ordering migrations.
/// This is separate from Safety - OperationKind is about ordering (drops before creates),
/// while Safety is about data loss risk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationKind {
    /// Creates a new database object
    Create,
    /// Drops an existing database object
    Drop,
    /// Modifies an existing database object (ALTER, COMMENT, GRANT, etc.)
    Alter,
}

pub use aggregate::*;
pub use comments::*;
pub use constraint::*;
pub use domain::*;
pub use extension::*;
pub use function::*;
pub use grant::*;
pub use index::*;
pub use policy::*;
pub use schema::*;
pub use sequence::*;
pub use table::*;
pub use trigger::*;
pub use types::*;
pub use view::*;

pub mod aggregate;
pub mod comments;
pub mod constraint;
pub mod domain;
pub mod extension;
pub mod function;
pub mod grant;
pub mod index;
pub mod policy;
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
    Domain(DomainOperation),
    Sequence(SequenceOperation),
    Function(FunctionOperation),
    Aggregate(AggregateOperation),
    Index(IndexOperation),
    Constraint(ConstraintOperation),
    Trigger(TriggerOperation),
    Policy(PolicyOperation),
    Extension(ExtensionOperation),
    Grant(GrantOperation),
}

impl MigrationStep {
    /// Returns the database object ID for this migration step
    pub fn id(&self) -> DbObjectId {
        self.db_object_id()
    }

    /// Returns the kind of operation (Create, Drop, or Alter).
    /// Used for ordering migrations - drops should happen before creates for the same object.
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            Self::Schema(op) => op.operation_kind(),
            Self::Table(op) => op.operation_kind(),
            Self::View(op) => op.operation_kind(),
            Self::Type(op) => op.operation_kind(),
            Self::Domain(op) => op.operation_kind(),
            Self::Sequence(op) => op.operation_kind(),
            Self::Function(op) => op.operation_kind(),
            Self::Aggregate(op) => op.operation_kind(),
            Self::Index(op) => op.operation_kind(),
            Self::Constraint(op) => op.operation_kind(),
            Self::Trigger(op) => op.operation_kind(),
            Self::Policy(op) => op.operation_kind(),
            Self::Extension(op) => op.operation_kind(),
            Self::Grant(op) => op.operation_kind(),
        }
    }

    /// Returns true if any of the rendered SQL statements are destructive (risk data loss).
    /// This checks the Safety of each RenderedSql produced by to_sql().
    pub fn has_destructive_sql(&self) -> bool {
        self.to_sql()
            .iter()
            .any(|s| s.safety == Safety::Destructive)
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

    /// Returns step-level dependencies that may not be in the catalog's forward_deps.
    /// This is used for dynamically generated steps (like REVOKE for missing defaults)
    /// that aren't part of the catalog but still need proper ordering.
    pub fn dependencies(&self) -> Vec<DbObjectId> {
        match self {
            MigrationStep::Grant(GrantOperation::Grant { grant }) => grant.depends_on.clone(),
            MigrationStep::Grant(GrantOperation::Revoke { grant }) => grant.depends_on.clone(),
            // Other operations use catalog.forward_deps exclusively
            _ => vec![],
        }
    }
}
