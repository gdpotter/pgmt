//! A shared "attribute target": the thing a comment or grant attaches to.
//!
//! Both comments and grants point at either a whole object or a sub-object (a
//! column). `DbObjectId` is the pure identity; `AttrTarget` is the render target
//! that pairs an object identity with an optional sub-object. The keyword and
//! SQL reference are derived per-renderer (see `render::comment` and
//! `render::sql::grant`), since the same object renders differently in a COMMENT
//! vs a GRANT.

use super::id::DbObjectId;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum SubObject {
    /// A column of a relation (table, view) or attribute of a composite type.
    /// Keyed by name only — `attnum` is a physical coordinate that is not stable
    /// across databases, so it never enters the model.
    Column { name: String },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AttrTarget {
    pub object: DbObjectId,
    pub sub: Option<SubObject>,
}

impl AttrTarget {
    /// Target a whole object.
    pub fn object(object: DbObjectId) -> Self {
        Self { object, sub: None }
    }

    /// Target a column/attribute of a relation or composite type. `parent` is
    /// the owning object's id (table, view, or type).
    pub fn column(parent: DbObjectId, name: impl Into<String>) -> Self {
        Self {
            object: parent,
            sub: Some(SubObject::Column { name: name.into() }),
        }
    }

    /// The identity this target depends on / orders after. Column targets
    /// resolve to their parent object (the column lives inside it).
    pub fn db_object_id(&self) -> DbObjectId {
        self.object.clone()
    }

    // The following accessors are part of the public API and exercised by the
    // integration test crate; the binary target re-declares the modules and so
    // does not see that usage (matching the project's existing dead_code allows).

    /// The owning object's schema (empty for schema-less objects).
    #[allow(dead_code)]
    pub fn schema(&self) -> String {
        self.schema_and_name().0
    }

    /// The owning object's own name.
    #[allow(dead_code)]
    pub fn name(&self) -> String {
        self.schema_and_name().1
    }

    /// The column name, if this targets a column/attribute.
    #[allow(dead_code)]
    pub fn column_name(&self) -> Option<&str> {
        match &self.sub {
            Some(SubObject::Column { name }) => Some(name.as_str()),
            None => None,
        }
    }

    /// The owning table, for objects that live on a table (constraint, trigger,
    /// policy) or for a column target.
    #[allow(dead_code)]
    pub fn table(&self) -> Option<&str> {
        match &self.object {
            DbObjectId::Constraint { table, .. }
            | DbObjectId::Trigger { table, .. }
            | DbObjectId::Policy { table, .. }
            | DbObjectId::Column { table, .. } => Some(table.as_str()),
            _ => None,
        }
    }

    /// The (schema, name) of the owning object, for routing/grouping. Column
    /// targets report their parent relation. Objects without a schema (extensions)
    /// report an empty schema.
    pub fn schema_and_name(&self) -> (String, String) {
        match &self.object {
            DbObjectId::Schema { name } => (name.clone(), name.clone()),
            DbObjectId::Table { schema, name }
            | DbObjectId::View { schema, name }
            | DbObjectId::Type { schema, name }
            | DbObjectId::Domain { schema, name }
            | DbObjectId::Sequence { schema, name }
            | DbObjectId::Index { schema, name } => (schema.clone(), name.clone()),
            DbObjectId::Function { schema, name, .. }
            | DbObjectId::Procedure { schema, name, .. }
            | DbObjectId::Aggregate { schema, name, .. }
            | DbObjectId::Operator { schema, name, .. } => (schema.clone(), name.clone()),
            DbObjectId::Constraint { schema, name, .. }
            | DbObjectId::Trigger { schema, name, .. }
            | DbObjectId::Policy { schema, name, .. } => (schema.clone(), name.clone()),
            DbObjectId::Extension { name } => (String::new(), name.clone()),
            DbObjectId::Grant { .. } | DbObjectId::Comment { .. } | DbObjectId::Column { .. } => {
                (String::new(), String::new())
            }
        }
    }
}
