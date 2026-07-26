//! The logical aggregate: what an aggregate is once names are resolved and
//! OIDs are gone.
//!
//! Loading lives in `catalog::raw::aggregate`.

use super::id::{DbObjectId, DependsOn};

/// Represents a PostgreSQL aggregate function
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Aggregate {
    pub schema: String,
    pub name: String,
    /// Formatted argument types (e.g., "integer, text")
    pub arguments: String,
    /// State type element name for dependency tracking (arrays resolve to base type)
    pub state_type: String,
    pub state_type_schema: String,
    /// Full formatted state type for SQL rendering (preserves array brackets)
    pub state_type_formatted: String,
    /// State transition function (SFUNC)
    pub state_func: String,
    pub state_func_schema: String,
    /// Final function (FINALFUNC), optional
    pub final_func: Option<String>,
    pub final_func_schema: Option<String>,
    /// Combine function for parallel aggregation (COMBINEFUNC), optional
    pub combine_func: Option<String>,
    pub combine_func_schema: Option<String>,
    /// Initial state value (INITCOND), optional
    pub initial_value: Option<String>,
    /// Complete CREATE AGGREGATE statement (reconstructed)
    pub definition: String,
    pub comment: Option<String>,
    pub depends_on: Vec<DbObjectId>,
}

impl Aggregate {
    pub fn id(&self) -> DbObjectId {
        DbObjectId::Aggregate {
            schema: self.schema.clone(),
            name: self.name.clone(),
            arguments: self.arguments.clone(),
        }
    }
}

impl DependsOn for Aggregate {
    fn id(&self) -> DbObjectId {
        DbObjectId::Aggregate {
            schema: self.schema.clone(),
            name: self.name.clone(),
            arguments: self.arguments.clone(),
        }
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}
