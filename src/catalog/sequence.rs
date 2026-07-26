//! The logical sequence: what a sequence is once names are resolved and OIDs
//! are gone.
//!
//! Loading lives in `catalog::raw::sequence`.

use crate::catalog::id::{DbObjectId, DependsOn};

#[derive(Debug, Clone)]
pub struct Sequence {
    pub schema: String,
    pub name: String,
    pub data_type: String, // INTEGER, BIGINT, SMALLINT
    pub start_value: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub increment: i64,
    pub cycle: bool,
    pub owned_by: Option<String>, // For SERIAL columns: "schema.table.column"
    pub comment: Option<String>,  // comment on the sequence
    pub depends_on: Vec<DbObjectId>,
}

impl Sequence {
    pub fn id(&self) -> DbObjectId {
        DbObjectId::Sequence {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }
}

impl DependsOn for Sequence {
    fn id(&self) -> DbObjectId {
        self.id()
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}
