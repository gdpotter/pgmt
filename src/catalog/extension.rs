//! The logical extension: what an installed extension is once names are
//! resolved and OIDs are gone.
//!
//! Loading lives in `catalog::raw::extension`.

use crate::catalog::{DependsOn, id::DbObjectId};

/// Represents a PostgreSQL extension
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Extension {
    pub name: String,
    pub schema: String,
    pub version: String,
    pub relocatable: bool,
    pub comment: Option<String>,
    pub depends_on: Vec<DbObjectId>,
}

impl DependsOn for Extension {
    fn id(&self) -> DbObjectId {
        DbObjectId::Extension {
            name: self.name.clone(),
        }
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}
