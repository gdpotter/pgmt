//! The logical view: what a view is once names are resolved and OIDs are gone.
//!
//! Views are read through `catalog::raw::view`, which fetches the OID-keyed
//! rows and converts them into these structs.
use super::id::{DbObjectId, DependsOn};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ViewColumn {
    pub name: String,
    pub type_: Option<String>, // PostgreSQL doesn't always expose this directly
    pub comment: Option<String>,
}

#[derive(Debug, Clone)]
pub struct View {
    pub schema: String,
    pub name: String,
    pub definition: String, // raw `SELECT …`
    pub columns: Vec<ViewColumn>,
    pub comment: Option<String>,     // comment on the view
    pub security_invoker: bool,      // PG 15+: execute with invoker's permissions (default: false)
    pub security_barrier: bool,      // prevent predicate pushdown for security (default: false)
    pub depends_on: Vec<DbObjectId>, // populated from pg_depend
}

impl View {
    pub fn id(&self) -> DbObjectId {
        DbObjectId::View {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }
}

impl DependsOn for View {
    fn id(&self) -> DbObjectId {
        DbObjectId::View {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}
