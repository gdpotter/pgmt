//! The logical domain: what a domain is once names are resolved and OIDs are
//! gone.
//!
//! Domains are read through `catalog::raw::domain`, which fetches the OID-keyed
//! rows and converts them into these structs.

use super::id::{DbObjectId, DependsOn};

/// A CHECK constraint on a domain
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainCheckConstraint {
    pub name: String,
    pub expression: String,
}

/// Represents a PostgreSQL domain
#[derive(Debug, Clone)]
pub struct Domain {
    pub schema: String,
    pub name: String,
    pub base_type: String,
    pub not_null: bool,
    pub default: Option<String>,
    pub collation: Option<String>,
    pub check_constraints: Vec<DomainCheckConstraint>,
    pub comment: Option<String>,
    pub depends_on: Vec<DbObjectId>,
}

impl Domain {
    pub fn id(&self) -> DbObjectId {
        DbObjectId::Domain {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }
}

impl DependsOn for Domain {
    fn id(&self) -> DbObjectId {
        DbObjectId::Domain {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}
