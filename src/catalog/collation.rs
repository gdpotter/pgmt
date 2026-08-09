//! The logical collation, once names are resolved and OIDs are gone.
//!
//! Loading lives in `catalog::raw::collation`.

use crate::catalog::{DependsOn, id::DbObjectId};

/// The provider backing a collation (`pg_collation.collprovider`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollationProvider {
    /// 'c' — operating system libc locales
    Libc,
    /// 'i' — ICU locales
    Icu,
    /// 'b' — PostgreSQL builtin locales (PG17+)
    Builtin,
}

/// A schema-qualified reference to a collation, as used by objects that carry a
/// COLLATE clause.
///
/// Same-named collations can exist in different schemas, so the bare `collname`
/// is not a usable identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollationRef {
    pub schema: String,
    pub name: String,
}

impl CollationRef {
    /// The identity of the collation this reference names, for a dependency edge.
    pub fn id(&self) -> DbObjectId {
        DbObjectId::Collation {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }
}

/// Represents a PostgreSQL collation.
///
/// `collversion` is deliberately excluded from this model: it records the
/// provider library version the collation was created under, which varies by
/// machine and ICU build, so including it in equality would produce spurious
/// diffs between dev, shadow, and target databases. `collencoding` is likewise
/// excluded — user-created collations are always encoding-agnostic (-1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Collation {
    pub schema: String,
    pub name: String,
    pub provider: CollationProvider,
    /// `collisdeterministic`; only ICU collations can be non-deterministic.
    pub deterministic: bool,
    /// ICU/builtin locale (None for libc collations).
    pub locale: Option<String>,
    /// libc LC_COLLATE (None for ICU/builtin collations).
    pub lc_collate: Option<String>,
    /// libc LC_CTYPE (None for ICU/builtin collations).
    pub lc_ctype: Option<String>,
    /// ICU tailoring rules (`collicurules`, PG16+; None on older servers).
    pub rules: Option<String>,
    pub comment: Option<String>,
    pub depends_on: Vec<DbObjectId>,
}

impl Collation {
    pub fn id(&self) -> DbObjectId {
        DbObjectId::Collation {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }
}

impl DependsOn for Collation {
    fn id(&self) -> DbObjectId {
        self.id()
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}
