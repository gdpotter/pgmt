//! Named reasons a raw row does not become a logical object.
//!
//! Dropping a row is a decision, and a converter that drops it silently cannot
//! be audited: "excluded on purpose" and "lost by accident" look identical from
//! the outside. Every drop therefore carries a reason from [`ExclusionReason`],
//! and a converter reports the excluded rows alongside the converted ones, so
//! that raw rows = converted + excluded with nothing unaccounted for.
//!
//! These are *physical-layer* exclusions: rows that are not user schema at all.
//! They are not the managed-universe scoping that
//! `ObjectFilter::from_config` applies — that is config-driven, name-based, and
//! belongs after the conversion boundary. Conversion still yields the physical
//! logical catalog.

use sqlx::postgres::types::Oid;

/// Why a raw row did not become a logical object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExclusionReason {
    /// The object lives in a schema PostgreSQL owns (`pg_catalog`,
    /// `information_schema`, `pg_toast`), so it is never pgmt's to manage.
    SystemSchema,
    /// The object was installed by an extension, recorded as a `pg_depend` row
    /// with `deptype = 'e'`. Its lifecycle belongs to `CREATE EXTENSION`, not to
    /// a schema file.
    ExtensionOwned { extension: String },
}

impl ExclusionReason {
    /// A stable identifier for the reason, independent of its payload.
    pub fn name(&self) -> &'static str {
        match self {
            ExclusionReason::SystemSchema => "SystemSchema",
            ExclusionReason::ExtensionOwned { .. } => "ExtensionOwned",
        }
    }
}

/// One raw row that a converter dropped, named the way the row itself names the
/// object.
///
/// The OID is kept because this is raw-side state: it is what lets an excluded
/// row be reconciled against a catalog enumeration without re-deriving an
/// identity for an object that deliberately has none.
#[derive(Debug, Clone)]
pub struct Excluded {
    pub oid: Oid,
    /// The kind of raw row, in the singular ("table", "operator").
    pub kind: &'static str,
    pub schema: String,
    pub name: String,
    pub reason: ExclusionReason,
}

impl Excluded {
    pub fn new(
        oid: Oid,
        kind: &'static str,
        schema: &str,
        name: &str,
        reason: ExclusionReason,
    ) -> Self {
        Self {
            oid,
            kind,
            schema: schema.to_string(),
            name: name.to_string(),
            reason,
        }
    }

    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }
}

/// What a converter produces: the objects that crossed into the logical world,
/// and the rows that did not, each with its reason.
///
/// Sub-rows of an object (a table's columns and primary key) are not listed
/// separately — they follow the row they belong to, and are dropped exactly when
/// it is.
#[derive(Debug, Clone)]
pub struct Converted<T> {
    pub objects: Vec<T>,
    pub excluded: Vec<Excluded>,
}

impl<T> Converted<T> {
    pub fn new() -> Self {
        Self {
            objects: Vec::new(),
            excluded: Vec::new(),
        }
    }

    /// Replace the converted objects, keeping the exclusions — for a `load`
    /// that maps its converter's output (dropping the OIDs it carried) without
    /// losing the accounting.
    pub fn map<U>(self, f: impl FnMut(T) -> U) -> Converted<U> {
        Converted {
            objects: self.objects.into_iter().map(f).collect(),
            excluded: self.excluded,
        }
    }

    /// The excluded rows carrying one reason, by the reason's name.
    pub fn excluded_for(&self, reason: &str) -> impl Iterator<Item = &Excluded> {
        self.excluded
            .iter()
            .filter(move |row| row.reason.name() == reason)
    }
}

impl<T> Default for Converted<T> {
    fn default() -> Self {
        Self::new()
    }
}
