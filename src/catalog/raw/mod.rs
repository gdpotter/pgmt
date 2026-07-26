//! OID-keyed catalog layer, upstream of the logical catalog.
//!
//! The logical structs (`Table`, `Operator`, …) are compared by value across two
//! different databases, so their identity must be name-based: a physical
//! coordinate such as an OID differs between two databases holding the same
//! schema and would turn every comparison into a spurious drop/recreate.
//! OIDs therefore live in this module (and in the converters that read it) and
//! never appear on a logical struct.
//!
//! What lives here is the cross-cutting state that is addressed by OID in
//! `pg_catalog` — the namespace map, extension-ownership edges, and
//! `pg_description` rows — plus the [`oid_index::OidIndex`] that turns an OID
//! back into a logical [`crate::catalog::id::DbObjectId`].
//!
//! A converter also accounts for what it drops: every raw row that does not
//! become a logical object carries an [`exclusion::ExclusionReason`], so the
//! rows a converter deliberately excludes can be told apart from rows it lost.

// The layer is consumed by converters and by tests, not by the CLI binary's
// module tree, so unused-item warnings here would be noise.
#![allow(dead_code)]

use std::collections::HashSet;

use crate::catalog::id::DbObjectId;

/// Drop repeated dependencies, keeping the first occurrence of each.
///
/// A converter derives one edge per catalog row it reads, and `pg_catalog`
/// records the same referent once per column, attribute or argument that uses
/// it — a self-referencing foreign key, a composite with two attributes of one
/// type, an aggregate whose SFUNC is also its FINALFUNC. The surviving order is
/// the derivation order, which is what callers see.
pub fn dedup_preserving_order(dependencies: &mut Vec<DbObjectId>) {
    let mut seen = HashSet::new();
    dependencies.retain(|dependency| seen.insert(dependency.clone()));
}

pub mod aggregate;
pub mod cast;
pub mod constraint;
pub mod custom_type;
pub mod domain;
pub mod exclusion;
pub mod extension;
pub mod function;
pub mod index;
pub mod oid_index;
pub mod operator;
pub mod policy;
pub mod schema;
pub mod sequence;
pub mod shared;
pub mod snapshot;
pub mod table;
pub mod trigger;
pub mod view;
