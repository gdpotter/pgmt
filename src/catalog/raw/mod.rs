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
//! `pg_description` rows — plus the [`index::OidIndex`] that turns an OID back
//! into a logical [`crate::catalog::id::DbObjectId`].
//!
//! A converter also accounts for what it drops: every raw row that does not
//! become a logical object carries an [`exclusion::ExclusionReason`], so the
//! rows a converter deliberately excludes can be told apart from rows it lost.

// The layer is consumed by converters and by tests, not by the CLI binary's
// module tree, so unused-item warnings here would be noise.
#![allow(dead_code)]

pub mod aggregate;
pub mod cast;
pub mod custom_type;
pub mod domain;
pub mod exclusion;
pub mod function;
pub mod index;
pub mod operator;
pub mod shared;
pub mod table;
pub mod view;
