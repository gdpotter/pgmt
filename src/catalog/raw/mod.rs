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
//!
//! Items here marked `#[allow(dead_code)]` are reached only from the library's
//! tests: this source is compiled twice, once as the library and once as the
//! binary's own crate, and in the binary build a test-only caller does not
//! exist. Anything with no caller at all belongs deleted, not annotated.

use anyhow::{Context, Result};
use std::collections::HashSet;

use crate::catalog::id::DbObjectId;
use oid_index::OidIndex;

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

/// Fold the per-kind OID indexes of one catalog load into a single index.
///
/// Each kind indexes the addresses its own OID-addressed state is resolved
/// through, and a kind may register an object under more than one catalog table
/// (a table and its primary-key constraint, a composite type and its backing
/// relation). Merging them is what makes the addresses of a whole load
/// resolvable in one lookup, rather than only within the kind that produced
/// them.
///
/// Two kinds registering different identities under one `(catalog table, OID)`
/// is a contradiction — one of them has misderived an identity — and
/// [`OidIndex::insert`] rejects it; the kind is named so the conflict points at
/// the converter that has to be fixed.
pub fn merge_indexes(indexes: Vec<(&'static str, OidIndex)>) -> Result<OidIndex> {
    let mut merged = OidIndex::new();
    for (kind, index) in indexes {
        for (class, oid, id) in index.iter() {
            merged
                .insert(class, oid, id.clone())
                .with_context(|| format!("merging the OID index of every {kind}"))?;
        }
    }
    Ok(merged)
}

pub mod aggregate;
pub mod cast;
pub mod constraint;
pub mod custom_type;
pub mod domain;
pub mod exclusion;
pub mod extension;
pub mod function;
pub mod grant;
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

#[cfg(test)]
mod tests {
    use super::*;
    use shared::class;
    use sqlx::postgres::types::Oid;

    fn table(name: &str) -> DbObjectId {
        DbObjectId::Table {
            schema: "public".to_string(),
            name: name.to_string(),
        }
    }

    #[test]
    fn test_merge_keeps_every_kind_addressable() {
        let tables = OidIndex::from_pairs(class::PG_CLASS, [(Oid(16400), table("users"))]).unwrap();
        let types = OidIndex::from_pairs(
            class::PG_TYPE,
            [(
                Oid(16500),
                DbObjectId::Type {
                    schema: "public".to_string(),
                    name: "status".to_string(),
                },
            )],
        )
        .unwrap();

        let merged = merge_indexes(vec![("table", tables), ("type", types)]).unwrap();

        assert_eq!(merged.len(), 2);
        assert_eq!(
            merged.get(class::PG_CLASS, Oid(16400)),
            Some(&table("users"))
        );
        assert!(merged.contains(class::PG_TYPE, Oid(16500)));
    }

    /// One catalog address resolving to two identities means a converter has
    /// misderived one of them, and every comment or edge routed through that
    /// address would be attached to the wrong object. The merge refuses, and the
    /// error names the kind whose index brought the conflict in.
    #[test]
    fn test_merge_reports_the_kind_a_conflicting_address_came_from() {
        let tables = OidIndex::from_pairs(class::PG_CLASS, [(Oid(16400), table("users"))]).unwrap();
        let views = OidIndex::from_pairs(
            class::PG_CLASS,
            [(
                Oid(16400),
                DbObjectId::View {
                    schema: "public".to_string(),
                    name: "user_emails".to_string(),
                },
            )],
        )
        .unwrap();

        let error = merge_indexes(vec![("table", tables), ("view", views)]).unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("view"), "message was: {message}");
        assert!(message.contains("16400"), "message was: {message}");
    }

    /// A kind may register one object under more than one catalog table (a
    /// composite type under `pg_type` and its backing relation under
    /// `pg_class`), so the same identity legitimately appears twice.
    #[test]
    fn test_merge_accepts_one_identity_under_several_addresses() {
        let composite = DbObjectId::Type {
            schema: "public".to_string(),
            name: "point2d".to_string(),
        };
        let mut types = OidIndex::new();
        types
            .insert(class::PG_TYPE, Oid(16500), composite.clone())
            .unwrap();
        types
            .insert(class::PG_CLASS, Oid(16501), composite.clone())
            .unwrap();

        let merged = merge_indexes(vec![("type", types)]).unwrap();

        assert_eq!(merged.len(), 2);
        assert_eq!(merged.get(class::PG_CLASS, Oid(16501)), Some(&composite));
    }
}
