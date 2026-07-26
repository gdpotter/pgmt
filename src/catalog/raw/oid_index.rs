//! The OID → logical-identity index.
//!
//! This is the crossing point of the firewall: OID-addressed catalog state
//! (`pg_description` rows, `pg_depend` edges) is resolved through the index into
//! a [`DbObjectId`], so nothing downstream of a converter has to hold an OID.
//!
//! Ordering matters when building it: an object's identity must be known before
//! anything OID-addressed can be attached to it, so identities are computed
//! first, the index is built, and comments/edges attach in a second pass.

use anyhow::{Result, bail};
use sqlx::postgres::types::Oid;
use std::collections::BTreeMap;

use super::shared::Descriptions;
use crate::catalog::id::DbObjectId;

/// Maps a catalog address — `(catalog table, OID)` — to the logical identity of
/// the object it addresses.
///
/// An OID identifies a row within one catalog table, which is why `pg_depend`
/// and `pg_description` qualify theirs by `classid`/`classoid`; the index keys
/// the same way, so one index can hold the several catalogs an object is
/// addressed through (a table under `pg_class` and its primary key under
/// `pg_constraint`, a composite type under `pg_type` and its backing relation
/// under `pg_class`) without their OID spaces colliding.
///
/// [`OidIndex::insert`] enforces that one address resolves to one identity:
/// registering the same address under two different identities is an error
/// rather than a silent overwrite, which would misattribute every comment and
/// edge that resolves through it.
#[derive(Debug, Clone, Default)]
pub struct OidIndex {
    by_key: BTreeMap<(&'static str, u32), DbObjectId>,
}

impl OidIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an object's identity under its address in `class`.
    ///
    /// Re-registering the same identity is a no-op; a conflicting identity is an
    /// error naming both sides.
    pub fn insert(&mut self, class: &'static str, oid: Oid, id: DbObjectId) -> Result<()> {
        if let Some(existing) = self.by_key.get(&(class, oid.0)) {
            if *existing == id {
                return Ok(());
            }
            bail!(
                "{} OID {} already indexed as {}, cannot also index it as {}",
                class,
                oid.0,
                existing,
                id
            );
        }
        self.by_key.insert((class, oid.0), id);
        Ok(())
    }

    /// Build an index whose entries all come from one catalog table.
    pub fn from_pairs(
        class: &'static str,
        pairs: impl IntoIterator<Item = (Oid, DbObjectId)>,
    ) -> Result<Self> {
        let mut index = Self::new();
        for (oid, id) in pairs {
            index.insert(class, oid, id)?;
        }
        Ok(index)
    }

    #[allow(dead_code)]
    pub fn get(&self, class: &'static str, oid: Oid) -> Option<&DbObjectId> {
        self.by_key.get(&(class, oid.0))
    }

    #[allow(dead_code)]
    pub fn contains(&self, class: &'static str, oid: Oid) -> bool {
        self.by_key.contains_key(&(class, oid.0))
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.by_key.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&'static str, Oid, &DbObjectId)> {
        self.by_key
            .iter()
            .map(|((class, oid), id)| (*class, Oid(*oid), id))
    }

    /// The entries addressed through one catalog table, in OID order.
    fn entries_of(
        &self,
        class: &'static str,
    ) -> impl Iterator<Item = (Oid, &DbObjectId)> + use<'_> {
        self.by_key
            .range((class, u32::MIN)..=(class, u32::MAX))
            .map(|((_, oid), id)| (Oid(*oid), id))
    }

    /// The comments on the indexed objects of one catalog class, keyed by
    /// identity rather than by OID.
    ///
    /// This is the crossing itself: `pg_description` addresses objects by
    /// `(classoid, objoid)`, and the index is what turns that address into
    /// something a logical struct can be found by.
    pub fn object_comments<'a>(
        &'a self,
        descriptions: &'a Descriptions,
        class: &'static str,
    ) -> BTreeMap<&'a DbObjectId, &'a str> {
        self.entries_of(class)
            .filter_map(|(oid, id)| descriptions.object(class, oid).map(|comment| (id, comment)))
            .collect()
    }

    /// The sub-object comments on the indexed objects of one catalog class,
    /// keyed by the owning object's identity and then by `objsubid`.
    ///
    /// `pg_description` addresses a sub-object as a `objsubid` under its parent
    /// — a column's attnum under its table's OID — so the sub-object identity
    /// (the column's name) is only known to the converter that holds the
    /// attnum-to-name correspondence. The index carries the lookup as far as the
    /// parent.
    pub fn subobject_comments<'a>(
        &'a self,
        descriptions: &'a Descriptions,
        class: &'static str,
    ) -> BTreeMap<&'a DbObjectId, BTreeMap<i32, &'a str>> {
        self.entries_of(class)
            .filter_map(|(oid, id)| {
                let subs: BTreeMap<i32, &str> = descriptions.subobjects(class, oid).collect();
                if subs.is_empty() {
                    None
                } else {
                    Some((id, subs))
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::raw::shared::class;

    fn table(name: &str) -> DbObjectId {
        DbObjectId::Table {
            schema: "public".to_string(),
            name: name.to_string(),
        }
    }

    fn constraint(name: &str) -> DbObjectId {
        DbObjectId::Constraint {
            schema: "public".to_string(),
            table: "users".to_string(),
            name: name.to_string(),
        }
    }

    #[test]
    fn test_lookup_returns_indexed_identity() {
        let index = OidIndex::from_pairs(
            class::PG_CLASS,
            [(Oid(16400), table("users")), (Oid(16401), table("orders"))],
        )
        .unwrap();

        assert_eq!(
            index.get(class::PG_CLASS, Oid(16400)),
            Some(&table("users"))
        );
        assert_eq!(
            index.get(class::PG_CLASS, Oid(16401)),
            Some(&table("orders"))
        );
        assert_eq!(index.get(class::PG_CLASS, Oid(99999)), None);
        assert_eq!(index.len(), 2);
    }

    #[test]
    fn test_reinserting_same_identity_is_idempotent() {
        let mut index = OidIndex::new();
        index
            .insert(class::PG_CLASS, Oid(16400), table("users"))
            .unwrap();
        index
            .insert(class::PG_CLASS, Oid(16400), table("users"))
            .unwrap();
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn test_conflicting_identity_for_one_address_is_an_error() {
        let mut index = OidIndex::new();
        index
            .insert(class::PG_CLASS, Oid(16400), table("users"))
            .unwrap();

        let err = index
            .insert(class::PG_CLASS, Oid(16400), table("orders"))
            .unwrap_err();
        let message = err.to_string();
        assert!(message.contains("16400"), "message was: {message}");
        assert!(message.contains("public.users"), "message was: {message}");
        assert!(message.contains("public.orders"), "message was: {message}");
    }

    /// The same OID number in two catalogs addresses two different objects —
    /// `pg_class` and `pg_constraint` allocate from one cluster-wide counter but
    /// each holds its own rows, and after counter wraparound the numbers can
    /// coincide. Keying by catalog keeps both entries, and lookups see only the
    /// one from the catalog they ask about.
    #[test]
    fn test_same_oid_in_two_catalogs_addresses_two_objects() {
        let mut index = OidIndex::new();
        index
            .insert(class::PG_CLASS, Oid(16400), table("users"))
            .unwrap();
        index
            .insert(class::PG_CONSTRAINT, Oid(16400), constraint("users_pkey"))
            .unwrap();

        assert_eq!(index.len(), 2);
        assert_eq!(
            index.get(class::PG_CLASS, Oid(16400)),
            Some(&table("users"))
        );
        assert_eq!(
            index.get(class::PG_CONSTRAINT, Oid(16400)),
            Some(&constraint("users_pkey"))
        );
        assert!(!index.contains(class::PG_TYPE, Oid(16400)));
    }

    #[test]
    fn test_iteration_is_ordered_by_oid_within_a_class() {
        let index = OidIndex::from_pairs(
            class::PG_CLASS,
            [
                (Oid(16402), table("c")),
                (Oid(16400), table("a")),
                (Oid(16401), table("b")),
            ],
        )
        .unwrap();

        let oids: Vec<u32> = index.iter().map(|(_, oid, _)| oid.0).collect();
        assert_eq!(oids, vec![16400, 16401, 16402]);
    }
}
