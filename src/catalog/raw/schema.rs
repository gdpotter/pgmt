//! Conversion of raw namespace rows into logical schemas.
//!
//! Schemas need no fetch of their own: the shared namespace map is already the
//! unresolved `pg_namespace` enumeration every other converter resolves names
//! against, and a schema's identity is nothing but its name. What remains is the
//! exclusion — the namespaces PostgreSQL owns — and the comment attached through
//! the OID index.
//!
//! Extension membership is deliberately not an exclusion here. A schema an
//! extension was installed into (`CREATE EXTENSION ... SCHEMA app`) is the
//! user's: the extension depends on it, not the other way round.

use anyhow::Result;
use sqlx::postgres::types::Oid;

use super::exclusion::{Converted, Excluded, ExclusionReason, is_system_schema};
use super::oid_index::OidIndex;
use super::shared::{SharedCatalog, class};
use crate::catalog::id::DbObjectId;
use crate::catalog::schema::Schema;

/// Convert the shared namespace map into the logical catalog, with each schema's
/// comment attached through the OID index.
#[allow(dead_code)]
pub fn load(shared: &SharedCatalog) -> Result<Vec<Schema>> {
    Ok(load_with_exclusions(shared)?.log_and_take_objects("schema"))
}

/// The same load, keeping the named reason for every namespace that did not
/// become a schema.
pub fn load_with_exclusions(shared: &SharedCatalog) -> Result<Converted<Schema>> {
    let mut converted = convert(shared)?;

    // Identity first, then the index, then the OID-addressed state: a comment
    // can only be attached to an object whose identity is already known.
    let oids = OidIndex::from_pairs(
        class::PG_NAMESPACE,
        converted.objects.iter().map(|(oid, schema)| {
            (
                *oid,
                DbObjectId::Schema {
                    name: schema.name.clone(),
                },
            )
        }),
    )?;
    let comments = oids.object_comments(&shared.descriptions, class::PG_NAMESPACE);
    for (_, schema) in &mut converted.objects {
        let id = DbObjectId::Schema {
            name: schema.name.clone(),
        };
        schema.comment = comments.get(&id).map(|text| text.to_string());
    }

    converted.index = oids;

    Ok(converted.map(|(_, schema)| schema))
}

/// Resolve namespaces into logical schemas, keeping each schema's OID beside it
/// so OID-addressed state can still be attached before the identities cross the
/// firewall.
///
/// The catalog's own namespaces and the per-session temporary ones are dropped
/// here with their named reason.
pub fn convert(shared: &SharedCatalog) -> Result<Converted<(Oid, Schema)>> {
    let mut converted: Converted<(Oid, Schema)> = Converted::new();

    for (oid, name) in shared.namespaces.iter() {
        if is_system_schema(name) {
            converted.excluded.push(Excluded::new(
                oid,
                "schema",
                name,
                name,
                ExclusionReason::SystemSchema,
            ));
            continue;
        }

        converted.objects.push((
            oid,
            Schema {
                name: name.to_string(),
                comment: None,
            },
        ));
    }

    // The namespace map is ordered by OID; ordering by name is what callers see.
    converted
        .objects
        .sort_by(|(_, a), (_, b)| a.name.cmp(&b.name));

    Ok(converted)
}
