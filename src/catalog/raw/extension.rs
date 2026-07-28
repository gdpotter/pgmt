//! Raw extension rows and their conversion into logical extensions.
//!
//! The fetch keeps the OIDs the converter resolves with; an extension has no
//! server-side-function output to carry, since `pg_extension` already holds its
//! name, version and relocatability. Schema-name resolution, the exclusion of the
//! extension every database ships with, the schema dependency and comment
//! attachment happen in the converter, where the OIDs die.
//!
//! An extension is never itself extension-owned: it is what owns things. The
//! only reason one does not become a logical extension is that PostgreSQL put it
//! there.

use anyhow::{Context, Result};
use sqlx::postgres::PgConnection;
use sqlx::postgres::types::Oid;
use tracing::info;

use super::exclusion::{BUILT_IN_EXTENSIONS, Converted, Excluded, ExclusionReason};
use super::oid_index::OidIndex;
use super::shared::{SharedCatalog, class};
use crate::catalog::DependsOn;
use crate::catalog::extension::Extension;
use crate::catalog::id::DbObjectId;

/// One `pg_extension` row, before names are resolved and OIDs are discarded.
#[derive(Debug, Clone)]
pub struct RawExtension {
    pub oid: Oid,
    pub name: String,
    /// The schema the extension's objects were installed into.
    pub namespace: Oid,
    pub version: String,
    pub relocatable: bool,
}

/// Fetch every installed extension, unresolved and unfiltered.
pub async fn fetch(conn: &mut PgConnection) -> Result<Vec<RawExtension>> {
    info!("Fetching extensions...");
    let rows = sqlx::query!(
        r#"
        SELECT
            e.oid AS "oid!",
            e.extname AS "name!",
            e.extnamespace AS "namespace!",
            e.extversion AS "version!",
            e.extrelocatable AS "relocatable!"
        FROM pg_extension e
        ORDER BY e.oid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawExtension {
            oid: row.oid,
            name: row.name,
            namespace: row.namespace,
            version: row.version,
            relocatable: row.relocatable,
        })
        .collect())
}

/// Fetch extensions and convert them into the logical catalog, with each
/// extension's comment attached through the OID index.
#[allow(dead_code)]
pub async fn load(conn: &mut PgConnection, shared: &SharedCatalog) -> Result<Vec<Extension>> {
    Ok(load_with_exclusions(conn, shared)
        .await?
        .log_and_take_objects("extension"))
}

/// The same load, keeping the named reason for every raw row that did not become
/// an extension.
pub async fn load_with_exclusions(
    conn: &mut PgConnection,
    shared: &SharedCatalog,
) -> Result<Converted<Extension>> {
    let raw = fetch(conn).await?;
    let mut converted = convert(&raw, shared)?;

    // Identity first, then the index, then the OID-addressed state: a comment
    // can only be attached to an object whose identity is already known.
    let index = OidIndex::from_pairs(
        class::PG_EXTENSION,
        converted
            .objects
            .iter()
            .map(|(oid, extension)| (*oid, extension.id())),
    )?;
    let comments = index.object_comments(&shared.descriptions, class::PG_EXTENSION);
    for (_, extension) in &mut converted.objects {
        extension.comment = comments.get(&extension.id()).map(|text| text.to_string());
    }

    converted.index = index;

    Ok(converted.map(|(_, extension)| extension))
}

/// Resolve raw extensions into logical ones, keeping each extension's OID beside
/// it so OID-addressed state can still be attached before the identities cross
/// the firewall.
pub fn convert(
    raw: &[RawExtension],
    shared: &SharedCatalog,
) -> Result<Converted<(Oid, Extension)>> {
    let mut converted: Converted<(Oid, Extension)> = Converted::new();

    for row in raw {
        let schema = shared
            .namespaces
            .name(row.namespace)
            .with_context(|| format!("extension {} has no namespace entry", row.name))?;

        if BUILT_IN_EXTENSIONS.contains(&row.name.as_str()) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "extension",
                schema,
                &row.name,
                ExclusionReason::BuiltInExtension,
            ));
            continue;
        }

        // An extension installed into a schema of its own must be created after
        // that schema. Nothing is recorded for `public`: it is present in every
        // database from initdb onward, so the edge could never order anything.
        // Recording it, as most converters do, is equally correct.
        let depends_on = if schema == "public" {
            Vec::new()
        } else {
            vec![DbObjectId::Schema {
                name: schema.to_string(),
            }]
        };

        converted.objects.push((
            row.oid,
            Extension {
                name: row.name.clone(),
                schema: schema.to_string(),
                version: row.version.clone(),
                relocatable: row.relocatable,
                comment: None,
                depends_on,
            },
        ));
    }

    // The raw fetch orders by OID; ordering by name is what callers see.
    converted
        .objects
        .sort_by(|(_, a), (_, b)| a.name.cmp(&b.name));

    Ok(converted)
}
