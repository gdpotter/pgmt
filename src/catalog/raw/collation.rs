//! Raw `pg_collation` rows and their conversion into logical collations.
//!
//! The fetch keeps the OIDs the converter classifies with, plus the locale
//! columns read through `to_jsonb` because their names move between server
//! versions. Everything else — schema-name resolution, extension-ownership and
//! system-schema exclusion, dependency derivation, comment attachment — happens
//! in the converter, where the OIDs die.

use anyhow::{Context, Result};
use sqlx::postgres::PgConnection;
use sqlx::postgres::types::Oid;
use tracing::info;

use super::exclusion::{Converted, Excluded, ExclusionReason, is_system_schema};
use super::oid_index::OidIndex;
use super::shared::{SharedCatalog, class};
use crate::catalog::collation::{Collation, CollationProvider};
use crate::catalog::id::DbObjectId;

/// One `pg_collation` row, before names are resolved and OIDs are discarded.
///
/// The locale columns arrive unassigned: which of them carries the locale
/// depends on the provider, and that classification is the converter's.
#[derive(Debug, Clone)]
pub struct RawCollation {
    pub oid: Oid,
    pub namespace: Oid,
    pub name: String,

    /// `collprovider`, as the single character the catalog stores.
    pub provider: String,
    pub deterministic: bool,

    /// `collcollate`: LC_COLLATE for a libc collation, and the locale itself for
    /// an ICU collation on the server versions that had no column of its own.
    pub collcollate: Option<String>,
    /// `collctype`: LC_CTYPE, libc only.
    pub collctype: Option<String>,
    /// `colllocale` (PG17+).
    pub colllocale: Option<String>,
    /// `colliculocale` (PG15-16).
    pub colliculocale: Option<String>,
    /// `collicurules` (PG16+).
    pub collicurules: Option<String>,
}

/// Fetch every collation in the database, unresolved and unfiltered.
pub async fn fetch(conn: &mut PgConnection) -> Result<Vec<RawCollation>> {
    info!("Fetching collations...");

    // The ICU locale column moved across versions: `collcollate` (PG13-14),
    // `colliculocale` (PG15-16), `colllocale` (PG17+); `collicurules` exists
    // only on PG16+. The query must run against all of them, but sqlx prepares
    // it against a single server version — so version-dependent columns are
    // read through `to_jsonb(c)` (absent keys yield NULL) instead of direct
    // column references.
    let rows = sqlx::query!(
        r#"
        SELECT
            c.oid AS "oid!",
            c.collnamespace AS "namespace!",
            c.collname AS "name!",
            c.collprovider::text AS "provider!",
            c.collisdeterministic AS "deterministic!",
            to_jsonb(c)->>'collcollate' AS "collcollate?",
            to_jsonb(c)->>'collctype' AS "collctype?",
            to_jsonb(c)->>'colllocale' AS "colllocale?",
            to_jsonb(c)->>'colliculocale' AS "colliculocale?",
            to_jsonb(c)->>'collicurules' AS "collicurules?"
        FROM pg_collation c
        ORDER BY c.oid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawCollation {
            oid: row.oid,
            namespace: row.namespace,
            name: row.name,
            provider: row.provider,
            deterministic: row.deterministic,
            collcollate: row.collcollate,
            collctype: row.collctype,
            colllocale: row.colllocale,
            colliculocale: row.colliculocale,
            collicurules: row.collicurules,
        })
        .collect())
}

/// Fetch collations and convert them into the logical catalog, with comments
/// attached through the OID index.
#[allow(dead_code)]
pub async fn load(conn: &mut PgConnection, shared: &SharedCatalog) -> Result<Vec<Collation>> {
    Ok(load_with_exclusions(conn, shared)
        .await?
        .log_and_take_objects("collation"))
}

/// The same load, keeping the named reason for every raw row that did not
/// become a collation.
pub async fn load_with_exclusions(
    conn: &mut PgConnection,
    shared: &SharedCatalog,
) -> Result<Converted<Collation>> {
    let raw = fetch(conn).await?;
    let mut converted = convert(&raw, shared)?;

    // Identity first, then the index, then the OID-addressed state: a comment
    // can only be attached to an object whose identity is already known.
    let index = OidIndex::from_pairs(
        class::PG_COLLATION,
        converted
            .objects
            .iter()
            .map(|(oid, collation)| (*oid, collation.id())),
    )?;
    let comments = index.object_comments(&shared.descriptions, class::PG_COLLATION);
    for (_, collation) in &mut converted.objects {
        collation.comment = comments.get(&collation.id()).map(|text| text.to_string());
    }

    converted.index = index;

    Ok(converted.map(|(_, collation)| collation))
}

/// Resolve raw collations into logical ones, keeping each collation's OID beside
/// it so OID-addressed state can still be attached before the identities cross
/// the firewall.
///
/// Collations in a system schema and collations owned by an extension are
/// dropped here, each recorded with its named reason. `pg_catalog` holds the
/// built-in `default`, `C` and `POSIX` collations plus one per libc locale the
/// server found at initdb, so the system-schema exclusion is the bulk of it.
pub fn convert(
    raw: &[RawCollation],
    shared: &SharedCatalog,
) -> Result<Converted<(Oid, Collation)>> {
    let namespaces = &shared.namespaces;
    let mut converted: Converted<(Oid, Collation)> = Converted::new();

    for row in raw {
        let schema = namespaces
            .name(row.namespace)
            .with_context(|| format!("collation {} has no namespace entry", row.name))?;

        if is_system_schema(schema) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "collation",
                schema,
                &row.name,
                ExclusionReason::SystemSchema,
            ));
            continue;
        }
        if let Some(extension) = shared.extensions.owner(class::PG_COLLATION, row.oid) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "collation",
                schema,
                &row.name,
                ExclusionReason::ExtensionOwned {
                    extension: extension.to_string(),
                },
            ));
            continue;
        }

        let provider = match row.provider.as_str() {
            "c" => CollationProvider::Libc,
            "i" => CollationProvider::Icu,
            "b" => CollationProvider::Builtin,
            other => anyhow::bail!(
                "unexpected collation provider {:?} for collation {}.{}",
                other,
                schema,
                row.name
            ),
        };

        // Which column holds the locale is the provider's business: libc splits
        // it across LC_COLLATE and LC_CTYPE, while ICU and builtin carry one
        // locale string whose column name moved across server versions.
        let locale = || {
            row.colllocale
                .clone()
                .or_else(|| row.colliculocale.clone())
                .or_else(|| row.collcollate.clone())
        };
        let (locale, lc_collate, lc_ctype, rules) = match provider {
            CollationProvider::Libc => (None, row.collcollate.clone(), row.collctype.clone(), None),
            // Tailoring rules are ICU's alone; a builtin locale has none.
            CollationProvider::Icu => (locale(), None, None, row.collicurules.clone()),
            CollationProvider::Builtin => (locale(), None, None, None),
        };

        converted.objects.push((
            row.oid,
            Collation {
                schema: schema.to_string(),
                name: row.name.clone(),
                provider,
                deterministic: row.deterministic,
                locale,
                lc_collate,
                lc_ctype,
                rules,
                comment: None,
                depends_on: vec![DbObjectId::Schema {
                    name: schema.to_string(),
                }],
            },
        ));
    }

    // The raw fetch orders by OID; ordering by resolved name is what callers see.
    converted
        .objects
        .sort_by(|(_, a), (_, b)| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));

    Ok(converted)
}
