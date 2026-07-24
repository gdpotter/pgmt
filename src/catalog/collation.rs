//! src/catalog/collation
//! Fetch PostgreSQL collations via pg_catalog

use anyhow::{Result, bail};
use sqlx::postgres::PgConnection;
use tracing::info;

use super::id::{DbObjectId, DependsOn};
use super::utils::DependencyBuilder;

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
/// COLLATE clause (domains today; table/view columns eventually).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollationRef {
    pub schema: String,
    pub name: String,
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

/// Fetch all user-defined collations from the database.
pub async fn fetch(conn: &mut PgConnection) -> Result<Vec<Collation>> {
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
            n.nspname AS "schema!",
            c.collname AS "name!",
            c.collprovider::text AS "provider!",
            c.collisdeterministic AS "deterministic!",
            CASE WHEN c.collprovider = 'c' THEN to_jsonb(c)->>'collcollate' END AS "lc_collate?",
            CASE WHEN c.collprovider = 'c' THEN to_jsonb(c)->>'collctype' END AS "lc_ctype?",
            CASE WHEN c.collprovider IN ('i', 'b') THEN
                COALESCE(
                    to_jsonb(c)->>'colllocale',
                    to_jsonb(c)->>'colliculocale',
                    to_jsonb(c)->>'collcollate'
                )
            END AS "locale?",
            CASE WHEN c.collprovider = 'i' THEN to_jsonb(c)->>'collicurules' END AS "rules?",
            d.description AS "comment?"
        FROM pg_collation c
        JOIN pg_namespace n ON c.collnamespace = n.oid
        LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          -- Exclude collations that belong to extensions (collations are
          -- first-class objects with their own pg_depend 'e' entry)
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend dep
              WHERE dep.objid = c.oid
              AND dep.deptype = 'e'
          )
        ORDER BY n.nspname, c.collname
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    let mut collations = Vec::new();
    for row in rows {
        let provider = match row.provider.as_str() {
            "c" => CollationProvider::Libc,
            "i" => CollationProvider::Icu,
            "b" => CollationProvider::Builtin,
            other => bail!(
                "unexpected collation provider {:?} for collation {}.{}",
                other,
                row.schema,
                row.name
            ),
        };

        let depends_on = DependencyBuilder::new(row.schema.clone()).build();

        collations.push(Collation {
            schema: row.schema,
            name: row.name,
            provider,
            deterministic: row.deterministic,
            locale: row.locale,
            lc_collate: row.lc_collate,
            lc_ctype: row.lc_ctype,
            rules: row.rules,
            comment: row.comment,
            depends_on,
        });
    }

    Ok(collations)
}
