//! Standalone fetches for the catalog state that every object kind needs.
//!
//! Each of these is one query whose result is consulted by lookup instead of
//! being re-joined into every per-kind query: the namespace map (OID → schema
//! name), the `deptype = 'e'` extension-ownership edges, and the
//! `pg_description` rows.
//!
//! These queries must run on the same connection as the rest of a catalog load:
//! `pg_get_function_identity_arguments()` renders type names relative to
//! `search_path`, so a shared fetch on a different connection could resolve
//! names the per-kind fetches would not.

use anyhow::Result;
use sqlx::postgres::PgConnection;
use sqlx::postgres::types::Oid;
use std::collections::BTreeMap;
use tracing::info;

/// Names of the `pg_catalog` tables an OID can be addressed through.
///
/// An OID identifies a row within one catalog table, so both `pg_depend`
/// (`classid`) and `pg_description` (`classoid`) qualify their OIDs by catalog
/// table. Lookups here take the same qualification.
pub mod class {
    pub const PG_CLASS: &str = "pg_class";
    pub const PG_PROC: &str = "pg_proc";
    pub const PG_TYPE: &str = "pg_type";
    pub const PG_NAMESPACE: &str = "pg_namespace";
    pub const PG_OPERATOR: &str = "pg_operator";
    pub const PG_CAST: &str = "pg_cast";
    pub const PG_CONSTRAINT: &str = "pg_constraint";
    pub const PG_TRIGGER: &str = "pg_trigger";
    pub const PG_POLICY: &str = "pg_policy";
    pub const PG_EXTENSION: &str = "pg_extension";
}

/// Every namespace in the database, by OID.
///
/// Unfiltered on purpose: this is the resolution table a converter uses to turn
/// `relnamespace`/`typnamespace`/… into a schema name. Deciding that a schema
/// is out of scope is a separate, name-based step.
#[derive(Debug, Clone, Default)]
pub struct NamespaceMap {
    by_oid: BTreeMap<u32, String>,
}

impl NamespaceMap {
    pub fn name(&self, oid: Oid) -> Option<&str> {
        self.by_oid.get(&oid.0).map(String::as_str)
    }

    pub fn len(&self) -> usize {
        self.by_oid.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_oid.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (Oid, &str)> {
        self.by_oid
            .iter()
            .map(|(oid, name)| (Oid(*oid), name.as_str()))
    }
}

/// Which objects belong to an extension, from the `pg_depend` rows with
/// `deptype = 'e'`.
#[derive(Debug, Clone, Default)]
pub struct ExtensionOwnership {
    /// (catalog table, object OID) → owning extension name.
    owners: BTreeMap<(String, u32), String>,
}

impl ExtensionOwnership {
    /// The extension owning this object, for object classes that carry their own
    /// `deptype = 'e'` row: tables, views, functions, types, sequences,
    /// operators, casts — every first-class object.
    pub fn owner(&self, class: &str, oid: Oid) -> Option<&str> {
        self.owners
            .get(&(class.to_string(), oid.0))
            .map(String::as_str)
    }

    pub fn is_owned(&self, class: &str, oid: Oid) -> bool {
        self.owner(class, oid).is_some()
    }

    /// The extension owning a sub-object of a relation — a constraint, index,
    /// trigger, or policy — given the OID of its parent table.
    ///
    /// Such sub-objects never get a `deptype = 'e'` row of their own, even when
    /// an extension script created them: membership is recorded only on the
    /// parent table. Asking about the sub-object's own OID always answers "not
    /// owned" and leaks the object.
    pub fn owner_of_relation_subobject(&self, parent_relation: Oid) -> Option<&str> {
        self.owner(class::PG_CLASS, parent_relation)
    }

    pub fn is_relation_subobject_owned(&self, parent_relation: Oid) -> bool {
        self.owner_of_relation_subobject(parent_relation).is_some()
    }

    pub fn len(&self) -> usize {
        self.owners.len()
    }

    pub fn is_empty(&self) -> bool {
        self.owners.is_empty()
    }
}

/// Comments, keyed the way `pg_description` keys them:
/// `(classoid, objoid, objsubid)`, where a non-zero `objsubid` addresses a
/// sub-object (a column, by attnum).
#[derive(Debug, Clone, Default)]
pub struct Descriptions {
    by_key: BTreeMap<(String, u32, i32), String>,
}

impl Descriptions {
    /// The comment on an object itself (`objsubid = 0`).
    pub fn object(&self, class: &str, oid: Oid) -> Option<&str> {
        self.get(class, oid, 0)
    }

    pub fn get(&self, class: &str, oid: Oid, objsubid: i32) -> Option<&str> {
        self.by_key
            .get(&(class.to_string(), oid.0, objsubid))
            .map(String::as_str)
    }

    /// Every sub-object comment on this object, as `(objsubid, comment)` in
    /// ascending `objsubid` order. For a relation the `objsubid` is the column's
    /// attnum.
    pub fn subobjects(&self, class: &str, oid: Oid) -> impl Iterator<Item = (i32, &str)> {
        let start = (class.to_string(), oid.0, 1);
        let end = (class.to_string(), oid.0, i32::MAX);
        self.by_key
            .range(start..=end)
            .map(|((_, _, subid), text)| (*subid, text.as_str()))
    }

    pub fn len(&self) -> usize {
        self.by_key.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }
}

/// The cross-cutting catalog state, fetched once per catalog load.
#[derive(Debug, Clone, Default)]
pub struct SharedCatalog {
    pub namespaces: NamespaceMap,
    pub extensions: ExtensionOwnership,
    pub descriptions: Descriptions,
}

/// Fetch all shared state on one connection.
///
/// Must be the same connection (and therefore the same `search_path`) the
/// per-kind fetches use.
pub async fn fetch(conn: &mut PgConnection) -> Result<SharedCatalog> {
    info!("Fetching shared catalog state...");
    let namespaces = fetch_namespaces(&mut *conn).await?;
    let extensions = fetch_extension_ownership(&mut *conn).await?;
    let descriptions = fetch_descriptions(&mut *conn).await?;

    Ok(SharedCatalog {
        namespaces,
        extensions,
        descriptions,
    })
}

pub async fn fetch_namespaces(conn: &mut PgConnection) -> Result<NamespaceMap> {
    let rows = sqlx::query!(
        r#"
        SELECT
            n.oid AS "oid!",
            n.nspname AS "name!"
        FROM pg_namespace n
        ORDER BY n.oid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(NamespaceMap {
        by_oid: rows.into_iter().map(|row| (row.oid.0, row.name)).collect(),
    })
}

pub async fn fetch_extension_ownership(conn: &mut PgConnection) -> Result<ExtensionOwnership> {
    let rows = sqlx::query!(
        r#"
        SELECT
            cl.relname AS "class_name!",
            dep.objid AS "objid!",
            e.extname AS "extension!"
        FROM pg_depend dep
        JOIN pg_class cl ON cl.oid = dep.classid
        JOIN pg_extension e ON e.oid = dep.refobjid
        WHERE dep.deptype = 'e'
          AND dep.refclassid = 'pg_extension'::regclass
        ORDER BY cl.relname, dep.objid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(ExtensionOwnership {
        owners: rows
            .into_iter()
            .map(|row| ((row.class_name, row.objid.0), row.extension))
            .collect(),
    })
}

pub async fn fetch_descriptions(conn: &mut PgConnection) -> Result<Descriptions> {
    // Comments on pinned system-catalog objects are never pgmt's to manage, and
    // there are thousands of them. Every object created after initdb — user
    // objects and extension members alike — is allocated an OID at or above
    // FirstNormalObjectId (16384), so that bound separates them.
    let rows = sqlx::query!(
        r#"
        SELECT
            cl.relname AS "class_name!",
            d.objoid AS "objoid!",
            d.objsubid AS "objsubid!",
            d.description AS "description!"
        FROM pg_description d
        JOIN pg_class cl ON cl.oid = d.classoid
        WHERE d.objoid >= 16384
        ORDER BY cl.relname, d.objoid, d.objsubid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(Descriptions {
        by_key: rows
            .into_iter()
            .map(|row| {
                (
                    (row.class_name, row.objoid.0, row.objsubid),
                    row.description,
                )
            })
            .collect(),
    })
}
