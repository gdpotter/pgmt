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

use crate::catalog::id::DbObjectId;
use crate::catalog::utils::resolve_type_dependency;

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
    pub const PG_OPCLASS: &str = "pg_opclass";
    pub const PG_TRIGGER: &str = "pg_trigger";
    pub const PG_POLICY: &str = "pg_policy";
    pub const PG_EXTENSION: &str = "pg_extension";

    /// The catalog tables above, the only ones anything here is addressed
    /// through.
    pub const ALL: [&str; 11] = [
        PG_CLASS,
        PG_PROC,
        PG_TYPE,
        PG_NAMESPACE,
        PG_OPERATOR,
        PG_CAST,
        PG_CONSTRAINT,
        PG_OPCLASS,
        PG_TRIGGER,
        PG_POLICY,
        PG_EXTENSION,
    ];

    /// The constant naming this catalog table, for a name that arrives as a
    /// `relname` string from `pg_depend` or `pg_description`.
    ///
    /// The shared maps key on the constants rather than on owned strings, so a
    /// lookup allocates nothing. A row addressed through some other catalog
    /// table (a comment on a language, an extension owning a text-search
    /// configuration) has no constant and is not interned: nothing here looks it
    /// up.
    pub fn intern(name: &str) -> Option<&'static str> {
        ALL.into_iter().find(|known| *known == name)
    }
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
    /// A map of the given `(oid, name)` pairs.
    #[allow(dead_code)]
    pub fn from_pairs(pairs: impl IntoIterator<Item = (Oid, String)>) -> Self {
        Self {
            by_oid: pairs.into_iter().map(|(oid, name)| (oid.0, name)).collect(),
        }
    }

    pub fn name(&self, oid: Oid) -> Option<&str> {
        self.by_oid.get(&oid.0).map(String::as_str)
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.by_oid.len()
    }

    /// Nothing calls this; a public `len` without it is a clippy lint.
    #[allow(dead_code)]
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
    owners: BTreeMap<(&'static str, u32), String>,
}

impl ExtensionOwnership {
    /// The extension owning this object, for object classes that carry their own
    /// `deptype = 'e'` row: tables, views, functions, types, sequences,
    /// operators, casts — every first-class object.
    pub fn owner(&self, class: &'static str, oid: Oid) -> Option<&str> {
        self.owners.get(&(class, oid.0)).map(String::as_str)
    }

    #[allow(dead_code)]
    pub fn is_owned(&self, class: &'static str, oid: Oid) -> bool {
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

    #[allow(dead_code)]
    pub fn is_relation_subobject_owned(&self, parent_relation: Oid) -> bool {
        self.owner_of_relation_subobject(parent_relation).is_some()
    }
}

/// Comments, keyed the way `pg_description` keys them:
/// `(classoid, objoid, objsubid)`, where a non-zero `objsubid` addresses a
/// sub-object (a column, by attnum).
#[derive(Debug, Clone, Default)]
pub struct Descriptions {
    by_key: BTreeMap<(&'static str, u32, i32), String>,
}

impl Descriptions {
    /// The comment on an object itself (`objsubid = 0`).
    pub fn object(&self, class: &'static str, oid: Oid) -> Option<&str> {
        self.get(class, oid, 0)
    }

    pub fn get(&self, class: &'static str, oid: Oid, objsubid: i32) -> Option<&str> {
        self.by_key
            .get(&(class, oid.0, objsubid))
            .map(String::as_str)
    }

    /// Every sub-object comment on this object, as `(objsubid, comment)` in
    /// ascending `objsubid` order. For a relation the `objsubid` is the column's
    /// attnum.
    pub fn subobjects(&self, class: &'static str, oid: Oid) -> impl Iterator<Item = (i32, &str)> {
        self.by_key
            .range((class, oid.0, 1)..=(class, oid.0, i32::MAX))
            .map(|((_, _, subid), text)| (*subid, text.as_str()))
    }
}

/// One `pg_type` row, as far as classifying a reference to it requires.
#[derive(Debug, Clone)]
pub struct TypeEntry {
    pub oid: Oid,
    pub namespace: Oid,
    pub name: String,
    /// `pg_type.typtype`: 'd' domain, 'c' composite, 'e' enum, 'r' range, …
    pub typtype: String,
    /// `pg_type.typelem`: non-zero when the type has an element type.
    pub typelem: Oid,
    /// `relkind` of `typrelid`, present only for composite types, and what
    /// distinguishes a table's row type from a view's from a standalone
    /// `CREATE TYPE ... AS`.
    pub relkind: Option<String>,
}

/// Every type in the database, by OID.
///
/// This is the classification table a converter resolves `atttypid`,
/// `prorettype`, `oprleft`, … against: an OID reference to a type says nothing
/// about whether the target is a domain, an enum, a table's row type, or an
/// array of any of those, and that distinction decides which dependency the
/// referring object gets.
#[derive(Debug, Clone, Default)]
pub struct TypeMap {
    by_oid: BTreeMap<u32, TypeEntry>,
}

impl TypeMap {
    pub fn get(&self, oid: Oid) -> Option<&TypeEntry> {
        self.by_oid.get(&oid.0)
    }

    /// The type a reference actually depends on: for an array, its element
    /// type; otherwise the type itself.
    ///
    /// Arrays are detected through `typelem`, never by the leading underscore in
    /// the array type's name — `_internal_status` is a legitimate type name and
    /// stripping the prefix invents a type that does not exist.
    pub fn element_or_self(&self, oid: Oid) -> Option<&TypeEntry> {
        let entry = self.get(oid)?;
        if entry.typelem.0 != 0 {
            // A type whose element type is missing from the map cannot occur
            // (typelem references pg_type), but fall back to the array type
            // rather than dropping the reference entirely.
            return self.get(entry.typelem).or(Some(entry));
        }
        Some(entry)
    }
}

/// A type reference resolved into everything a converter needs to name it and
/// to decide what depends on what: array references are already resolved to
/// their element type, and the schema and owning extension are looked up.
#[derive(Debug, Clone, Copy)]
pub struct ResolvedType<'a> {
    pub schema: Option<&'a str>,
    pub name: &'a str,
    pub typtype: &'a str,
    pub relkind: Option<&'a str>,
    /// The extension providing this type, if any.
    pub extension: Option<&'a str>,
    /// The reference was to an array of this type.
    pub is_array: bool,
}

impl ResolvedType<'_> {
    /// The dependency a reference to this type creates: the extension for an
    /// extension-provided type, the domain/table/view/type otherwise, and
    /// nothing for a built-in.
    pub fn dependency(&self) -> Option<DbObjectId> {
        resolve_type_dependency(
            self.schema,
            Some(self.name),
            Some(self.typtype),
            self.relkind,
            self.extension.is_some(),
            self.extension,
        )
    }
}

/// The cross-cutting catalog state, fetched once per catalog load.
#[derive(Debug, Clone, Default)]
pub struct SharedCatalog {
    pub namespaces: NamespaceMap,
    pub extensions: ExtensionOwnership,
    pub descriptions: Descriptions,
    pub types: TypeMap,
}

impl SharedCatalog {
    /// Resolve a type reference (an `atttypid`, `prorettype`, `oprleft`, …)
    /// through the array indirection, the namespace map, and the extension
    /// ownership edges in one step.
    pub fn resolve_type(&self, oid: Oid) -> Option<ResolvedType<'_>> {
        let entry = self.types.element_or_self(oid)?;
        Some(ResolvedType {
            schema: self.namespaces.name(entry.namespace),
            name: &entry.name,
            typtype: &entry.typtype,
            relkind: entry.relkind.as_deref(),
            extension: self.extensions.owner(class::PG_TYPE, entry.oid),
            is_array: entry.oid != oid,
        })
    }
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
    let types = fetch_types(&mut *conn).await?;

    Ok(SharedCatalog {
        namespaces,
        extensions,
        descriptions,
        types,
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
            .filter_map(|row| {
                let class = class::intern(&row.class_name)?;
                Some(((class, row.objid.0), row.extension))
            })
            .collect(),
    })
}

pub async fn fetch_descriptions(conn: &mut PgConnection) -> Result<Descriptions> {
    // Comments on pinned system-catalog objects are never pgmt's to manage, and
    // there are thousands of them. Every object created after initdb — user
    // objects and extension members alike — is allocated an OID at or above
    // FirstNormalObjectId (16384), so that bound separates them.
    //
    // Namespaces are the exception: `public` is created by initdb with a pinned
    // OID, yet its comment is a user's to set and pgmt's to manage, so every
    // `pg_namespace` comment is kept regardless of OID. The handful of extra
    // rows belong to schemas that are excluded as system schemas anyway.
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
           OR d.classoid = 'pg_namespace'::regclass
        ORDER BY cl.relname, d.objoid, d.objsubid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(Descriptions {
        by_key: rows
            .into_iter()
            .filter_map(|row| {
                let class = class::intern(&row.class_name)?;
                Some(((class, row.objoid.0, row.objsubid), row.description))
            })
            .collect(),
    })
}

pub async fn fetch_types(conn: &mut PgConnection) -> Result<TypeMap> {
    let rows = sqlx::query!(
        r#"
        SELECT
            t.oid AS "oid!",
            t.typnamespace AS "namespace!",
            t.typname AS "name!",
            t.typtype::text AS "typtype!",
            t.typelem AS "typelem!",
            rel.relkind::text AS "relkind?"
        FROM pg_type t
        LEFT JOIN pg_class rel ON rel.oid = t.typrelid AND t.typrelid != 0
        ORDER BY t.oid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(TypeMap {
        by_oid: rows
            .into_iter()
            .map(|row| {
                (
                    row.oid.0,
                    TypeEntry {
                        oid: row.oid,
                        namespace: row.namespace,
                        name: row.name,
                        typtype: row.typtype,
                        typelem: row.typelem,
                        relkind: row.relkind,
                    },
                )
            })
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::class;

    /// The shared maps can only be looked up through the class constants, so a
    /// constant that the interning misses would silently make every lookup of
    /// that catalog table answer "absent".
    #[test]
    fn test_interning_covers_every_class_constant() {
        for name in class::ALL {
            assert_eq!(class::intern(name), Some(name));
        }
        // A catalog table nothing here is addressed through stays uninterned.
        assert_eq!(class::intern("pg_language"), None);
    }
}
