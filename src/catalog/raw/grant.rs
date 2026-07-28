//! Raw ACL rows and their conversion into logical grants.
//!
//! A grant is state *attached* to an object, not an object of its own: its
//! identity is the identity of the thing it is on, and the catalog-wide OID
//! index already holds every one of those identities. So this converter carries
//! no exclusion rules — an ACL row whose `(catalog table, OID)` is absent from
//! the index sits on something no converter kept (an object in a system schema,
//! an extension's, an index, a relation's row type, an array type), and its
//! grants are dropped for exactly the reason the object was. That one rule
//! replaces the six `deptype = 'e'` subqueries and the schema-name filters the
//! per-kind grant queries each used to spell for themselves.
//!
//! `aclexplode` and `acldefault` stay in SQL: an `aclitem[]` has no client-side
//! representation, and the default ACL of an object with no explicit one is
//! PostgreSQL's to state.

use anyhow::Result;
use sqlx::postgres::PgConnection;
use sqlx::postgres::types::Oid;
use std::collections::BTreeMap;
use tracing::{debug, info};

use super::oid_index::OidIndex;
use super::shared::class;
use crate::catalog::grant::{Grant, GranteeType, target_key};
use crate::catalog::id::DbObjectId;
use crate::catalog::target::AttrTarget;

/// The OID `aclexplode` reports for a grant to `PUBLIC`.
const PUBLIC_GRANTEE: u32 = 0;

/// One exploded `aclitem`, addressed the way `pg_description` addresses a
/// comment: by catalog table, OID, and a sub-object id that is zero for the
/// object itself and a column's attnum otherwise.
#[derive(Debug, Clone)]
pub struct RawAclRow {
    /// The `pg_catalog` table `oid` addresses a row of.
    pub class: String,
    pub oid: Oid,
    /// Zero for the object's own ACL; a column's attnum for a column ACL.
    pub subid: i32,
    /// The column's name, present exactly when `subid` is non-zero. `attnum` is
    /// a physical coordinate and never enters the model, so the name is read
    /// beside it.
    pub column_name: Option<String>,
    /// `aclitem.grantee`, with [`PUBLIC_GRANTEE`] for `PUBLIC`.
    pub grantee: Oid,
    pub privilege: String,
    pub grantable: bool,
    /// The object's owner, whose grants are implicit in PostgreSQL and are
    /// dropped by the diff rather than by the fetch.
    pub owner: Oid,
    /// The object has no ACL of its own, so these rows are PostgreSQL's
    /// defaults expanded rather than privileges anyone granted.
    pub is_default_acl: bool,
}

/// Role names by OID, for the grantee and owner of an ACL row.
///
/// Roles are cluster-wide and pgmt does not manage them, so they are not
/// catalog objects and have no place in the OID index; this is the one lookup
/// that turns a role OID into the name a `GRANT` statement names.
#[derive(Debug, Clone, Default)]
pub struct RoleMap {
    by_oid: BTreeMap<u32, String>,
}

impl RoleMap {
    pub fn name(&self, oid: Oid) -> Option<&str> {
        self.by_oid.get(&oid.0).map(String::as_str)
    }
}

/// Everything the grant converter reads out of `pg_catalog`.
#[derive(Debug, Clone, Default)]
pub struct RawGrants {
    pub acl_rows: Vec<RawAclRow>,
    pub roles: RoleMap,
}

/// Fetch every ACL row in the database, unresolved and unfiltered, plus the
/// role names they refer to.
pub async fn fetch(conn: &mut PgConnection) -> Result<RawGrants> {
    info!("Fetching grants...");
    let roles = fetch_roles(&mut *conn).await?;
    let acl_rows = fetch_acl_rows(&mut *conn).await?;

    Ok(RawGrants { acl_rows, roles })
}

/// Fetch ACL rows and resolve them into logical grants through the OID index of
/// the whole catalog load.
pub async fn load(conn: &mut PgConnection, index: &OidIndex) -> Result<Vec<Grant>> {
    let raw = fetch(conn).await?;
    Ok(convert(&raw, index))
}

/// One resolved ACL row, before privileges are grouped into grants.
struct AclEntry {
    target: AttrTarget,
    grantee: GranteeType,
    privilege: String,
    grantable: bool,
    owner: String,
    is_default_acl: bool,
}

/// Resolve raw ACL rows into logical grants.
///
/// Rows on an object the index does not know are dropped; so are rows naming a
/// role that no longer exists, and the `public` schema's own ACL (see
/// [`acl_is_tracked`]). Grants are not part of the converters' exclusion
/// accounting — that accounting is per *object row*, and a grant is not an
/// object — so the drops are reported as a count per catalog table instead.
pub fn convert(raw: &RawGrants, index: &OidIndex) -> Vec<Grant> {
    let mut entries: Vec<AclEntry> = Vec::new();
    let mut dropped: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut untracked = 0usize;

    for row in &raw.acl_rows {
        let Some(class) = class::intern(&row.class) else {
            continue;
        };
        let Some(id) = index.get(class, row.oid) else {
            *dropped.entry(class).or_default() += 1;
            continue;
        };
        if !acl_is_tracked(id) {
            untracked += 1;
            continue;
        }

        let Some(owner) = raw.roles.name(row.owner) else {
            *dropped.entry(class).or_default() += 1;
            continue;
        };
        let grantee = if row.grantee.0 == PUBLIC_GRANTEE {
            GranteeType::Public
        } else {
            match raw.roles.name(row.grantee) {
                Some(name) => GranteeType::Role(name.to_string()),
                None => {
                    *dropped.entry(class).or_default() += 1;
                    continue;
                }
            }
        };

        let target = match &row.column_name {
            Some(column) => AttrTarget::column(id.clone(), column.clone()),
            None => AttrTarget::object(id.clone()),
        };

        entries.push(AclEntry {
            target,
            grantee,
            privilege: row.privilege.clone(),
            grantable: row.grantable,
            owner: owner.to_string(),
            // A column ACL is never NULL where it is fetched from, so a column
            // grant is always an explicit one.
            is_default_acl: row.is_default_acl && row.subid == 0,
        });
    }

    // The raw fetch orders by OID; ordering by name is what callers see. Within
    // one grantee on one target the order is the privilege name, which is what
    // makes the privilege list of a grant alphabetical and the grouping below
    // reproducible.
    entries.sort_by_cached_key(|entry| {
        let (is_public, role) = match &entry.grantee {
            GranteeType::Public => (true, String::new()),
            GranteeType::Role(name) => (false, name.clone()),
        };
        (
            target_key(&entry.target),
            is_public,
            role,
            entry.privilege.clone(),
        )
    });

    // One grant per run of privileges sharing a grantee, a target and a grant
    // option. Grantability splits a run because `WITH GRANT OPTION` is a
    // property of the whole statement, not of a privilege.
    let mut grants: Vec<Grant> = Vec::new();
    for entry in entries {
        match grants.last_mut() {
            Some(last)
                if last.grantee == entry.grantee
                    && last.target == entry.target
                    && last.with_grant_option == entry.grantable =>
            {
                last.privileges.push(entry.privilege);
            }
            _ => grants.push(Grant {
                // A grant depends on the object it is on, and on nothing else:
                // the grantee role is assumed to exist outside pgmt.
                depends_on: vec![entry.target.db_object_id()],
                target: entry.target,
                grantee: entry.grantee,
                privileges: vec![entry.privilege],
                with_grant_option: entry.grantable,
                object_owner: entry.owner,
                is_default_acl: entry.is_default_acl,
            }),
        }
    }

    let total_dropped: usize = dropped.values().sum();
    debug!(
        "Converted {} grants from {} ACL rows, dropped {total_dropped} on objects outside the \
         catalog ({}) and {untracked} on the public schema",
        grants.len(),
        raw.acl_rows.len(),
        dropped
            .iter()
            .map(|(class, count)| format!("{class}: {count}"))
            .collect::<Vec<_>>()
            .join(", ")
    );

    grants
}

/// Whether pgmt tracks this object's privileges at all.
///
/// The `public` schema is the single carve-out. It is created by `initdb`
/// rather than by a schema file, and its ACL is part of the image a database is
/// provisioned from — managing it would make every target whose `public` schema
/// was hardened diverge from a shadow database that was not. Every other object
/// whose grants are dropped is dropped because the object itself is not in the
/// catalog.
fn acl_is_tracked(id: &DbObjectId) -> bool {
    !matches!(id, DbObjectId::Schema { name } if name == "public")
}

async fn fetch_roles(conn: &mut PgConnection) -> Result<RoleMap> {
    let rows = sqlx::query!(
        r#"
        SELECT
            r.oid AS "oid!",
            r.rolname AS "name!"
        FROM pg_roles r
        ORDER BY r.oid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(RoleMap {
        by_oid: rows.into_iter().map(|row| (row.oid.0, row.name)).collect(),
    })
}

/// Every ACL row of every grantable catalog, in one shape.
///
/// The branches differ only in which catalog they read and which `acldefault`
/// object type expands an absent ACL — `'r'` for a relation, `'S'` for a
/// sequence, `'f'` for a routine, `'n'` for a schema, `'T'` for a type. What
/// the rows mean afterwards is the index's to say, so nothing here filters by
/// schema or by extension membership.
///
/// The two `relkind`/`typtype` lists are not exclusions: they keep the query
/// from expanding the default ACL of every index, TOAST table and built-in base
/// type in the database, none of which the index could resolve. Anything they
/// let through that the catalog does not manage is still dropped by the
/// converter.
async fn fetch_acl_rows(conn: &mut PgConnection) -> Result<Vec<RawAclRow>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            acl_rows.class AS "class!",
            acl_rows.oid AS "oid!",
            acl_rows.subid AS "subid!",
            acl_rows.column_name AS "column_name?",
            acl_rows.grantee AS "grantee!",
            acl_rows.privilege AS "privilege!",
            acl_rows.grantable AS "grantable!",
            acl_rows.owner AS "owner!",
            acl_rows.is_default_acl AS "is_default_acl!"
        FROM (
            SELECT
                'pg_class'::text AS class,
                c.oid AS oid,
                0::int AS subid,
                NULL::text AS column_name,
                acl.grantee AS grantee,
                acl.privilege_type AS privilege,
                acl.is_grantable AS grantable,
                c.relowner AS owner,
                (c.relacl IS NULL) AS is_default_acl
            FROM pg_class c
            CROSS JOIN LATERAL aclexplode(COALESCE(
                c.relacl,
                acldefault(CASE WHEN c.relkind = 'S' THEN 'S' ELSE 'r' END::"char", c.relowner)
            )) AS acl
            WHERE c.relkind IN ('r', 'p', 'v', 'm', 'S')

            UNION ALL

            SELECT
                'pg_class'::text,
                a.attrelid,
                a.attnum::int,
                a.attname,
                acl.grantee,
                acl.privilege_type,
                acl.is_grantable,
                c.relowner,
                false
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            CROSS JOIN LATERAL aclexplode(a.attacl) AS acl
            WHERE c.relkind IN ('r', 'p', 'v', 'm')
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND a.attacl IS NOT NULL

            UNION ALL

            SELECT
                'pg_proc'::text,
                p.oid,
                0::int,
                NULL::text,
                acl.grantee,
                acl.privilege_type,
                acl.is_grantable,
                p.proowner,
                (p.proacl IS NULL)
            FROM pg_proc p
            CROSS JOIN LATERAL aclexplode(COALESCE(
                p.proacl, acldefault('f'::"char", p.proowner)
            )) AS acl

            UNION ALL

            SELECT
                'pg_namespace'::text,
                n.oid,
                0::int,
                NULL::text,
                acl.grantee,
                acl.privilege_type,
                acl.is_grantable,
                n.nspowner,
                (n.nspacl IS NULL)
            FROM pg_namespace n
            CROSS JOIN LATERAL aclexplode(COALESCE(
                n.nspacl, acldefault('n'::"char", n.nspowner)
            )) AS acl

            UNION ALL

            SELECT
                'pg_type'::text,
                t.oid,
                0::int,
                NULL::text,
                acl.grantee,
                acl.privilege_type,
                acl.is_grantable,
                t.typowner,
                (t.typacl IS NULL)
            FROM pg_type t
            CROSS JOIN LATERAL aclexplode(COALESCE(
                t.typacl, acldefault('T'::"char", t.typowner)
            )) AS acl
            WHERE t.typtype IN ('e', 'd', 'c', 'r')
        ) AS acl_rows
        ORDER BY acl_rows.class, acl_rows.oid, acl_rows.subid, acl_rows.grantee, acl_rows.privilege
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawAclRow {
            class: row.class,
            oid: row.oid,
            subid: row.subid,
            column_name: row.column_name,
            grantee: row.grantee,
            privilege: row.privilege,
            grantable: row.grantable,
            owner: row.owner,
            is_default_acl: row.is_default_acl,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(name: &str) -> DbObjectId {
        DbObjectId::Table {
            schema: "app".to_string(),
            name: name.to_string(),
        }
    }

    fn roles(pairs: &[(u32, &str)]) -> RoleMap {
        RoleMap {
            by_oid: pairs
                .iter()
                .map(|(oid, name)| (*oid, name.to_string()))
                .collect(),
        }
    }

    fn acl_row(oid: u32, privilege: &str, grantable: bool, grantee: u32) -> RawAclRow {
        RawAclRow {
            class: class::PG_CLASS.to_string(),
            oid: Oid(oid),
            subid: 0,
            column_name: None,
            grantee: Oid(grantee),
            privilege: privilege.to_string(),
            grantable,
            owner: Oid(10),
            is_default_acl: false,
        }
    }

    fn index_of(oid: u32, id: DbObjectId) -> OidIndex {
        OidIndex::from_pairs(class::PG_CLASS, [(Oid(oid), id)]).unwrap()
    }

    /// Privileges of one grantee on one object collapse into a single grant,
    /// listed alphabetically, so the same ACL always yields the same grant.
    #[test]
    fn test_privileges_of_one_grantee_group_into_one_grant() {
        let raw = RawGrants {
            acl_rows: vec![
                acl_row(16400, "SELECT", false, 20),
                acl_row(16400, "INSERT", false, 20),
            ],
            roles: roles(&[(10, "owner"), (20, "app_user")]),
        };

        let grants = convert(&raw, &index_of(16400, table("users")));

        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].privileges, vec!["INSERT", "SELECT"]);
        assert_eq!(grants[0].grantee, GranteeType::Role("app_user".to_string()));
        assert_eq!(grants[0].depends_on, vec![table("users")]);
    }

    /// `WITH GRANT OPTION` is a property of a whole statement, so privileges
    /// held with and without it cannot share a grant.
    #[test]
    fn test_grant_option_splits_a_grantee_into_two_grants() {
        let raw = RawGrants {
            acl_rows: vec![
                acl_row(16400, "INSERT", false, 20),
                acl_row(16400, "SELECT", true, 20),
            ],
            roles: roles(&[(10, "owner"), (20, "app_user")]),
        };

        let grants = convert(&raw, &index_of(16400, table("users")));

        assert_eq!(grants.len(), 2);
        assert!(!grants[0].with_grant_option);
        assert_eq!(grants[0].privileges, vec!["INSERT"]);
        assert!(grants[1].with_grant_option);
        assert_eq!(grants[1].privileges, vec!["SELECT"]);
    }

    /// An ACL row on an object no converter kept has no identity to attach to.
    /// Dropping it is the same decision that dropped the object, taken once.
    #[test]
    fn test_acl_rows_on_unindexed_objects_are_dropped() {
        let raw = RawGrants {
            acl_rows: vec![acl_row(16400, "SELECT", false, 20)],
            roles: roles(&[(10, "owner"), (20, "app_user")]),
        };

        assert!(convert(&raw, &OidIndex::new()).is_empty());
    }

    /// The `public` schema's own privileges come from the image a database is
    /// provisioned from, not from a schema file.
    #[test]
    fn test_public_schema_privileges_are_not_tracked() {
        let mut row = acl_row(2200, "USAGE", false, 0);
        row.class = class::PG_NAMESPACE.to_string();
        let raw = RawGrants {
            acl_rows: vec![row],
            roles: roles(&[(10, "owner")]),
        };
        let index = OidIndex::from_pairs(
            class::PG_NAMESPACE,
            [(
                Oid(2200),
                DbObjectId::Schema {
                    name: "public".to_string(),
                },
            )],
        )
        .unwrap();

        assert!(convert(&raw, &index).is_empty());
    }
}
