//! Raw domain rows and their conversion into logical domains.
//!
//! The fetches keep the OIDs the converter resolves with, plus the outputs of
//! the server-side functions that cannot be computed in Rust: `format_type` for
//! the rendered base type, `pg_get_expr` for the default, and
//! `pg_get_constraintdef` for each CHECK constraint. Everything else —
//! schema-name resolution, extension-ownership and system-schema exclusion,
//! base-type classification, comment attachment — happens in the converter,
//! where the OIDs die.

use anyhow::{Context, Result};
use sqlx::postgres::PgConnection;
use sqlx::postgres::types::Oid;
use std::collections::BTreeMap;
use tracing::info;

use super::exclusion::{Converted, Excluded, ExclusionReason};
use super::oid_index::OidIndex;
use super::shared::{SharedCatalog, class};
use crate::catalog::domain::{Domain, DomainCheckConstraint};
use crate::catalog::id::DbObjectId;
use crate::catalog::utils::resolve_type_dependency;

/// Schemas whose domains are never pgmt's to manage.
const SYSTEM_SCHEMAS: [&str; 3] = ["pg_catalog", "information_schema", "pg_toast"];

/// One `pg_type` row of `typtype = 'd'`, before names are resolved and OIDs are
/// discarded.
#[derive(Debug, Clone)]
pub struct RawDomain {
    pub oid: Oid,
    pub namespace: Oid,
    pub name: String,
    /// `format_type(typbasetype, typtypmod)` — carries type modifiers and array
    /// brackets, which no Rust-side reconstruction can recover.
    pub base_type: String,
    /// `typbasetype`, unresolved: an array's own OID, not its element type's.
    pub base_type_oid: Oid,
    pub not_null: bool,
    /// `pg_get_expr` of `typdefaultbin`.
    pub default: Option<String>,
    /// The collation name, present only when the domain overrides the default
    /// collation.
    pub collation: Option<String>,
}

/// One CHECK constraint on a domain, already rendered by
/// `pg_get_constraintdef`.
#[derive(Debug, Clone)]
pub struct RawDomainCheckConstraint {
    pub domain_oid: Oid,
    pub name: String,
    pub expression: String,
}

/// Everything the domain converter reads out of `pg_catalog`.
#[derive(Debug, Clone, Default)]
pub struct RawDomains {
    pub domains: Vec<RawDomain>,
    pub check_constraints: Vec<RawDomainCheckConstraint>,
}

/// Fetch every domain and domain CHECK constraint in the database, unresolved
/// and unfiltered.
pub async fn fetch(conn: &mut PgConnection) -> Result<RawDomains> {
    info!("Fetching domains...");
    let domains = fetch_domains(&mut *conn).await?;
    info!("Fetching domain constraints...");
    let check_constraints = fetch_check_constraints(&mut *conn).await?;

    Ok(RawDomains {
        domains,
        check_constraints,
    })
}

/// Fetch domains and convert them into the logical catalog, with comments
/// attached through the OID index.
pub async fn load(conn: &mut PgConnection, shared: &SharedCatalog) -> Result<Vec<Domain>> {
    Ok(load_with_exclusions(conn, shared).await?.objects)
}

/// The same load, keeping the named reason for every raw row that did not
/// become a domain.
pub async fn load_with_exclusions(
    conn: &mut PgConnection,
    shared: &SharedCatalog,
) -> Result<Converted<Domain>> {
    let raw = fetch(conn).await?;
    let mut converted = convert(&raw, shared)?;

    // Identity first, then the index, then the OID-addressed state: a comment
    // can only be attached to an object whose identity is already known.
    let index = OidIndex::from_pairs(
        class::PG_TYPE,
        converted
            .objects
            .iter()
            .map(|(oid, domain)| (*oid, domain.id())),
    )?;
    let comments = index.object_comments(&shared.descriptions, class::PG_TYPE);
    for (_, domain) in &mut converted.objects {
        domain.comment = comments.get(&domain.id()).map(|text| text.to_string());
    }

    Ok(converted.map(|(_, domain)| domain))
}

/// Resolve raw domains into logical ones, keeping each domain's OID beside it so
/// OID-addressed state can still be attached before the identities cross the
/// firewall.
///
/// Domains in a system schema and domains owned by an extension are dropped
/// here, each recorded with its named reason, along with the CHECK constraints
/// belonging to them.
pub fn convert(raw: &RawDomains, shared: &SharedCatalog) -> Result<Converted<(Oid, Domain)>> {
    let namespaces = &shared.namespaces;
    let constraints = check_constraints_by_domain(raw);
    let mut converted: Converted<(Oid, Domain)> = Converted::new();

    for row in &raw.domains {
        let schema = namespaces
            .name(row.namespace)
            .with_context(|| format!("domain {} has no namespace entry", row.name))?;

        if SYSTEM_SCHEMAS.contains(&schema) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "domain",
                schema,
                &row.name,
                ExclusionReason::SystemSchema,
            ));
            continue;
        }
        if let Some(extension) = shared.extensions.owner(class::PG_TYPE, row.oid) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "domain",
                schema,
                &row.name,
                ExclusionReason::ExtensionOwned {
                    extension: extension.to_string(),
                },
            ));
            continue;
        }

        let mut depends_on = vec![DbObjectId::Schema {
            name: schema.to_string(),
        }];
        // The base type: an extension-provided type is depended on through its
        // extension, a user-defined one directly. A relation's row type is not
        // distinguished here — a domain's base type is depended on as a type.
        if let Some(dep) = shared.resolve_type(row.base_type_oid).and_then(|base| {
            resolve_type_dependency(
                base.schema,
                Some(base.name),
                Some(base.typtype),
                None,
                base.extension.is_some(),
                base.extension,
            )
        }) {
            depends_on.push(dep);
        }

        converted.objects.push((
            row.oid,
            Domain {
                schema: schema.to_string(),
                name: row.name.clone(),
                base_type: row.base_type.clone(),
                not_null: row.not_null,
                default: row.default.clone(),
                collation: row.collation.clone(),
                check_constraints: constraints.get(&row.oid.0).cloned().unwrap_or_default(),
                comment: None,
                depends_on,
            },
        ));
    }

    // The raw fetch orders by OID; ordering by name is what callers see.
    converted
        .objects
        .sort_by(|(_, a), (_, b)| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));

    Ok(converted)
}

/// The CHECK constraints of each domain, in constraint-name order, keyed by the
/// domain's OID.
fn check_constraints_by_domain(raw: &RawDomains) -> BTreeMap<u32, Vec<DomainCheckConstraint>> {
    let mut by_domain: BTreeMap<u32, Vec<DomainCheckConstraint>> = BTreeMap::new();
    for row in &raw.check_constraints {
        by_domain
            .entry(row.domain_oid.0)
            .or_default()
            .push(DomainCheckConstraint {
                name: row.name.clone(),
                expression: row.expression.clone(),
            });
    }
    by_domain
}

async fn fetch_domains(conn: &mut PgConnection) -> Result<Vec<RawDomain>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            t.oid AS "oid!",
            t.typnamespace AS "namespace!",
            t.typname AS "name!",
            pg_catalog.format_type(t.typbasetype, t.typtypmod) AS "base_type!",
            t.typbasetype AS "base_type_oid!",
            t.typnotnull AS "not_null!",
            pg_catalog.pg_get_expr(t.typdefaultbin, 0) AS "default?",
            CASE
                WHEN t.typcollation != 0 AND t.typcollation != (
                    SELECT oid FROM pg_collation WHERE collname = 'default'
                ) THEN (SELECT collname FROM pg_collation WHERE oid = t.typcollation)
                ELSE NULL
            END AS "collation?"
        FROM pg_type t
        WHERE t.typtype = 'd'
        ORDER BY t.oid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawDomain {
            oid: row.oid,
            namespace: row.namespace,
            name: row.name,
            base_type: row.base_type,
            base_type_oid: row.base_type_oid,
            not_null: row.not_null,
            default: row.default,
            collation: row.collation,
        })
        .collect())
}

async fn fetch_check_constraints(conn: &mut PgConnection) -> Result<Vec<RawDomainCheckConstraint>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            con.contypid AS "domain_oid!",
            con.conname AS "name!",
            pg_catalog.pg_get_constraintdef(con.oid, true) AS "expression!"
        FROM pg_constraint con
        WHERE con.contype = 'c'
          AND con.contypid != 0
        ORDER BY con.contypid, con.conname
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawDomainCheckConstraint {
            domain_oid: row.domain_oid,
            name: row.name,
            expression: row.expression,
        })
        .collect())
}
