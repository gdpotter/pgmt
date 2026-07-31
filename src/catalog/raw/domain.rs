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

use super::constraint::RawConstraintDependency;
use super::dedup_preserving_order;
use super::exclusion::{Converted, Excluded, ExclusionReason, is_system_schema};
use super::oid_index::OidIndex;
use super::reference::RawReference;
use super::shared::{SharedCatalog, class};
use crate::catalog::domain::{Domain, DomainCheckConstraint};
use crate::catalog::id::DbObjectId;
use crate::catalog::utils::resolve_type_dependency;

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
    /// The `pg_depend` edges out of every constraint in the database; the
    /// converter keeps the ones whose constraint is on a domain. Table and
    /// domain constraints share `pg_constraint`, so they share one fetch
    /// (`raw::constraint::fetch_dependencies`).
    pub constraint_dependencies: Vec<RawConstraintDependency>,
    /// The `pg_depend` edges the domain row itself carries — what its DEFAULT
    /// expression names. `source_oid` is the domain's OID.
    pub dependencies: Vec<RawReference>,
}

/// Fetch every domain and domain CHECK constraint in the database, unresolved
/// and unfiltered.
pub async fn fetch(conn: &mut PgConnection) -> Result<RawDomains> {
    info!("Fetching domains...");
    let domains = fetch_domains(&mut *conn).await?;
    info!("Fetching domain constraints...");
    let check_constraints = fetch_check_constraints(&mut *conn).await?;
    let constraint_dependencies = super::constraint::fetch_dependencies(&mut *conn).await?;
    info!("Fetching domain dependencies...");
    let dependencies = fetch_dependencies(&mut *conn).await?;

    Ok(RawDomains {
        domains,
        check_constraints,
        constraint_dependencies,
        dependencies,
    })
}

/// The `pg_depend` edges a domain's own row carries: what its DEFAULT
/// expression names.
///
/// Only `deptype = 'n'` rows are edges the definition created; a domain's
/// automatic edges point at the things that own it.
async fn fetch_dependencies(conn: &mut PgConnection) -> Result<Vec<RawReference>> {
    let rows = sqlx::query!(
        r#"
        SELECT DISTINCT
            d.objid AS "domain_oid!",
            cl.relname AS "ref_class!",
            d.refobjid AS "ref_oid!",
            p.pronamespace AS "function_namespace?",
            p.proname AS "function_name?",
            pg_catalog.pg_get_function_identity_arguments(p.oid) AS "function_args?",
            o.oprnamespace AS "operator_namespace?",
            o.oprname AS "operator_name?",
            NULLIF(pg_catalog.format_type(o.oprleft, NULL), '-') AS "operator_left_type?",
            NULLIF(pg_catalog.format_type(o.oprright, NULL), '-') AS "operator_right_type?"
        FROM pg_depend d
        JOIN pg_type t ON t.oid = d.objid AND t.typtype = 'd'
        JOIN pg_class cl ON cl.oid = d.refclassid
        LEFT JOIN pg_proc p ON d.refclassid = 'pg_proc'::regclass AND d.refobjid = p.oid
        LEFT JOIN pg_operator o ON d.refclassid = 'pg_operator'::regclass AND d.refobjid = o.oid
        WHERE d.classid = 'pg_type'::regclass
          AND d.deptype = 'n'
          AND d.refclassid IN ('pg_type'::regclass, 'pg_proc'::regclass, 'pg_operator'::regclass)
        ORDER BY d.objid, cl.relname, d.refobjid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawReference {
            source_oid: row.domain_oid,
            ref_class: row.ref_class,
            ref_oid: row.ref_oid,
            function_namespace: row.function_namespace,
            function_name: row.function_name,
            function_args: row.function_args,
            operator_namespace: row.operator_namespace,
            operator_name: row.operator_name,
            operator_left_type: row.operator_left_type,
            operator_right_type: row.operator_right_type,
        })
        .collect())
}

/// Fetch domains and convert them into the logical catalog, with comments
/// attached through the OID index.
#[allow(dead_code)]
pub async fn load(conn: &mut PgConnection, shared: &SharedCatalog) -> Result<Vec<Domain>> {
    Ok(load_with_exclusions(conn, shared)
        .await?
        .log_and_take_objects("domain"))
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

    converted.index = index;

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
    // Where each surviving domain landed, so its constraints' dependency edges
    // can be pushed onto it once the identities are resolved.
    let mut kept: BTreeMap<u32, usize> = BTreeMap::new();

    for row in &raw.domains {
        let schema = namespaces
            .name(row.namespace)
            .with_context(|| format!("domain {} has no namespace entry", row.name))?;

        if is_system_schema(schema) {
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

        kept.insert(row.oid.0, converted.objects.len());
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

    // What a domain's DEFAULT and CHECK expressions name. Neither is an object
    // of its own — both are rendered inside CREATE DOMAIN — so a function or
    // operator either one calls has to be a dependency of the domain itself, or
    // the domain is created before it exists and the CREATE fails.
    let own = raw.dependencies.iter();
    let from_constraints = raw.constraint_dependencies.iter().map(|row| {
        // A domain constraint's edges are keyed by the domain it constrains, not
        // by the constraint, which is not an object of the catalog.
        (&row.reference, row.domain_oid)
    });
    for (reference, domain_oid) in own.map(|row| (row, row.source_oid)).chain(from_constraints) {
        let Some(&idx) = kept.get(&domain_oid.0) else {
            continue;
        };
        let (_, domain) = &mut converted.objects[idx];
        if let Some(dep) = reference.dependency(shared)
            && dep != domain.id()
        {
            domain.depends_on.push(dep);
        }
    }

    for (_, domain) in &mut converted.objects {
        // One constraint can name the same function or type more than once, and
        // several constraints can name the same one.
        dedup_preserving_order(&mut domain.depends_on);
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
