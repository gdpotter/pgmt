//! src/catalog/domain
//! Fetch PostgreSQL domains via pg_catalog

use anyhow::Result;
use sqlx::postgres::PgConnection;
use tracing::info;

use super::comments::Commentable;
use super::id::{DbObjectId, DependsOn};
use super::utils::DependencyBuilder;

/// A CHECK constraint on a domain
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainCheckConstraint {
    pub name: String,
    pub expression: String,
}

/// Represents a PostgreSQL domain
#[derive(Debug, Clone)]
pub struct Domain {
    pub schema: String,
    pub name: String,
    pub base_type: String,
    pub not_null: bool,
    pub default: Option<String>,
    pub collation: Option<String>,
    pub check_constraints: Vec<DomainCheckConstraint>,
    pub comment: Option<String>,
    pub depends_on: Vec<DbObjectId>,
}

impl Domain {
    pub fn id(&self) -> DbObjectId {
        DbObjectId::Domain {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }
}

impl DependsOn for Domain {
    fn id(&self) -> DbObjectId {
        DbObjectId::Domain {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}

impl Commentable for Domain {
    fn comment(&self) -> &Option<String> {
        &self.comment
    }
}

/// Fetch all domains from the database
pub async fn fetch(conn: &mut PgConnection) -> Result<Vec<Domain>> {
    // 1. Fetch basic domain information from pg_type
    info!("Fetching domains...");
    let domain_rows = sqlx::query!(
        r#"
        SELECT
            t.oid AS "oid!",
            n.nspname AS "schema!",
            t.typname AS "name!",
            format_type(t.typbasetype, t.typtypmod) AS "base_type!",
            t.typnotnull AS "not_null!",
            pg_get_expr(t.typdefaultbin, 0) AS "default?",
            CASE
                WHEN t.typcollation != 0 AND t.typcollation != (
                    SELECT oid FROM pg_collation WHERE collname = 'default'
                ) THEN (SELECT collname FROM pg_collation WHERE oid = t.typcollation)
                ELSE NULL
            END AS "collation?",
            d.description AS "comment?",
            -- For dependency tracking on the base type (resolve array element type)
            CASE
                WHEN bt.typelem != 0 THEN elem_btn.nspname
                ELSE btn.nspname
            END AS "base_type_schema?",
            CASE
                WHEN bt.typelem != 0 THEN elem_bt.typname
                ELSE bt.typname
            END AS "base_type_name?",
            -- Check if base type (or element type for arrays) is from an extension
            ext_types.extname IS NOT NULL AS "is_base_type_extension!: bool",
            ext_types.extname AS "base_type_extension_name?",
            -- Get typtype to distinguish domains ('d') from other types
            CASE
                WHEN bt.typelem != 0 THEN elem_bt.typtype::text
                ELSE bt.typtype::text
            END AS "base_type_typtype?"
        FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        LEFT JOIN pg_type bt ON t.typbasetype = bt.oid
        LEFT JOIN pg_namespace btn ON bt.typnamespace = btn.oid
        -- Element type for array base types
        LEFT JOIN pg_type elem_bt ON bt.typelem = elem_bt.oid AND bt.typelem != 0
        LEFT JOIN pg_namespace elem_btn ON elem_bt.typnamespace = elem_btn.oid
        LEFT JOIN pg_description d ON d.objoid = t.oid AND d.objsubid = 0
        -- Extension type lookup: compute once as derived table, then hash join
        LEFT JOIN (
            SELECT DISTINCT dep.objid AS type_oid, e.extname
            FROM pg_depend dep
            JOIN pg_extension e ON dep.refobjid = e.oid
            WHERE dep.deptype = 'e'
        ) ext_types ON ext_types.type_oid = COALESCE(NULLIF(bt.typelem, 0::oid), bt.oid)
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND t.typtype = 'd'  -- domain only
          -- Exclude domains that belong to extensions
          AND NOT EXISTS (
            SELECT 1 FROM pg_depend dep
            WHERE dep.objid = t.oid
            AND dep.deptype = 'e'
          )
        ORDER BY n.nspname, t.typname
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    // 2. Fetch CHECK constraints for all domains
    info!("Fetching domain constraints...");
    let check_constraints = sqlx::query!(
        r#"
        SELECT
            t.oid AS "domain_oid!",
            con.conname AS "constraint_name!",
            pg_get_constraintdef(con.oid, true) AS "expression!"
        FROM pg_constraint con
        JOIN pg_type t ON con.contypid = t.oid
        JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND t.typtype = 'd'
          AND con.contype = 'c'
        ORDER BY t.oid, con.conname
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    // 3. Organize CHECK constraints by domain OID
    let mut constraints_by_domain: std::collections::HashMap<u32, Vec<DomainCheckConstraint>> =
        std::collections::HashMap::new();
    for con in check_constraints {
        constraints_by_domain
            .entry(con.domain_oid.0)
            .or_default()
            .push(DomainCheckConstraint {
                name: con.constraint_name,
                expression: con.expression,
            });
    }

    // 4. Build Domain objects
    let mut domains = Vec::new();
    for row in domain_rows {
        let mut builder = DependencyBuilder::new(row.schema.clone());

        // Add dependency on the base type (extension, domain, or custom type)
        builder.add_type_dependency(
            row.base_type_schema.clone(),
            row.base_type_name.clone(),
            row.base_type_typtype.clone(),
            None, // relkind not available for domain base types (rarely table/view types)
            row.is_base_type_extension,
            row.base_type_extension_name.clone(),
        );

        let depends_on = builder.build();
        let check_constraints = constraints_by_domain.remove(&row.oid.0).unwrap_or_default();

        domains.push(Domain {
            schema: row.schema,
            name: row.name,
            base_type: row.base_type,
            not_null: row.not_null,
            default: row.default,
            collation: row.collation,
            check_constraints,
            comment: row.comment,
            depends_on,
        });
    }

    Ok(domains)
}
