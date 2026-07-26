//! Raw constraint rows and their conversion into logical constraints.
//!
//! The fetch keeps the OIDs the converter resolves with, plus the outputs of the
//! server-side functions that cannot be computed in Rust: `pg_get_constraintdef`
//! for a CHECK expression, `pg_get_indexdef` per element of an exclusion
//! constraint, `pg_get_expr` for its predicate, and the column lists
//! `conkey`/`confkey` resolve to. Everything else — schema-name resolution,
//! extension-ownership and system-schema exclusion, the constraint-kind
//! classification, dependency derivation, comment attachment — happens in the
//! converter, where the OIDs die.
//!
//! Primary keys are not raw rows here: the table catalog carries a table's
//! primary key, including its comment.

use anyhow::{Context, Result, bail};
use sqlx::postgres::PgConnection;
use sqlx::postgres::types::Oid;
use tracing::info;

use super::exclusion::{Converted, Excluded, ExclusionReason, is_system_schema};
use super::oid_index::OidIndex;
use super::shared::{SharedCatalog, class};
use crate::catalog::constraint::{Constraint, ConstraintType};
use crate::catalog::id::DbObjectId;

/// One `pg_constraint` row, before names are resolved and OIDs are discarded.
#[derive(Debug, Clone)]
pub struct RawConstraint {
    pub oid: Oid,
    pub name: String,
    /// `pg_constraint.contype`: 'u' unique, 'f' foreign key, 'c' check, 'x'
    /// exclusion.
    pub contype: String,
    /// The constrained relation. Its OID is what decides extension ownership: a
    /// constraint gets no `deptype = 'e'` row of its own, only its table does.
    pub table_oid: Oid,
    pub table_namespace: Oid,
    pub table_name: String,
    /// The constrained columns, in key order.
    pub columns: Vec<String>,

    /// Referenced relation and columns, present only for a foreign key.
    pub referenced_namespace: Option<Oid>,
    pub referenced_table: Option<String>,
    pub referenced_columns: Vec<String>,
    /// `pg_constraint.confdeltype` / `confupdtype`: the referential action char.
    pub on_delete: Option<String>,
    pub on_update: Option<String>,
    pub deferrable: bool,
    pub initially_deferred: bool,

    /// `pg_get_constraintdef` of a CHECK constraint.
    pub check_clause: Option<String>,

    /// Exclusion-constraint shape, from the index that implements it.
    pub exclusion_elements: Vec<String>,
    pub exclusion_opcnames: Vec<String>,
    pub exclusion_operators: Vec<String>,
    pub index_method: Option<String>,
    pub predicate: Option<String>,
}

/// Fetch every non-primary-key table constraint in the database, unresolved and
/// unfiltered.
pub async fn fetch(conn: &mut PgConnection) -> Result<Vec<RawConstraint>> {
    info!("Fetching constraints...");
    let rows = sqlx::query!(
        r#"
        SELECT
            c.oid AS "oid!",
            c.conname AS "name!",
            c.contype::text AS "contype!",
            cl.oid AS "table_oid!",
            cl.relnamespace AS "table_namespace!",
            cl.relname AS "table_name!",

            CASE
                WHEN c.contype IN ('u', 'f') THEN
                    ARRAY(
                        SELECT a.attname
                        FROM pg_attribute a
                        WHERE a.attrelid = c.conrelid
                          AND a.attnum = ANY(c.conkey)
                        ORDER BY array_position(c.conkey, a.attnum)
                    )
                ELSE ARRAY[]::name[]
            END AS "columns!: Vec<String>",

            fcl.relnamespace AS "referenced_namespace?",
            fcl.relname AS "referenced_table?",
            CASE
                WHEN c.contype = 'f' THEN
                    ARRAY(
                        SELECT a.attname
                        FROM pg_attribute a
                        WHERE a.attrelid = c.confrelid
                          AND a.attnum = ANY(c.confkey)
                        ORDER BY array_position(c.confkey, a.attnum)
                    )
                ELSE ARRAY[]::name[]
            END AS "referenced_columns!: Vec<String>",

            NULLIF(c.confdeltype, '')::text AS "on_delete?",
            NULLIF(c.confupdtype, '')::text AS "on_update?",
            c.condeferrable AS "deferrable!",
            c.condeferred AS "initially_deferred!",

            CASE
                WHEN c.contype = 'c' THEN pg_catalog.pg_get_constraintdef(c.oid, true)
                ELSE NULL
            END AS "check_clause?",

            CASE
                WHEN c.contype = 'x' THEN
                    ARRAY(
                        SELECT pg_catalog.pg_get_indexdef(idx.indexrelid, col_pos, true)
                        FROM pg_index idx
                        CROSS JOIN generate_series(1, idx.indnatts) AS col_pos
                        WHERE idx.indexrelid = c.conindid
                        ORDER BY col_pos
                    )
                ELSE ARRAY[]::text[]
            END AS "exclusion_elements!: Vec<String>",

            CASE
                WHEN c.contype = 'x' THEN
                    ARRAY(
                        SELECT opc.opcname
                        FROM pg_index idx
                        CROSS JOIN generate_series(1, idx.indnatts) AS col_pos
                        -- oidvectors are 0-based
                        LEFT JOIN pg_opclass opc ON opc.oid = idx.indclass[col_pos - 1]
                        WHERE idx.indexrelid = c.conindid
                        ORDER BY col_pos
                    )
                ELSE ARRAY[]::name[]
            END AS "exclusion_opcnames!: Vec<String>",

            CASE
                WHEN c.contype = 'x' THEN
                    ARRAY(
                        SELECT po.oprname
                        FROM generate_series(1, cardinality(c.conexclop)) AS col_pos
                        JOIN pg_operator po ON po.oid = c.conexclop[col_pos]
                        ORDER BY col_pos
                    )
                ELSE ARRAY[]::name[]
            END AS "exclusion_operators!: Vec<String>",

            CASE
                WHEN c.contype = 'x' THEN
                    (SELECT am.amname
                     FROM pg_index idx
                     JOIN pg_class idx_cl ON idx.indexrelid = idx_cl.oid
                     JOIN pg_am am ON idx_cl.relam = am.oid
                     WHERE idx.indexrelid = c.conindid)
                ELSE NULL
            END AS "index_method?",

            CASE
                WHEN c.contype = 'x' THEN
                    (SELECT pg_catalog.pg_get_expr(idx.indpred, idx.indrelid, true)
                     FROM pg_index idx
                     WHERE idx.indexrelid = c.conindid AND idx.indpred IS NOT NULL)
                ELSE NULL
            END AS "predicate?"

        FROM pg_constraint c
        JOIN pg_class cl ON c.conrelid = cl.oid
        LEFT JOIN pg_class fcl ON c.confrelid = fcl.oid
        WHERE cl.relkind = 'r'
          AND c.contype IN ('u', 'f', 'c', 'x')
        ORDER BY c.oid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawConstraint {
            oid: row.oid,
            name: row.name,
            contype: row.contype,
            table_oid: row.table_oid,
            table_namespace: row.table_namespace,
            table_name: row.table_name,
            columns: row.columns,
            referenced_namespace: row.referenced_namespace,
            referenced_table: row.referenced_table,
            referenced_columns: row.referenced_columns,
            on_delete: row.on_delete,
            on_update: row.on_update,
            deferrable: row.deferrable,
            initially_deferred: row.initially_deferred,
            check_clause: row.check_clause,
            exclusion_elements: row.exclusion_elements,
            exclusion_opcnames: row.exclusion_opcnames,
            exclusion_operators: row.exclusion_operators,
            index_method: row.index_method,
            predicate: row.predicate,
        })
        .collect())
}

/// Fetch constraints and convert them into the logical catalog, with each
/// constraint's comment attached through the OID index.
pub async fn load(conn: &mut PgConnection, shared: &SharedCatalog) -> Result<Vec<Constraint>> {
    Ok(load_with_exclusions(conn, shared).await?.objects)
}

/// The same load, keeping the named reason for every raw row that did not become
/// a constraint.
pub async fn load_with_exclusions(
    conn: &mut PgConnection,
    shared: &SharedCatalog,
) -> Result<Converted<Constraint>> {
    let raw = fetch(conn).await?;
    let mut converted = convert(&raw, shared)?;

    // Identity first, then the index, then the OID-addressed state: a comment
    // can only be attached to an object whose identity is already known.
    let oids = OidIndex::from_pairs(
        class::PG_CONSTRAINT,
        converted
            .objects
            .iter()
            .map(|(oid, constraint)| (*oid, constraint.id())),
    )?;
    let comments = oids.object_comments(&shared.descriptions, class::PG_CONSTRAINT);
    for (_, constraint) in &mut converted.objects {
        constraint.comment = comments.get(&constraint.id()).map(|text| text.to_string());
    }

    Ok(converted.map(|(_, constraint)| constraint))
}

/// Resolve raw constraints into logical ones, keeping each constraint's OID
/// beside it so OID-addressed state can still be attached before the identities
/// cross the firewall.
///
/// Constraints on a system table and constraints whose table belongs to an
/// extension are dropped here, each with its named reason.
pub fn convert(
    raw: &[RawConstraint],
    shared: &SharedCatalog,
) -> Result<Converted<(Oid, Constraint)>> {
    let mut converted: Converted<(Oid, Constraint)> = Converted::new();

    for row in raw {
        let schema = shared
            .namespaces
            .name(row.table_namespace)
            .with_context(|| format!("constraint {} has no namespace entry", row.name))?;

        if is_system_schema(schema) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "constraint",
                schema,
                &row.name,
                ExclusionReason::SystemSchema,
            ));
            continue;
        }
        // A constraint never carries extension membership itself, even when an
        // extension script created it; only its table does.
        if let Some(extension) = shared.extensions.owner_of_relation_subobject(row.table_oid) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "constraint",
                schema,
                &row.name,
                ExclusionReason::ExtensionOwned {
                    extension: extension.to_string(),
                },
            ));
            continue;
        }

        let mut depends_on = vec![DbObjectId::Table {
            schema: schema.to_string(),
            name: row.table_name.clone(),
        }];

        let constraint_type = match row.contype.as_str() {
            "u" => ConstraintType::Unique {
                columns: row.columns.clone(),
            },
            "f" => {
                // A foreign key always names a referenced relation, and the
                // namespace map is the unfiltered enumeration read in the same
                // snapshot: a referent that will not resolve means the fetch and
                // the shared state disagree, not that the key has no referent.
                // Naming it with empty strings would render
                // `REFERENCES ""."" ` and make the constraint depend on a table
                // that cannot exist.
                let (Some(referenced_namespace), Some(referenced_table)) =
                    (row.referenced_namespace, row.referenced_table.clone())
                else {
                    bail!(
                        "foreign key {}.{}.{} was fetched without a referenced table",
                        schema,
                        row.table_name,
                        row.name
                    );
                };
                let referenced_schema = shared
                    .namespaces
                    .name(referenced_namespace)
                    .with_context(|| {
                        format!(
                            "foreign key {}.{}.{} references a table whose namespace is not in \
                             the namespace map",
                            schema, row.table_name, row.name
                        )
                    })?
                    .to_string();

                depends_on.push(DbObjectId::Table {
                    schema: referenced_schema.clone(),
                    name: referenced_table.clone(),
                });

                ConstraintType::ForeignKey {
                    columns: row.columns.clone(),
                    referenced_schema,
                    referenced_table,
                    referenced_columns: row.referenced_columns.clone(),
                    on_delete: referential_action(row.on_delete.as_deref()),
                    on_update: referential_action(row.on_update.as_deref()),
                    deferrable: row.deferrable,
                    initially_deferred: row.initially_deferred,
                }
            }
            "c" => ConstraintType::Check {
                expression: row.check_clause.clone().unwrap_or_default(),
            },
            "x" => ConstraintType::Exclusion {
                elements: row.exclusion_elements.clone(),
                operator_classes: row.exclusion_opcnames.clone(),
                operators: row.exclusion_operators.clone(),
                index_method: row.index_method.clone().unwrap_or_default(),
                predicate: row.predicate.clone(),
            },
            other => bail!("Unknown constraint type: {}", other),
        };

        converted.objects.push((
            row.oid,
            Constraint {
                schema: schema.to_string(),
                table_name: row.table_name.clone(),
                name: row.name.clone(),
                constraint_type,
                comment: None,
                depends_on,
            },
        ));
    }

    // The raw fetch orders by OID; ordering by name is what callers see.
    converted.objects.sort_by(|(_, a), (_, b)| {
        (&a.schema, &a.table_name, &a.name).cmp(&(&b.schema, &b.table_name, &b.name))
    });

    Ok(converted)
}

/// The referential action a `confdeltype`/`confupdtype` char names. NO ACTION is
/// the default and is left unstated.
fn referential_action(action: Option<&str>) -> Option<String> {
    match action? {
        "r" => Some("RESTRICT".to_string()),
        "c" => Some("CASCADE".to_string()),
        "n" => Some("SET NULL".to_string()),
        "d" => Some("SET DEFAULT".to_string()),
        // "a" is NO ACTION; anything else is not a referential action at all
        // (a non-foreign-key constraint stores the zero char).
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::raw::shared::NamespaceMap;

    /// A raw foreign key on `app.orders`, with whatever referent the caller
    /// gives it.
    fn foreign_key(
        referenced_namespace: Option<Oid>,
        referenced_table: Option<&str>,
    ) -> RawConstraint {
        RawConstraint {
            oid: Oid(20001),
            name: "orders_user_fk".to_string(),
            contype: "f".to_string(),
            table_oid: Oid(20000),
            table_namespace: Oid(100),
            table_name: "orders".to_string(),
            columns: vec!["user_id".to_string()],
            referenced_namespace,
            referenced_table: referenced_table.map(String::from),
            referenced_columns: vec!["id".to_string()],
            on_delete: None,
            on_update: None,
            deferrable: false,
            initially_deferred: false,
            check_clause: None,
            exclusion_elements: Vec::new(),
            exclusion_opcnames: Vec::new(),
            exclusion_operators: Vec::new(),
            index_method: None,
            predicate: None,
        }
    }

    fn shared_with_app_schema() -> SharedCatalog {
        SharedCatalog {
            namespaces: NamespaceMap::from_pairs([(Oid(100), "app".to_string())]),
            ..SharedCatalog::default()
        }
    }

    #[test]
    fn test_foreign_key_without_a_referent_is_an_error() {
        let raw = [foreign_key(None, None)];
        let error = convert(&raw, &shared_with_app_schema())
            .expect_err("a foreign key with no referenced table must not convert");
        assert!(
            error.to_string().contains("without a referenced table"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_foreign_key_with_an_unknown_namespace_is_an_error() {
        let raw = [foreign_key(Some(Oid(999)), Some("users"))];
        let error = convert(&raw, &shared_with_app_schema())
            .expect_err("a foreign key whose referent has no namespace must not convert");
        assert!(
            error.to_string().contains("namespace map"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_foreign_key_names_its_referent() {
        let raw = [foreign_key(Some(Oid(100)), Some("users"))];
        let converted = convert(&raw, &shared_with_app_schema()).expect("converts");
        let (_, constraint) = &converted.objects[0];
        assert!(constraint.depends_on.contains(&DbObjectId::Table {
            schema: "app".to_string(),
            name: "users".to_string(),
        }));
    }

    #[test]
    fn test_referential_actions_name_every_char_but_no_action() {
        assert_eq!(referential_action(Some("r")).as_deref(), Some("RESTRICT"));
        assert_eq!(referential_action(Some("c")).as_deref(), Some("CASCADE"));
        assert_eq!(referential_action(Some("n")).as_deref(), Some("SET NULL"));
        assert_eq!(
            referential_action(Some("d")).as_deref(),
            Some("SET DEFAULT")
        );
        assert_eq!(referential_action(Some("a")), None);
        assert_eq!(referential_action(None), None);
    }
}
