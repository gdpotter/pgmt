//! Raw trigger rows and their conversion into logical triggers.
//!
//! The fetch keeps the OIDs the converter resolves with, plus the outputs of the
//! server-side functions that cannot be computed in Rust: `pg_get_triggerdef`,
//! which is the authoritative rendering of a trigger (timing, events, WHEN
//! clause, arguments), and `pg_get_function_identity_arguments` for the trigger
//! function's identity. Everything else — schema-name resolution,
//! extension-ownership and system-schema exclusion, dependency derivation,
//! comment attachment — happens in the converter, where the OIDs die.

use anyhow::{Context, Result};
use sqlx::postgres::PgConnection;
use sqlx::postgres::types::Oid;
use std::collections::BTreeMap;
use tracing::info;

use super::dedup_preserving_order;
use super::exclusion::{Converted, Excluded, ExclusionReason, is_system_schema};
use super::oid_index::OidIndex;
use super::reference::RawReference;
use super::shared::{SharedCatalog, class};
use crate::catalog::DependsOn;
use crate::catalog::id::DbObjectId;
use crate::catalog::triggers::Trigger;

/// One `pg_trigger` row, before names are resolved and OIDs are discarded.
#[derive(Debug, Clone)]
pub struct RawTrigger {
    pub oid: Oid,
    pub name: String,
    /// The triggering relation. Its OID is what decides extension ownership: a
    /// trigger gets no `deptype = 'e'` row of its own, only its table does.
    pub table_oid: Oid,
    pub table_namespace: Oid,
    pub table_name: String,
    /// `pg_trigger.tgisinternal`: the trigger enforces a constraint rather than
    /// being one a user wrote.
    pub is_internal: bool,
    pub function_namespace: Oid,
    pub function_name: String,
    /// `pg_get_function_identity_arguments` of the trigger function.
    pub function_args: String,
    /// `pg_get_triggerdef` — the authoritative definition a recreate replays.
    pub definition: String,
}

/// Everything the trigger converter reads out of `pg_catalog`.
#[derive(Debug, Clone, Default)]
pub struct RawTriggers {
    pub triggers: Vec<RawTrigger>,
    /// The `pg_depend` edges out of each trigger row: the trigger function, and
    /// whatever its WHEN clause names. `source_oid` is the trigger's OID.
    pub dependencies: Vec<RawReference>,
}

/// Fetch every trigger on a table, view or materialized view, unresolved and
/// unfiltered.
///
/// The relation kinds are the shape of what pgmt models, so they are selected
/// here; which of those triggers are pgmt's to manage is the converter's call.
pub async fn fetch(conn: &mut PgConnection) -> Result<RawTriggers> {
    Ok(RawTriggers {
        triggers: fetch_triggers(&mut *conn).await?,
        dependencies: fetch_dependencies(&mut *conn).await?,
    })
}

/// The `pg_depend` edges out of every `pg_trigger` row.
///
/// `tgfoid` is one of them, but so is every routine or operator the WHEN clause
/// names, which appears nowhere else in the trigger row.
async fn fetch_dependencies(conn: &mut PgConnection) -> Result<Vec<RawReference>> {
    let rows = sqlx::query!(
        r#"
        SELECT DISTINCT
            d.objid AS "trigger_oid!",
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
        JOIN pg_class cl ON cl.oid = d.refclassid
        LEFT JOIN pg_proc p ON d.refclassid = 'pg_proc'::regclass AND d.refobjid = p.oid
        LEFT JOIN pg_operator o ON d.refclassid = 'pg_operator'::regclass AND d.refobjid = o.oid
        WHERE d.classid = 'pg_trigger'::regclass
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
            source_oid: row.trigger_oid,
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

async fn fetch_triggers(conn: &mut PgConnection) -> Result<Vec<RawTrigger>> {
    info!("Fetching triggers...");
    let rows = sqlx::query!(
        r#"
        SELECT
            t.oid AS "oid!",
            t.tgname AS "name!",
            c.oid AS "table_oid!",
            c.relnamespace AS "table_namespace!",
            c.relname AS "table_name!",
            t.tgisinternal AS "is_internal!",
            p.pronamespace AS "function_namespace!",
            p.proname AS "function_name!",
            pg_catalog.pg_get_function_identity_arguments(p.oid) AS "function_args!",
            pg_catalog.pg_get_triggerdef(t.oid) AS "definition!"
        FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        JOIN pg_proc p ON t.tgfoid = p.oid
        WHERE c.relkind IN ('r', 'v', 'm')
        ORDER BY t.oid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawTrigger {
            oid: row.oid,
            name: row.name,
            table_oid: row.table_oid,
            table_namespace: row.table_namespace,
            table_name: row.table_name,
            is_internal: row.is_internal,
            function_namespace: row.function_namespace,
            function_name: row.function_name,
            function_args: row.function_args,
            definition: row.definition,
        })
        .collect())
}

/// Fetch triggers and convert them into the logical catalog, with each trigger's
/// comment attached through the OID index.
#[allow(dead_code)]
pub async fn load(conn: &mut PgConnection, shared: &SharedCatalog) -> Result<Vec<Trigger>> {
    Ok(load_with_exclusions(conn, shared)
        .await?
        .log_and_take_objects("trigger"))
}

/// The same load, keeping the named reason for every raw row that did not become
/// a trigger.
pub async fn load_with_exclusions(
    conn: &mut PgConnection,
    shared: &SharedCatalog,
) -> Result<Converted<Trigger>> {
    let raw = fetch(conn).await?;
    let mut converted = convert(&raw, shared)?;

    // Identity first, then the index, then the OID-addressed state: a comment
    // can only be attached to an object whose identity is already known.
    let index = OidIndex::from_pairs(
        class::PG_TRIGGER,
        converted
            .objects
            .iter()
            .map(|(oid, trigger)| (*oid, trigger.id())),
    )?;
    let comments = index.object_comments(&shared.descriptions, class::PG_TRIGGER);
    for (_, trigger) in &mut converted.objects {
        trigger.comment = comments.get(&trigger.id()).map(|text| text.to_string());
    }

    converted.index = index;

    Ok(converted.map(|(_, trigger)| trigger))
}

/// Resolve raw triggers into logical ones, keeping each trigger's OID beside it
/// so OID-addressed state can still be attached before the identities cross the
/// firewall.
///
/// Triggers on a system table, triggers whose table belongs to an extension, and
/// the internal triggers that enforce constraints are dropped here, each with its
/// named reason.
pub fn convert(raw: &RawTriggers, shared: &SharedCatalog) -> Result<Converted<(Oid, Trigger)>> {
    let mut converted: Converted<(Oid, Trigger)> = Converted::new();
    // Where each surviving trigger landed, so its dependency edges can be
    // pushed onto it once the identities are resolved.
    let mut kept: BTreeMap<u32, usize> = BTreeMap::new();

    for row in &raw.triggers {
        let schema = shared
            .namespaces
            .name(row.table_namespace)
            .with_context(|| format!("trigger {} has no namespace entry", row.name))?;

        if is_system_schema(schema) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "trigger",
                schema,
                &row.name,
                ExclusionReason::SystemSchema,
            ));
            continue;
        }
        // A trigger never carries extension membership itself, even when an
        // extension script created it; only its table does.
        if let Some(extension) = shared.extensions.owner_of_relation_subobject(row.table_oid) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "trigger",
                schema,
                &row.name,
                ExclusionReason::ExtensionOwned {
                    extension: extension.to_string(),
                },
            ));
            continue;
        }
        if row.is_internal {
            converted.excluded.push(Excluded::new(
                row.oid,
                "trigger",
                schema,
                &row.name,
                ExclusionReason::InternalTrigger,
            ));
            continue;
        }

        let function_schema = shared
            .namespaces
            .name(row.function_namespace)
            .with_context(|| {
                format!(
                    "trigger function {} has no namespace entry",
                    row.function_name
                )
            })?;

        let depends_on = vec![
            DbObjectId::Table {
                schema: schema.to_string(),
                name: row.table_name.clone(),
            },
            DbObjectId::Function {
                schema: function_schema.to_string(),
                name: row.function_name.clone(),
                arguments: row.function_args.clone(),
            },
        ];

        kept.insert(row.oid.0, converted.objects.len());
        converted.objects.push((
            row.oid,
            Trigger {
                schema: schema.to_string(),
                table_name: row.table_name.clone(),
                name: row.name.clone(),
                function_schema: function_schema.to_string(),
                function_name: row.function_name.clone(),
                function_args: row.function_args.clone(),
                comment: None,
                depends_on,
                definition: row.definition.clone(),
            },
        ));
    }

    // What the WHEN clause names. `tgfoid` is already an edge above; anything a
    // WHEN condition calls appears only in `pg_depend`, and a trigger created
    // before it fails with `function ... does not exist`.
    for row in &raw.dependencies {
        let Some(&idx) = kept.get(&row.source_oid.0) else {
            continue;
        };
        if let Some(dep) = row.dependency(shared) {
            converted.objects[idx].1.depends_on.push(dep);
        }
    }

    for (_, trigger) in &mut converted.objects {
        // The trigger function is both `tgfoid` and a `pg_depend` edge.
        dedup_preserving_order(&mut trigger.depends_on);
    }

    // The raw fetch orders by OID; ordering by name is what callers see.
    converted.objects.sort_by(|(_, a), (_, b)| {
        (&a.schema, &a.table_name, &a.name).cmp(&(&b.schema, &b.table_name, &b.name))
    });

    Ok(converted)
}
