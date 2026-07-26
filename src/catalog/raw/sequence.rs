//! Raw sequence rows and their conversion into logical sequences.
//!
//! The fetch keeps the OIDs the converter resolves with — the sequence's
//! namespace, its value type, and the relation a `pg_depend` edge ties it to.
//! Everything else — schema-name resolution, extension-ownership and
//! system-schema exclusion, the identity-column check, comment attachment —
//! happens in the converter, where the OIDs die.

use anyhow::{Context, Result};
use sqlx::postgres::PgConnection;
use sqlx::postgres::types::Oid;
use std::collections::BTreeMap;
use tracing::info;

use super::exclusion::{Converted, Excluded, ExclusionReason, is_system_schema};
use super::oid_index::OidIndex;
use super::shared::{SharedCatalog, class};
use crate::catalog::sequence::Sequence;
use crate::catalog::utils::DependencyBuilder;

/// One `pg_class` row of `relkind = 'S'` with its `pg_sequence` parameters,
/// before names are resolved and OIDs are discarded.
#[derive(Debug, Clone)]
pub struct RawSequence {
    pub oid: Oid,
    pub namespace: Oid,
    pub name: String,
    /// `pg_sequence.seqtypid`, unresolved: the converter names it through the
    /// shared type map.
    pub type_oid: Option<Oid>,
    pub start_value: Option<i64>,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub increment: Option<i64>,
    pub cycle: Option<bool>,
}

/// A `pg_depend` edge tying a sequence to a table column.
///
/// The `deptype` is what distinguishes the two cases that share this shape: `a`
/// is a `SERIAL` column's sequence, which the column merely defaults from, and
/// `i` is an identity column's sequence, which is internal to the column.
#[derive(Debug, Clone)]
pub struct RawSequenceOwnership {
    pub sequence_oid: Oid,
    pub deptype: String,
    pub table_namespace: Oid,
    pub table_name: String,
    pub column_name: String,
}

/// Everything the sequence converter reads out of `pg_catalog`.
#[derive(Debug, Clone, Default)]
pub struct RawSequences {
    pub sequences: Vec<RawSequence>,
    pub ownerships: Vec<RawSequenceOwnership>,
}

/// Fetch every sequence in the database, unresolved and unfiltered.
pub async fn fetch(conn: &mut PgConnection) -> Result<RawSequences> {
    info!("Fetching sequences...");
    let sequences = fetch_sequences(&mut *conn).await?;
    info!("Fetching sequence ownership...");
    let ownerships = fetch_ownerships(&mut *conn).await?;

    Ok(RawSequences {
        sequences,
        ownerships,
    })
}

/// Fetch sequences and convert them into the logical catalog, with each
/// sequence's comment attached through the OID index.
pub async fn load(conn: &mut PgConnection, shared: &SharedCatalog) -> Result<Vec<Sequence>> {
    Ok(load_with_exclusions(conn, shared)
        .await?
        .log_and_take_objects("sequence"))
}

/// The same load, keeping the named reason for every raw row that did not become
/// a sequence.
pub async fn load_with_exclusions(
    conn: &mut PgConnection,
    shared: &SharedCatalog,
) -> Result<Converted<Sequence>> {
    let raw = fetch(conn).await?;
    let mut converted = convert(&raw, shared)?;

    // Identity first, then the index, then the OID-addressed state: a comment
    // can only be attached to an object whose identity is already known.
    let index = OidIndex::from_pairs(
        class::PG_CLASS,
        converted
            .objects
            .iter()
            .map(|(oid, sequence)| (*oid, sequence.id())),
    )?;
    let comments = index.object_comments(&shared.descriptions, class::PG_CLASS);
    for (_, sequence) in &mut converted.objects {
        sequence.comment = comments.get(&sequence.id()).map(|text| text.to_string());
    }

    converted.index = index;

    Ok(converted.map(|(_, sequence)| sequence))
}

/// Resolve raw sequences into logical ones, keeping each sequence's OID beside
/// it so OID-addressed state can still be attached before the identities cross
/// the firewall.
///
/// Sequences in a system schema, sequences owned by an extension, and the
/// sequences behind identity columns are dropped here, each with its named
/// reason.
pub fn convert(raw: &RawSequences, shared: &SharedCatalog) -> Result<Converted<(Oid, Sequence)>> {
    let mut identity_owners: BTreeMap<u32, &RawSequenceOwnership> = BTreeMap::new();
    let mut serial_owners: BTreeMap<u32, &RawSequenceOwnership> = BTreeMap::new();
    for row in &raw.ownerships {
        match row.deptype.as_str() {
            "i" => identity_owners.insert(row.sequence_oid.0, row),
            "a" => serial_owners.insert(row.sequence_oid.0, row),
            _ => None,
        };
    }

    let mut converted: Converted<(Oid, Sequence)> = Converted::new();

    for row in &raw.sequences {
        let schema = shared
            .namespaces
            .name(row.namespace)
            .with_context(|| format!("sequence {} has no namespace entry", row.name))?;

        if is_system_schema(schema) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "sequence",
                schema,
                &row.name,
                ExclusionReason::SystemSchema,
            ));
            continue;
        }
        if let Some(extension) = shared.extensions.owner(class::PG_CLASS, row.oid) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "sequence",
                schema,
                &row.name,
                ExclusionReason::ExtensionOwned {
                    extension: extension.to_string(),
                },
            ));
            continue;
        }
        if let Some(owner) = identity_owners.get(&row.oid.0) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "sequence",
                schema,
                &row.name,
                ExclusionReason::IdentityOwnedSequence {
                    table: owner.table_name.clone(),
                    column: owner.column_name.clone(),
                },
            ));
            continue;
        }

        let owned_by = serial_owners.get(&row.oid.0).and_then(|owner| {
            let table_schema = shared.namespaces.name(owner.table_namespace)?;
            Some(format!(
                "{}.{}.{}",
                table_schema, owner.table_name, owner.column_name
            ))
        });

        // A sequence's dependency is its schema alone. The table an owned
        // sequence belongs to is deliberately not a dependency: the table
        // depends on the sequence through its column default, and the reverse
        // edge would close a cycle. Ownership is restated by its own
        // `ALTER SEQUENCE ... OWNED BY` step.
        let depends_on = DependencyBuilder::new(schema.to_string()).build();

        converted.objects.push((
            row.oid,
            Sequence {
                schema: schema.to_string(),
                name: row.name.clone(),
                data_type: data_type_name(row.type_oid, shared),
                start_value: row.start_value.unwrap_or(1),
                min_value: row.min_value.unwrap_or(1),
                max_value: row.max_value.unwrap_or(i64::MAX),
                increment: row.increment.unwrap_or(1),
                cycle: row.cycle.unwrap_or(false),
                owned_by,
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

/// The SQL name of a sequence's value type. `pg_type` stores the internal
/// spelling (`int4`), and the rendered `CREATE SEQUENCE ... AS` clause takes the
/// standard one.
fn data_type_name(type_oid: Option<Oid>, shared: &SharedCatalog) -> String {
    let name = type_oid
        .and_then(|oid| shared.types.get(oid))
        .map(|entry| entry.name.as_str())
        .unwrap_or("integer");

    match name {
        "int2" => "smallint".to_string(),
        "int4" => "integer".to_string(),
        "int8" => "bigint".to_string(),
        other => other.to_string(),
    }
}

async fn fetch_sequences(conn: &mut PgConnection) -> Result<Vec<RawSequence>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            c.oid AS "oid!",
            c.relnamespace AS "namespace!",
            c.relname AS "name!",
            s.seqtypid AS "type_oid?",
            s.seqstart AS "start_value?",
            s.seqmin AS "min_value?",
            s.seqmax AS "max_value?",
            s.seqincrement AS "increment?",
            s.seqcycle AS "cycle?"
        FROM pg_class c
        LEFT JOIN pg_sequence s ON s.seqrelid = c.oid
        WHERE c.relkind = 'S'
        ORDER BY c.oid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawSequence {
            oid: row.oid,
            namespace: row.namespace,
            name: row.name,
            type_oid: row.type_oid,
            start_value: row.start_value,
            min_value: row.min_value,
            max_value: row.max_value,
            increment: row.increment,
            cycle: row.cycle,
        })
        .collect())
}

async fn fetch_ownerships(conn: &mut PgConnection) -> Result<Vec<RawSequenceOwnership>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            d.objid AS "sequence_oid!",
            d.deptype::text AS "deptype!",
            t.relnamespace AS "table_namespace!",
            t.relname AS "table_name!",
            a.attname AS "column_name!"
        FROM pg_depend d
        JOIN pg_class s ON s.oid = d.objid AND s.relkind = 'S'
        JOIN pg_class t ON t.oid = d.refobjid
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid
        WHERE d.classid = 'pg_class'::regclass
          AND d.objsubid = 0
          AND d.refclassid = 'pg_class'::regclass
          AND d.refobjsubid > 0
          AND d.deptype IN ('a', 'i')
        ORDER BY d.objid, d.deptype
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawSequenceOwnership {
            sequence_oid: row.sequence_oid,
            deptype: row.deptype,
            table_namespace: row.table_namespace,
            table_name: row.table_name,
            column_name: row.column_name,
        })
        .collect())
}
