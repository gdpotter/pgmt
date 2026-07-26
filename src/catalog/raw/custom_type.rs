//! Raw enum/composite/range type rows and their conversion into logical custom
//! types.
//!
//! The fetches keep the OIDs and attnums the converter resolves with, plus the
//! one server-side-function output that cannot be computed in Rust:
//! `format_type` for a composite attribute's rendered type. Everything else —
//! schema-name resolution, extension-ownership and system-schema exclusion,
//! attribute-type classification, dependency derivation, comment attachment —
//! happens in the converter, where the OIDs die.

use anyhow::{Context, Result};
use sqlx::postgres::PgConnection;
use sqlx::postgres::types::Oid;
use std::collections::BTreeMap;
use tracing::info;

use super::exclusion::{Converted, Excluded, ExclusionReason, SYSTEM_SCHEMAS};
use super::oid_index::OidIndex;
use super::shared::{SharedCatalog, class};
use crate::catalog::custom_type::{CompositeAttribute, CustomType, EnumValue, TypeKind};
use crate::catalog::id::DbObjectId;
use crate::catalog::utils::resolve_type_dependency;

/// One `pg_type` row of `typtype` enum, composite or range, before names are
/// resolved and OIDs are discarded.
#[derive(Debug, Clone)]
pub struct RawCustomType {
    pub oid: Oid,
    pub namespace: Oid,
    pub name: String,
    /// `pg_type.typtype`: 'e' enum, 'c' composite, 'r' range.
    pub typtype: String,
    /// `typrelid`, the `pg_class` entry backing a composite type, and `None` for
    /// every other kind. Attribute comments are addressed under this OID, not
    /// under the type's.
    pub relation_oid: Option<Oid>,
}

/// One `pg_enum` label of an enum type.
#[derive(Debug, Clone)]
pub struct RawEnumValue {
    pub type_oid: Oid,
    pub name: String,
    pub sort_order: f32,
}

/// One attribute of a composite type.
#[derive(Debug, Clone)]
pub struct RawCompositeAttribute {
    pub type_oid: Oid,
    pub attnum: i32,
    pub name: String,
    /// `atttypid`, unresolved: an array's own OID, not its element type's.
    pub attribute_type_oid: Oid,
    /// `format_type(atttypid, atttypmod)` — carries type modifiers and array
    /// brackets, which no Rust-side reconstruction can recover.
    pub formatted_type: String,
    pub attndims: i32,
}

/// Everything the custom-type converter reads out of `pg_catalog`.
#[derive(Debug, Clone, Default)]
pub struct RawCustomTypes {
    pub types: Vec<RawCustomType>,
    pub enum_values: Vec<RawEnumValue>,
    pub composite_attributes: Vec<RawCompositeAttribute>,
}

/// A converted type, still beside the OIDs the comment pass addresses it by.
#[derive(Debug, Clone)]
pub struct ConvertedCustomType {
    pub oid: Oid,
    /// The composite type's backing `pg_class` entry, which its attribute
    /// comments are keyed by.
    pub relation_oid: Option<Oid>,
    pub custom_type: CustomType,
    /// The attnum of each attribute of `custom_type`, positionally aligned.
    /// Attribute comments are addressed by attnum in `pg_description` but by
    /// name in the logical world; this is the correspondence, and it does not
    /// outlive the conversion.
    pub attribute_attnums: Vec<i32>,
}

/// Fetch every enum, composite and range type in the database, with their
/// labels and attributes, unresolved and unfiltered.
pub async fn fetch(conn: &mut PgConnection) -> Result<RawCustomTypes> {
    info!("Fetching types...");
    let types = fetch_types(&mut *conn).await?;
    info!("Fetching enum values...");
    let enum_values = fetch_enum_values(&mut *conn).await?;
    info!("Fetching composite type attributes...");
    let composite_attributes = fetch_composite_attributes(&mut *conn).await?;

    Ok(RawCustomTypes {
        types,
        enum_values,
        composite_attributes,
    })
}

/// Fetch types and convert them into the logical catalog, with the type's own
/// comment and its composite attributes' comments attached through the OID
/// index.
pub async fn load(conn: &mut PgConnection, shared: &SharedCatalog) -> Result<Vec<CustomType>> {
    Ok(load_with_exclusions(conn, shared).await?.objects)
}

/// The same load, keeping the named reason for every raw row that did not
/// become a type.
pub async fn load_with_exclusions(
    conn: &mut PgConnection,
    shared: &SharedCatalog,
) -> Result<Converted<CustomType>> {
    let raw = fetch(conn).await?;
    let mut converted = convert(&raw, shared)?;

    // Identity first, then the index, then the OID-addressed state: a comment
    // can only be attached to an object whose identity is already known. A
    // composite type is indexed under both its OIDs, because `pg_description`
    // keys the type's own comment by `pg_type` and its attributes' comments by
    // the backing `pg_class` entry.
    let mut index = OidIndex::new();
    for entry in &converted.objects {
        let id = entry.custom_type.id();
        index.insert(class::PG_TYPE, entry.oid, id.clone())?;
        if let Some(relation_oid) = entry.relation_oid {
            index.insert(class::PG_CLASS, relation_oid, id)?;
        }
    }

    let type_comments = index.object_comments(&shared.descriptions, class::PG_TYPE);
    let attribute_comments = index.subobject_comments(&shared.descriptions, class::PG_CLASS);

    for entry in &mut converted.objects {
        let id = entry.custom_type.id();
        entry.custom_type.comment = type_comments.get(&id).map(|text| text.to_string());

        if let Some(by_attnum) = attribute_comments.get(&id) {
            for (attribute, attnum) in entry
                .custom_type
                .composite_attributes
                .iter_mut()
                .zip(&entry.attribute_attnums)
            {
                attribute.comment = by_attnum.get(attnum).map(|text| text.to_string());
            }
        }
    }

    Ok(converted.map(|entry| entry.custom_type))
}

/// Resolve raw types into logical ones, keeping each type's OIDs (and its
/// attributes' attnums) beside it so OID-addressed state can still be attached
/// before the identities cross the firewall.
///
/// Types in a system schema and types owned by an extension are dropped here,
/// each recorded with its named reason, along with the labels and attributes
/// belonging to them.
pub fn convert(
    raw: &RawCustomTypes,
    shared: &SharedCatalog,
) -> Result<Converted<ConvertedCustomType>> {
    let namespaces = &shared.namespaces;

    // The types that survive filtering, by OID, so every label and attribute row
    // can be routed to its type (or dropped with it).
    let mut kept: BTreeMap<u32, usize> = BTreeMap::new();
    let mut converted: Converted<ConvertedCustomType> = Converted::new();

    for row in &raw.types {
        let schema = namespaces
            .name(row.namespace)
            .with_context(|| format!("type {} has no namespace entry", row.name))?;

        if SYSTEM_SCHEMAS.contains(&schema) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "type",
                schema,
                &row.name,
                ExclusionReason::SystemSchema,
            ));
            continue;
        }
        if let Some(extension) = shared.extensions.owner(class::PG_TYPE, row.oid) {
            converted.excluded.push(Excluded::new(
                row.oid,
                "type",
                schema,
                &row.name,
                ExclusionReason::ExtensionOwned {
                    extension: extension.to_string(),
                },
            ));
            continue;
        }

        kept.insert(row.oid.0, converted.objects.len());
        converted.objects.push(ConvertedCustomType {
            oid: row.oid,
            relation_oid: row.relation_oid,
            custom_type: CustomType {
                schema: schema.to_string(),
                name: row.name.clone(),
                kind: TypeKind::from_typtype(&row.typtype),
                enum_values: Vec::new(),
                composite_attributes: Vec::new(),
                comment: None,
                depends_on: vec![DbObjectId::Schema {
                    name: schema.to_string(),
                }],
            },
            attribute_attnums: Vec::new(),
        });
    }

    for row in &raw.enum_values {
        let Some(&idx) = kept.get(&row.type_oid.0) else {
            continue;
        };
        converted.objects[idx]
            .custom_type
            .enum_values
            .push(EnumValue {
                name: row.name.clone(),
                sort_order: row.sort_order,
            });
    }

    for row in &raw.composite_attributes {
        let Some(&idx) = kept.get(&row.type_oid.0) else {
            continue;
        };
        let entry = &mut converted.objects[idx];
        let resolved = shared.resolve_type(row.attribute_type_oid);

        // An attribute's type is depended on as a type: an extension-provided one
        // through its extension, a user-defined one directly, and a relation's
        // row type not distinguished from a standalone composite.
        if let Some(dep) = resolved.as_ref().and_then(|attr| {
            resolve_type_dependency(
                attr.schema,
                Some(attr.name),
                Some(attr.typtype),
                None,
                attr.extension.is_some(),
                attr.extension,
            )
        }) {
            entry.custom_type.depends_on.push(dep);
        }

        entry
            .custom_type
            .composite_attributes
            .push(CompositeAttribute {
                name: row.name.clone(),
                type_name: row.formatted_type.clone(),
                type_schema: resolved.as_ref().and_then(|t| t.schema.map(String::from)),
                raw_type_name: resolved.as_ref().map(|t| t.name.to_string()),
                attndims: row.attndims,
                comment: None,
            });
        entry.attribute_attnums.push(row.attnum);
    }

    // The raw fetches order by OID; ordering by name is what callers see.
    converted.objects.sort_by(|a, b| {
        (&a.custom_type.schema, &a.custom_type.name)
            .cmp(&(&b.custom_type.schema, &b.custom_type.name))
    });

    Ok(converted)
}

async fn fetch_types(conn: &mut PgConnection) -> Result<Vec<RawCustomType>> {
    // A relation's row type is not a type pgmt manages — it exists because the
    // table, view, materialized view or sequence does, and is created and
    // dropped with it.
    let rows = sqlx::query!(
        r#"
        SELECT
            t.oid AS "oid!",
            t.typnamespace AS "namespace!",
            t.typname AS "name!",
            t.typtype::text AS "typtype!",
            NULLIF(t.typrelid, 0::oid) AS "relation_oid?"
        FROM pg_type t
        WHERE t.typtype IN ('e', 'c', 'r')
          AND NOT EXISTS (
            SELECT 1 FROM pg_class c
            WHERE c.reltype = t.oid
              AND c.relkind IN ('r', 'v', 'm', 'S')
          )
        ORDER BY t.oid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawCustomType {
            oid: row.oid,
            namespace: row.namespace,
            name: row.name,
            typtype: row.typtype,
            relation_oid: row.relation_oid,
        })
        .collect())
}

async fn fetch_enum_values(conn: &mut PgConnection) -> Result<Vec<RawEnumValue>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            e.enumtypid AS "type_oid!",
            e.enumlabel AS "name!",
            e.enumsortorder AS "sort_order!"
        FROM pg_enum e
        ORDER BY e.enumtypid, e.enumsortorder
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawEnumValue {
            type_oid: row.type_oid,
            name: row.name,
            sort_order: row.sort_order,
        })
        .collect())
}

async fn fetch_composite_attributes(conn: &mut PgConnection) -> Result<Vec<RawCompositeAttribute>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            t.oid AS "type_oid!",
            a.attnum AS "attnum!",
            a.attname AS "name!",
            a.atttypid AS "attribute_type_oid!",
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS "formatted_type!",
            COALESCE(a.attndims, 0)::int AS "attndims!: i32"
        FROM pg_type t
        JOIN pg_class c ON t.typrelid = c.oid
        JOIN pg_attribute a ON a.attrelid = c.oid
        WHERE t.typtype = 'c'
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY t.oid, a.attnum
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RawCompositeAttribute {
            type_oid: row.type_oid,
            attnum: row.attnum as i32,
            name: row.name,
            attribute_type_oid: row.attribute_type_oid,
            formatted_type: row.formatted_type,
            attndims: row.attndims,
        })
        .collect())
}
