//! src/catalog/custom_type
//! Fetch custom PostgreSQL types via pg_catalog
use anyhow::Result;
use sqlx::postgres::PgConnection;
use tracing::info;

use super::comments::Commentable;
use super::id::{DbObjectId, DependsOn};
use super::utils::DependencyBuilder;

/* ---------- Data structures ---------- */

#[derive(Hash, PartialEq, Debug, Clone)]
pub enum TypeKind {
    Enum,
    Composite,
    Range,
    Other(String),
}

impl TypeKind {
    fn from_typtype(typtype: &str) -> Self {
        match typtype {
            "e" => TypeKind::Enum,
            "c" => TypeKind::Composite,
            "r" => TypeKind::Range,
            other => TypeKind::Other(other.to_string()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EnumValue {
    pub name: String,
    pub sort_order: f32,
}

#[derive(Debug, Clone)]
pub struct CompositeAttribute {
    pub name: String,
    pub type_name: String,
    pub type_schema: Option<String>,
    pub raw_type_name: Option<String>,
    #[allow(dead_code)] // Used in SQL query but not in rendering; kept for potential future use
    pub attndims: i32,
}

#[derive(Debug, Clone)]
pub struct CustomType {
    pub schema: String,
    pub name: String,
    pub kind: TypeKind,
    pub enum_values: Vec<EnumValue>,
    pub composite_attributes: Vec<CompositeAttribute>,
    pub comment: Option<String>,
    pub depends_on: Vec<DbObjectId>,
}

impl CustomType {
    pub fn id(&self) -> DbObjectId {
        DbObjectId::Type {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }
}

impl DependsOn for CustomType {
    fn id(&self) -> DbObjectId {
        DbObjectId::Type {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}

impl Commentable for CustomType {
    fn comment(&self) -> &Option<String> {
        &self.comment
    }
}

/* ---------- Fetch query (catalog-based) ---------- */

pub async fn fetch(conn: &mut PgConnection) -> Result<Vec<CustomType>> {
    // 1. Fetch basic type information from pg_type (excluding domains, which are handled separately)
    info!("Fetching types...");
    let type_rows = sqlx::query!(
        r#"
        SELECT
            n.nspname AS "schema!",
            t.typname AS "name!",
            t.typtype::text AS "typtype!",
            d.description AS "comment?"
        FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        LEFT JOIN pg_description d ON d.objoid = t.oid AND d.objsubid = 0
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND t.typtype IN ('e', 'c', 'r')  -- enum, composite, range (domains handled separately)
          AND NOT EXISTS (
            SELECT 1 FROM pg_class c
            WHERE c.reltype = t.oid
              AND c.relkind IN ('r', 'v', 'm', 'S')  -- exclude table, view, materialized view, and sequence row types
          )  -- exclude table, view, materialized view, and sequence row types
          -- Exclude types that belong to extensions
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

    // 2. Fetch enum values for enum types
    info!("Fetching enum values...");
    let enum_values = sqlx::query!(
        r#"
        SELECT
            n.nspname AS "schema!",
            t.typname AS "type_name!",
            e.enumlabel AS "enum_value!",
            e.enumsortorder AS "sort_order!"
        FROM pg_enum e
        JOIN pg_type t ON e.enumtypid = t.oid
        JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        ORDER BY n.nspname, t.typname, e.enumsortorder
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    // 3. Fetch composite type attributes with raw type information for dependency analysis
    info!("Fetching composite type attributes...");
    let composite_attrs = sqlx::query!(
        r#"
        SELECT
            n.nspname AS "schema!",
            t.typname AS "type_name!",
            a.attname AS "attr_name!",
            format_type(a.atttypid, a.atttypmod) AS "attr_type!",
            a.attnum AS "ordinal_position!",
            -- Resolve array element type schema/name correctly
            CASE
                WHEN attr_t.typelem != 0 THEN elem_tn.nspname
                ELSE tn.nspname
            END AS "attr_type_schema?",
            CASE
                WHEN attr_t.typelem != 0 THEN elem_t.typname
                ELSE attr_t.typname
            END AS "attr_type_name?",
            COALESCE(a.attndims, 0)::int AS "attr_attndims!: i32",
            -- Check if attribute type (or element type for arrays) is from an extension
            ext_types.extname IS NOT NULL AS "is_extension_type!: bool",
            ext_types.extname AS "extension_name?",
            -- Get typtype to distinguish domains ('d') from other types
            CASE
                WHEN attr_t.typelem != 0 THEN elem_t.typtype::text
                ELSE attr_t.typtype::text
            END AS "attr_type_typtype?"
        FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        JOIN pg_class c ON t.typrelid = c.oid
        JOIN pg_attribute a ON c.oid = a.attrelid
        LEFT JOIN pg_type attr_t ON a.atttypid = attr_t.oid
        LEFT JOIN pg_namespace tn ON attr_t.typnamespace = tn.oid
        -- Element type for array attributes
        LEFT JOIN pg_type elem_t ON attr_t.typelem = elem_t.oid AND attr_t.typelem != 0
        LEFT JOIN pg_namespace elem_tn ON elem_t.typnamespace = elem_tn.oid
        -- Extension type lookup: compute once as derived table, then hash join
        LEFT JOIN (
            SELECT DISTINCT dep.objid AS type_oid, e.extname
            FROM pg_depend dep
            JOIN pg_extension e ON dep.refobjid = e.oid
            WHERE dep.deptype = 'e'
        ) ext_types ON ext_types.type_oid = COALESCE(NULLIF(attr_t.typelem, 0::oid), attr_t.oid)
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND t.typtype = 'c'  -- composite
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY n.nspname, t.typname, a.attnum
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    // 4. Organize enum values by type
    let mut enum_values_by_type = std::collections::HashMap::new();
    for ev in enum_values {
        enum_values_by_type
            .entry((ev.schema.clone(), ev.type_name.clone()))
            .or_insert_with(Vec::new)
            .push(EnumValue {
                name: ev.enum_value,
                sort_order: ev.sort_order,
            });
    }

    // 5. Organize composite attributes by type (with extension info for dependency building)
    // Tuple: (CompositeAttribute, is_extension_type, extension_name, typtype)
    type AttrWithExtInfo = (CompositeAttribute, bool, Option<String>, Option<String>);
    let mut composite_attrs_by_type: std::collections::HashMap<
        (String, String),
        Vec<AttrWithExtInfo>,
    > = std::collections::HashMap::new();
    for attr in composite_attrs {
        composite_attrs_by_type
            .entry((attr.schema.clone(), attr.type_name.clone()))
            .or_default()
            .push((
                CompositeAttribute {
                    name: attr.attr_name,
                    type_name: attr.attr_type,
                    type_schema: attr.attr_type_schema,
                    raw_type_name: attr.attr_type_name,
                    attndims: attr.attr_attndims,
                },
                attr.is_extension_type,
                attr.extension_name,
                attr.attr_type_typtype,
            ));
    }

    // 6. Build CustomType objects
    let mut custom_types = Vec::new();
    for row in type_rows {
        let enum_values = enum_values_by_type
            .remove(&(row.schema.clone(), row.name.clone()))
            .unwrap_or_default();

        let attrs_with_ext_info = composite_attrs_by_type
            .remove(&(row.schema.clone(), row.name.clone()))
            .unwrap_or_default();

        // Separate attributes from extension info
        let composite_attributes: Vec<CompositeAttribute> = attrs_with_ext_info
            .iter()
            .map(|(attr, _, _, _)| attr.clone())
            .collect();

        // Build dependencies using DependencyBuilder
        let mut builder = DependencyBuilder::new(row.schema.clone());

        // Add dependencies for composite types - analyze each attribute
        if row.typtype == "c" {
            for (attr, is_extension, extension_name, typtype) in &attrs_with_ext_info {
                builder.add_type_dependency(
                    attr.type_schema.clone(),
                    attr.raw_type_name.clone(),
                    typtype.clone(),
                    *is_extension,
                    extension_name.clone(),
                );
            }
        }

        let depends_on = builder.build();

        custom_types.push(CustomType {
            schema: row.schema,
            name: row.name,
            kind: TypeKind::from_typtype(&row.typtype),
            enum_values,
            composite_attributes,
            comment: row.comment,
            depends_on,
        });
    }

    Ok(custom_types)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::custom_types::diff;
    use crate::diff::operations::{MigrationStep, TypeOperation};

    fn make_enum_type(schema: &str, name: &str, values: Vec<&str>) -> CustomType {
        let enum_values = values
            .into_iter()
            .enumerate()
            .map(|(i, name)| EnumValue {
                name: name.to_string(),
                sort_order: i as f32,
            })
            .collect();

        CustomType {
            schema: schema.to_string(),
            name: name.to_string(),
            kind: TypeKind::Enum,
            enum_values,
            composite_attributes: vec![],
            comment: None,
            depends_on: vec![],
        }
    }

    fn make_composite_type(schema: &str, name: &str, attributes: Vec<(&str, &str)>) -> CustomType {
        let composite_attributes = attributes
            .into_iter()
            .map(|(name, type_name)| CompositeAttribute {
                name: name.to_string(),
                type_name: type_name.to_string(),
                type_schema: None,
                raw_type_name: None,
                attndims: 0,
            })
            .collect();

        CustomType {
            schema: schema.to_string(),
            name: name.to_string(),
            kind: TypeKind::Composite,
            enum_values: vec![],
            composite_attributes,
            comment: None,
            depends_on: vec![],
        }
    }

    #[test]
    fn test_create_enum_type() {
        let new_type = make_enum_type("public", "status", vec!["active", "inactive", "pending"]);

        let steps = diff(None, Some(&new_type));

        assert_eq!(steps.len(), 1);
        match &steps[0] {
            MigrationStep::Type(TypeOperation::Create {
                schema,
                name,
                kind,
                definition,
            }) => {
                assert_eq!(schema, "public");
                assert_eq!(name, "status");
                assert_eq!(kind, "ENUM");
                assert!(definition.contains("'active'"));
                assert!(definition.contains("'inactive'"));
                assert!(definition.contains("'pending'"));
            }
            _ => panic!("Expected CreateType step"),
        }
    }

    #[test]
    fn test_drop_enum_type() {
        let old_type = make_enum_type("public", "status", vec!["active", "inactive"]);

        let steps = diff(Some(&old_type), None);

        assert_eq!(steps.len(), 1);
        match &steps[0] {
            MigrationStep::Type(TypeOperation::Drop { schema, name }) => {
                assert_eq!(schema, "public");
                assert_eq!(name, "status");
            }
            _ => panic!("Expected DropType step"),
        }
    }

    #[test]
    fn test_add_enum_value() {
        let old_type = make_enum_type("public", "status", vec!["active", "inactive"]);
        let new_type = make_enum_type("public", "status", vec!["active", "inactive", "pending"]);

        let steps = diff(Some(&old_type), Some(&new_type));

        assert_eq!(steps.len(), 1);
        match &steps[0] {
            MigrationStep::Type(TypeOperation::Alter {
                schema,
                name,
                action,
                definition,
            }) => {
                assert_eq!(schema, "public");
                assert_eq!(name, "status");
                assert_eq!(action, "ADD VALUE");
                // The definition should include the added value and AFTER clause
                assert!(definition.contains("'pending'"));
                assert!(definition.contains("AFTER"));
            }
            _ => panic!("Expected AlterType step"),
        }
    }

    #[test]
    fn test_add_multiple_enum_values() {
        let old_type = make_enum_type("public", "status", vec!["active"]);
        let new_type = make_enum_type("public", "status", vec!["active", "inactive", "pending"]);

        let steps = diff(Some(&old_type), Some(&new_type));

        // PostgreSQL requires separate ALTER TYPE statements for each new value
        assert_eq!(steps.len(), 2);

        // Check the first ALTER TYPE statement (adds 'inactive')
        match &steps[0] {
            MigrationStep::Type(TypeOperation::Alter {
                schema,
                name,
                action,
                definition,
            }) => {
                assert_eq!(schema, "public");
                assert_eq!(name, "status");
                assert_eq!(action, "ADD VALUE");
                assert!(definition.contains("'inactive'"));
                assert!(definition.contains("AFTER 'active'"));
            }
            _ => panic!("Expected AlterType step"),
        }

        // Check the second ALTER TYPE statement (adds 'pending')
        match &steps[1] {
            MigrationStep::Type(TypeOperation::Alter {
                schema,
                name,
                action,
                definition,
            }) => {
                assert_eq!(schema, "public");
                assert_eq!(name, "status");
                assert_eq!(action, "ADD VALUE");
                assert!(definition.contains("'pending'"));
                assert!(definition.contains("AFTER 'inactive'"));
            }
            _ => panic!("Expected AlterType step"),
        }
    }

    #[test]
    fn test_create_composite_type() {
        let new_type = make_composite_type(
            "public",
            "address",
            vec![("street", "text"), ("city", "text"), ("zip", "varchar(10)")],
        );

        let steps = diff(None, Some(&new_type));

        assert_eq!(steps.len(), 1);
        match &steps[0] {
            MigrationStep::Type(TypeOperation::Create {
                schema,
                name,
                kind,
                definition,
            }) => {
                assert_eq!(schema, "public");
                assert_eq!(name, "address");
                assert_eq!(kind, "COMPOSITE");
                assert!(definition.contains("street text"));
                assert!(definition.contains("city text"));
                assert!(definition.contains("zip varchar(10)"));
            }
            _ => panic!("Expected CreateType step"),
        }
    }
}
