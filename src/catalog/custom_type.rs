//! src/catalog/custom_type
//! Fetch custom PostgreSQL types via pg_catalog
use anyhow::Result;
use sqlx::PgPool;

use super::comments::Commentable;
use super::id::{DbObjectId, DependsOn};
use super::utils::{DependencyBuilder, is_system_schema};

/* ---------- Data structures ---------- */

#[derive(Hash, PartialEq, Debug, Clone)]
pub enum TypeKind {
    Enum,
    Composite,
    Domain,
    Range,
    Other(String),
}

impl TypeKind {
    fn from_typtype(typtype: &str) -> Self {
        match typtype {
            "e" => TypeKind::Enum,
            "c" => TypeKind::Composite,
            "d" => TypeKind::Domain,
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
    pub attndims: i32,
}

#[derive(Debug, Clone)]
pub struct CustomType {
    pub schema: String,
    pub name: String,
    pub kind: TypeKind,
    pub enum_values: Vec<EnumValue>,
    pub composite_attributes: Vec<CompositeAttribute>,
    pub base_type: Option<String>, // For domains, the underlying type
    pub comment: Option<String>,   // comment on the type
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

pub async fn fetch(pool: &PgPool) -> Result<Vec<CustomType>> {
    // 1. Fetch basic type information from pg_type
    let type_rows = sqlx::query!(
        r#"
        SELECT
            n.nspname AS "schema!",
            t.typname AS "name!",
            t.typtype::text AS "typtype!",
            t.typbasetype AS "typbasetype",
            bt.typname AS "base_type_name?",
            btn.nspname AS "base_type_schema?",
            d.description AS "comment?"
        FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        LEFT JOIN pg_type bt ON t.typbasetype = bt.oid
        LEFT JOIN pg_namespace btn ON bt.typnamespace = btn.oid
        LEFT JOIN pg_description d ON d.objoid = t.oid AND d.objsubid = 0
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND t.typtype IN ('e', 'c', 'd', 'r')  -- enum, composite, domain, range
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
    .fetch_all(pool)
    .await?;

    // 2. Fetch enum values for enum types
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
    .fetch_all(pool)
    .await?;

    // 3. Fetch composite type attributes with raw type information for dependency analysis
    let composite_attrs = sqlx::query!(
        r#"
        SELECT
            n.nspname AS "schema!",
            t.typname AS "type_name!",
            a.attname AS "attr_name!",
            format_type(a.atttypid, a.atttypmod) AS "attr_type!",
            a.attnum AS "ordinal_position!",
            tn.nspname AS "attr_type_schema?",
            attr_t.typname AS "attr_type_name?",
            COALESCE(a.attndims, 0)::int AS "attr_attndims!: i32"
        FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        JOIN pg_class c ON t.typrelid = c.oid
        JOIN pg_attribute a ON c.oid = a.attrelid
        LEFT JOIN pg_type attr_t ON a.atttypid = attr_t.oid
        LEFT JOIN pg_namespace tn ON attr_t.typnamespace = tn.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND t.typtype = 'c'  -- composite
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY n.nspname, t.typname, a.attnum
        "#
    )
    .fetch_all(pool)
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

    // 5. Organize composite attributes by type
    let mut composite_attrs_by_type = std::collections::HashMap::new();
    for attr in composite_attrs {
        composite_attrs_by_type
            .entry((attr.schema.clone(), attr.type_name.clone()))
            .or_insert_with(Vec::new)
            .push(CompositeAttribute {
                name: attr.attr_name,
                type_name: attr.attr_type,
                type_schema: attr.attr_type_schema,
                raw_type_name: attr.attr_type_name,
                attndims: attr.attr_attndims,
            });
    }

    // 6. Build CustomType objects
    let mut custom_types = Vec::new();
    for row in type_rows {
        let enum_values = enum_values_by_type
            .remove(&(row.schema.clone(), row.name.clone()))
            .unwrap_or_default();

        let composite_attributes = composite_attrs_by_type
            .remove(&(row.schema.clone(), row.name.clone()))
            .unwrap_or_default();

        // Build dependencies using DependencyBuilder
        let mut builder = DependencyBuilder::new(row.schema.clone());

        // Add dependency for domain types (but exclude built-in types)
        if row.typtype == "d"
            && let (Some(base_schema), Some(base_name)) =
                (&row.base_type_schema, &row.base_type_name)
            && !is_system_schema(base_schema)
        {
            builder.add_custom_type(Some(base_schema.clone()), Some(base_name.clone()));
        }

        // Add dependencies for composite types - analyze each attribute
        if row.typtype == "c" {
            for attr in &composite_attributes {
                if let (Some(type_schema), Some(raw_type_name)) =
                    (&attr.type_schema, &attr.raw_type_name)
                    && !is_system_schema(type_schema)
                {
                    // Handle array types: PostgreSQL array types have names prefixed with '_'
                    // For example, 'custom_type[]' is stored as '_custom_type' in pg_type.typname
                    let base_type_name = if attr.attndims > 0 {
                        raw_type_name.trim_start_matches('_').to_string()
                    } else {
                        raw_type_name.clone()
                    };
                    builder.add_custom_type(Some(type_schema.clone()), Some(base_type_name));
                }
            }
        }

        let depends_on = builder.build();

        // Create base_type string for domains
        let base_type = if row.typtype == "d" && row.base_type_name.is_some() {
            let schema_ref = row.base_type_schema.as_deref().unwrap_or("");
            let name_ref = row.base_type_name.as_ref().unwrap();
            if is_system_schema(schema_ref) {
                // Standard types don't need schema qualification
                Some(name_ref.clone())
            } else {
                Some(format!("{}.{}", schema_ref, name_ref))
            }
        } else {
            None
        };

        custom_types.push(CustomType {
            schema: row.schema,
            name: row.name,
            kind: TypeKind::from_typtype(&row.typtype),
            enum_values,
            composite_attributes,
            base_type,
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
            base_type: None,
            comment: None,
            depends_on: vec![],
        }
    }

    fn make_domain_type(schema: &str, name: &str, base_type: &str) -> CustomType {
        CustomType {
            schema: schema.to_string(),
            name: name.to_string(),
            kind: TypeKind::Domain,
            enum_values: vec![],
            composite_attributes: vec![],
            base_type: Some(base_type.to_string()),
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
            base_type: None,
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
    fn test_create_domain_type() {
        let new_type = make_domain_type("public", "email", "text");

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
                assert_eq!(name, "email");
                assert_eq!(kind, "DOMAIN");
                assert_eq!(definition, "AS text");
            }
            _ => panic!("Expected CreateType step"),
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
