//! The logical custom type: what an enum, composite or range type is once
//! names are resolved and OIDs are gone.
//!
//! Types are read through `catalog::raw::custom_type`, which fetches the
//! OID-keyed rows and converts them into these structs.

use super::id::{DbObjectId, DependsOn};

/* ---------- Data structures ---------- */

#[derive(Hash, PartialEq, Debug, Clone)]
pub enum TypeKind {
    Enum,
    Composite,
    Range,
    Other(String),
}

impl TypeKind {
    pub(crate) fn from_typtype(typtype: &str) -> Self {
        match typtype {
            "e" => TypeKind::Enum,
            "c" => TypeKind::Composite,
            "r" => TypeKind::Range,
            other => TypeKind::Other(other.to_string()),
        }
    }
}

/// One label of an enum type. The labels' order is the `Vec`'s order; the
/// `pg_enum.enumsortorder` float that produced it is physical — two databases
/// reach the same label order through different floats once a label has been
/// added with `ADD VALUE BEFORE` — and dies in the converter.
#[derive(Debug, Clone)]
pub struct EnumValue {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct CompositeAttribute {
    pub name: String,
    /// The attribute's type as the server renders it, with modifiers and array
    /// brackets. This is what the CREATE statement emits.
    pub type_name: String,
    pub comment: Option<String>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::custom_types::diff;
    use crate::diff::operations::{MigrationStep, TypeOperation};

    fn make_enum_type(schema: &str, name: &str, values: Vec<&str>) -> CustomType {
        let enum_values = values
            .into_iter()
            .map(|name| EnumValue {
                name: name.to_string(),
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
                comment: None,
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
