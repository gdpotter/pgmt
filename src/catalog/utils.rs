use crate::catalog::id::DbObjectId;

/// Helper to check if a schema name is a system schema.
/// Used for dependency tracking to avoid tracking dependencies on system objects.
pub fn is_system_schema(schema: &str) -> bool {
    matches!(schema, "pg_catalog" | "information_schema" | "pg_toast")
        || schema.starts_with("pg_temp_")
}

/// Builder for constructing dependency lists for database objects.
/// Provides consistent dependency tracking across all catalog object types.
pub struct DependencyBuilder {
    deps: Vec<DbObjectId>,
}

impl DependencyBuilder {
    /// Create a new dependency builder with the object's parent schema as the first dependency.
    /// All database objects depend on their containing schema.
    pub fn new(schema: String) -> Self {
        Self {
            deps: vec![DbObjectId::Schema { name: schema }],
        }
    }

    /// Add a custom type dependency if the type is not a system type.
    /// Used when objects reference user-defined types (ENUMs, DOMAINs, COMPOSITE).
    pub fn add_custom_type(&mut self, type_schema: Option<String>, type_name: Option<String>) {
        if let (Some(schema), Some(name)) = (type_schema, type_name)
            && !is_system_schema(&schema)
        {
            self.deps.push(DbObjectId::Type { schema, name });
        }
    }

    /// Build the final dependency list.
    pub fn build(self) -> Vec<DbObjectId> {
        self.deps
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_system_schema() {
        assert!(is_system_schema("pg_catalog"));
        assert!(is_system_schema("information_schema"));
        assert!(is_system_schema("pg_toast"));
        assert!(is_system_schema("pg_temp_1234"));

        assert!(!is_system_schema("public"));
        assert!(!is_system_schema("my_schema"));
        assert!(!is_system_schema("pg_something"));
    }

    #[test]
    fn test_dependency_builder() {
        let mut builder = DependencyBuilder::new("test_schema".to_string());
        builder.add_custom_type(
            Some("custom_schema".to_string()),
            Some("my_type".to_string()),
        );
        builder.add_custom_type(Some("pg_catalog".to_string()), Some("text".to_string())); // Should be ignored

        let deps = builder.build();
        assert_eq!(deps.len(), 2);
        assert_eq!(
            deps[0],
            DbObjectId::Schema {
                name: "test_schema".to_string()
            }
        );
        assert_eq!(
            deps[1],
            DbObjectId::Type {
                schema: "custom_schema".to_string(),
                name: "my_type".to_string()
            }
        );
    }
}
