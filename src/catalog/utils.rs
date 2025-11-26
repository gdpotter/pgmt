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
    /// Note: Prefer `add_type_or_extension()` when extension type detection is available.
    #[allow(dead_code)] // Used in tests; kept as a simpler API when extension detection isn't needed
    pub fn add_custom_type(&mut self, type_schema: Option<String>, type_name: Option<String>) {
        if let (Some(schema), Some(name)) = (type_schema, type_name)
            && !is_system_schema(&schema)
        {
            self.deps.push(DbObjectId::Type { schema, name });
        }
    }

    /// Add a type or extension dependency based on whether the type is extension-provided.
    ///
    /// This is the single place where the "extension type vs custom type" decision is made.
    /// Catalog files should query `pg_depend` to get `is_extension` and `extension_name`,
    /// then call this method with the results.
    ///
    /// **Important**: For array types, the `type_name` parameter should be the element type name,
    /// NOT the array type name (e.g., pass "priority" not "_priority"). Callers should resolve
    /// this using `pg_type.typelem` in their SQL queries with a pattern like:
    /// ```sql
    /// CASE WHEN t.typelem != 0 THEN elem_t.typname ELSE t.typname END AS "type_name?"
    /// ```
    pub fn add_type_or_extension(
        &mut self,
        type_schema: Option<String>,
        type_name: Option<String>,
        is_extension: bool,
        extension_name: Option<String>,
    ) {
        if is_extension {
            if let Some(ext_name) = extension_name {
                self.deps.push(DbObjectId::Extension { name: ext_name });
            }
        } else if let (Some(schema), Some(name)) = (type_schema, type_name)
            && !is_system_schema(&schema)
        {
            // Type name should already be resolved to element type for arrays via SQL
            self.deps.push(DbObjectId::Type { schema, name });
        }
    }

    /// Add a type dependency with proper distinction between domains and other custom types.
    ///
    /// This method uses `typtype` from pg_type to correctly categorize:
    /// - 'd' = domain → `DbObjectId::Domain`
    /// - 'e' = enum, 'c' = composite, 'r' = range → `DbObjectId::Type`
    ///
    /// Use this method when you have access to `typtype` information (e.g., for function
    /// parameters and return types). Falls back to `DbObjectId::Type` if typtype is unknown.
    pub fn add_type_dependency(
        &mut self,
        type_schema: Option<String>,
        type_name: Option<String>,
        typtype: Option<String>,
        is_extension: bool,
        extension_name: Option<String>,
    ) {
        if is_extension {
            if let Some(ext_name) = extension_name {
                self.deps.push(DbObjectId::Extension { name: ext_name });
            }
        } else if let (Some(schema), Some(name)) = (type_schema, type_name)
            && !is_system_schema(&schema)
        {
            if typtype.as_deref() == Some("d") {
                self.deps.push(DbObjectId::Domain { schema, name });
            } else {
                self.deps.push(DbObjectId::Type { schema, name });
            }
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

    #[test]
    fn test_add_type_or_extension_with_extension() {
        let mut builder = DependencyBuilder::new("test_schema".to_string());
        builder.add_type_or_extension(
            Some("public".to_string()),
            Some("citext".to_string()),
            true, // is_extension
            Some("citext".to_string()),
        );

        let deps = builder.build();
        assert_eq!(deps.len(), 2);
        assert_eq!(
            deps[1],
            DbObjectId::Extension {
                name: "citext".to_string()
            }
        );
    }

    #[test]
    fn test_add_type_or_extension_with_custom_type() {
        let mut builder = DependencyBuilder::new("test_schema".to_string());
        builder.add_type_or_extension(
            Some("app".to_string()),
            Some("priority".to_string()),
            false, // not an extension
            None,
        );

        let deps = builder.build();
        assert_eq!(deps.len(), 2);
        assert_eq!(
            deps[1],
            DbObjectId::Type {
                schema: "app".to_string(),
                name: "priority".to_string()
            }
        );
    }

    #[test]
    fn test_add_type_or_extension_preserves_type_name() {
        // Type name should be pre-resolved to element type via SQL queries
        // (using pg_type.typelem), so we pass the element type name directly
        let mut builder = DependencyBuilder::new("test_schema".to_string());
        builder.add_type_or_extension(
            Some("app".to_string()),
            Some("priority".to_string()), // Element type name (already resolved)
            false,
            None,
        );

        let deps = builder.build();
        assert_eq!(deps.len(), 2);
        assert_eq!(
            deps[1],
            DbObjectId::Type {
                schema: "app".to_string(),
                name: "priority".to_string()
            }
        );
    }

    #[test]
    fn test_add_type_or_extension_handles_underscore_prefixed_types() {
        // A custom type that legitimately starts with underscore should be preserved
        let mut builder = DependencyBuilder::new("test_schema".to_string());
        builder.add_type_or_extension(
            Some("app".to_string()),
            Some("_internal_status".to_string()), // Type legitimately named with underscore
            false,
            None,
        );

        let deps = builder.build();
        assert_eq!(deps.len(), 2);
        assert_eq!(
            deps[1],
            DbObjectId::Type {
                schema: "app".to_string(),
                name: "_internal_status".to_string() // Preserved correctly
            }
        );
    }

    #[test]
    fn test_add_type_dependency_with_domain() {
        let mut builder = DependencyBuilder::new("test_schema".to_string());
        builder.add_type_dependency(
            Some("app".to_string()),
            Some("positive_int".to_string()),
            Some("d".to_string()), // domain
            false,
            None,
        );

        let deps = builder.build();
        assert_eq!(deps.len(), 2);
        assert_eq!(
            deps[1],
            DbObjectId::Domain {
                schema: "app".to_string(),
                name: "positive_int".to_string()
            }
        );
    }

    #[test]
    fn test_add_type_dependency_with_enum() {
        let mut builder = DependencyBuilder::new("test_schema".to_string());
        builder.add_type_dependency(
            Some("app".to_string()),
            Some("status".to_string()),
            Some("e".to_string()), // enum
            false,
            None,
        );

        let deps = builder.build();
        assert_eq!(deps.len(), 2);
        assert_eq!(
            deps[1],
            DbObjectId::Type {
                schema: "app".to_string(),
                name: "status".to_string()
            }
        );
    }

    #[test]
    fn test_add_type_dependency_with_extension() {
        let mut builder = DependencyBuilder::new("test_schema".to_string());
        builder.add_type_dependency(
            Some("public".to_string()),
            Some("citext".to_string()),
            Some("d".to_string()), // typtype doesn't matter when is_extension=true
            true,
            Some("citext".to_string()),
        );

        let deps = builder.build();
        assert_eq!(deps.len(), 2);
        assert_eq!(
            deps[1],
            DbObjectId::Extension {
                name: "citext".to_string()
            }
        );
    }
}
