use crate::catalog::id::DbObjectId;

/// Helper to check if a schema name is a system schema.
/// Used for dependency tracking to avoid tracking dependencies on system objects.
pub fn is_system_schema(schema: &str) -> bool {
    matches!(schema, "pg_catalog" | "information_schema" | "pg_toast")
        || schema.starts_with("pg_temp_")
}

/// Resolve type metadata into the appropriate DbObjectId.
///
/// This is the canonical function for converting PostgreSQL type information
/// (from pg_type, pg_class, pg_depend) into a dependency identifier. It handles:
/// - Extension types → `DbObjectId::Extension`
/// - Domains (typtype='d') → `DbObjectId::Domain`
/// - Composite types from tables (typtype='c', relkind='r'/'p') → `DbObjectId::Table`
/// - Composite types from views (typtype='c', relkind='v'/'m') → `DbObjectId::View`
/// - Explicit composite types (typtype='c', no relkind) → `DbObjectId::Type`
/// - Enums, ranges, and other types → `DbObjectId::Type`
///
/// Returns `None` for system types (pg_catalog, information_schema, etc.).
///
/// **Important**: For array types, callers should pass the element type (resolved via
/// `pg_type.typelem`) rather than the array type itself.
pub fn resolve_type_dependency(
    type_schema: Option<&str>,
    type_name: Option<&str>,
    typtype: Option<&str>,
    relkind: Option<&str>,
    is_extension: bool,
    extension_name: Option<&str>,
) -> Option<DbObjectId> {
    // Extension types depend on the extension, not the type
    if is_extension {
        return extension_name.map(|n| DbObjectId::Extension {
            name: n.to_string(),
        });
    }

    // Get schema and name, filtering out system schemas
    let (schema, name) = match (type_schema, type_name) {
        (Some(s), Some(n)) if !is_system_schema(s) => (s.to_string(), n.to_string()),
        _ => return None,
    };

    Some(match typtype {
        Some("d") => DbObjectId::Domain { schema, name },
        Some("c") => {
            // Composite type - check if from table/view or explicit CREATE TYPE
            match relkind {
                Some("r") | Some("p") => DbObjectId::Table { schema, name },
                Some("v") | Some("m") => DbObjectId::View { schema, name },
                _ => DbObjectId::Type { schema, name },
            }
        }
        _ => DbObjectId::Type { schema, name },
    })
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
    /// Note: Prefer `add_type_dependency()` when extension and typtype information is available.
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
    ///
    /// Note: Prefer `add_type_dependency()` when you have access to `typtype` information,
    /// as it correctly distinguishes domains from other custom types.
    #[allow(dead_code)] // Used in tests; kept as a simpler API when typtype isn't available
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

    /// Add a type dependency with proper distinction between domains, tables, views, and custom types.
    ///
    /// This method uses `typtype` and `relkind` from pg_type/pg_class to correctly categorize:
    /// - 'd' = domain → `DbObjectId::Domain`
    /// - 'c' + relkind 'r'/'p' = table composite type → `DbObjectId::Table`
    /// - 'c' + relkind 'v'/'m' = view composite type → `DbObjectId::View`
    /// - 'c' + no relkind = explicit composite → `DbObjectId::Type`
    /// - 'e' = enum, 'r' = range, etc. → `DbObjectId::Type`
    ///
    /// Use this method when you have access to `typtype` information (e.g., for function
    /// parameters and return types). Falls back to `DbObjectId::Type` if typtype is unknown.
    pub fn add_type_dependency(
        &mut self,
        type_schema: Option<String>,
        type_name: Option<String>,
        typtype: Option<String>,
        relkind: Option<String>,
        is_extension: bool,
        extension_name: Option<String>,
    ) {
        if let Some(dep) = resolve_type_dependency(
            type_schema.as_deref(),
            type_name.as_deref(),
            typtype.as_deref(),
            relkind.as_deref(),
            is_extension,
            extension_name.as_deref(),
        ) {
            self.deps.push(dep);
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
            None,                  // relkind doesn't matter for domains
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
            None,                  // no relkind for enum types
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
            None,                  // relkind doesn't matter for extensions
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

    #[test]
    fn test_add_type_dependency_with_table_composite() {
        let mut builder = DependencyBuilder::new("test_schema".to_string());
        builder.add_type_dependency(
            Some("app".to_string()),
            Some("policies".to_string()),
            Some("c".to_string()), // composite
            Some("r".to_string()), // table
            false,
            None,
        );

        let deps = builder.build();
        assert_eq!(deps.len(), 2);
        assert_eq!(
            deps[1],
            DbObjectId::Table {
                schema: "app".to_string(),
                name: "policies".to_string()
            }
        );
    }

    #[test]
    fn test_add_type_dependency_with_view_composite() {
        let mut builder = DependencyBuilder::new("test_schema".to_string());
        builder.add_type_dependency(
            Some("app".to_string()),
            Some("policy_view".to_string()),
            Some("c".to_string()), // composite
            Some("v".to_string()), // view
            false,
            None,
        );

        let deps = builder.build();
        assert_eq!(deps.len(), 2);
        assert_eq!(
            deps[1],
            DbObjectId::View {
                schema: "app".to_string(),
                name: "policy_view".to_string()
            }
        );
    }

    #[test]
    fn test_add_type_dependency_with_explicit_composite() {
        let mut builder = DependencyBuilder::new("test_schema".to_string());
        builder.add_type_dependency(
            Some("app".to_string()),
            Some("address".to_string()),
            Some("c".to_string()), // composite
            None,                  // no relkind = explicit CREATE TYPE ... AS
            false,
            None,
        );

        let deps = builder.build();
        assert_eq!(deps.len(), 2);
        assert_eq!(
            deps[1],
            DbObjectId::Type {
                schema: "app".to_string(),
                name: "address".to_string()
            }
        );
    }

    // Tests for standalone resolve_type_dependency() function

    #[test]
    fn test_resolve_type_dependency_extension() {
        let result = resolve_type_dependency(
            Some("public"),
            Some("citext"),
            Some("d"), // typtype is ignored for extensions
            None,
            true,
            Some("citext"),
        );
        assert_eq!(
            result,
            Some(DbObjectId::Extension {
                name: "citext".to_string()
            })
        );
    }

    #[test]
    fn test_resolve_type_dependency_domain() {
        let result = resolve_type_dependency(
            Some("app"),
            Some("positive_int"),
            Some("d"),
            None,
            false,
            None,
        );
        assert_eq!(
            result,
            Some(DbObjectId::Domain {
                schema: "app".to_string(),
                name: "positive_int".to_string()
            })
        );
    }

    #[test]
    fn test_resolve_type_dependency_table_composite() {
        let result = resolve_type_dependency(
            Some("app"),
            Some("orders"),
            Some("c"),
            Some("r"), // regular table
            false,
            None,
        );
        assert_eq!(
            result,
            Some(DbObjectId::Table {
                schema: "app".to_string(),
                name: "orders".to_string()
            })
        );
    }

    #[test]
    fn test_resolve_type_dependency_partitioned_table_composite() {
        let result = resolve_type_dependency(
            Some("app"),
            Some("events"),
            Some("c"),
            Some("p"), // partitioned table
            false,
            None,
        );
        assert_eq!(
            result,
            Some(DbObjectId::Table {
                schema: "app".to_string(),
                name: "events".to_string()
            })
        );
    }

    #[test]
    fn test_resolve_type_dependency_view_composite() {
        let result = resolve_type_dependency(
            Some("app"),
            Some("order_summary"),
            Some("c"),
            Some("v"), // view
            false,
            None,
        );
        assert_eq!(
            result,
            Some(DbObjectId::View {
                schema: "app".to_string(),
                name: "order_summary".to_string()
            })
        );
    }

    #[test]
    fn test_resolve_type_dependency_materialized_view_composite() {
        let result = resolve_type_dependency(
            Some("app"),
            Some("cached_stats"),
            Some("c"),
            Some("m"), // materialized view
            false,
            None,
        );
        assert_eq!(
            result,
            Some(DbObjectId::View {
                schema: "app".to_string(),
                name: "cached_stats".to_string()
            })
        );
    }

    #[test]
    fn test_resolve_type_dependency_explicit_composite() {
        let result = resolve_type_dependency(
            Some("app"),
            Some("address"),
            Some("c"),
            None, // no relkind = explicit CREATE TYPE
            false,
            None,
        );
        assert_eq!(
            result,
            Some(DbObjectId::Type {
                schema: "app".to_string(),
                name: "address".to_string()
            })
        );
    }

    #[test]
    fn test_resolve_type_dependency_enum() {
        let result =
            resolve_type_dependency(Some("app"), Some("status"), Some("e"), None, false, None);
        assert_eq!(
            result,
            Some(DbObjectId::Type {
                schema: "app".to_string(),
                name: "status".to_string()
            })
        );
    }

    #[test]
    fn test_resolve_type_dependency_system_schema() {
        let result = resolve_type_dependency(
            Some("pg_catalog"),
            Some("int4"),
            Some("b"),
            None,
            false,
            None,
        );
        assert_eq!(result, None);
    }

    #[test]
    fn test_resolve_type_dependency_missing_info() {
        // Missing schema
        assert_eq!(
            resolve_type_dependency(None, Some("mytype"), Some("e"), None, false, None),
            None
        );
        // Missing name
        assert_eq!(
            resolve_type_dependency(Some("app"), None, Some("e"), None, false, None),
            None
        );
    }
}
