use crate::catalog::id::DbObjectId;

/// Whether a schema name is one PostgreSQL owns — the single definition, next to
/// the SQL fragment mirroring it. Dependency tracking uses it to avoid tracking
/// dependencies on system objects.
pub use crate::catalog::raw::exclusion::is_system_schema;

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

#[cfg(test)]
mod tests {
    use super::*;

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
