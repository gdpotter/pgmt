use crate::catalog;
use crate::config::types::{ObjectExclude, ObjectInclude, Objects, TrackingTable};
use glob::Pattern;

/// Object filter for determining which database objects pgmt should manage
pub struct ObjectFilter {
    include: ObjectInclude,
    exclude: ObjectExclude,
    grants: bool,
    triggers: bool,
    extensions: bool,
    tracking_table: TrackingTable,
}

impl ObjectFilter {
    /// Create a new object filter from configuration
    pub fn new(config: &Objects, tracking_table: &TrackingTable) -> Self {
        Self {
            include: config.include.clone(),
            exclude: config.exclude.clone(),
            grants: config.grants,
            triggers: config.triggers,
            extensions: config.extensions,
            tracking_table: tracking_table.clone(),
        }
    }

    /// Check if a schema should be included
    pub fn should_include_schema(&self, schema_name: &str) -> bool {
        // Check exclude patterns first
        if self.matches_patterns(&self.exclude.schemas, schema_name) {
            return false;
        }

        // If include patterns are specified, schema must match one of them
        if !self.include.schemas.is_empty() {
            return self.matches_patterns(&self.include.schemas, schema_name);
        }

        // Default: include if not excluded
        true
    }

    /// Check if a table should be included
    pub fn should_include_table(&self, schema_name: &str, table_name: &str) -> bool {
        // Exclude pgmt internal tables (migrations tracking, sections, etc.)
        if self.is_pgmt_internal_table(schema_name, table_name) {
            return false;
        }

        // First check if the schema is included
        if !self.should_include_schema(schema_name) {
            return false;
        }

        // Check exclude patterns for tables
        if self.matches_patterns(&self.exclude.tables, table_name) {
            return false;
        }

        // If include patterns are specified, table must match one of them
        if !self.include.tables.is_empty() {
            return self.matches_patterns(&self.include.tables, table_name);
        }

        // Default: include if not excluded
        true
    }

    /// Check if grants should be managed
    pub fn should_manage_grants(&self) -> bool {
        self.grants
    }

    /// Check if triggers should be managed
    pub fn should_manage_triggers(&self) -> bool {
        self.triggers
    }

    /// Check if extensions should be managed
    pub fn should_manage_extensions(&self) -> bool {
        self.extensions
    }

    /// Check if this is a pgmt internal table (migration tracking, sections, etc.)
    /// These tables are infrastructure managed by pgmt itself, not part of the user's schema.
    pub fn is_pgmt_internal_table(&self, schema_name: &str, table_name: &str) -> bool {
        if schema_name != self.tracking_table.schema {
            return false;
        }

        // Check all pgmt internal table patterns
        let internal_tables = [
            self.tracking_table.name.as_str(), // pgmt_migrations
            &format!("{}_sections", self.tracking_table.name), // pgmt_migrations_sections
        ];

        internal_tables.contains(&table_name)
    }

    /// Apply filter to a catalog, removing objects that shouldn't be managed
    pub fn filter_catalog(&self, mut catalog: catalog::Catalog) -> catalog::Catalog {
        // Filter schemas
        catalog
            .schemas
            .retain(|schema| self.should_include_schema(&schema.name));

        // Filter tables
        catalog
            .tables
            .retain(|table| self.should_include_table(&table.schema, &table.name));

        // Filter views (apply same table filtering logic)
        catalog
            .views
            .retain(|view| self.should_include_table(&view.schema, &view.name));

        // Filter functions by schema
        catalog
            .functions
            .retain(|function| self.should_include_schema(&function.schema));

        // Filter custom types by schema
        catalog
            .types
            .retain(|custom_type| self.should_include_schema(&custom_type.schema));

        // Filter sequences by schema
        catalog
            .sequences
            .retain(|sequence| self.should_include_schema(&sequence.schema));

        // Filter indexes by table inclusion
        catalog
            .indexes
            .retain(|index| self.should_include_table(&index.schema, &index.table_name));

        // Filter constraints by table inclusion
        catalog
            .constraints
            .retain(|constraint| self.should_include_table(&constraint.schema, &constraint.table));

        // Conditionally filter object types based on management settings
        if !self.should_manage_triggers() {
            catalog.triggers.clear();
        } else {
            // Filter triggers by table inclusion
            catalog
                .triggers
                .retain(|trigger| self.should_include_table(&trigger.schema, &trigger.table_name));
        }

        if !self.should_manage_grants() {
            catalog.grants.clear();
        }

        if !self.should_manage_extensions() {
            catalog.extensions.clear();
        }

        catalog
    }

    /// Check if a name matches any of the glob patterns
    fn matches_patterns(&self, patterns: &[String], name: &str) -> bool {
        if patterns.is_empty() {
            return false;
        }

        patterns.iter().any(|pattern| {
            Pattern::new(pattern)
                .map(|p| p.matches(name))
                .unwrap_or(false)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_objects() -> Objects {
        Objects {
            include: ObjectInclude {
                schemas: vec!["public".to_string(), "app".to_string()],
                tables: vec!["users".to_string(), "posts".to_string()],
            },
            exclude: ObjectExclude {
                schemas: vec!["pg_*".to_string(), "information_schema".to_string()],
                tables: vec!["temp_*".to_string()],
            },
            comments: true,
            grants: true,
            triggers: false,
            extensions: true,
        }
    }

    fn create_test_tracking_table() -> TrackingTable {
        TrackingTable {
            schema: "public".to_string(),
            name: "pgmt_migrations".to_string(),
        }
    }

    #[test]
    fn test_schema_filtering() {
        let filter = ObjectFilter::new(&create_test_objects(), &create_test_tracking_table());

        // Should include specified schemas
        assert!(filter.should_include_schema("public"));
        assert!(filter.should_include_schema("app"));

        // Should exclude postgres system schemas
        assert!(!filter.should_include_schema("pg_catalog"));
        assert!(!filter.should_include_schema("information_schema"));

        // Should not include schemas not in the include list
        assert!(!filter.should_include_schema("other"));
    }

    #[test]
    fn test_table_filtering() {
        let filter = ObjectFilter::new(&create_test_objects(), &create_test_tracking_table());

        // Should include specified tables in included schemas
        assert!(filter.should_include_table("public", "users"));
        assert!(filter.should_include_table("app", "posts"));

        // Should exclude tables matching exclude patterns
        assert!(!filter.should_include_table("public", "temp_data"));

        // Should not include tables not in the include list
        assert!(!filter.should_include_table("public", "other_table"));

        // Should not include tables in excluded schemas
        assert!(!filter.should_include_table("pg_catalog", "pg_tables"));

        // Should NOT include migration table in declarative management
        assert!(!filter.should_include_table("public", "pgmt_migrations"));
    }

    #[test]
    fn test_object_type_management() {
        let filter = ObjectFilter::new(&create_test_objects(), &create_test_tracking_table());

        assert!(filter.should_manage_grants());
        assert!(!filter.should_manage_triggers());
        assert!(filter.should_manage_extensions());
    }

    #[test]
    fn test_pgmt_internal_tables() {
        let filter = ObjectFilter::new(&create_test_objects(), &create_test_tracking_table());

        // Main migrations table
        assert!(filter.is_pgmt_internal_table("public", "pgmt_migrations"));
        // Sections table
        assert!(filter.is_pgmt_internal_table("public", "pgmt_migrations_sections"));
        // Not internal - wrong schema
        assert!(!filter.is_pgmt_internal_table("other", "pgmt_migrations"));
        // Not internal - different table
        assert!(!filter.is_pgmt_internal_table("public", "users"));
    }

    #[test]
    fn test_empty_include_patterns() {
        let objects = Objects {
            include: ObjectInclude {
                schemas: vec![], // Empty means include all
                tables: vec![],
            },
            exclude: ObjectExclude {
                schemas: vec!["pg_*".to_string()],
                tables: vec!["temp_*".to_string()],
            },
            comments: true,
            grants: true,
            triggers: true,
            extensions: true,
        };

        let filter = ObjectFilter::new(&objects, &create_test_tracking_table());

        // Should include schemas not in exclude list
        assert!(filter.should_include_schema("public"));
        assert!(filter.should_include_schema("app"));

        // Should still exclude patterns
        assert!(!filter.should_include_schema("pg_catalog"));
    }

    #[test]
    fn test_migration_table_handling() {
        let tracking_table = TrackingTable {
            schema: "internal".to_string(),
            name: "migration_history".to_string(),
        };

        let objects = Objects {
            include: ObjectInclude {
                schemas: vec!["public".to_string()], // Note: doesn't include "internal"
                tables: vec!["users".to_string()],   // Note: doesn't include migration table
            },
            exclude: ObjectExclude {
                schemas: vec![],
                tables: vec![],
            },
            comments: true,
            grants: true,
            triggers: true,
            extensions: true,
        };

        let filter = ObjectFilter::new(&objects, &tracking_table);

        // Migration table should NOT be included in declarative management
        // Even though it's the migration table, it's managed imperatively
        assert!(!filter.should_include_table("internal", "migration_history"));
        assert!(filter.is_pgmt_internal_table("internal", "migration_history"));

        // Sections table should also be excluded
        assert!(!filter.should_include_table("internal", "migration_history_sections"));
        assert!(filter.is_pgmt_internal_table("internal", "migration_history_sections"));

        // Other tables in the same schema should not be included
        assert!(!filter.should_include_table("internal", "other_table"));

        // Regular filtering should still work for non-migration tables
        assert!(filter.should_include_table("public", "users"));
        assert!(!filter.should_include_table("public", "posts")); // not in include list
    }
}
