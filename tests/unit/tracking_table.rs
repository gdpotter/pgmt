use anyhow::Result;
use pgmt::config::types::{ObjectExclude, ObjectInclude, Objects, TrackingTable};
use pgmt::config::{ConfigBuilder, ConfigInput, MigrationInput, ObjectFilter, TrackingTableInput};

#[test]
fn test_tracking_table_defaults() {
    let tracking_table = TrackingTable::default();
    assert_eq!(tracking_table.schema, "public");
    assert_eq!(tracking_table.name, "pgmt_migrations");
}

#[test]
fn test_tracking_table_configuration_parsing() -> Result<()> {
    let config_input = ConfigInput {
        databases: None,
        directories: None,
        objects: None,
        migration: Some(MigrationInput {
            default_mode: None,
            validate_baseline_consistency: None,
            create_baselines_by_default: None,
            tracking_table: Some(TrackingTableInput {
                schema: Some("internal".to_string()),
                name: Some("migration_history".to_string()),
            }),
        }),
        schema: None,
        docker: None,
    };

    let config = ConfigBuilder::new().with_file(config_input).resolve()?;

    assert_eq!(config.migration.tracking_table.schema, "internal");
    assert_eq!(config.migration.tracking_table.name, "migration_history");

    Ok(())
}

#[test]
fn test_tracking_table_partial_configuration() -> Result<()> {
    // Test with only schema specified
    let config_input = ConfigInput {
        databases: None,
        directories: None,
        objects: None,
        migration: Some(MigrationInput {
            default_mode: None,
            validate_baseline_consistency: None,
            create_baselines_by_default: None,
            tracking_table: Some(TrackingTableInput {
                schema: Some("custom_schema".to_string()),
                name: None, // Use default name
            }),
        }),
        schema: None,
        docker: None,
    };

    let config = ConfigBuilder::new().with_file(config_input).resolve()?;

    assert_eq!(config.migration.tracking_table.schema, "custom_schema");
    assert_eq!(config.migration.tracking_table.name, "pgmt_migrations"); // Default

    Ok(())
}

#[test]
fn test_tracking_table_fallback_to_defaults() -> Result<()> {
    // Test with no tracking table configuration
    let config_input = ConfigInput {
        databases: None,
        directories: None,
        objects: None,
        migration: None, // No migration config at all
        schema: None,
        docker: None,
    };

    let config = ConfigBuilder::new().with_file(config_input).resolve()?;

    // Should use defaults
    assert_eq!(config.migration.tracking_table.schema, "public");
    assert_eq!(config.migration.tracking_table.name, "pgmt_migrations");

    Ok(())
}

#[test]
fn test_object_filter_pgmt_internal_table_recognition() {
    let tracking_table = TrackingTable {
        schema: "internal".to_string(),
        name: "migration_history".to_string(),
    };

    let objects = Objects::default();
    let filter = ObjectFilter::new(&objects, &tracking_table);

    // Should recognize the main migrations table
    assert!(filter.is_pgmt_internal_table("internal", "migration_history"));

    // Should recognize the sections table
    assert!(filter.is_pgmt_internal_table("internal", "migration_history_sections"));

    // Should not recognize other tables
    assert!(!filter.is_pgmt_internal_table("internal", "other_table"));
    assert!(!filter.is_pgmt_internal_table("public", "migration_history"));
    assert!(!filter.is_pgmt_internal_table("public", "pgmt_migrations"));
}

#[test]
fn test_object_filter_migration_table_inclusion() {
    let tracking_table = TrackingTable {
        schema: "internal".to_string(),
        name: "migration_history".to_string(),
    };

    // Create restrictive objects config that would normally exclude "internal" schema
    let objects = Objects {
        include: ObjectInclude {
            schemas: vec!["public".to_string()], // Only public schema
            tables: vec!["users".to_string()],   // Only users table
        },
        exclude: ObjectExclude {
            schemas: vec![],
            tables: vec![],
        },
    };

    let filter = ObjectFilter::new(&objects, &tracking_table);

    // Migration table should NOT be included in declarative management
    // (it's managed imperatively by pgmt itself)
    assert!(!filter.should_include_table("internal", "migration_history"));

    // Sections table should also NOT be included
    assert!(!filter.should_include_table("internal", "migration_history_sections"));

    // Other tables in "internal" schema should still be excluded
    assert!(!filter.should_include_table("internal", "other_table"));

    // Regular filtering should still work
    assert!(filter.should_include_table("public", "users"));
    assert!(!filter.should_include_table("public", "posts"));
}
