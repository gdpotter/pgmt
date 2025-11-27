use super::*;
use crate::config::merge::Merge;
use crate::config::types::*;

#[test]
fn test_config_input_merge() {
    let file_config = ConfigInput {
        databases: Some(DatabasesInput {
            dev_url: Some("postgres://localhost/dev".to_string()),
            shadow_url: None,
            target_url: None,
            shadow: Some(ShadowDatabaseInput {
                auto: Some(true),
                url: None,
                docker: None,
            }),
        }),
        directories: Some(DirectoriesInput {
            schema_dir: Some("schema".to_string()),
            migrations_dir: None,
            baselines_dir: None,
            roles_file: None,
        }),
        objects: None,
        migration: None,
        schema: None,
        docker: None,
    };

    let cli_config = ConfigInput {
        databases: Some(DatabasesInput {
            dev_url: None, // CLI doesn't override this
            shadow_url: Some("postgres://localhost/shadow_override".to_string()),
            target_url: Some("postgres://localhost/target".to_string()),
            shadow: None,
        }),
        directories: Some(DirectoriesInput {
            schema_dir: None,
            migrations_dir: Some("migrations_override".to_string()),
            baselines_dir: Some("baselines".to_string()),
            roles_file: Some("roles.sql".to_string()),
        }),
        objects: Some(ObjectsInput {
            include: None,
            exclude: Some(ObjectExcludeInput {
                exclude_schemas: Some(vec!["temp_*".to_string()]),
                exclude_tables: None,
            }),
        }),
        migration: None,
        schema: None,
        docker: None,
    };

    let merged = file_config.merge(cli_config);

    // CLI should override file config
    assert_eq!(
        merged.databases.as_ref().unwrap().dev_url,
        Some("postgres://localhost/dev".to_string())
    );
    assert_eq!(
        merged.databases.as_ref().unwrap().shadow_url,
        Some("postgres://localhost/shadow_override".to_string())
    );
    assert_eq!(
        merged.databases.as_ref().unwrap().target_url,
        Some("postgres://localhost/target".to_string())
    );

    // CLI should override specific directory settings
    assert_eq!(
        merged.directories.as_ref().unwrap().schema_dir,
        Some("schema".to_string())
    );
    assert_eq!(
        merged.directories.as_ref().unwrap().migrations_dir,
        Some("migrations_override".to_string())
    );
    assert_eq!(
        merged.directories.as_ref().unwrap().baselines_dir,
        Some("baselines".to_string())
    );

    // CLI object config should be present with exclude patterns
    assert!(merged.objects.is_some());
    assert_eq!(
        merged
            .objects
            .as_ref()
            .unwrap()
            .exclude
            .as_ref()
            .unwrap()
            .exclude_schemas,
        Some(vec!["temp_*".to_string()])
    );
}

#[test]
fn test_config_builder_resolve() {
    let config_input = ConfigInput {
        databases: Some(DatabasesInput {
            dev_url: Some("postgres://localhost/dev".to_string()),
            shadow_url: None,
            target_url: None,
            shadow: None, // Auto mode by default
        }),
        directories: None, // Use defaults
        objects: Some(ObjectsInput {
            include: None,
            exclude: Some(ObjectExcludeInput {
                exclude_schemas: Some(vec!["pg_*".to_string()]),
                exclude_tables: Some(vec!["temp_*".to_string()]),
            }),
        }),
        migration: None, // Use defaults
        schema: None,    // Use defaults
        docker: None,    // Use defaults
    };

    let config = ConfigBuilder::new()
        .with_file(config_input)
        .resolve()
        .unwrap();

    // Check database configuration
    assert_eq!(config.databases.dev, "postgres://localhost/dev");
    match config.databases.shadow {
        ShadowDatabase::Auto => {} // Expected
        _ => panic!("Expected auto shadow database"),
    }
    assert_eq!(config.databases.target, None);

    // Check directory defaults
    assert_eq!(config.directories.schema, "schema");
    assert_eq!(config.directories.migrations, "migrations");
    assert_eq!(config.directories.baselines, "schema_baselines");
    assert_eq!(config.directories.roles, "roles.sql");

    // Check object filtering
    assert_eq!(config.objects.exclude.schemas, vec!["pg_*".to_string()]);
    assert_eq!(config.objects.exclude.tables, vec!["temp_*".to_string()]);

    // Check migration defaults
    assert_eq!(config.migration.default_mode, "safe_only");
    assert!(config.migration.validate_baseline_consistency);

    // Check docker defaults
    assert!(config.docker.auto_cleanup);
    assert!(config.docker.check_system_identifier);
}

#[tokio::test]
async fn test_shadow_database_url_mode() {
    // Test explicit URL mode (this doesn't require Docker)
    let shadow_db = ShadowDatabase::Url("postgres://localhost/explicit_shadow".to_string());
    let url = shadow_db.get_connection_string().await.unwrap();
    assert_eq!(url, "postgres://localhost/explicit_shadow");

    // Note: Auto and Docker modes require a Docker daemon and are tested
    // in integration tests (tests/integration/docker.rs)
}

#[test]
fn test_object_filter_schema_filtering() {
    let objects = Objects {
        include: ObjectInclude {
            schemas: vec!["public".to_string(), "app".to_string()],
            tables: vec![],
        },
        exclude: ObjectExclude {
            schemas: vec!["pg_*".to_string(), "information_schema".to_string()],
            tables: vec![],
        },
    };

    let tracking_table = TrackingTable::default();
    let filter = ObjectFilter::new(&objects, &tracking_table);

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
fn test_object_filter_table_filtering() {
    let objects = Objects {
        include: ObjectInclude {
            schemas: vec!["public".to_string()],
            tables: vec!["users".to_string(), "posts".to_string()],
        },
        exclude: ObjectExclude {
            schemas: vec!["pg_*".to_string()],
            tables: vec!["temp_*".to_string()],
        },
    };

    let tracking_table = TrackingTable::default();
    let filter = ObjectFilter::new(&objects, &tracking_table);

    // Should include specified tables in included schemas
    assert!(filter.should_include_table("public", "users"));
    assert!(filter.should_include_table("public", "posts"));

    // Should exclude tables matching exclude patterns
    assert!(!filter.should_include_table("public", "temp_data"));

    // Should not include tables not in the include list
    assert!(!filter.should_include_table("public", "other_table"));

    // Should not include tables in excluded schemas
    assert!(!filter.should_include_table("pg_catalog", "pg_tables"));
}

#[test]
fn test_empty_include_patterns_means_include_all() {
    let objects = Objects {
        include: ObjectInclude {
            schemas: vec![], // Empty means include all
            tables: vec![],
        },
        exclude: ObjectExclude {
            schemas: vec!["pg_*".to_string()],
            tables: vec![],
        },
    };

    let tracking_table = TrackingTable::default();
    let filter = ObjectFilter::new(&objects, &tracking_table);

    // Should include schemas not in exclude list
    assert!(filter.should_include_schema("public"));
    assert!(filter.should_include_schema("app"));
    assert!(filter.should_include_schema("custom"));

    // Should still exclude patterns
    assert!(!filter.should_include_schema("pg_catalog"));
    assert!(!filter.should_include_schema("pg_stat"));
}

#[test]
fn test_shadow_docker_version_resolution() {
    // Test version field converts to image
    let config = ShadowDockerConfig {
        version: Some("16".to_string()),
        image: ShadowDockerConfig::default().image.clone(),
        environment: Default::default(),
        container_name: None,
        auto_cleanup: true,
        volumes: None,
        network: None,
    };
    assert_eq!(config.resolved_image(), "postgres:16-alpine");

    // Test explicit image takes precedence over version
    let config_with_both = ShadowDockerConfig {
        version: Some("16".to_string()),
        image: "postgres:14-bullseye".to_string(),
        environment: Default::default(),
        container_name: None,
        auto_cleanup: true,
        volumes: None,
        network: None,
    };
    assert_eq!(config_with_both.resolved_image(), "postgres:14-bullseye");

    // Test default image (no version specified)
    let default_config = ShadowDockerConfig::default();
    assert_eq!(default_config.resolved_image(), "postgres:18-alpine");
}

#[test]
fn test_config_builder_shadow_docker_version() {
    // Test version configuration through ConfigBuilder
    let config_input = ConfigInput {
        databases: Some(DatabasesInput {
            dev_url: Some("postgres://localhost/dev".to_string()),
            shadow_url: None,
            target_url: None,
            shadow: Some(ShadowDatabaseInput {
                auto: None,
                url: None,
                docker: Some(ShadowDockerInput {
                    version: Some("16".to_string()),
                    image: None,
                    environment: None,
                    container_name: None,
                    auto_cleanup: None,
                    volumes: None,
                    network: None,
                }),
            }),
        }),
        directories: None,
        objects: None,
        migration: None,
        schema: None,
        docker: None,
    };

    let config = ConfigBuilder::new()
        .with_file(config_input)
        .resolve()
        .unwrap();

    // Verify version was set correctly
    match config.databases.shadow {
        ShadowDatabase::Docker(docker_config) => {
            assert_eq!(docker_config.version, Some("16".to_string()));
            assert_eq!(docker_config.resolved_image(), "postgres:16-alpine");
        }
        _ => panic!("Expected Docker shadow database"),
    }
}

#[test]
fn test_config_builder_shadow_docker_explicit_image() {
    // Test explicit image configuration (should take precedence)
    let config_input = ConfigInput {
        databases: Some(DatabasesInput {
            dev_url: Some("postgres://localhost/dev".to_string()),
            shadow_url: None,
            target_url: None,
            shadow: Some(ShadowDatabaseInput {
                auto: None,
                url: None,
                docker: Some(ShadowDockerInput {
                    version: Some("16".to_string()), // This should be ignored
                    image: Some("postgres:14-bullseye".to_string()),
                    environment: None,
                    container_name: None,
                    auto_cleanup: None,
                    volumes: None,
                    network: None,
                }),
            }),
        }),
        directories: None,
        objects: None,
        migration: None,
        schema: None,
        docker: None,
    };

    let config = ConfigBuilder::new()
        .with_file(config_input)
        .resolve()
        .unwrap();

    // Verify explicit image takes precedence
    match config.databases.shadow {
        ShadowDatabase::Docker(docker_config) => {
            assert_eq!(docker_config.resolved_image(), "postgres:14-bullseye");
        }
        _ => panic!("Expected Docker shadow database"),
    }
}

#[test]
fn test_default_shadow_docker_version() {
    // Verify default version is PostgreSQL 18
    let default_config = ShadowDockerConfig::default();
    assert_eq!(default_config.image, "postgres:18-alpine");
    assert_eq!(default_config.resolved_image(), "postgres:18-alpine");
}
