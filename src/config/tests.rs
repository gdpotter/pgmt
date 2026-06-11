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
                schemas: Some(vec!["temp_*".to_string()]),
                tables: None,
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
            .schemas,
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
                schemas: Some(vec!["pg_*".to_string()]),
                tables: Some(vec!["temp_*".to_string()]),
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
        platform: None,
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
        platform: None,
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
                    platform: None,
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
                    platform: None,
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
    // Default requests no specific platform (host-native).
    assert_eq!(default_config.platform, None);
}

#[test]
fn test_config_builder_shadow_docker_platform() {
    // Platform flows through the builder for single-arch images (e.g. PostGIS).
    let config_input = ConfigInput {
        databases: Some(DatabasesInput {
            dev_url: Some("postgres://localhost/dev".to_string()),
            shadow_url: None,
            target_url: None,
            shadow: Some(ShadowDatabaseInput {
                auto: None,
                url: None,
                docker: Some(ShadowDockerInput {
                    version: None,
                    image: Some("postgis/postgis:16-3.5".to_string()),
                    platform: Some("linux/amd64".to_string()),
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

    match config.databases.shadow {
        ShadowDatabase::Docker(docker_config) => {
            assert_eq!(docker_config.resolved_image(), "postgis/postgis:16-3.5");
            assert_eq!(docker_config.platform.as_deref(), Some("linux/amd64"));
        }
        _ => panic!("Expected Docker shadow database"),
    }
}

#[test]
fn test_shadow_merge_docker_over_docker_preserves_unanswered_fields() {
    // Both layers are docker mode: the overlay only replaces fields it sets,
    // so hand-maintained fields (environment, container_name, auto_cleanup)
    // survive a re-init that re-answers docker mode.
    let base = ShadowDatabaseInput {
        auto: None,
        url: None,
        docker: Some(ShadowDockerInput {
            image: Some("postgis/postgis:16-3.4".to_string()),
            container_name: Some("pgmt_shadow_app".to_string()),
            auto_cleanup: Some(false),
            environment: Some([("POSTGRES_PASSWORD".to_string(), "secret".to_string())].into()),
            ..Default::default()
        }),
    };
    let overlay = ShadowDatabaseInput {
        auto: None,
        url: None,
        docker: Some(ShadowDockerInput {
            image: Some("postgis/postgis:17-3.5".to_string()),
            ..Default::default()
        }),
    };

    let merged = base.merge_with(overlay);
    let docker = merged.docker.expect("docker mode preserved");
    assert_eq!(docker.image.as_deref(), Some("postgis/postgis:17-3.5"));
    assert_eq!(docker.container_name.as_deref(), Some("pgmt_shadow_app"));
    assert_eq!(docker.auto_cleanup, Some(false));
    assert!(docker.environment.is_some());
    assert_eq!(merged.url, None);
}

#[test]
fn test_shadow_merge_mode_switch_to_auto_clears_docker() {
    // Picking Auto is a deliberate mode choice: the stale docker block must
    // not survive, or the resolver would still pick docker mode.
    let base = ShadowDatabaseInput {
        auto: None,
        url: None,
        docker: Some(ShadowDockerInput {
            image: Some("postgis/postgis:16-3.5".to_string()),
            container_name: Some("pgmt_shadow_app".to_string()),
            ..Default::default()
        }),
    };
    let overlay = ShadowDatabaseInput {
        auto: Some(true),
        url: None,
        docker: None,
    };

    let merged = base.merge_with(overlay);
    assert_eq!(merged.auto, Some(true));
    assert_eq!(merged.url, None);
    assert!(
        merged.docker.is_none(),
        "stale docker block must be cleared"
    );
}

#[test]
fn test_shadow_merge_mode_switch_to_url_clears_docker() {
    let base = ShadowDatabaseInput {
        auto: None,
        url: None,
        docker: Some(ShadowDockerInput {
            image: Some("postgis/postgis:16-3.5".to_string()),
            ..Default::default()
        }),
    };
    let overlay = ShadowDatabaseInput {
        auto: Some(false),
        url: Some("postgres://localhost/shadow".to_string()),
        docker: None,
    };

    let merged = base.merge_with(overlay);
    assert_eq!(merged.url.as_deref(), Some("postgres://localhost/shadow"));
    assert!(
        merged.docker.is_none(),
        "stale docker block must be cleared"
    );
}

#[test]
fn test_shadow_merge_mode_switch_url_to_docker_clears_url() {
    let base = ShadowDatabaseInput {
        auto: Some(false),
        url: Some("postgres://localhost/shadow".to_string()),
        docker: None,
    };
    let overlay = ShadowDatabaseInput {
        auto: None,
        url: None,
        docker: Some(ShadowDockerInput {
            image: Some("postgres:18-alpine".to_string()),
            ..Default::default()
        }),
    };

    let merged = base.merge_with(overlay);
    assert!(merged.url.is_none(), "stale url must be cleared");
    assert_eq!(
        merged.docker.and_then(|d| d.image).as_deref(),
        Some("postgres:18-alpine")
    );
}

#[test]
fn test_object_exclude_accepts_both_key_spellings() {
    // `exclude.schemas` is the documented key (mirrors include.schemas);
    // `exclude_schemas` is the legacy spelling accepted via serde alias.
    let new_style: ConfigInput = serde_yaml::from_str(
        "objects:\n  exclude:\n    schemas: [\"pg_*\"]\n    tables: [\"cache_*\"]\n",
    )
    .unwrap();
    let legacy: ConfigInput = serde_yaml::from_str(
        "objects:\n  exclude:\n    exclude_schemas: [\"pg_*\"]\n    exclude_tables: [\"cache_*\"]\n",
    )
    .unwrap();

    assert_eq!(new_style.objects, legacy.objects);
    let exclude = new_style.objects.unwrap().exclude.unwrap();
    assert_eq!(exclude.schemas, Some(vec!["pg_*".to_string()]));
    assert_eq!(exclude.tables, Some(vec!["cache_*".to_string()]));
}
