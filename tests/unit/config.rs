use anyhow::Result;
use pgmt::config::{
    ConfigBuilder, ConfigInput, DatabasesInput, ObjectExcludeInput, ObjectsInput,
    ShadowDatabaseInput,
};
use std::fs;
use tempfile::TempDir;

use crate::helpers::docker::with_docker_cleanup;

/// Integration tests for command-specific configuration requirements
/// Tests the interaction between config files, CLI args, and command validation
mod config_integration_tests {
    use super::*;

    #[test]
    fn test_config_merge_cli_overrides_file() -> Result<()> {
        let file_config = ConfigInput {
            databases: Some(DatabasesInput {
                dev_url: Some("postgres://localhost/file_dev".to_string()),
                shadow_url: None,
                target_url: None,
                shadow: Some(ShadowDatabaseInput {
                    auto: Some(true),
                    url: None,
                    docker: None,
                }),
            }),
            directories: None,
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

        let cli_config = ConfigInput {
            databases: Some(DatabasesInput {
                dev_url: None,
                shadow_url: Some("postgres://localhost/cli_shadow".to_string()),
                target_url: Some("postgres://localhost/cli_target".to_string()),
                shadow: None,
            }),
            directories: None,
            objects: Some(ObjectsInput {
                include: None,
                exclude: None,
            }),
            migration: None,
            schema: None,
            docker: None,
        };

        let config = ConfigBuilder::new()
            .with_file(file_config)
            .with_cli_args(cli_config)
            .resolve()?;

        assert_eq!(config.databases.dev, "postgres://localhost/file_dev");
        assert_eq!(
            config.databases.target,
            Some("postgres://localhost/cli_target".to_string())
        );

        // Verify exclude patterns from file config
        assert_eq!(config.objects.exclude.schemas, vec!["temp_*".to_string()]);

        Ok(())
    }

    #[tokio::test]
    async fn test_shadow_database_auto_url_generation() -> Result<()> {
        with_docker_cleanup(async {
            let config_input = ConfigInput {
                databases: Some(DatabasesInput {
                    dev_url: Some("postgres://user:pass@localhost:5432/myapp".to_string()),
                    shadow_url: None,
                    target_url: None,
                    shadow: Some(ShadowDatabaseInput {
                        auto: Some(true),
                        url: None,
                        docker: None,
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

            // Test shadow database URL generation
            match &config.databases.shadow {
                pgmt::config::ShadowDatabase::Auto => {
                    // Auto mode now uses Docker containers, so skip this test if Docker isn't available
                    match config.databases.shadow.get_connection_string().await {
                        Ok(shadow_url) => {
                            // Verify it's a usable postgres connection string
                            assert!(
                                shadow_url.starts_with("postgres://"),
                                "Should be a postgres URL: {}",
                                shadow_url
                            );
                        }
                        Err(e) => {
                            // Docker not available, skip test
                            println!(
                                "ℹ️  Skipping Docker test - Docker daemon not available: {}",
                                e
                            );
                        }
                    }
                }
                _ => panic!("Expected auto shadow database"),
            }
        })
        .await;

        Ok(())
    }

    #[tokio::test]
    async fn test_shadow_database_manual_url() -> Result<()> {
        let config_input = ConfigInput {
            databases: Some(DatabasesInput {
                dev_url: Some("postgres://localhost/dev".to_string()),
                shadow_url: None,
                target_url: None,
                shadow: Some(ShadowDatabaseInput {
                    auto: Some(false),
                    url: Some("postgres://localhost/manual_shadow".to_string()),
                    docker: None,
                }),
            }),
            directories: None,
            objects: None,
            migration: None,
            schema: None,
            docker: None,
        };

        let config = ConfigBuilder::new().with_file(config_input).resolve()?;

        // Test manual shadow database URL
        match &config.databases.shadow {
            pgmt::config::ShadowDatabase::Url(url) => {
                assert_eq!(url, "postgres://localhost/manual_shadow");
                let shadow_url = config.databases.shadow.get_connection_string().await?;
                assert_eq!(shadow_url, "postgres://localhost/manual_shadow");
            }
            _ => panic!("Expected manual shadow database URL"),
        }

        Ok(())
    }

    #[test]
    fn test_object_filtering_configuration() -> Result<()> {
        let config_input = ConfigInput {
            databases: Some(DatabasesInput {
                dev_url: Some("postgres://localhost/dev".to_string()),
                shadow_url: None,
                target_url: None,
                shadow: None,
            }),
            directories: None,
            objects: Some(ObjectsInput {
                include: None,
                exclude: Some(ObjectExcludeInput {
                    exclude_schemas: Some(vec![
                        "pg_*".to_string(),
                        "information_schema".to_string(),
                    ]),
                    exclude_tables: Some(vec!["temp_*".to_string(), "cache_*".to_string()]),
                }),
            }),
            migration: None,
            schema: None,
            docker: None,
        };

        let config = ConfigBuilder::new().with_file(config_input).resolve()?;

        let filter =
            pgmt::config::ObjectFilter::new(&config.objects, &config.migration.tracking_table);

        // Test schema filtering
        assert!(!filter.should_include_schema("pg_catalog"));
        assert!(!filter.should_include_schema("information_schema"));
        assert!(filter.should_include_schema("public"));
        assert!(filter.should_include_schema("app"));

        // Test table filtering
        assert!(!filter.should_include_table("public", "temp_data"));
        assert!(!filter.should_include_table("public", "cache_sessions"));
        assert!(filter.should_include_table("public", "users"));

        Ok(())
    }

    #[test]
    fn test_config_defaults_with_empty_input() -> Result<()> {
        let empty_config = ConfigInput::default();

        let config = ConfigBuilder::new().with_file(empty_config).resolve()?;

        // Verify defaults are applied
        assert_eq!(config.databases.dev, "postgres://localhost/pgmt_dev");
        assert_eq!(config.directories.schema, "schema");
        assert_eq!(config.directories.migrations, "migrations");
        assert_eq!(config.directories.baselines, "schema_baselines");
        assert_eq!(config.directories.roles, "roles.sql");

        // Verify default exclude patterns
        assert_eq!(
            config.objects.exclude.schemas,
            vec!["pg_*".to_string(), "information_schema".to_string()]
        );

        // Verify migration defaults
        assert_eq!(config.migration.default_mode, "safe_only");
        assert!(config.migration.validate_baseline_consistency);

        // Verify docker defaults
        assert!(config.docker.auto_cleanup);
        assert!(config.docker.check_system_identifier);

        Ok(())
    }

    #[test]
    fn test_config_partial_override() -> Result<()> {
        // Test that partial config input properly merges with defaults
        let partial_config = ConfigInput {
            databases: Some(DatabasesInput {
                dev_url: Some("postgres://custom/dev".to_string()),
                shadow_url: None,
                target_url: None,
                shadow: None, // Should use default (auto)
            }),
            directories: None, // Should use all defaults
            objects: Some(ObjectsInput {
                include: None,
                exclude: Some(ObjectExcludeInput {
                    exclude_schemas: Some(vec!["custom_*".to_string()]),
                    exclude_tables: None,
                }),
            }),
            migration: None, // Use defaults
            schema: None,
            docker: None, // Use defaults
        };

        let config = ConfigBuilder::new().with_file(partial_config).resolve()?;

        // Custom settings should be applied
        assert_eq!(config.databases.dev, "postgres://custom/dev");
        assert_eq!(config.objects.exclude.schemas, vec!["custom_*".to_string()]);

        // Defaults should be used for unspecified values
        assert_eq!(config.directories.schema, "schema");
        assert_eq!(config.migration.default_mode, "safe_only");

        Ok(())
    }

    #[test]
    fn test_config_file_loading_integration() -> Result<()> {
        // Create a temporary config file
        let temp_dir = TempDir::new()?;
        let config_path = temp_dir.path().join("pgmt.yaml");

        let config_content = r#"
databases:
  dev_url: postgres://localhost/test_dev
  shadow:
    auto: false
    url: postgres://localhost/test_shadow

directories:
  schema_dir: custom_schema/
  migrations_dir: custom_migrations/

objects:
  exclude:
    exclude_schemas: ["temp_*", "test_*"]

migration:
  default_mode: force_all
  validate_baseline_consistency: false
"#;

        fs::write(&config_path, config_content)?;

        // Load and parse the config
        let (config_input, _) = pgmt::config::load_config(config_path.to_str().unwrap())?;
        let config = ConfigBuilder::new().with_file(config_input).resolve()?;

        // Verify the loaded configuration
        assert_eq!(config.databases.dev, "postgres://localhost/test_dev");
        assert_eq!(config.directories.schema, "custom_schema/");
        assert_eq!(config.directories.migrations, "custom_migrations/");
        assert_eq!(config.objects.exclude.schemas, vec!["temp_*", "test_*"]);
        assert_eq!(config.migration.default_mode, "force_all");
        assert!(!config.migration.validate_baseline_consistency);

        // Verify shadow database configuration
        match &config.databases.shadow {
            pgmt::config::ShadowDatabase::Url(url) => {
                assert_eq!(url, "postgres://localhost/test_shadow");
            }
            _ => panic!("Expected manual shadow database URL"),
        }

        Ok(())
    }

    #[test]
    fn test_config_validation_edge_cases() -> Result<()> {
        // Test with minimal valid config
        let minimal_config = ConfigInput {
            databases: Some(DatabasesInput {
                dev_url: Some("postgres://localhost/minimal".to_string()),
                shadow_url: None,
                target_url: None,
                shadow: None,
            }),
            directories: None,
            objects: None,
            migration: None,
            schema: None,
            docker: None,
        };

        let config = ConfigBuilder::new().with_file(minimal_config).resolve()?;

        assert_eq!(config.databases.dev, "postgres://localhost/minimal");
        assert!(matches!(
            config.databases.shadow,
            pgmt::config::ShadowDatabase::Auto
        ));

        Ok(())
    }

    #[test]
    fn test_column_order_config_default() -> Result<()> {
        let empty_config = ConfigInput::default();
        let config = ConfigBuilder::new().with_file(empty_config).resolve()?;

        // Default should be Strict
        assert_eq!(
            config.migration.column_order,
            pgmt::config::ColumnOrderMode::Strict
        );

        Ok(())
    }

    #[test]
    fn test_column_order_config_from_file() -> Result<()> {
        // Create a temporary config file
        let temp_dir = TempDir::new()?;
        let config_path = temp_dir.path().join("pgmt.yaml");

        let config_content = r#"
migration:
  column_order: warn
"#;

        fs::write(&config_path, config_content)?;

        // Load and parse the config
        let (config_input, _) = pgmt::config::load_config(config_path.to_str().unwrap())?;
        let config = ConfigBuilder::new().with_file(config_input).resolve()?;

        assert_eq!(
            config.migration.column_order,
            pgmt::config::ColumnOrderMode::Warn
        );

        Ok(())
    }

    #[test]
    fn test_column_order_config_relaxed() -> Result<()> {
        // Create a temporary config file
        let temp_dir = TempDir::new()?;
        let config_path = temp_dir.path().join("pgmt.yaml");

        let config_content = r#"
migration:
  column_order: relaxed
"#;

        fs::write(&config_path, config_content)?;

        // Load and parse the config
        let (config_input, _) = pgmt::config::load_config(config_path.to_str().unwrap())?;
        let config = ConfigBuilder::new().with_file(config_input).resolve()?;

        assert_eq!(
            config.migration.column_order,
            pgmt::config::ColumnOrderMode::Relaxed
        );

        Ok(())
    }
}
