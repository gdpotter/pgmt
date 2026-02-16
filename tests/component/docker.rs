//! Integration tests for Docker shadow database functionality

use anyhow::Result;
use pgmt::config::types::*;
use pgmt::docker::DockerManager;
use std::collections::HashMap;
use uuid::Uuid;

use crate::helpers::docker::with_docker_cleanup;

#[tokio::test]
async fn test_docker_postgres_container() -> Result<()> {
    with_docker_cleanup(async {
        let docker_manager = match DockerManager::new().await {
            Ok(manager) => manager,
            Err(e) => {
                println!("Skipping Docker test - Docker daemon not available: {}", e);
                return;
            }
        };

        let mut environment = HashMap::new();
        environment.insert("POSTGRES_PASSWORD".to_string(), "test_password".to_string());

        // Use dynamic container name to avoid conflicts
        let container_name = format!("pgmt_test_container_{}", Uuid::new_v4().simple());

        let config = ShadowDockerConfig {
            version: None,
            image: "postgres:18-alpine".to_string(),
            environment,
            container_name: Some(container_name),
            auto_cleanup: true,
            volumes: None,
            network: None,
        };

        // Start container - RAII pattern automatically cleans up when scope ends
        let shadow_db = docker_manager.start_shadow_database(&config).await.unwrap();

        // Verify connection string format
        assert!(shadow_db.connection_string().starts_with("postgres://"));
        assert!(shadow_db.connection_string().contains("test_password"));
        assert!(shadow_db.connection_string().contains("pgmt_shadow"));

        // Container is automatically cleaned up when shadow_db goes out of scope
    })
    .await;

    Ok(())
}

#[tokio::test]
async fn test_shadow_database_failure_includes_logs() -> Result<()> {
    with_docker_cleanup(async {
        let docker_manager = match DockerManager::new().await {
            Ok(manager) => manager,
            Err(e) => {
                println!("Skipping Docker test - Docker daemon not available: {}", e);
                return;
            }
        };

        let mut environment = HashMap::new();
        environment.insert("POSTGRES_PASSWORD".to_string(), "test_password".to_string());
        // Pass an invalid initdb flag to guarantee the container fails during startup
        environment.insert(
            "POSTGRES_INITDB_ARGS".to_string(),
            "--invalid-flag".to_string(),
        );

        let container_name = format!("pgmt_test_fail_{}", Uuid::new_v4().simple());

        let config = ShadowDockerConfig {
            version: None,
            image: "postgres:18-alpine".to_string(),
            environment,
            container_name: Some(container_name),
            auto_cleanup: true,
            volumes: None,
            network: None,
        };

        let result = docker_manager.start_shadow_database(&config).await;
        let error_msg = match result {
            Err(e) => e.to_string(),
            Ok(_) => panic!("Expected start_shadow_database to fail"),
        };
        assert!(
            error_msg.contains("Container logs"),
            "Error should contain inline logs, got: {}",
            error_msg
        );
        assert!(
            error_msg.contains("PGMT_KEEP_SHADOW_ON_FAILURE"),
            "Error should mention PGMT_KEEP_SHADOW_ON_FAILURE, got: {}",
            error_msg
        );
    })
    .await;

    Ok(())
}

#[tokio::test]
async fn test_shadow_database_config_docker() -> Result<()> {
    let mut environment = HashMap::new();
    environment.insert("POSTGRES_PASSWORD".to_string(), "test_password".to_string());

    let docker_input = ShadowDockerInput {
        version: None,
        image: Some("postgres:14-alpine".to_string()),
        environment: Some(environment),
        container_name: Some("test_container".to_string()),
        auto_cleanup: Some(false),
        volumes: None,
        network: None,
    };

    let shadow_input = ShadowDatabaseInput {
        auto: Some(false),
        url: None,
        docker: Some(docker_input),
    };

    let databases_input = DatabasesInput {
        dev_url: Some("postgres://localhost/test_dev".to_string()),
        shadow_url: None,
        target_url: None,
        shadow: Some(shadow_input),
    };

    let config_input = ConfigInput {
        databases: Some(databases_input),
        directories: None,
        objects: None,
        migration: None,
        schema: None,
        docker: None,
    };

    // Test configuration building (without actually creating containers)
    let config = pgmt::config::ConfigBuilder::new()
        .with_file(config_input)
        .resolve()?;

    // Verify the Docker configuration was properly resolved
    match &config.databases.shadow {
        ShadowDatabase::Docker(docker_config) => {
            assert_eq!(docker_config.image, "postgres:14-alpine");
            assert_eq!(
                docker_config.container_name,
                Some("test_container".to_string())
            );
            assert!(!docker_config.auto_cleanup);
        }
        _ => panic!("Expected Docker shadow database configuration"),
    }

    Ok(())
}
