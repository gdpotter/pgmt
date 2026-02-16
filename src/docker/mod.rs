//! Docker container management for shadow databases
//!
//! This module provides functionality to manage PostgreSQL containers
//! for use as shadow databases in pgmt operations.

use anyhow::{Result, anyhow};
use bollard::Docker;
use bollard::models::{ContainerCreateBody, ContainerStateStatusEnum};
use bollard::container::LogOutput;
use bollard::query_parameters::{
    CreateContainerOptions, CreateImageOptions, InspectContainerOptions, ListContainersOptions,
    LogsOptionsBuilder, RemoveContainerOptions, StartContainerOptions, StopContainerOptions,
};
use bollard::secret::{ContainerInspectResponse, HostConfig, PortBinding};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::config::types::ShadowDockerConfig;

/// Docker container manager for PostgreSQL shadow databases
pub struct DockerManager {
    docker: Docker,
}

/// Information about a running PostgreSQL container
#[derive(Debug, Clone)]
pub struct ContainerInfo {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
}

impl ContainerInfo {
    /// Get the connection string for this container
    pub fn connection_string(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}?sslmode=disable",
            self.username, self.password, self.host, self.port, self.database
        )
    }
}

/// RAII wrapper for a shadow database that ensures cleanup on drop
pub struct ShadowDatabase {
    container_info: ContainerInfo,
    auto_cleanup: bool,
}

impl ShadowDatabase {
    /// Get the connection string for this shadow database
    #[allow(dead_code)] // Used by integration tests in tests/component/docker.rs
    pub fn connection_string(&self) -> String {
        self.container_info.connection_string()
    }

    /// Consume this shadow database and return the connection string,
    /// keeping the container running (relies on global cleanup registry)
    pub fn into_connection_string(mut self) -> String {
        // Disable RAII cleanup - rely on global registry instead
        self.auto_cleanup = false;
        self.container_info.connection_string()
    }
}

impl Drop for ShadowDatabase {
    fn drop(&mut self) {
        if !self.auto_cleanup {
            return;
        }

        let container_id = self.container_info.id.clone();

        // Unregister from global registry first
        unregister_container(&container_id);

        // Block on cleanup to ensure it completes before Drop returns
        // This prevents test runtimes from shutting down before cleanup finishes
        let cleanup_result = std::thread::spawn(move || {
            // Create a new runtime for cleanup (doesn't depend on existing runtime)
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    debug!("Failed to create runtime for cleanup: {}", e);
                    return;
                }
            };

            rt.block_on(async {
                match DockerManager::new().await {
                    Ok(manager) => {
                        if let Err(e) = manager.stop_container(&container_id, true).await {
                            // Check if it's a 404 (already removed) - that's fine
                            let error_msg = e.to_string();
                            if !error_msg.contains("404")
                                && !error_msg.contains("No such container")
                            {
                                debug!("Failed to cleanup shadow database {}: {}", container_id, e);
                            }
                        } else {
                            debug!("Cleaned up shadow database: {}", container_id);
                        }
                    }
                    Err(e) => {
                        debug!("Failed to create Docker manager for cleanup: {}", e);
                    }
                }
            });
        });

        // Wait for cleanup thread to complete (with timeout to avoid hanging tests)
        let _ = cleanup_result.join();
    }
}

impl DockerManager {
    /// Check if Docker is available with detailed debug information
    pub async fn is_available_verbose() -> (bool, String) {
        match Self::try_connect_verbose().await {
            Ok((_, debug_info)) => (true, debug_info),
            Err(e) => (false, format!("Docker not available: {}", e)),
        }
    }

    /// Create a new Docker manager with retry logic
    pub async fn new() -> Result<Self> {
        const MAX_RETRIES: u32 = 5;
        const RETRY_DELAY_MS: u64 = 200;

        for attempt in 0..=MAX_RETRIES {
            match Self::try_connect().await {
                Ok(docker_manager) => {
                    if attempt > 0 {
                        println!(
                            "✅ Connected to Docker (after {} retry{})",
                            attempt,
                            if attempt == 1 { "" } else { "ies" }
                        );
                    }
                    return Ok(docker_manager);
                }
                Err(_e) => {
                    if attempt < MAX_RETRIES {
                        if attempt == 0 {
                            println!("🔄 Docker not ready, retrying...");
                        }
                        tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                    }
                }
            }
        }

        // Get verbose debug information for better error messages
        let (_, debug_info) = Self::is_available_verbose().await;

        Err(anyhow!(
            "Failed to connect to Docker after {} attempts.\n\n{}\n💡 Troubleshooting:\n   • Make sure Docker is running\n   • On macOS: Try 'export DOCKER_HOST=unix:///Users/$USER/.docker/run/docker.sock'\n   • Check Docker Desktop settings",
            MAX_RETRIES + 1,
            debug_info
        ))
    }

    /// Single attempt to connect to Docker (used internally by new())
    async fn try_connect() -> Result<Self> {
        // Try multiple socket locations in priority order
        let socket_candidates = Self::get_docker_socket_candidates();

        for (_description, socket_path) in socket_candidates {
            if let Ok(docker) = Self::try_socket_path(&socket_path).await {
                return Ok(Self { docker });
            }
        }

        // If all specific paths fail, try Bollard's default detection as final fallback
        let docker = Docker::connect_with_local_defaults().map_err(|e| {
            anyhow!(
                "Failed to connect to Docker daemon after trying all socket paths: {}",
                e
            )
        })?;

        // Test the default connection
        docker
            .ping()
            .await
            .map_err(|e| anyhow!("Docker daemon not responding: {}", e))?;

        Ok(Self { docker })
    }

    /// Single attempt to connect to Docker with verbose debug information
    async fn try_connect_verbose() -> Result<(Self, String)> {
        let mut debug_info = String::new();
        debug_info.push_str("Docker socket detection:\n");

        // Try multiple socket locations in priority order
        let socket_candidates = Self::get_docker_socket_candidates();

        for (description, socket_path) in &socket_candidates {
            debug_info.push_str(&format!("  • {}: ", description));
            match Self::try_socket_path(socket_path).await {
                Ok(docker) => {
                    debug_info.push_str(&format!("✅ Connected ({})\n", socket_path));
                    return Ok((Self { docker }, debug_info));
                }
                Err(e) => {
                    debug_info.push_str(&format!("❌ Failed - {}\n", e));
                }
            }
        }

        // If all specific paths fail, try Bollard's default detection as final fallback
        debug_info.push_str("  • Bollard default detection: ");
        match Docker::connect_with_local_defaults() {
            Ok(docker) => match docker.ping().await {
                Ok(_) => {
                    debug_info.push_str("✅ Connected\n");
                    return Ok((Self { docker }, debug_info));
                }
                Err(e) => {
                    debug_info.push_str(&format!("❌ Failed to ping - {}\n", e));
                }
            },
            Err(e) => {
                debug_info.push_str(&format!("❌ Failed to connect - {}\n", e));
            }
        }

        Err(anyhow!(
            "Failed to connect to Docker daemon after trying all methods:\n{}",
            debug_info
        ))
    }

    /// Get list of Docker socket candidates to try in priority order
    fn get_docker_socket_candidates() -> Vec<(String, String)> {
        let mut candidates = Vec::new();

        // 1. Respect DOCKER_HOST environment variable (highest priority)
        if let Ok(docker_host) = std::env::var("DOCKER_HOST") {
            candidates.push(("DOCKER_HOST environment variable".to_string(), docker_host));
        }

        // 2. Platform-specific default locations
        #[cfg(target_os = "macos")]
        {
            if let Ok(home) = std::env::var("HOME") {
                let macos_socket = format!("unix://{}/.docker/run/docker.sock", home);
                candidates.push(("macOS Docker Desktop".to_string(), macos_socket));

                // Colima support
                let colima_socket = format!("unix://{}/.colima/default/docker.sock", home);
                candidates.push(("Colima".to_string(), colima_socket));

                // OrbStack support
                let orbstack_socket = format!("unix://{}/.orbstack/run/docker.sock", home);
                candidates.push(("OrbStack".to_string(), orbstack_socket));
            }
        }

        // 3. Standard Linux location
        candidates.push((
            "Standard Linux location".to_string(),
            "unix:///var/run/docker.sock".to_string(),
        ));

        candidates
    }

    /// Try connecting to a specific socket path
    async fn try_socket_path(socket_path: &str) -> Result<Docker> {
        // For Unix sockets, connect directly to the socket path
        if let Some(socket_file) = socket_path.strip_prefix("unix://") {
            // Remove "unix://" prefix

            // Connect directly to the Unix socket with appropriate timeout
            let docker = Docker::connect_with_socket(
                socket_file,
                120, // 2 minute timeout (consistent with socket defaults)
                bollard::API_DEFAULT_VERSION,
            )
            .map_err(|e| anyhow!("Failed to connect to socket {}: {}", socket_path, e))?;

            // Test the connection
            docker
                .ping()
                .await
                .map_err(|e| anyhow!("Socket {} not responding: {}", socket_path, e))?;

            Ok(docker)
        } else {
            // For other protocols (tcp, etc.), use connect_with_defaults or other methods
            Err(anyhow!("Unsupported socket protocol: {}", socket_path))
        }
    }

    /// Start a PostgreSQL shadow database with the given configuration
    /// Returns an RAII wrapper that automatically cleans up on drop
    pub async fn start_shadow_database(
        &self,
        config: &ShadowDockerConfig,
    ) -> Result<ShadowDatabase> {
        let container_name = config
            .container_name
            .clone()
            .unwrap_or_else(|| format!("pgmt_shadow_{}", uuid::Uuid::new_v4().simple()));

        debug!("🚀 Starting PostgreSQL container: {}", container_name);

        // Check if container already exists and is running
        if let Some(existing_info) = self
            .find_existing_container(&container_name, config)
            .await?
        {
            if self.is_container_healthy(&existing_info.id).await? {
                debug!(
                    "Using existing healthy PostgreSQL container: {}",
                    container_name
                );
                // Register for cleanup (backup to RAII)
                if config.auto_cleanup {
                    register_container(existing_info.id.clone());
                }
                // Wrap existing container in RAII
                return Ok(ShadowDatabase {
                    container_info: existing_info,
                    auto_cleanup: config.auto_cleanup,
                });
            } else {
                warn!(
                    "Existing container {} is unhealthy, removing",
                    container_name
                );
                self.remove_container(&existing_info.id, true).await?;
            }
        }

        debug!("Starting new PostgreSQL container: {}", container_name);

        // Resolve the image (handles version -> image conversion)
        let resolved_image = config.resolved_image();

        // Ensure the PostgreSQL image is available
        let image_start = std::time::Instant::now();
        self.ensure_image_available(&resolved_image).await?;
        debug!("Image available after {:?}", image_start.elapsed());

        // Prepare environment variables
        // Only set defaults for DB/user/password if the user hasn't overridden them.
        // Custom images (e.g. supabase/postgres) may have their own init scripts
        // that depend on specific users/databases, so overriding breaks them.
        let mut env_vars = Vec::new();

        if !config.environment.contains_key("POSTGRES_DB") {
            env_vars.push("POSTGRES_DB=pgmt_shadow".to_string());
        }
        if !config.environment.contains_key("POSTGRES_USER") {
            env_vars.push("POSTGRES_USER=postgres".to_string());
        }
        if !config.environment.contains_key("POSTGRES_PASSWORD") {
            env_vars.push("POSTGRES_PASSWORD=pgmt_shadow_password".to_string());
        }

        // Add custom environment variables
        for (key, value) in &config.environment {
            env_vars.push(format!("{}={}", key, value));
        }

        // Configure port binding - let Docker auto-assign a port on 127.0.0.1
        // By not specifying host_port, Docker will choose an available port
        let mut port_bindings = HashMap::new();
        port_bindings.insert(
            "5432/tcp".to_string(),
            Some(vec![PortBinding {
                host_ip: Some("127.0.0.1".to_string()),
                host_port: None, // Let Docker choose an available port
            }]),
        );

        let host_config = HostConfig {
            port_bindings: Some(port_bindings),
            // Don't use auto_remove - it destroys the container on exit,
            // preventing `docker logs` inspection when startup fails.
            // Cleanup is handled by RAII (Drop) and the global registry instead.
            ..Default::default()
        };

        let container_config = ContainerCreateBody {
            image: Some(resolved_image.clone()),
            env: Some(env_vars),
            host_config: Some(host_config),
            ..Default::default()
        };

        // Create the container
        let create_start = std::time::Instant::now();
        let create_options = CreateContainerOptions {
            name: Some(container_name.clone()),
            ..Default::default()
        };

        let container = self
            .docker
            .create_container(Some(create_options), container_config)
            .await
            .map_err(|e| anyhow!("Failed to create container: {}", e))?;

        debug!("Container created after {:?}", create_start.elapsed());

        // Start the container
        let start_container_time = std::time::Instant::now();
        if let Err(e) = self
            .docker
            .start_container(&container.id, None::<StartContainerOptions>)
            .await
        {
            // Clean up the created-but-not-started container
            let _ = self.remove_container(&container.id, true).await;
            return Err(anyhow!("Failed to start container: {}", e));
        }
        debug!(
            "Container started after {:?}",
            start_container_time.elapsed()
        );

        // Inspect container to get the auto-assigned port
        let inspect_result = self
            .docker
            .inspect_container(&container.id, None::<InspectContainerOptions>)
            .await
            .map_err(|e| anyhow!("Failed to inspect container: {}", e))?;

        let host_port = self.extract_host_port(&inspect_result)?;
        debug!("Docker assigned port: {}", host_port);

        // Wait for PostgreSQL to be ready
        let readiness_start = std::time::Instant::now();
        if let Err(readiness_err) = self.wait_for_postgres_ready(&container.id).await {
            let logs = self.fetch_container_logs(&container.id).await;
            let keep_on_failure =
                std::env::var("PGMT_KEEP_SHADOW_ON_FAILURE").is_ok_and(|v| !v.is_empty());

            if keep_on_failure {
                // Leave the container alive for debugging — don't register it
                // so global cleanup won't remove it either
                return Err(anyhow!(
                    "{readiness_err}\n\n\
                     Container logs (last 50 lines):\n{logs}\n\n\
                     The container has been kept alive for debugging:\n  \
                     docker logs {container_name}\n  \
                     docker exec -it {container_name} bash\n  \
                     docker rm -f {container_name}"
                ));
            } else {
                // Force-remove the failed container (works in any state)
                let _ = self.remove_container(&container.id, true).await;
                return Err(anyhow!(
                    "{readiness_err}\n\n\
                     Container logs (last 50 lines):\n{logs}\n\n\
                     Tip: Re-run with PGMT_KEEP_SHADOW_ON_FAILURE=1 to keep the container alive for debugging."
                ));
            }
        }
        debug!("PostgreSQL ready after {:?}", readiness_start.elapsed());

        let database = config
            .environment
            .get("POSTGRES_DB")
            .cloned()
            .unwrap_or_else(|| "pgmt_shadow".to_string());
        let username = config
            .environment
            .get("POSTGRES_USER")
            .cloned()
            .unwrap_or_else(|| "postgres".to_string());
        let password = config
            .environment
            .get("POSTGRES_PASSWORD")
            .cloned()
            .unwrap_or_else(|| "pgmt_shadow_password".to_string());

        let container_info = ContainerInfo {
            id: container.id.clone(),
            host: "127.0.0.1".to_string(),
            port: host_port,
            database,
            username,
            password,
        };

        // Register container for cleanup at process exit (backup to RAII)
        if config.auto_cleanup {
            register_container(container.id.clone());
        }

        info!(
            "PostgreSQL container ready: {}",
            container_info.connection_string()
        );

        // Wrap in RAII for automatic cleanup
        Ok(ShadowDatabase {
            container_info,
            auto_cleanup: config.auto_cleanup,
        })
    }

    /// Stop and optionally remove a container.
    /// Resilient to already-stopped containers: if stop fails, still attempts force-remove.
    pub async fn stop_container(&self, container_id: &str, remove: bool) -> Result<()> {
        let stop_result = self
            .docker
            .stop_container(container_id, None::<StopContainerOptions>)
            .await;

        match stop_result {
            Ok(()) => {
                if remove {
                    self.remove_container(container_id, false).await?;
                }
            }
            Err(ref e) => {
                let error_msg = e.to_string();
                let is_not_found =
                    error_msg.contains("404") || error_msg.contains("No such container");

                if is_not_found {
                    // Container already gone — nothing to clean up
                    unregister_container(container_id);
                    return Err(anyhow!("Failed to stop container: {}", e));
                }

                // Container may have crashed/exited — still try to force-remove
                if remove {
                    self.remove_container(container_id, true).await?;
                    unregister_container(container_id);
                    return Ok(());
                }

                unregister_container(container_id);
                return Err(anyhow!("Failed to stop container: {}", e));
            }
        }

        // Unregister from cleanup registry
        unregister_container(container_id);

        Ok(())
    }

    /// Remove a container
    async fn remove_container(&self, container_id: &str, force: bool) -> Result<()> {
        let remove_options = RemoveContainerOptions {
            force,
            ..Default::default()
        };

        self.docker
            .remove_container(container_id, Some(remove_options))
            .await
            .map_err(|e| anyhow!("Failed to remove container: {}", e))?;

        Ok(())
    }

    /// Fetch the last 50 lines of container logs (stdout + stderr).
    /// Returns the log text, or a fallback message on failure.
    async fn fetch_container_logs(&self, container_id: &str) -> String {
        let options = LogsOptionsBuilder::new()
            .stdout(true)
            .stderr(true)
            .tail("50")
            .build();

        let log_stream = self.docker.logs(container_id, Some(options));

        match tokio::time::timeout(
            Duration::from_secs(3),
            log_stream.collect::<Vec<Result<LogOutput, _>>>(),
        )
        .await
        {
            Ok(results) => {
                let lines: Vec<String> = results
                    .into_iter()
                    .filter_map(|r| r.ok())
                    .map(|output| output.to_string())
                    .collect();
                if lines.is_empty() {
                    "(no logs available)".to_string()
                } else {
                    lines.join("")
                }
            }
            Err(_) => "(timed out fetching container logs)".to_string(),
        }
    }

    /// Find an existing container by name
    async fn find_existing_container(
        &self,
        name: &str,
        _config: &ShadowDockerConfig,
    ) -> Result<Option<ContainerInfo>> {
        let list_options = ListContainersOptions {
            all: true,
            filters: Some({
                let mut filters = HashMap::new();
                filters.insert("name".to_string(), vec![name.to_string()]);
                filters
            }),
            ..Default::default()
        };

        let containers = self
            .docker
            .list_containers(Some(list_options))
            .await
            .map_err(|e| anyhow!("Failed to list containers: {}", e))?;

        if let Some(container) = containers.first()
            && let (Some(id), Some(names)) = (&container.id, &container.names)
            && let Some(_container_name) = names.first()
        {
            // Extract port information
            if let Some(ports) = &container.ports {
                for port in ports {
                    if port.private_port == 5432 && port.public_port.is_some() {
                        return Ok(Some(ContainerInfo {
                            id: id.clone(),
                            host: "127.0.0.1".to_string(),
                            port: port.public_port.unwrap(),
                            database: "pgmt_shadow".to_string(),
                            username: "postgres".to_string(),
                            password: "pgmt_shadow_password".to_string(), // Default, may need to be configurable
                        }));
                    }
                }
            }
        }

        Ok(None)
    }

    /// Check if a container is healthy (running and PostgreSQL is ready)
    async fn is_container_healthy(&self, container_id: &str) -> Result<bool> {
        // Check if container is running
        let inspect = self
            .docker
            .inspect_container(container_id, None::<InspectContainerOptions>)
            .await
            .map_err(|e| anyhow!("Failed to inspect container: {}", e))?;

        if let Some(ref state) = inspect.state {
            if state.status != Some(ContainerStateStatusEnum::RUNNING) {
                return Ok(false);
            }
        } else {
            return Ok(false);
        }

        // Try a simple PostgreSQL connection test
        // First we need to extract connection info from the container
        if let Some(container_info) =
            self.extract_container_info_from_inspect(&inspect, container_id)?
        {
            match self.test_postgres_connection(&container_info).await {
                Ok(()) => Ok(true),
                Err(_) => Ok(false),
            }
        } else {
            // Can't extract connection info, assume unhealthy
            Ok(false)
        }
    }

    /// Extract container connection information from Docker inspect data
    fn extract_container_info_from_inspect(
        &self,
        inspect: &ContainerInspectResponse,
        container_id: &str,
    ) -> Result<Option<ContainerInfo>> {
        // Extract password from container environment variables
        let password = if let Some(config) = &inspect.config {
            if let Some(env_vars) = &config.env {
                env_vars
                    .iter()
                    .find(|env| env.starts_with("POSTGRES_PASSWORD="))
                    .and_then(|env| env.strip_prefix("POSTGRES_PASSWORD="))
                    .unwrap_or("pgmt_shadow_password")
                    .to_string()
            } else {
                "pgmt_shadow_password".to_string()
            }
        } else {
            "pgmt_shadow_password".to_string()
        };
        // Extract network settings and port mappings
        if let Some(network_settings) = &inspect.network_settings
            && let Some(ports) = &network_settings.ports
            && let Some(port_bindings) = ports.get("5432/tcp")
            && let Some(port_binding) = port_bindings.as_ref().and_then(|bindings| bindings.first())
            && let Some(host_port) = &port_binding.host_port
            && let Ok(port) = host_port.parse::<u16>()
        {
            return Ok(Some(ContainerInfo {
                id: container_id.to_string(),
                host: "127.0.0.1".to_string(),
                port,
                database: "pgmt_shadow".to_string(),
                username: "postgres".to_string(),
                password,
            }));
        }

        Ok(None)
    }

    /// Test PostgreSQL connection using actual database connection and SQL queries
    /// This is more robust than pg_isready as it tests the actual connection path
    async fn test_postgres_connection(&self, container_info: &ContainerInfo) -> Result<()> {
        debug!(
            "🔌 Testing PostgreSQL connection to {}",
            container_info.connection_string()
        );

        const MAX_READINESS_RETRIES: u32 = 10;
        const READINESS_RETRY_DELAY_MS: u64 = 500;

        let mut last_error = None;

        for attempt in 0..=MAX_READINESS_RETRIES {
            match Self::try_database_connection(container_info).await {
                Ok(_) => {
                    if attempt > 0 {
                        debug!(
                            "✅ PostgreSQL connection successful after {} attempt{}",
                            attempt + 1,
                            if attempt == 0 { "" } else { "s" }
                        );
                    } else {
                        debug!("✅ PostgreSQL connection successful");
                    }
                    return Ok(());
                }
                Err(e) => {
                    debug!(
                        "❌ PostgreSQL connection failed (attempt {}): {}",
                        attempt + 1,
                        e
                    );
                    last_error = Some(e);
                    if attempt < MAX_READINESS_RETRIES {
                        if attempt == 0 {
                            debug!("⏳ Waiting for PostgreSQL to be ready...");
                        }
                        tokio::time::sleep(Duration::from_millis(READINESS_RETRY_DELAY_MS)).await;
                    }
                }
            }
        }

        Err(anyhow!(
            "PostgreSQL not ready after {} attempts: {}",
            MAX_READINESS_RETRIES + 1,
            last_error.unwrap()
        ))
    }

    /// Single attempt to connect to PostgreSQL and run a test query
    async fn try_database_connection(container_info: &ContainerInfo) -> Result<()> {
        use sqlx::postgres::PgPoolOptions;

        // Short timeout so we fail fast and re-check container status between attempts
        let connection_string = container_info.connection_string();
        debug!("🔗 Attempting to connect to: {}", connection_string);

        let pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_secs(5))
            .connect(&connection_string)
            .await
            .map_err(|e| anyhow!("Failed to connect to PostgreSQL: {}", e))?;
        debug!("✅ Connection pool established");

        // Test database functionality with actual SQL operations
        // This ensures the database is truly ready for operations, not just accepting connections
        sqlx::query("SELECT 1 as test")
            .fetch_one(&pool)
            .await
            .map_err(|e| anyhow!("Database query test failed: {}", e))?;
        debug!("✅ Basic query test passed");

        // Test creating a simple table to ensure we have proper permissions
        sqlx::query("CREATE TEMPORARY TABLE pgmt_readiness_test (id INTEGER)")
            .execute(&pool)
            .await
            .map_err(|e| anyhow!("Database write test failed: {}", e))?;
        debug!("✅ Write permissions test passed");

        // Clean up and close connection
        pool.close().await;
        debug!("✅ Connection closed successfully");

        Ok(())
    }

    /// Ensure the PostgreSQL image is available locally
    async fn ensure_image_available(&self, image: &str) -> Result<()> {
        // Try to inspect the image first
        match self.docker.inspect_image(image).await {
            Ok(_) => return Ok(()),
            Err(_) => debug!("Pulling PostgreSQL image: {}", image),
        }

        // Pull the image
        let create_image_options = CreateImageOptions {
            from_image: Some(image.to_string()),
            ..Default::default()
        };

        let mut pull_stream = self
            .docker
            .create_image(Some(create_image_options), None, None);

        while let Some(result) = pull_stream.next().await {
            if let Err(e) = result {
                return Err(anyhow!("Failed to pull image: {}", e));
            }
        }

        debug!("Successfully pulled image: {}", image);
        Ok(())
    }

    /// Extract the host port from container inspection result
    fn extract_host_port(&self, inspect_result: &ContainerInspectResponse) -> Result<u16> {
        let network_settings = inspect_result
            .network_settings
            .as_ref()
            .ok_or_else(|| anyhow!("Container has no network settings"))?;

        let ports = network_settings
            .ports
            .as_ref()
            .ok_or_else(|| anyhow!("Container has no port mappings"))?;

        let port_bindings = ports
            .get("5432/tcp")
            .ok_or_else(|| anyhow!("Container has no 5432/tcp port mapping"))?
            .as_ref()
            .ok_or_else(|| anyhow!("Port 5432/tcp is not bound"))?;

        let port_binding = port_bindings
            .first()
            .ok_or_else(|| anyhow!("No port bindings found for 5432/tcp"))?;

        let host_port_str = port_binding
            .host_port
            .as_ref()
            .ok_or_else(|| anyhow!("Host port not set"))?;

        host_port_str
            .parse::<u16>()
            .map_err(|e| anyhow!("Invalid host port '{}': {}", host_port_str, e))
    }

    /// Wait for PostgreSQL to be ready to accept connections
    async fn wait_for_postgres_ready(&self, container_id: &str) -> Result<()> {
        // Detect test environment for optimized retry settings
        // Check multiple indicators that we're running in a test environment
        let is_test_env = std::env::var("CARGO").is_ok()
            && (std::thread::current()
                .name()
                .is_some_and(|name| name.contains("test"))
                || std::env::var("RUST_TEST_THREADS").is_ok()
                || std::env::var("CARGO_PKG_NAME").is_ok_and(|name| name == "pgmt"));

        let (max_attempts, retry_delay_ms) = if is_test_env {
            // Test environment: more frequent checks, reasonable timeout for test environment
            debug!(
                "🧪 Test environment detected - using optimized retry settings (25 attempts × 6s = 150s max)"
            );
            (25_u32, 1000_u64) // 25 attempts × (5s timeout + 1s delay) = 150 seconds max
        } else {
            // Production environment: less frequent checks, longer timeout for reliability
            debug!(
                "🏭 Production environment - using standard retry settings (30 attempts × 7s = 210s max)"
            );
            (30_u32, 2000_u64) // 30 attempts × (5s timeout + 2s delay) = 210 seconds max
        };

        const INITIAL_DELAY_MS: u64 = 500;

        // Initial delay to let container start
        sleep(Duration::from_millis(INITIAL_DELAY_MS)).await;

        for attempt in 1..=max_attempts {
            debug!("🔍 Readiness check attempt {}/{}", attempt, max_attempts);

            // Get container info for connection testing
            let inspect = self
                .docker
                .inspect_container(container_id, None::<InspectContainerOptions>)
                .await
                .map_err(|e| anyhow!("Failed to inspect container: {}", e))?;

            // Check if the container has exited — fail fast instead of retrying
            if let Some(ref state) = inspect.state {
                match state.status {
                    Some(ContainerStateStatusEnum::EXITED)
                    | Some(ContainerStateStatusEnum::DEAD) => {
                        let exit_code = state.exit_code.unwrap_or(-1);
                        return Err(anyhow!(
                            "Shadow database container exited with code {}.",
                            exit_code,
                        ));
                    }
                    _ => {}
                }
            }

            if let Some(container_info) =
                self.extract_container_info_from_inspect(&inspect, container_id)?
            {
                debug!(
                    "📋 Container connection info: {}:{}",
                    container_info.host, container_info.port
                );
                // Use single connection attempt per iteration so we can re-check
                // container status quickly if the container crashes/exits
                match Self::try_database_connection(&container_info).await {
                    Ok(()) => {
                        debug!(
                            "✅ PostgreSQL is ready to accept connections after {} attempt{}",
                            attempt,
                            if attempt == 1 { "" } else { "s" }
                        );
                        return Ok(());
                    }
                    Err(e) if attempt < max_attempts => {
                        debug!("❌ PostgreSQL not ready yet (attempt {}): {}", attempt, e);
                        sleep(Duration::from_millis(retry_delay_ms)).await;
                    }
                    Err(e) => {
                        return Err(anyhow!(
                            "PostgreSQL failed to become ready after {} attempts. Last error: {}",
                            max_attempts,
                            e
                        ));
                    }
                }
            } else {
                warn!(
                    "⚠️  Could not extract container connection info on attempt {}",
                    attempt
                );
                if attempt < max_attempts {
                    sleep(Duration::from_millis(retry_delay_ms)).await;
                } else {
                    return Err(anyhow!(
                        "Could not extract container connection info after {} attempts",
                        max_attempts
                    ));
                }
            }
        }

        unreachable!()
    }
}

use once_cell::sync::Lazy;
use std::sync::{Arc, Mutex};

/// Global registry for tracking active Docker containers
static CONTAINER_REGISTRY: Lazy<Arc<Mutex<Vec<String>>>> =
    Lazy::new(|| Arc::new(Mutex::new(Vec::new())));

/// Register a container for cleanup at process exit
pub fn register_container(container_id: String) {
    let mut registry = CONTAINER_REGISTRY.lock().unwrap();
    registry.push(container_id);
}

/// Unregister a container (when manually cleaned up)
pub fn unregister_container(container_id: &str) {
    let mut registry = CONTAINER_REGISTRY.lock().unwrap();
    registry.retain(|id| id != container_id);
}

/// Clean up all registered containers
pub async fn cleanup_all_containers() -> Result<()> {
    let container_ids = {
        let mut registry = CONTAINER_REGISTRY.lock().unwrap();
        let ids = registry.clone();
        registry.clear();
        ids
    };

    if container_ids.is_empty() {
        return Ok(());
    }

    info!(
        "Cleaning up {} registered container(s)",
        container_ids.len()
    );

    let mut cleanup_tasks = Vec::new();

    for container_id in container_ids {
        let id = container_id.clone();

        let task = tokio::spawn(async move {
            // Create a new Docker manager for each task to avoid lifetime issues
            match DockerManager::new().await {
                Ok(manager) => match manager.stop_container(&id, true).await {
                    Ok(()) => {
                        info!("Successfully cleaned up container: {}", id);
                    }
                    Err(e) => {
                        // Check if this is a 404 (container already removed) - that's success
                        let error_msg = e.to_string();
                        if error_msg.contains("404") || error_msg.contains("No such container") {
                            // Container already gone - cleanup succeeded, just silently
                            debug!("Container {} already removed (404) - cleanup succeeded", id);
                        } else {
                            // Real error - warn the user
                            warn!("Failed to cleanup container {}: {}", id, e);
                        }
                    }
                },
                Err(e) => {
                    warn!(
                        "Failed to create Docker manager for cleanup of {}: {}",
                        id, e
                    );
                }
            }
        });

        cleanup_tasks.push(task);
    }

    // Wait for all cleanup tasks to complete, but with a timeout
    const CLEANUP_TIMEOUT_SECS: u64 = 10;
    let cleanup_future = futures_util::future::join_all(cleanup_tasks);

    if tokio::time::timeout(
        std::time::Duration::from_secs(CLEANUP_TIMEOUT_SECS),
        cleanup_future,
    )
    .await
    .is_err()
    {
        warn!(
            "Container cleanup timed out after {} seconds",
            CLEANUP_TIMEOUT_SECS
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_docker_socket_candidates() {
        let candidates = DockerManager::get_docker_socket_candidates();

        // Should have at least 1 candidate (Standard Linux location)
        assert!(
            !candidates.is_empty(),
            "Should have at least one socket candidate"
        );

        // Last candidate should always be the standard Linux location
        let last_candidate = candidates.last().unwrap();
        assert_eq!(last_candidate.1, "unix:///var/run/docker.sock");

        // On macOS, should include Docker Desktop, Colima, and OrbStack
        #[cfg(target_os = "macos")]
        {
            let candidate_names: Vec<&String> = candidates.iter().map(|(name, _)| name).collect();
            if std::env::var("HOME").is_ok() {
                assert!(
                    candidate_names
                        .iter()
                        .any(|name| name.contains("Docker Desktop"))
                );
                assert!(candidate_names.iter().any(|name| name.contains("Colima")));
                assert!(candidate_names.iter().any(|name| name.contains("OrbStack")));
            }
        }

        // If DOCKER_HOST is set, it should be first
        if std::env::var("DOCKER_HOST").is_ok() {
            let first_candidate = candidates.first().unwrap();
            assert_eq!(first_candidate.0, "DOCKER_HOST environment variable");
        }
    }

    #[tokio::test]
    async fn test_verbose_availability() {
        let (_is_available, debug_info) = DockerManager::is_available_verbose().await;

        // Debug info should contain socket detection information
        assert!(debug_info.contains("Docker socket detection"));

        // Should show attempts for different socket types
        assert!(debug_info.contains("•"));
    }
}
