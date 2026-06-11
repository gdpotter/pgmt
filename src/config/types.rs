use clap::Args;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Raw configuration input - all fields Optional for merging
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub struct ConfigInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub databases: Option<DatabasesInput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub directories: Option<DirectoriesInput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub objects: Option<ObjectsInput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub migration: Option<MigrationInput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaInput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub docker: Option<DockerInput>,
}

/// Resolved configuration with all defaults applied
#[derive(Debug, Clone, Default)]
pub struct Config {
    pub databases: Databases,
    pub directories: Directories,
    pub objects: Objects,
    pub migration: Migration,
    pub schema: Schema,
    pub docker: Docker,
}

// Database configuration
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub struct DatabasesInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dev_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shadow_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shadow: Option<ShadowDatabaseInput>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub struct ShadowDatabaseInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// How an external `url` shadow is brought back to its baseline between
    /// runs. Ignored for Docker-managed shadows (always `template`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reset: Option<ShadowResetMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub docker: Option<ShadowDockerInput>,
}

/// Reset strategy for an external `shadow.url` database.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ShadowResetMode {
    /// Drop the schemas pgmt manages; never create or drop databases. Safe
    /// when the server is shared or its lifecycle belongs to something else.
    #[default]
    Clean,
    /// Treat the database as pgmt's own: snapshot its first-contact state
    /// into a template database and drop/recreate it from that template each
    /// run. Requires CREATEDB. Set this only when the database exists solely
    /// for pgmt (e.g. a CI service container).
    Template,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub struct ShadowDockerInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    /// Platform to request when pulling/running the image (e.g. "linux/amd64").
    /// Needed for images only published for one architecture (e.g. postgis/postgis
    /// has no arm64 build) so they can run under emulation on other hosts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub environment: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_cleanup: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volumes: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Databases {
    pub dev: String,
    pub shadow: ShadowDatabase,
    pub target: Option<String>,
}

#[derive(Debug, Clone)]
pub enum ShadowDatabase {
    Auto, // Docker mode with default configuration
    Url {
        url: String,
        reset: ShadowResetMode,
    },
    Docker(ShadowDockerConfig),
}

impl ShadowDatabase {
    /// Get a connection string for the shadow database
    pub async fn get_connection_string(&self) -> anyhow::Result<String> {
        match self {
            ShadowDatabase::Auto => {
                // Auto mode is just Docker with default configuration
                let default_config = ShadowDockerConfig::default();
                Self::generate_docker_shadow_url(&default_config).await
            }
            ShadowDatabase::Url { url, reset } => {
                if *reset == ShadowResetMode::Template {
                    crate::db::template::ensure_reset_by_url(url).await?;
                }
                Ok(url.clone())
            }
            ShadowDatabase::Docker(config) => Self::generate_docker_shadow_url(config).await,
        }
    }

    /// Generate a shadow database URL for Docker mode
    async fn generate_docker_shadow_url(config: &ShadowDockerConfig) -> anyhow::Result<String> {
        use crate::docker::DockerManager;

        let docker_manager = DockerManager::new().await?;
        let shadow_db = docker_manager.start_shadow_database(config).await?;
        // Convert to connection string, keeping container alive via global registry
        Ok(shadow_db.into_connection_string())
    }
}

#[derive(Debug, Clone)]
pub struct ShadowDockerConfig {
    pub version: Option<String>,
    pub image: String,
    /// Platform to request when pulling/running the image (e.g. "linux/amd64").
    pub platform: Option<String>,
    pub environment: HashMap<String, String>,
    pub container_name: Option<String>,
    pub auto_cleanup: bool,
    #[allow(dead_code)] // Future feature: Docker volume mounting
    pub volumes: Option<Vec<String>>,
    #[allow(dead_code)] // Future feature: Docker network configuration
    pub network: Option<String>,
}

impl ShadowDockerConfig {
    /// Resolve the Docker image from version or use the image directly
    /// Precedence: explicit image > version > default
    pub fn resolved_image(&self) -> String {
        // If an explicit image is set, use it directly
        if !self.image.is_empty() && self.image != Self::default_image() {
            return self.image.clone();
        }

        // If a version is specified, construct the image
        if let Some(version) = &self.version {
            return format!("postgres:{}-alpine", version);
        }

        // Fall back to the configured image (which may be default)
        self.image.clone()
    }

    /// Get the default PostgreSQL image
    fn default_image() -> String {
        "postgres:18-alpine".to_string()
    }
}

// Directory configuration
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub struct DirectoriesInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub migrations_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub baselines_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub roles_file: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Directories {
    pub schema: String,
    pub migrations: String,
    pub baselines: String,
    pub roles: String,
}

// Object filtering configuration
// Note: Boolean toggles (comments, grants, triggers, extensions) have been removed.
// Schema files are now the source of truth - what's in your files is what gets managed.
// Use exclude patterns to filter objects during init import.
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub struct ObjectsInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include: Option<ObjectIncludeInput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclude: Option<ObjectExcludeInput>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub struct ObjectIncludeInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schemas: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tables: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub struct ObjectExcludeInput {
    /// Accepts the legacy `exclude_schemas` key for configs written before
    /// the rename to match `include.schemas`.
    #[serde(alias = "exclude_schemas", skip_serializing_if = "Option::is_none")]
    pub schemas: Option<Vec<String>>,
    #[serde(alias = "exclude_tables", skip_serializing_if = "Option::is_none")]
    pub tables: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default)]
pub struct Objects {
    pub include: ObjectInclude,
    pub exclude: ObjectExclude,
}

#[derive(Debug, Clone, Default)]
pub struct ObjectInclude {
    pub schemas: Vec<String>,
    pub tables: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ObjectExclude {
    pub schemas: Vec<String>,
    pub tables: Vec<String>,
}

// Migration configuration
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub struct MigrationInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validate_baseline_consistency: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_baselines_by_default: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tracking_table: Option<TrackingTableInput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column_order: Option<ColumnOrderMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename_prefix: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub struct TrackingTableInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Migration {
    pub default_mode: String,
    pub validate_baseline_consistency: bool,
    pub create_baselines_by_default: bool,
    pub tracking_table: TrackingTable,
    pub column_order: ColumnOrderMode,
    pub filename_prefix: String,
}

#[derive(Debug, Clone)]
pub struct TrackingTable {
    pub schema: String,
    pub name: String,
}

/// Column order validation mode for migration generation
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnOrderMode {
    /// Error if new columns aren't at the end of the table
    #[default]
    Strict,
    /// Warn but generate migration anyway
    Warn,
    /// No validation, allow any ordering
    Relaxed,
}

// Docker configuration
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub struct DockerInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_cleanup: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub check_system_identifier: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct Docker {
    pub auto_cleanup: bool,
    pub check_system_identifier: bool,
}

// Schema configuration
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub struct SchemaInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub augment_dependencies_from_files: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validate_file_dependencies: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verbose_file_processing: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub augment_dependencies_from_files: bool,
    pub validate_file_dependencies: bool,
    pub verbose_file_processing: bool,
}

// CLI argument groups for command-specific options
#[derive(Debug, Clone, Default, Args)]
pub struct DatabaseArgs {
    #[arg(long, help = "Development database URL")]
    pub dev_url: Option<String>,

    #[arg(long, help = "Shadow database URL (overrides auto mode)")]
    pub shadow_url: Option<String>,

    #[arg(long, help = "Production/target database URL")]
    pub target_url: Option<String>,
}

#[derive(Debug, Clone, Default, Args)]
pub struct DirectoryArgs {
    #[arg(long, help = "Schema directory path")]
    pub schema_dir: Option<String>,

    #[arg(long, help = "Migrations directory path")]
    pub migrations_dir: Option<String>,

    #[arg(long, help = "Baselines directory path")]
    pub baselines_dir: Option<String>,

    #[arg(long, help = "Roles SQL file path")]
    pub roles_file: Option<String>,
}

#[derive(Debug, Clone, Default, Args)]
pub struct SchemaArgs {
    #[arg(long, help = "Enable file-based dependency augmentation")]
    pub augment_file_dependencies: bool,

    #[arg(long, help = "Disable file-based dependency augmentation")]
    pub no_augment_file_dependencies: bool,

    #[arg(long, help = "Enable validation of file dependencies")]
    pub validate_file_dependencies: bool,

    #[arg(long, help = "Disable validation of file dependencies")]
    pub no_validate_file_dependencies: bool,

    #[arg(long, help = "Enable verbose file processing output")]
    pub verbose_file_processing: bool,
}

#[derive(Debug, Clone, Default, Args)]
pub struct ObjectFilterArgs {
    #[arg(long, help = "Include only these schemas (glob patterns)")]
    pub schemas: Option<Vec<String>>,

    #[arg(long, help = "Include only these tables (glob patterns)")]
    pub tables: Option<Vec<String>>,

    #[arg(long, help = "Exclude these schemas (glob patterns)")]
    pub exclude_schemas: Option<Vec<String>>,

    #[arg(long, help = "Exclude these tables (glob patterns)")]
    pub exclude_tables: Option<Vec<String>>,
}

// Conversion functions from CLI args to config input
impl From<DatabaseArgs> for DatabasesInput {
    fn from(args: DatabaseArgs) -> Self {
        Self {
            dev_url: args.dev_url,
            shadow_url: args.shadow_url,
            target_url: args.target_url,
            shadow: None, // Shadow config comes from file only
        }
    }
}

impl From<DirectoryArgs> for DirectoriesInput {
    fn from(args: DirectoryArgs) -> Self {
        Self {
            schema_dir: args.schema_dir,
            migrations_dir: args.migrations_dir,
            baselines_dir: args.baselines_dir,
            roles_file: args.roles_file,
        }
    }
}

impl From<ObjectFilterArgs> for ObjectsInput {
    fn from(args: ObjectFilterArgs) -> Self {
        let include = if args.schemas.is_some() || args.tables.is_some() {
            Some(ObjectIncludeInput {
                schemas: args.schemas,
                tables: args.tables,
            })
        } else {
            None
        };

        let exclude = if args.exclude_schemas.is_some() || args.exclude_tables.is_some() {
            Some(ObjectExcludeInput {
                schemas: args.exclude_schemas,
                tables: args.exclude_tables,
            })
        } else {
            None
        };

        Self { include, exclude }
    }
}

impl From<SchemaArgs> for SchemaInput {
    fn from(args: SchemaArgs) -> Self {
        Self {
            augment_dependencies_from_files: if args.no_augment_file_dependencies {
                Some(false)
            } else if args.augment_file_dependencies {
                Some(true)
            } else {
                None
            },
            validate_file_dependencies: if args.no_validate_file_dependencies {
                Some(false)
            } else if args.validate_file_dependencies {
                Some(true)
            } else {
                None
            },
            verbose_file_processing: if args.verbose_file_processing {
                Some(true)
            } else {
                None
            },
        }
    }
}
