use crate::config::{merge::Merge, types::*};
use anyhow::{Result, anyhow};

pub struct ConfigBuilder {
    config_input: ConfigInput,
}

impl ConfigBuilder {
    pub fn new() -> Self {
        Self {
            config_input: ConfigInput::default(),
        }
    }

    pub fn with_file(mut self, file_input: ConfigInput) -> Self {
        self.config_input = self.config_input.merge(file_input);
        self
    }

    pub fn with_cli_args(mut self, cli_input: ConfigInput) -> Self {
        self.config_input = self.config_input.merge(cli_input);
        self
    }

    pub fn resolve(self) -> Result<Config> {
        let defaults = Config::default();

        Ok(Config {
            databases: self.resolve_databases(&defaults.databases)?,
            directories: self.resolve_directories(&defaults.directories),
            objects: self.resolve_objects(&defaults.objects),
            migration: self.resolve_migration(&defaults.migration),
            schema: self.resolve_schema(&defaults.schema),
            docker: self.resolve_docker(&defaults.docker),
        })
    }

    fn resolve_databases(&self, defaults: &Databases) -> Result<Databases> {
        let db_input = self.config_input.databases.as_ref();

        let dev_url = db_input
            .and_then(|d| d.dev_url.as_ref())
            .cloned()
            .or_else(|| std::env::var("DEV_DATABASE_URL").ok())
            .unwrap_or_else(|| defaults.dev.clone());

        let shadow = self.resolve_shadow_database(db_input, &defaults.shadow)?;

        let target = db_input
            .and_then(|d| d.target_url.as_ref())
            .cloned()
            .or_else(|| std::env::var("TARGET_DATABASE_URL").ok())
            .or_else(|| defaults.target.clone());

        Ok(Databases {
            dev: dev_url,
            shadow,
            target,
        })
    }

    fn resolve_shadow_database(
        &self,
        db_input: Option<&DatabasesInput>,
        _default: &ShadowDatabase,
    ) -> Result<ShadowDatabase> {
        // CLI shadow_url takes highest precedence
        if let Some(url) = db_input.and_then(|d| d.shadow_url.as_ref()) {
            return Ok(ShadowDatabase::Url(url.clone()));
        }

        // Check config file shadow configuration
        if let Some(shadow_input) = db_input.and_then(|d| d.shadow.as_ref()) {
            // Explicit URL in config
            if let Some(url) = &shadow_input.url {
                return Ok(ShadowDatabase::Url(url.clone()));
            }

            // Docker configuration
            if let Some(docker_input) = &shadow_input.docker {
                let defaults = ShadowDockerConfig::default();
                let docker_config = ShadowDockerConfig {
                    version: docker_input.version.clone(),
                    image: docker_input
                        .image
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| defaults.image.clone()),
                    environment: docker_input
                        .environment
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| defaults.environment.clone()),
                    container_name: docker_input.container_name.clone(),
                    auto_cleanup: docker_input.auto_cleanup.unwrap_or(defaults.auto_cleanup),
                    volumes: docker_input.volumes.clone(),
                    network: docker_input.network.clone(),
                };
                return Ok(ShadowDatabase::Docker(docker_config));
            }

            // Check auto flag
            if let Some(false) = shadow_input.auto {
                return Err(anyhow!(
                    "Shadow database auto mode is disabled but no URL provided. Use --shadow-url or enable auto mode"
                ));
            }
        }

        // Default to auto mode
        Ok(ShadowDatabase::Auto)
    }

    fn resolve_directories(&self, defaults: &Directories) -> Directories {
        let dir_input = self.config_input.directories.as_ref();

        Directories {
            schema: dir_input
                .and_then(|d| d.schema_dir.as_ref())
                .cloned()
                .unwrap_or_else(|| defaults.schema.clone()),
            migrations: dir_input
                .and_then(|d| d.migrations_dir.as_ref())
                .cloned()
                .unwrap_or_else(|| defaults.migrations.clone()),
            baselines: dir_input
                .and_then(|d| d.baselines_dir.as_ref())
                .cloned()
                .unwrap_or_else(|| defaults.baselines.clone()),
            roles: dir_input
                .and_then(|d| d.roles_file.as_ref())
                .cloned()
                .unwrap_or_else(|| defaults.roles.clone()),
        }
    }

    fn resolve_objects(&self, defaults: &Objects) -> Objects {
        let obj_input = self.config_input.objects.as_ref();

        let include = obj_input
            .and_then(|o| o.include.as_ref())
            .map(|i| ObjectInclude {
                schemas: i.schemas.as_ref().cloned().unwrap_or_default(),
                tables: i.tables.as_ref().cloned().unwrap_or_default(),
            })
            .unwrap_or_else(|| defaults.include.clone());

        let exclude = obj_input
            .and_then(|o| o.exclude.as_ref())
            .map(|e| ObjectExclude {
                schemas: e
                    .exclude_schemas
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| defaults.exclude.schemas.clone()),
                tables: e
                    .exclude_tables
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| defaults.exclude.tables.clone()),
            })
            .unwrap_or_else(|| defaults.exclude.clone());

        Objects { include, exclude }
    }

    fn resolve_migration(&self, defaults: &Migration) -> Migration {
        let mig_input = self.config_input.migration.as_ref();

        let tracking_table = mig_input
            .and_then(|m| m.tracking_table.as_ref())
            .map(|t| TrackingTable {
                schema: t
                    .schema
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| defaults.tracking_table.schema.clone()),
                name: t
                    .name
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| defaults.tracking_table.name.clone()),
            })
            .unwrap_or_else(|| defaults.tracking_table.clone());

        Migration {
            default_mode: mig_input
                .and_then(|m| m.default_mode.as_ref())
                .cloned()
                .unwrap_or_else(|| defaults.default_mode.clone()),
            validate_baseline_consistency: mig_input
                .and_then(|m| m.validate_baseline_consistency)
                .unwrap_or(defaults.validate_baseline_consistency),
            create_baselines_by_default: mig_input
                .and_then(|m| m.create_baselines_by_default)
                .unwrap_or(defaults.create_baselines_by_default),
            tracking_table,
            column_order: mig_input
                .and_then(|m| m.column_order)
                .unwrap_or(defaults.column_order),
            filename_prefix: mig_input
                .and_then(|m| m.filename_prefix.as_ref())
                .cloned()
                .unwrap_or_else(|| defaults.filename_prefix.clone()),
        }
    }

    fn resolve_schema(&self, defaults: &Schema) -> Schema {
        let schema_input = self.config_input.schema.as_ref();

        Schema {
            augment_dependencies_from_files: schema_input
                .and_then(|s| s.augment_dependencies_from_files)
                .unwrap_or(defaults.augment_dependencies_from_files),
            validate_file_dependencies: schema_input
                .and_then(|s| s.validate_file_dependencies)
                .unwrap_or(defaults.validate_file_dependencies),
            verbose_file_processing: schema_input
                .and_then(|s| s.verbose_file_processing)
                .unwrap_or(defaults.verbose_file_processing),
        }
    }

    fn resolve_docker(&self, defaults: &Docker) -> Docker {
        let docker_input = self.config_input.docker.as_ref();

        Docker {
            auto_cleanup: docker_input
                .and_then(|d| d.auto_cleanup)
                .unwrap_or(defaults.auto_cleanup),
            check_system_identifier: docker_input
                .and_then(|d| d.check_system_identifier)
                .unwrap_or(defaults.check_system_identifier),
        }
    }
}

impl Default for ConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}
