use crate::config::{merge::Merge, types::*};
use anyhow::Result;

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

    /// Resolve project configuration against built-in defaults.
    ///
    /// Database connections are NOT resolved here — they're typed values
    /// resolved at the command boundary (see `config::connections`), so the
    /// resolved Config carries no connection strings.
    pub fn resolve(self) -> Result<Config> {
        let defaults = Config::default();

        Ok(Config {
            directories: self.resolve_directories(&defaults.directories),
            objects: self.resolve_objects(&defaults.objects),
            migration: self.resolve_migration(&defaults.migration),
            schema: self.resolve_schema(&defaults.schema),
            docker: self.resolve_docker(&defaults.docker),
        })
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
        resolve_objects_input(self.config_input.objects.as_ref(), defaults)
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

/// Resolve an optional `objects` config input against defaults.
///
/// Used by `ConfigBuilder::resolve` and by `pgmt init`, which needs the scoping
/// from an existing pgmt.yaml before a full config can be resolved (to scope
/// the shadow clean during schema import).
pub fn resolve_objects_input(obj_input: Option<&ObjectsInput>, defaults: &Objects) -> Objects {
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
                .schemas
                .as_ref()
                .cloned()
                .unwrap_or_else(|| defaults.exclude.schemas.clone()),
            tables: e
                .tables
                .as_ref()
                .cloned()
                .unwrap_or_else(|| defaults.exclude.tables.clone()),
        })
        .unwrap_or_else(|| defaults.exclude.clone());

    Objects { include, exclude }
}
