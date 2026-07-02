use crate::config::types::*;

/// Trait for merging optional configuration values
pub trait Merge<T> {
    fn merge(self, other: T) -> T;
}

impl<T> Merge<Option<T>> for Option<T> {
    fn merge(self, other: Option<T>) -> Option<T> {
        other.or(self)
    }
}

impl Merge<ConfigInput> for ConfigInput {
    fn merge(self, other: ConfigInput) -> ConfigInput {
        ConfigInput {
            databases: match (self.databases, other.databases) {
                (None, None) => None,
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (Some(a), Some(b)) => Some(a.merge_with(b)),
            },
            directories: match (self.directories, other.directories) {
                (None, None) => None,
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (Some(a), Some(b)) => Some(a.merge_with(b)),
            },
            objects: match (self.objects, other.objects) {
                (None, None) => None,
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (Some(a), Some(b)) => Some(a.merge_with(b)),
            },
            // An overlay's `modules:` map replaces the whole map: partial
            // per-module merging would silently combine two different
            // partitions of the schema.
            modules: self.modules.merge(other.modules),
            migration: self.migration.merge(other.migration),
            schema: self.schema.merge(other.schema),
            docker: self.docker.merge(other.docker),
        }
    }
}

// Custom merge implementations for complex types
impl DatabasesInput {
    pub fn merge_with(self, other: DatabasesInput) -> DatabasesInput {
        DatabasesInput {
            dev_url: other.dev_url.or(self.dev_url),
            shadow_url: other.shadow_url.or(self.shadow_url),
            target_url: other.target_url.or(self.target_url),
            shadow: match (self.shadow, other.shadow) {
                (Some(a), Some(b)) => Some(a.merge_with(b)),
                (a, b) => b.or(a),
            },
        }
    }
}

impl ShadowDatabaseInput {
    /// An overlay replaces the shadow *mode* (auto / url / docker) wholesale,
    /// but when both layers are docker mode the overlay only replaces the
    /// fields it sets — hand-maintained fields like `environment` or
    /// `container_name` survive a re-init that re-answers docker mode.
    pub fn merge_with(self, other: ShadowDatabaseInput) -> ShadowDatabaseInput {
        match (self.docker, other.docker) {
            (Some(a), Some(b)) if other.url.is_none() => ShadowDatabaseInput {
                auto: other.auto,
                url: None,
                reset: other.reset.or(self.reset),
                docker: Some(a.merge_with(b)),
            },
            (_, b) => ShadowDatabaseInput {
                auto: other.auto,
                url: other.url,
                reset: other.reset.or(self.reset),
                docker: b,
            },
        }
    }
}

impl ShadowDockerInput {
    pub fn merge_with(self, other: ShadowDockerInput) -> ShadowDockerInput {
        ShadowDockerInput {
            version: other.version.or(self.version),
            image: other.image.or(self.image),
            platform: other.platform.or(self.platform),
            environment: other.environment.or(self.environment),
            container_name: other.container_name.or(self.container_name),
            auto_cleanup: other.auto_cleanup.or(self.auto_cleanup),
            volumes: other.volumes.or(self.volumes),
            network: other.network.or(self.network),
        }
    }
}

impl DirectoriesInput {
    pub fn merge_with(self, other: DirectoriesInput) -> DirectoriesInput {
        DirectoriesInput {
            schema_dir: other.schema_dir.or(self.schema_dir),
            migrations_dir: other.migrations_dir.or(self.migrations_dir),
            baselines_dir: other.baselines_dir.or(self.baselines_dir),
            roles_file: other.roles_file.or(self.roles_file),
        }
    }
}

impl ObjectsInput {
    pub fn merge_with(self, other: ObjectsInput) -> ObjectsInput {
        ObjectsInput {
            include: other.include.or(self.include),
            exclude: other.exclude.or(self.exclude),
        }
    }
}
