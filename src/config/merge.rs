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
            shadow: other.shadow.or(self.shadow),
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
            comments: other.comments.or(self.comments),
            grants: other.grants.or(self.grants),
            triggers: other.triggers.or(self.triggers),
            extensions: other.extensions.or(self.extensions),
        }
    }
}
