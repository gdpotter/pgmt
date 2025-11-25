use crate::config::types::*;
use std::collections::HashMap;

// Config now derives Default

impl Default for Databases {
    fn default() -> Self {
        Self {
            dev: "postgres://localhost/pgmt_dev".to_string(),
            shadow: ShadowDatabase::Auto,
            target: None,
        }
    }
}

impl Default for Directories {
    fn default() -> Self {
        Self {
            schema: "schema".to_string(),
            migrations: "migrations".to_string(),
            baselines: "schema_baselines".to_string(),
            roles: "roles.sql".to_string(),
        }
    }
}

impl Default for Objects {
    fn default() -> Self {
        Self {
            include: ObjectInclude::default(),
            exclude: ObjectExclude::default(),
            comments: true,
            grants: true,
            triggers: true,
            extensions: true,
        }
    }
}

// ObjectInclude can derive Default since it's just two empty Vecs

impl Default for ObjectExclude {
    fn default() -> Self {
        Self {
            schemas: vec!["pg_*".to_string(), "information_schema".to_string()],
            tables: vec![],
        }
    }
}

impl Default for Migration {
    fn default() -> Self {
        Self {
            default_mode: "safe_only".to_string(),
            validate_baseline_consistency: true,
            create_baselines_by_default: false,
            tracking_table: TrackingTable::default(),
        }
    }
}

impl Default for TrackingTable {
    fn default() -> Self {
        Self {
            schema: "public".to_string(),
            name: "pgmt_migrations".to_string(),
        }
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self {
            augment_dependencies_from_files: true,
            validate_file_dependencies: true,
            verbose_file_processing: false,
        }
    }
}

impl Default for Docker {
    fn default() -> Self {
        Self {
            auto_cleanup: true,
            check_system_identifier: true,
        }
    }
}

impl Default for ShadowDockerConfig {
    fn default() -> Self {
        Self {
            version: None, // Will use default image if not specified
            image: "postgres:18-alpine".to_string(),
            environment: {
                let mut env = HashMap::new();
                env.insert(
                    "POSTGRES_PASSWORD".to_string(),
                    "pgmt_shadow_password".to_string(),
                );
                env
            },
            container_name: None,
            auto_cleanup: true,
            volumes: None,
            network: None,
        }
    }
}
