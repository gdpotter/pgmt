//! Typed database connection resolution.
//!
//! Each database pgmt can talk to (dev, shadow, target) has a value type that
//! can only be obtained through its CLI args struct's `resolve()`. A command
//! that connects to a database therefore must flatten the matching args struct
//! — the flag in `--help`, the `PGMT_*` env override, and the yaml fallback
//! all come as a package, and forgetting one is a compile error, not a silent
//! help-text omission.
//!
//! Precedence, highest first: CLI flag > `PGMT_*` env var > pgmt.yaml.

use anyhow::{Result, anyhow};
use clap::Args;

use super::types::{
    ConfigInput, DatabasesInput, ShadowDatabase, ShadowDockerConfig, ShadowResetMode,
};

fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn databases(file: &ConfigInput) -> Option<&DatabasesInput> {
    file.databases.as_ref()
}

/// A resolved development-database connection string.
#[derive(Debug, Clone)]
pub struct DevUrl(String);

impl DevUrl {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Default, Args)]
pub struct DevUrlArgs {
    #[arg(long, help = "Development database URL [env: PGMT_DEV_URL]")]
    pub dev_url: Option<String>,
}

impl DevUrlArgs {
    pub fn resolve(&self, file: &ConfigInput) -> Result<DevUrl> {
        self.lookup(file).map(DevUrl).ok_or_else(|| {
            anyhow!(
                "No development database configured.\n\n\
                 Provide one via (highest precedence first):\n\
                 • --dev-url postgres://localhost/myapp_dev\n\
                 • PGMT_DEV_URL environment variable\n\
                 • databases.dev_url in pgmt.yaml"
            )
        })
    }

    /// The effective value without requiring it (used by `pgmt config` display)
    pub fn lookup(&self, file: &ConfigInput) -> Option<String> {
        self.dev_url
            .clone()
            .or_else(|| env_var("PGMT_DEV_URL"))
            .or_else(|| databases(file).and_then(|d| d.dev_url.clone()))
    }
}

/// A resolved target-database (deployment) connection string.
#[derive(Debug, Clone)]
pub struct TargetUrl(String);

impl TargetUrl {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Default, Args)]
pub struct TargetUrlArgs {
    #[arg(
        long,
        help = "Target (production/staging) database URL [env: PGMT_TARGET_URL]"
    )]
    pub target_url: Option<String>,
}

impl TargetUrlArgs {
    pub fn resolve(&self, file: &ConfigInput) -> Result<TargetUrl> {
        self.lookup(file).map(TargetUrl).ok_or_else(|| {
            anyhow!(
                "No target database configured.\n\n\
                 This command runs against a deployment target and needs an explicit URL:\n\
                 • --target-url postgres://prod-host/db\n\
                 • PGMT_TARGET_URL environment variable\n\
                 • databases.target_url in pgmt.yaml\n\n\
                 💡 Don't apply migrations to your dev database.\n\
                    Use 'pgmt apply' to keep dev in sync with schema files."
            )
        })
    }

    /// The effective value without requiring it (used by `pgmt config` display)
    pub fn lookup(&self, file: &ConfigInput) -> Option<String> {
        self.target_url
            .clone()
            .or_else(|| env_var("PGMT_TARGET_URL"))
            .or_else(|| databases(file).and_then(|d| d.target_url.clone()))
    }
}

#[derive(Debug, Clone, Default, Args)]
pub struct ShadowUrlArgs {
    #[arg(
        long,
        help = "Shadow database URL (overrides auto Docker mode) [env: PGMT_SHADOW_URL]"
    )]
    pub shadow_url: Option<String>,
}

impl ShadowUrlArgs {
    /// Resolve the shadow database. Unlike dev/target this never fails for
    /// being unset: the default is auto-provisioned Docker.
    pub fn resolve(&self, file: &ConfigInput) -> Result<ShadowDatabase> {
        // CLI flag, env override, or bare `shadow_url:` yaml key: explicit URL
        // with the default reset mode
        if let Some(url) = self
            .shadow_url
            .clone()
            .or_else(|| env_var("PGMT_SHADOW_URL"))
            .or_else(|| databases(file).and_then(|d| d.shadow_url.clone()))
        {
            return Ok(ShadowDatabase::Url {
                url,
                reset: ShadowResetMode::default(),
            });
        }

        // pgmt.yaml shadow configuration
        if let Some(shadow_input) = databases(file).and_then(|d| d.shadow.as_ref()) {
            // Explicit URL in config
            if let Some(url) = &shadow_input.url {
                return Ok(ShadowDatabase::Url {
                    url: url.clone(),
                    reset: shadow_input.reset.unwrap_or_default(),
                });
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
                    platform: docker_input.platform.clone(),
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
                    "Shadow database auto mode is disabled but no URL provided. \
                     Use --shadow-url, PGMT_SHADOW_URL, or enable auto mode"
                ));
            }
        }

        // Default to auto mode
        Ok(ShadowDatabase::Auto)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::types::ShadowDatabaseInput;

    fn file_with(dev: Option<&str>, target: Option<&str>) -> ConfigInput {
        ConfigInput {
            databases: Some(DatabasesInput {
                dev_url: dev.map(String::from),
                target_url: target.map(String::from),
                shadow_url: None,
                shadow: None,
            }),
            ..Default::default()
        }
    }

    #[test]
    fn dev_cli_beats_file() {
        let args = DevUrlArgs {
            dev_url: Some("postgres://cli/db".into()),
        };
        let resolved = args.resolve(&file_with(Some("postgres://file/db"), None)).unwrap();
        assert_eq!(resolved.as_str(), "postgres://cli/db");
    }

    #[test]
    fn dev_falls_back_to_file() {
        let args = DevUrlArgs::default();
        let resolved = args.resolve(&file_with(Some("postgres://file/db"), None)).unwrap();
        assert_eq!(resolved.as_str(), "postgres://file/db");
    }

    #[test]
    fn dev_unset_is_an_error() {
        let args = DevUrlArgs::default();
        let err = args.resolve(&ConfigInput::default()).unwrap_err().to_string();
        assert!(err.contains("PGMT_DEV_URL"));
        assert!(err.contains("databases.dev_url"));
    }

    #[test]
    fn target_unset_is_an_error_with_guidance() {
        let args = TargetUrlArgs::default();
        let err = args.resolve(&ConfigInput::default()).unwrap_err().to_string();
        assert!(err.contains("PGMT_TARGET_URL"));
        assert!(err.contains("--target-url"));
    }

    #[test]
    fn shadow_defaults_to_auto() {
        let shadow = ShadowUrlArgs::default().resolve(&ConfigInput::default()).unwrap();
        assert!(matches!(shadow, ShadowDatabase::Auto));
    }

    #[test]
    fn shadow_cli_url_beats_config_block() {
        let file = ConfigInput {
            databases: Some(DatabasesInput {
                dev_url: None,
                target_url: None,
                shadow_url: None,
                shadow: Some(ShadowDatabaseInput {
                    auto: Some(true),
                    url: Some("postgres://file-shadow/db".into()),
                    reset: None,
                    docker: None,
                }),
            }),
            ..Default::default()
        };
        let args = ShadowUrlArgs {
            shadow_url: Some("postgres://cli-shadow/db".into()),
        };
        match args.resolve(&file).unwrap() {
            ShadowDatabase::Url { url, .. } => assert_eq!(url, "postgres://cli-shadow/db"),
            other => panic!("expected Url, got {:?}", other),
        }
    }

    #[test]
    fn shadow_auto_disabled_without_url_is_an_error() {
        let file = ConfigInput {
            databases: Some(DatabasesInput {
                dev_url: None,
                target_url: None,
                shadow_url: None,
                shadow: Some(ShadowDatabaseInput {
                    auto: Some(false),
                    url: None,
                    reset: None,
                    docker: None,
                }),
            }),
            ..Default::default()
        };
        assert!(ShadowUrlArgs::default().resolve(&file).is_err());
    }
}
