pub mod commands;
pub mod import;
pub mod project;
pub mod prompts;

use clap::Parser;

// Re-export command functions and types
pub use commands::{
    BaselineCreationConfig, DatabaseState, InitOptions, ObjectManagementConfig, cmd_init_with_args,
};

/// CLI arguments for the init command
#[derive(Parser)]
pub struct InitArgs {
    /// Database URL for development
    #[clap(long)]
    pub dev_url: Option<String>,

    /// Skip import (create empty project)
    #[clap(long)]
    pub no_import: bool,

    /// Use defaults for all prompts (non-interactive mode)
    #[clap(long)]
    pub defaults: bool,

    /// Schema directory name
    #[clap(long, default_value = "schema")]
    pub schema_dir: String,

    /// Migrations directory name
    #[clap(long)]
    pub migrations_dir: Option<String>,

    /// Baselines directory name
    #[clap(long)]
    pub baselines_dir: Option<String>,

    /// Use auto shadow database (skip shadow database prompt)
    #[clap(long)]
    pub auto_shadow: bool,

    /// PostgreSQL version for auto shadow database (e.g., "14", "15", "16")
    #[clap(long)]
    pub shadow_pg_version: Option<String>,

    /// Docker image for the shadow database (e.g. "postgis/postgis:16-3.5")
    #[clap(long, conflicts_with_all = ["shadow_pg_version", "auto_shadow"])]
    pub shadow_image: Option<String>,

    /// Platform for the shadow Docker image (e.g. "linux/amd64"), for images
    /// only published for one architecture.
    #[clap(long, requires = "shadow_image")]
    pub shadow_platform: Option<String>,

    /// Use an external shadow database at this URL (skips Docker).
    #[clap(long, conflicts_with_all = ["shadow_image", "shadow_platform", "auto_shadow", "shadow_pg_version"])]
    pub shadow_url: Option<String>,

    /// Create baseline from existing database (non-interactive)
    #[clap(long)]
    pub create_baseline: bool,

    /// Skip baseline creation (non-interactive)
    #[clap(long)]
    pub no_baseline: bool,

    /// Custom description for baseline
    #[clap(long)]
    pub baseline_description: Option<String>,

    /// Path to roles file (default: auto-detect roles.sql)
    #[clap(long)]
    pub roles_file: Option<String>,

    /// Force fresh initialization (overwrite existing config)
    #[clap(long)]
    pub fresh: bool,
}
