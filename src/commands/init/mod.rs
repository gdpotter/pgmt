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

    /// Use auto shadow database (skip shadow database prompt)
    #[clap(long)]
    pub auto_shadow: bool,

    /// PostgreSQL version for auto shadow database (e.g., "14", "15", "16")
    #[clap(long)]
    pub shadow_pg_version: Option<String>,

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
}
