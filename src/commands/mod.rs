pub mod apply;
pub mod baseline;
pub mod config;
pub mod debug;
pub mod diff;
pub mod diff_output;
pub mod init;
pub mod migrate;
pub mod validate;

// Re-export all command functions
pub use apply::{cmd_apply, cmd_apply_watch};
pub use baseline::{cmd_baseline_clean, cmd_baseline_create, cmd_baseline_list};
pub use config::cmd_config;
pub use debug::cmd_debug_dependencies;
pub use diff::cmd_diff;
pub use init::cmd_init_with_args;
pub use migrate::{
    MigrateDiffArgs, cmd_migrate_apply, cmd_migrate_diff, cmd_migrate_new, cmd_migrate_status,
    cmd_migrate_update_specific, cmd_migrate_update_with_options, cmd_migrate_validate,
};
pub use validate::cmd_validate;
