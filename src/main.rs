mod baseline;
mod catalog;
mod commands;
mod config;
mod constants;
mod db;
mod diff;
mod docker;
mod migrate;
mod migration;
mod migration_tracking;
mod progress;
mod prompts;
mod render;
mod schema_generator;
mod schema_loader;
mod schema_ops;
mod validation;
mod validation_output;

use crate::commands::apply::ExecutionMode;
use crate::commands::diff_output::DiffFormat;
use anyhow::Result;
use clap::{Parser, Subcommand};
use dotenv::dotenv;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(long, default_value = "pgmt.yaml", global = true)]
    config_file: String,

    /// Enable verbose output (info level)
    #[arg(long, short = 'v', global = true)]
    verbose: bool,

    /// Suppress all non-essential output (error level only)
    #[arg(long, short = 'q', global = true)]
    quiet: bool,

    /// Enable debug output (debug level)
    #[arg(long, global = true)]
    debug: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Parser)]
struct ApplyArgs {
    /// Show what would be applied without making any changes
    #[arg(long, group = "mode")]
    dry_run: bool,

    /// Apply all changes without confirmation prompts
    #[arg(long, group = "mode")]
    force: bool,

    /// Apply only safe operations, skip destructive changes
    #[arg(long, group = "mode")]
    safe_only: bool,

    /// Fail if destructive operations exist (default in non-interactive mode)
    #[arg(long, group = "mode")]
    require_approval: bool,

    /// Watch for schema file changes and continuously apply them
    #[arg(long)]
    watch: bool,

    #[command(flatten)]
    dev: config::DevUrlArgs,

    #[command(flatten)]
    shadow: config::ShadowUrlArgs,
}

/// Arguments for pgmt diff (schema vs dev)
#[derive(Parser, Debug)]
pub struct DiffArgs {
    /// Output format
    #[arg(long, value_enum, default_value = "detailed")]
    pub format: DiffFormat,

    /// Save SQL output to file
    #[arg(long)]
    pub output_sql: Option<String>,

    #[command(flatten)]
    pub dev: config::DevUrlArgs,

    #[command(flatten)]
    pub shadow: config::ShadowUrlArgs,
}

/// Arguments for pgmt validate (dev vs schema files)
#[derive(Parser, Debug)]
pub struct ValidateArgs {
    #[command(flatten)]
    pub dev: config::DevUrlArgs,

    #[command(flatten)]
    pub shadow: config::ShadowUrlArgs,
}

/// Arguments for pgmt migrate diff (schema vs target)
#[derive(Parser, Debug)]
pub struct MigrateDiffArgs {
    /// Output format
    #[arg(long, value_enum, default_value = "detailed")]
    pub format: DiffFormat,

    /// Save SQL output to file
    #[arg(long)]
    pub output_sql: Option<String>,

    #[command(flatten)]
    pub target: config::TargetUrlArgs,

    #[command(flatten)]
    pub shadow: config::ShadowUrlArgs,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize project
    Init(commands::init::InitArgs),

    /// Apply modular schema files to development database
    Apply(ApplyArgs),

    /// Migration commands
    Migrate {
        #[command(subcommand)]
        command: MigrateCommands,
    },

    /// Compare schema files with dev database (preview what apply would do)
    Diff(DiffArgs),

    /// Validate schema consistency
    Validate(ValidateArgs),

    /// Manage configuration
    Config {
        #[command(subcommand)]
        command: Option<commands::config::ConfigCommands>,
    },

    /// Debug commands for troubleshooting
    Debug {
        #[command(subcommand)]
        command: DebugCommands,
    },
}

#[derive(Subcommand)]
enum MigrateCommands {
    /// Check target database for schema drift
    Diff(MigrateDiffArgs),

    /// Generate migration from diff
    New {
        /// Description for the migration
        description: Option<String>,

        /// Create a baseline file alongside the migration
        #[arg(long)]
        create_baseline: bool,

        #[command(flatten)]
        shadow: config::ShadowUrlArgs,
    },

    /// Update the latest migration with current changes
    Update {
        /// Migration version to update (e.g., V1734567890). If not provided, updates latest migration.
        migration_version: Option<String>,

        /// Create backup of original migration before updating
        #[arg(long)]
        backup: bool,

        /// Preview changes without applying them
        #[arg(long)]
        dry_run: bool,

        #[command(flatten)]
        shadow: config::ShadowUrlArgs,
    },

    /// Apply explicit migrations
    Apply {
        #[command(flatten)]
        target: config::TargetUrlArgs,
    },

    /// Provision a fresh database from a baseline + migrations
    Provision {
        #[command(flatten)]
        target: config::TargetUrlArgs,

        /// Preview what would be applied without changing the database
        #[arg(long)]
        dry_run: bool,
    },

    /// Check migration status
    Status {
        #[command(flatten)]
        dev: config::DevUrlArgs,
    },

    /// Validate migration consistency (for CI)
    Validate {
        #[command(flatten)]
        shadow: config::ShadowUrlArgs,

        /// Output format: human (default), json
        #[arg(long, default_value = "human")]
        format: String,

        /// Suppress verbose output (useful with --format=json)
        #[arg(long)]
        quiet: bool,

        /// Show detailed validation information
        #[arg(long)]
        verbose: bool,

        /// Ignore specific migrations during validation
        #[arg(long, value_delimiter = ',')]
        ignore_migrations: Vec<String>,
    },

    /// Create a baseline and optionally consolidate old migrations
    Baseline(MigrateBaselineArgs),
}

#[derive(clap::Args)]
struct MigrateBaselineArgs {
    #[command(subcommand)]
    command: Option<MigrateBaselineSubcommands>,

    /// Skip safety checks and force baseline creation
    #[arg(long)]
    force: bool,

    /// Don't delete old migrations after creating the baseline
    #[arg(long)]
    keep_migrations: bool,

    /// Preview what would happen without making changes
    #[arg(long)]
    dry_run: bool,

    #[command(flatten)]
    shadow: config::ShadowUrlArgs,
}

#[derive(Subcommand)]
enum MigrateBaselineSubcommands {
    /// List existing baselines
    List,
}

#[derive(Subcommand)]
enum DebugCommands {
    /// Show object dependencies (intrinsic from PostgreSQL + augmented from -- require:)
    Dependencies {
        /// Output format
        #[arg(long, value_enum, default_value = "json")]
        format: DebugOutputFormat,

        /// Filter to specific object (e.g., "public.users" or "Table:public.users")
        #[arg(long)]
        object: Option<String>,

        #[command(flatten)]
        shadow: config::ShadowUrlArgs,
    },
}

#[derive(clap::ValueEnum, Clone, Debug, PartialEq)]
pub enum DebugOutputFormat {
    /// JSON output for piping to jq
    Json,
    /// Human-readable text format
    Text,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let cli = Cli::parse();
    initialize_logging(&cli);
    let result = tokio::select! {
        result = run_main(cli) => result,
        _ = wait_for_shutdown_signal() => {
            info!("Received shutdown signal, cleaning up...");
            Ok(())
        }
    };

    // Drop shadow branches first, while any warm containers are still running
    if let Err(e) = db::branch::cleanup_all_branches().await {
        eprintln!("Warning: Failed to cleanup shadow branches: {}", e);
    }
    if let Err(e) = docker::cleanup_all_containers().await {
        eprintln!("Warning: Failed to cleanup Docker containers: {}", e);
    }

    result
}

async fn wait_for_shutdown_signal() {
    use tokio::signal;

    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

fn initialize_logging(cli: &Cli) {
    let level = if cli.debug {
        "debug"
    } else if cli.verbose {
        "info"
    } else if cli.quiet {
        "error"
    } else {
        "warn" // default level
    };

    let filter = if std::env::var("RUST_LOG").is_ok() {
        EnvFilter::from_default_env()
    } else {
        EnvFilter::new(level)
    };

    fmt().with_env_filter(filter).with_target(false).init();
}

async fn run_main(cli: Cli) -> Result<()> {
    match &cli.command {
        Commands::Init(args) => commands::cmd_init_with_args(args).await,
        _ => {
            let (file_config, root_dir) = config::load_config(&cli.config_file)?;

            match &cli.command {
                Commands::Init(_) => unreachable!(),
                Commands::Apply(args) => {
                    let config = config::ConfigBuilder::new()
                        .with_file(file_config.clone())
                        .resolve()?;
                    let dev = args.dev.resolve(&file_config)?;
                    let shadow = args.shadow.resolve(&file_config)?;

                    use commands::apply::ApplyOutcome;
                    use std::io::IsTerminal;

                    let execution_mode = if args.dry_run {
                        ExecutionMode::DryRun
                    } else if args.force {
                        ExecutionMode::Force
                    } else if args.safe_only {
                        ExecutionMode::SafeOnly
                    } else if args.require_approval {
                        ExecutionMode::RequireApproval
                    } else if std::io::stdin().is_terminal() {
                        // Interactive: auto-apply safe, prompt for destructive
                        ExecutionMode::Interactive
                    } else {
                        // Non-interactive: fail if destructive
                        ExecutionMode::RequireApproval
                    };

                    info!("Applying modular schema to dev");
                    let outcome = if args.watch {
                        commands::cmd_apply_watch(&config, &root_dir, execution_mode, &dev, &shadow)
                            .await?
                    } else {
                        commands::cmd_apply(&config, &root_dir, execution_mode, &dev, &shadow)
                            .await?
                    };

                    // Return appropriate exit code based on outcome
                    match outcome {
                        ApplyOutcome::DestructiveRequired => {
                            std::process::exit(2);
                        }
                        _ => Ok(()),
                    }
                }
                Commands::Migrate { command } => match command {
                    MigrateCommands::Diff(args) => {
                        let config = config::ConfigBuilder::new()
                            .with_file(file_config.clone())
                            .resolve()?;
                        let target = args.target.resolve(&file_config)?;
                        let shadow = args.shadow.resolve(&file_config)?;

                        let diff_args = commands::MigrateDiffArgs {
                            format: args.format.clone(),
                            output_sql: args.output_sql.clone(),
                        };

                        info!("Checking target database for drift");
                        commands::cmd_migrate_diff(&config, &root_dir, diff_args, &target, &shadow)
                            .await
                    }
                    MigrateCommands::New {
                        description,
                        create_baseline,
                        shadow,
                    } => {
                        let config = config::ConfigBuilder::new()
                            .with_file(file_config.clone())
                            .resolve()?;
                        let shadow = shadow.resolve(&file_config)?;

                        info!("Generating migration from diff");
                        commands::cmd_migrate_new(
                            &config,
                            &root_dir,
                            description.as_deref(),
                            *create_baseline,
                            &shadow,
                        )
                        .await
                    }
                    MigrateCommands::Update {
                        migration_version,
                        backup,
                        dry_run,
                        shadow,
                    } => {
                        let config = config::ConfigBuilder::new()
                            .with_file(file_config.clone())
                            .resolve()?;
                        let shadow = shadow.resolve(&file_config)?;

                        if let Some(version) = migration_version {
                            info!("Updating migration: {}", version);
                            commands::cmd_migrate_update_specific(
                                &config, &root_dir, version, *backup, *dry_run, &shadow,
                            )
                            .await
                        } else {
                            info!("Updating latest migration");
                            commands::cmd_migrate_update_with_options(
                                &config, &root_dir, *dry_run, &shadow,
                            )
                            .await
                        }
                    }
                    MigrateCommands::Apply { target } => {
                        let config = config::ConfigBuilder::new()
                            .with_file(file_config.clone())
                            .resolve()?;
                        let target = target.resolve(&file_config)?;

                        info!("Applying explicit migrations");
                        commands::cmd_migrate_apply(&config, &root_dir, &target).await
                    }
                    MigrateCommands::Provision { target, dry_run } => {
                        let config = config::ConfigBuilder::new()
                            .with_file(file_config.clone())
                            .resolve()?;
                        let target = target.resolve(&file_config)?;

                        info!("Provisioning database");
                        commands::cmd_migrate_provision(&config, &root_dir, &target, *dry_run).await
                    }
                    MigrateCommands::Status { dev } => {
                        let config = config::ConfigBuilder::new()
                            .with_file(file_config.clone())
                            .resolve()?;
                        let dev = dev.resolve(&file_config)?;

                        info!("Checking migration status");
                        commands::cmd_migrate_status(&config, &dev).await
                    }
                    MigrateCommands::Validate {
                        shadow,
                        format,
                        quiet,
                        verbose,
                        ignore_migrations,
                    } => {
                        let config = config::ConfigBuilder::new()
                            .with_file(file_config.clone())
                            .resolve()?;
                        let shadow = shadow.resolve(&file_config)?;

                        info!("Validating migration consistency");

                        let validation_options = validation_output::ValidationOutputOptions {
                            format: format.clone(),
                            quiet: *quiet,
                            verbose: *verbose,
                            ignore_migrations: ignore_migrations.clone(),
                        };

                        commands::cmd_migrate_validate(
                            &config,
                            &root_dir,
                            &validation_options,
                            &shadow,
                        )
                        .await
                    }
                    MigrateCommands::Baseline(args) => {
                        let config = config::ConfigBuilder::new()
                            .with_file(file_config.clone())
                            .resolve()?;

                        match &args.command {
                            Some(MigrateBaselineSubcommands::List) => {
                                info!("Listing existing baselines");
                                commands::cmd_baseline_list(&config, &root_dir).await
                            }
                            None => {
                                let shadow = args.shadow.resolve(&file_config)?;
                                info!("Creating baseline");
                                commands::cmd_migrate_baseline(
                                    &config,
                                    &root_dir,
                                    args.force,
                                    args.keep_migrations,
                                    args.dry_run,
                                    &shadow,
                                )
                                .await
                            }
                        }
                    }
                },
                Commands::Diff(args) => {
                    let config = config::ConfigBuilder::new()
                        .with_file(file_config.clone())
                        .resolve()?;
                    let dev = args.dev.resolve(&file_config)?;
                    let shadow = args.shadow.resolve(&file_config)?;

                    let diff_args = commands::diff::DiffArgs {
                        format: args.format.clone(),
                        output_sql: args.output_sql.clone(),
                    };

                    info!("Comparing schema files with dev database");
                    commands::cmd_diff(&config, &root_dir, diff_args, &dev, &shadow).await
                }
                Commands::Validate(args) => {
                    let config = config::ConfigBuilder::new()
                        .with_file(file_config.clone())
                        .resolve()?;
                    let dev = args.dev.resolve(&file_config)?;
                    let shadow = args.shadow.resolve(&file_config)?;

                    info!("Validating schema consistency");
                    commands::cmd_validate(&config, &root_dir, &dev, &shadow).await
                }
                Commands::Config { command } => {
                    match &command {
                        Some(_) => {
                            // Don't load config file for config commands, use the raw file path
                            let (file_config, _) = config::load_config(&cli.config_file)?;
                            let config = config::ConfigBuilder::new()
                                .with_file(file_config.clone())
                                .resolve()?;

                            info!("Managing configuration");
                            commands::cmd_config(&config, &file_config, command.clone()).await
                        }
                        None => {
                            // Just show help for config command
                            let config = config::ConfigBuilder::new()
                                .with_file(file_config.clone())
                                .resolve()?;
                            commands::cmd_config(&config, &file_config, None).await
                        }
                    }
                }
                Commands::Debug { command } => match command {
                    DebugCommands::Dependencies {
                        format,
                        object,
                        shadow,
                    } => {
                        let config = config::ConfigBuilder::new()
                            .with_file(file_config.clone())
                            .resolve()?;
                        let shadow = shadow.resolve(&file_config)?;

                        info!("Analyzing dependencies");
                        let output_format = match format {
                            DebugOutputFormat::Json => commands::debug::OutputFormat::Json,
                            DebugOutputFormat::Text => commands::debug::OutputFormat::Text,
                        };
                        commands::cmd_debug_dependencies(
                            &config,
                            &root_dir,
                            output_format,
                            object.as_deref(),
                            &shadow,
                        )
                        .await
                    }
                },
            }
        }
    }
}
