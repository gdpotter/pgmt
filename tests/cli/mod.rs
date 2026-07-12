/// CLI integration tests using the new hybrid approach with assert_cmd and expectrl
///
/// This module contains end-to-end CLI tests that exercise the actual binary.
///
/// ## Testing Approaches
///
/// ### Non-Interactive Commands (assert_cmd)
/// Use `CliTestHelper::command()` for straightforward commands that don't require user input:
/// ```rust
/// helper.command()
///     .args(["migrate", "status"])
///     .assert()
///     .success()
///     .stdout(predicate::str::contains("No migrations"));
/// ```
///
/// ### Interactive Commands (expectrl)
/// Use `CliTestHelper::interactive_command()` for commands that prompt for user input:
/// ```rust
/// let mut session = helper.interactive_command(&["migrate", "new"]).await?;
/// session.expect("Enter migration description")?;
/// session.send_line("my_migration")?;
/// session.expect("Migration created")?;
/// ```
pub mod apply_crash_states;
pub mod apply_locking;
pub mod apply_resume;
pub mod baseline_commands;
pub mod baseline_extension_ordering;
pub mod baseline_sections;
pub mod basic_interactive;
pub mod connection_precedence;
pub mod debug_commands;
pub mod diff;
pub mod error_handling;
pub mod help_matrix;
pub mod incomplete_baseline_guard;
pub mod init_interactive;
pub mod migrate_end_to_end;
pub mod migrate_new;
pub mod migrate_reconstruction;
pub mod migrate_validate_json;
pub mod modules_deploy;
pub mod modules_generation;
pub mod nontransactional_sections;
pub mod roles_file;
pub mod section_checksums;
pub mod substrate;
pub mod watermark_warning;
