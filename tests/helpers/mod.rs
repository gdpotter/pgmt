pub mod cli;
pub mod docker;
pub mod harness;
pub mod migration;

use tokio::sync::Mutex;

/// Serializes tests that create ephemeral shadow branches. Branch state lives in
/// process-global registries and `cleanup_all_branches` force-drops *every*
/// branch in the process, so two such tests running concurrently can drop each
/// other's in-flight branches. Hold this for the duration of any branch test.
pub static BRANCH_TEST_LOCK: Mutex<()> = Mutex::const_new(());
