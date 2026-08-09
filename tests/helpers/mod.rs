pub mod cli;
pub mod docker;
pub mod harness;
pub mod migration;
pub mod raw;

use tokio::sync::RwLock;

/// Guards the process-global shadow state: the ephemeral-branch registry
/// (`db::branch`) and the container registry (`docker`).
///
/// Both are drained by a "destroy everything" call — `cleanup_all_branches`
/// force-drops every branch, `cleanup_all_containers` stops every container —
/// and both kill whatever is still using them. A dropped branch takes its
/// sessions with it (`DROP DATABASE ... WITH (FORCE)`); a stopped container
/// SIGTERMs its postgres, which is a fast shutdown. Either way the test that
/// was using it fails with "terminating connection due to administrator
/// command", far from the test that caused it.
///
/// Holding shadow state and destroying all of it are not symmetric, so the
/// guard is not either:
///
/// - A test that CREATES a branch or container in-process takes
///   [`shadow_guard`]. Holders coexist, so this costs no parallelism.
/// - A test that drains either registry takes [`shadow_cleanup_guard`], which
///   excludes every holder for its duration. `with_docker_cleanup` takes it for
///   the tests that use it, so those must NOT also take the read guard.
///
/// One guard covers both registries because a test generally holds a branch
/// *inside* a container, and because a third registry with the same shape
/// should not need a third lock to be discovered.
///
/// Only in-process state is at stake: a test driving the `pgmt` binary as a
/// subprocess has its own registries and needs no guard.
static SHADOW_STATE_LOCK: RwLock<()> = RwLock::const_new(());

/// Hold while this test has a branch or container of its own.
pub async fn shadow_guard() -> tokio::sync::RwLockReadGuard<'static, ()> {
    SHADOW_STATE_LOCK.read().await
}

/// Hold while this test destroys every branch or container in the process.
pub async fn shadow_cleanup_guard() -> tokio::sync::RwLockWriteGuard<'static, ()> {
    SHADOW_STATE_LOCK.write().await
}
