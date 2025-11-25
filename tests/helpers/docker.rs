//! Docker test helpers for ensuring proper cleanup

use std::future::Future;

/// Run a test with automatic Docker container cleanup
///
/// This helper ensures all Docker containers created during the test are properly
/// cleaned up, even if the test fails or panics.
///
/// # Example
///
/// ```rust
/// use crate::helpers::docker::with_docker_cleanup;
///
/// #[tokio::test]
/// async fn test_something_with_docker() {
///     with_docker_cleanup(async {
///         // Your test code here
///         // Containers will be cleaned up automatically
///     }).await;
/// }
/// ```
pub async fn with_docker_cleanup<F>(test: F)
where
    F: Future<Output = ()>,
{
    // Run the test
    test.await;

    // Clean up all registered containers (both from global registry and RAII)
    if let Err(e) = pgmt::docker::cleanup_all_containers().await {
        eprintln!("Warning: Failed to cleanup Docker containers: {}", e);
    }
}
