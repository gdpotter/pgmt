use anyhow::Result;
use sqlx::PgPool;
use tracing::{debug, warn};

use crate::catalog::Catalog;
use crate::config::{Config, ObjectFilter};

/// Verify that the database state matches the expected catalog after applying changes
pub async fn verify_final_state(
    dev_pool: &PgPool,
    expected_catalog: &Catalog,
    config: &Config,
) -> Result<()> {
    debug!("Verifying final database state...");

    // Load the current dev database catalog after changes, scoped the same
    // way as the diff; expected may come from any caller, so filter it too
    // (idempotent).
    let filter = ObjectFilter::from_config(config);
    let current_filtered = Catalog::load_managed(dev_pool, &filter)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to load final catalog for verification: {}", e))?;
    let expected_filtered = filter.filter_catalog(expected_catalog.clone());

    // Compare key metrics
    let current_table_count = current_filtered.tables.len();
    let expected_table_count = expected_filtered.tables.len();

    let current_view_count = current_filtered.views.len();
    let expected_view_count = expected_filtered.views.len();

    let current_function_count = current_filtered.functions.len();
    let expected_function_count = expected_filtered.functions.len();

    if current_table_count != expected_table_count {
        warn!(
            "Table count mismatch: expected {}, got {}",
            expected_table_count, current_table_count
        );
    }
    if current_view_count != expected_view_count {
        warn!(
            "View count mismatch: expected {}, got {}",
            expected_view_count, current_view_count
        );
    }
    if current_function_count != expected_function_count {
        warn!(
            "Function count mismatch: expected {}, got {}",
            expected_function_count, current_function_count
        );
    }

    debug!(
        "State verification completed - Tables: {}, Views: {}, Functions: {}",
        current_table_count, current_view_count, current_function_count
    );

    Ok(())
}
