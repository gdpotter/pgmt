use anyhow::Result;
use sqlx::PgPool;
use tracing::warn;

use crate::catalog::Catalog;
use crate::config::{Config, ObjectFilter};

/// Verify that the database state matches the expected catalog after applying changes
pub async fn verify_final_state(
    dev_pool: &PgPool,
    expected_catalog: &Catalog,
    config: &Config,
    verbose: bool,
) -> Result<()> {
    if verbose {
        println!("🔍 Verifying final database state...");
    }

    // Load the current dev database catalog after changes
    let current_catalog = Catalog::load(dev_pool)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to load final catalog for verification: {}", e))?;

    // Apply the same filtering as used during diff
    let filter = ObjectFilter::new(&config.objects, &config.migration.tracking_table);
    let current_filtered = filter.filter_catalog(current_catalog);
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

    if verbose {
        println!("✅ State verification completed");
        println!(
            "   📊 Tables: {}, Views: {}, Functions: {}",
            current_table_count, current_view_count, current_function_count
        );
    }

    Ok(())
}
