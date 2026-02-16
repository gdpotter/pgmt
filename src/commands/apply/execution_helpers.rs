use anyhow::Result;
use console::style;
use sqlx::PgPool;
use tracing::{debug, info};

use crate::catalog::Catalog;
use crate::config::Config;
use crate::db::schema_executor::ApplyStepExecutor;
use crate::render::{RenderedSql, Safety};

use super::ApplyOutcome;
use super::verification::verify_final_state;

/// Apply all rendered steps (both safe and destructive)
pub async fn apply_all_rendered_steps(
    rendered: &[RenderedSql],
    dev_pool: &PgPool,
    expected_catalog: &Catalog,
    config: &Config,
) -> Result<ApplyOutcome> {
    let show_progress = tracing::enabled!(tracing::Level::INFO);
    let executor = ApplyStepExecutor::new(dev_pool.clone(), show_progress, true, false);
    let total = rendered.len();
    info!("Executing {} migration steps...", total);

    for (i, step) in rendered.iter().enumerate() {
        info!("Applying step {}/{}...", i + 1, total);
        debug!("{}", style(&step.sql).dim());

        executor.execute_step(&step.sql, step.safety, i + 1).await?;
    }

    verify_final_state(dev_pool, expected_catalog, config).await?;
    Ok(ApplyOutcome::Applied)
}

/// Apply only safe rendered steps, optionally showing destructive ones
pub async fn apply_safe_rendered_steps(
    rendered: &[RenderedSql],
    dev_pool: &PgPool,
    expected_catalog: &Catalog,
    config: &Config,
    show_skipped: bool,
) -> Result<ApplyOutcome> {
    let safe_count = rendered.iter().filter(|s| s.safety == Safety::Safe).count();
    let destructive_count = rendered
        .iter()
        .filter(|s| s.safety == Safety::Destructive)
        .count();

    let has_skipped = destructive_count > 0;

    // Handle destructive operations
    if destructive_count > 0 && show_skipped {
        println!(
            "⚠️  {} destructive operation{} detected but not applied in safe-only mode",
            destructive_count,
            if destructive_count == 1 { "" } else { "s" }
        );

        for (i, step) in rendered.iter().enumerate() {
            if step.safety == Safety::Destructive {
                let sql_preview = step.sql.lines().next().unwrap_or("").trim();
                println!("   ❌ Skipped: {}", sql_preview);
                info!("Skipped step {}: {}", i + 1, sql_preview);
            }
        }

        if safe_count == 0 {
            println!("⚠️  No safe operations to apply");
            return Ok(ApplyOutcome::Skipped);
        }
    }

    // Apply safe operations
    if safe_count > 0 {
        println!(
            "✅ Auto-applying {} safe operation{}...",
            safe_count,
            if safe_count == 1 { "" } else { "s" }
        );

        let show_progress = tracing::enabled!(tracing::Level::INFO);
        let executor = ApplyStepExecutor::new(dev_pool.clone(), show_progress, true, false);
        info!("Executing {} safe migration steps...", safe_count);

        let mut applied = 0;
        for (i, step) in rendered.iter().enumerate() {
            if step.safety == Safety::Safe {
                applied += 1;
                info!("Applying safe step {}/{}...", applied, safe_count);
                debug!("{}", style(&step.sql).dim());
                executor.execute_step(&step.sql, step.safety, i + 1).await?;
            }
        }

        info!("Safe operations completed successfully");

        verify_final_state(dev_pool, expected_catalog, config).await?;
    }

    if has_skipped {
        Ok(ApplyOutcome::Skipped)
    } else {
        Ok(ApplyOutcome::Applied)
    }
}
