use anyhow::Result;
use console::style;
use dialoguer::{Confirm, Select};
use sqlx::PgPool;

use crate::catalog::Catalog;
use crate::config::Config;
use crate::diff::operations::MigrationStep;
use crate::render::{RenderedSql, Safety};

use super::ApplyOutcome;
use super::execution_helpers;
use super::verification::verify_final_state;

/// Execute migration steps with enhanced user control and recovery options
pub async fn execute_with_user_control(
    rendered: &[RenderedSql],
    steps: &[MigrationStep],
    dev_pool: &PgPool,
    expected_catalog: &Catalog,
    config: &Config,
    verbose: bool,
) -> Result<ApplyOutcome> {
    loop {
        // Show current migration overview
        if verbose {
            println!("\n📋 {}", style("Migration Overview").bold().underlined());
            println!(
                "   ✅ {} safe operation{}",
                rendered.iter().filter(|s| s.safety == Safety::Safe).count(),
                if rendered.iter().filter(|s| s.safety == Safety::Safe).count() == 1 {
                    ""
                } else {
                    "s"
                }
            );

            let destructive_count = rendered
                .iter()
                .filter(|s| s.safety == Safety::Destructive)
                .count();
            if destructive_count > 0 {
                println!(
                    "   ⚠️  {} destructive operation{}",
                    destructive_count,
                    if destructive_count == 1 { "" } else { "s" }
                );
            }
        }

        // Present user options
        let options = vec![
            "Apply all steps",
            "Apply only safe steps",
            "Review destructive steps",
            "Refresh (reload schema and re-analyze)",
            "Cancel",
        ];

        let selection = Select::new()
            .with_prompt("🤔 How would you like to proceed?")
            .items(&options)
            .default(0)
            .interact()?;

        match selection {
            0 => {
                // Apply all steps
                if verbose {
                    println!("🚀 Applying all migration steps...");
                }
                let outcome = execution_helpers::apply_all_rendered_steps(
                    rendered,
                    dev_pool,
                    expected_catalog,
                    config,
                    verbose,
                )
                .await?;
                if !verbose {
                    println!("\n✅ Applied {} changes", steps.len());
                }
                return Ok(outcome);
            }
            1 => {
                // Apply only safe steps
                if verbose {
                    println!("🛡️  Applying only safe operations...");
                }
                let outcome = execution_helpers::apply_safe_rendered_steps(
                    rendered,
                    dev_pool,
                    expected_catalog,
                    config,
                    true,
                    verbose,
                )
                .await?;
                if !verbose {
                    let applied = rendered.iter().filter(|s| s.safety == Safety::Safe).count();
                    if applied > 0 {
                        println!("\n✅ Applied {} changes", applied);
                    }
                }
                return Ok(outcome);
            }
            2 => {
                // Review destructive steps, then auto-apply approved + safe
                return review_destructive_steps(
                    rendered,
                    steps,
                    dev_pool,
                    expected_catalog,
                    config,
                    verbose,
                )
                .await;
            }
            3 => {
                // Refresh option
                println!("🔄 Refresh requested - you can now:");
                println!("   • Manually apply SQL changes to your dev database");
                println!("   • Modify your schema files");
                println!("   • Fix any conflicts or issues");
                println!();

                let continue_refresh = Confirm::new()
                    .with_prompt("Ready to refresh and re-analyze? (this will reload schema and recompute differences)")
                    .default(true)
                    .interact()?;

                if continue_refresh {
                    println!("💡 Returning to schema analysis - pgmt will re-run the diff process");
                    return Err(anyhow::anyhow!("REFRESH_REQUESTED")); // Special error code for refresh
                } else {
                    continue; // Back to main menu
                }
            }
            4 => {
                // Cancel
                println!("❌ Migration cancelled by user");
                return Ok(ApplyOutcome::Cancelled);
            }
            _ => unreachable!(),
        }
    }
}

/// Review destructive steps individually, then auto-apply approved + safe steps
async fn review_destructive_steps(
    rendered: &[RenderedSql],
    _steps: &[MigrationStep],
    dev_pool: &PgPool,
    expected_catalog: &Catalog,
    config: &Config,
    verbose: bool,
) -> Result<ApplyOutcome> {
    // Collect destructive step indices (in rendered list) for review
    let destructive_indices: Vec<usize> = rendered
        .iter()
        .enumerate()
        .filter(|(_, s)| s.safety == Safety::Destructive)
        .map(|(i, _)| i)
        .collect();

    let mut approved_destructive: Vec<bool> = vec![false; rendered.len()];
    let mut skipped_any = false;

    println!(
        "\n⚠️  Reviewing {} destructive operation{}:",
        destructive_indices.len(),
        if destructive_indices.len() == 1 {
            ""
        } else {
            "s"
        }
    );

    for (review_num, &idx) in destructive_indices.iter().enumerate() {
        println!(
            "\n⚠️  Destructive step {}/{}: {}",
            review_num + 1,
            destructive_indices.len(),
            style("─".repeat(40)).dim()
        );
        println!("{}", rendered[idx].sql);
        println!("{}", style("─".repeat(60)).dim());

        let step_options = vec!["Approve", "Skip", "Cancel migration"];

        let step_selection = Select::new()
            .with_prompt(format!(
                "Action for destructive step {}?",
                review_num + 1
            ))
            .items(&step_options)
            .default(0)
            .interact()?;

        match step_selection {
            0 => {
                approved_destructive[idx] = true;
            }
            1 => {
                skipped_any = true;
            }
            2 => {
                println!("❌ Migration cancelled by user");
                return Ok(ApplyOutcome::Cancelled);
            }
            _ => unreachable!(),
        }
    }

    // Now apply: all safe steps + approved destructive steps
    let to_apply: Vec<&RenderedSql> = rendered
        .iter()
        .enumerate()
        .filter(|(i, s)| s.safety == Safety::Safe || approved_destructive[*i])
        .map(|(_, s)| s)
        .collect();

    if to_apply.is_empty() {
        println!("ℹ️  No steps to apply");
        return Ok(ApplyOutcome::Cancelled);
    }

    let executor =
        crate::db::schema_executor::ApplyStepExecutor::new(dev_pool.clone(), verbose, true, false);

    for (i, step) in to_apply.iter().enumerate() {
        if verbose {
            println!("{}", style(&step.sql).dim());
        }
        executor
            .execute_step(&step.sql, step.safety, i + 1)
            .await?;
    }

    verify_final_state(dev_pool, expected_catalog, config, verbose).await?;

    if !verbose {
        println!("\n✅ Applied {} changes", to_apply.len());
    }

    if skipped_any {
        Ok(ApplyOutcome::Skipped)
    } else {
        Ok(ApplyOutcome::Applied)
    }
}
