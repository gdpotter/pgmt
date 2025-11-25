use anyhow::Result;
use console::style;
use dialoguer::{Confirm, Select};
use sqlx::PgPool;

use crate::catalog::Catalog;
use crate::config::Config;
use crate::render::{RenderedSql, Safety};

use super::execute_sql_with_context;
use super::execution_helpers;
use super::verification::verify_final_state;

/// Execute migration steps with enhanced user control and recovery options
pub async fn execute_with_user_control(
    rendered: &[RenderedSql],
    dev_pool: &PgPool,
    expected_catalog: &Catalog,
    config: &Config,
) -> Result<()> {
    loop {
        // Show current migration overview
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

        // Present user options
        let options = vec![
            "Apply all steps",
            "Apply only safe steps",
            "Review steps individually",
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
                println!("🚀 Applying all migration steps...");
                return execution_helpers::apply_all_rendered_steps(
                    rendered,
                    dev_pool,
                    expected_catalog,
                    config,
                    true,
                )
                .await;
            }
            1 => {
                // Apply only safe steps
                println!("🛡️  Applying only safe operations...");
                return execution_helpers::apply_safe_rendered_steps(
                    rendered,
                    dev_pool,
                    expected_catalog,
                    config,
                    true,
                    true,
                )
                .await;
            }
            2 => {
                // Review steps individually
                return review_steps_individually(rendered, dev_pool, expected_catalog, config)
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
                return Ok(());
            }
            _ => unreachable!(),
        }
    }
}

/// Review and apply steps individually with granular control
async fn review_steps_individually(
    rendered: &[RenderedSql],
    dev_pool: &PgPool,
    expected_catalog: &Catalog,
    config: &Config,
) -> Result<()> {
    let mut applied_any = false;

    for (i, step) in rendered.iter().enumerate() {
        let (icon, safety_label) = match step.safety {
            Safety::Safe => ("✅", style("SAFE").green()),
            Safety::Destructive => ("⚠️", style("DESTRUCTIVE").red()),
        };

        println!(
            "\n{} Step {}/{}: {}",
            icon,
            i + 1,
            rendered.len(),
            safety_label
        );
        println!("{}", style("─".repeat(60)).dim());
        println!("{}", step.sql);
        println!("{}", style("─".repeat(60)).dim());

        let step_options = vec![
            "Apply this step",
            "Skip this step",
            "Apply remaining steps automatically",
            "Cancel migration",
        ];

        let step_selection = Select::new()
            .with_prompt(format!("Action for step {}?", i + 1))
            .items(&step_options)
            .default(0)
            .interact()?;

        match step_selection {
            0 => {
                // Apply this step
                println!("🚀 Applying step {}...", i + 1);
                execute_sql_with_context(dev_pool, &step.sql, &format!("step {}", i + 1)).await?;
                println!("✅ Step {} completed", i + 1);
                applied_any = true;
            }
            1 => {
                // Skip this step
                println!("⏭️  Skipped step {}", i + 1);
                continue;
            }
            2 => {
                // Apply remaining steps automatically
                println!("🚀 Applying remaining steps automatically...");
                for (j, remaining_step) in rendered.iter().enumerate().skip(i) {
                    let step_prefix = match remaining_step.safety {
                        Safety::Safe => "✅",
                        Safety::Destructive => "⚠️",
                    };
                    println!(
                        "{} Auto-applying step {}/{}",
                        step_prefix,
                        j + 1,
                        rendered.len()
                    );
                    execute_sql_with_context(
                        dev_pool,
                        &remaining_step.sql,
                        &format!("step {}", j + 1),
                    )
                    .await?;
                    applied_any = true;
                }
                break;
            }
            3 => {
                // Cancel migration
                println!("❌ Migration cancelled by user");
                if applied_any {
                    println!("⚠️  Some steps were already applied");
                }
                return Ok(());
            }
            _ => unreachable!(),
        }
    }

    if applied_any {
        println!("✅ Individual step review completed");
        // Verify final state
        verify_final_state(dev_pool, expected_catalog, config).await?;
    } else {
        println!("ℹ️  No steps were applied");
    }

    Ok(())
}
