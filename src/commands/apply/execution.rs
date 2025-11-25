use anyhow::Result;
use console::style;
use sqlx::PgPool;

use crate::catalog::Catalog;
use crate::config::Config;
use crate::diff::operations::{MigrationStep, SqlRenderer};
use crate::render::{RenderedSql, Safety};

use super::ExecutionMode;
use super::execution_helpers;
use super::user_interaction;

/// Execute migration plan based on execution mode
pub async fn execute_plan(
    steps: &[MigrationStep],
    dev_pool: &PgPool,
    mode: ExecutionMode,
    expected_catalog: &Catalog,
    config: &Config,
) -> Result<()> {
    let rendered: Vec<RenderedSql> = steps.iter().flat_map(|step| step.to_sql()).collect();

    print_migration_summary(&rendered);

    match mode {
        ExecutionMode::DryRun => {
            println!("✅ Dry run completed - no changes applied");
            Ok(())
        }

        ExecutionMode::ForceAll => {
            println!("🚀 Applying all migration steps without confirmation...");
            execution_helpers::apply_all_rendered_steps(
                &rendered,
                dev_pool,
                expected_catalog,
                config,
                true,
            )
            .await
        }

        ExecutionMode::ConfirmAll => {
            user_interaction::execute_with_user_control(
                &rendered,
                dev_pool,
                expected_catalog,
                config,
            )
            .await
        }

        ExecutionMode::SafeOnly => {
            execution_helpers::apply_safe_rendered_steps(
                &rendered,
                dev_pool,
                expected_catalog,
                config,
                true,
                true,
            )
            .await
        }

        ExecutionMode::AutoSafe => {
            // Check if all operations are safe
            let all_safe = rendered.iter().all(|s| s.safety == Safety::Safe);

            if all_safe {
                // Auto-apply when all operations are safe
                println!("🚀 All operations are safe - applying automatically...");
                execution_helpers::apply_all_rendered_steps(
                    &rendered,
                    dev_pool,
                    expected_catalog,
                    config,
                    true,
                )
                .await
            } else {
                // Prompt when any destructive operations are present
                user_interaction::execute_with_user_control(
                    &rendered,
                    dev_pool,
                    expected_catalog,
                    config,
                )
                .await
            }
        }
    }
}

/// Print detailed migration summary
pub fn print_migration_summary(rendered: &[RenderedSql]) {
    println!("\n📋 {}", style("Migration Plan").bold().underlined());

    let safe_count = rendered.iter().filter(|s| s.safety == Safety::Safe).count();
    let destructive_count = rendered
        .iter()
        .filter(|s| s.safety == Safety::Destructive)
        .count();

    println!(
        "   ✅ {} safe operation{}",
        safe_count,
        if safe_count == 1 { "" } else { "s" }
    );
    if destructive_count > 0 {
        println!(
            "   ⚠️  {} destructive operation{}",
            destructive_count,
            if destructive_count == 1 { "" } else { "s" }
        );
    }
    println!();

    for (i, step) in rendered.iter().enumerate() {
        let (icon, label) = match step.safety {
            Safety::Safe => ("✅", style("SAFE").green()),
            Safety::Destructive => ("⚠️", style("DESTRUCTIVE").red()),
        };

        println!(
            "{} Step {}: {} {}",
            icon,
            i + 1,
            label,
            style("─".repeat(50)).dim()
        );

        // Show a preview of the SQL (first line or two)
        let sql_preview = step.sql.lines().take(2).collect::<Vec<_>>().join("\n");

        if sql_preview.len() > 100 {
            println!("{}", style(format!("{}...", &sql_preview[..97])).dim());
        } else {
            println!("{}", style(&sql_preview).dim());
        }

        if step.sql.lines().count() > 2 {
            println!("{}", style("   ... (truncated)").dim().italic());
        }
        println!();
    }
}
