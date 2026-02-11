use anyhow::Result;
use console::style;
use sqlx::PgPool;

use crate::catalog::Catalog;
use crate::config::Config;
use crate::diff::operations::{MigrationStep, SqlRenderer};
use crate::render::{RenderedSql, Safety};

use super::ApplyOutcome;
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
    verbose: bool,
) -> Result<ApplyOutcome> {
    let rendered: Vec<RenderedSql> = steps.iter().flat_map(|step| step.to_sql()).collect();

    print_plan_header(steps);
    if verbose {
        print_migration_summary(&rendered);
    } else {
        print_concise_plan(steps);
    }
    println!();

    match mode {
        ExecutionMode::DryRun => {
            println!("✅ Dry run completed - no changes applied");
            Ok(ApplyOutcome::Applied) // Dry run shows what would happen
        }

        ExecutionMode::Force => {
            if verbose {
                println!("🚀 Applying all migration steps without confirmation...");
            }
            let outcome = execution_helpers::apply_all_rendered_steps(
                &rendered,
                dev_pool,
                expected_catalog,
                config,
                verbose,
            )
            .await?;
            if !verbose {
                println!("\n✅ Applied {} changes", steps.len());
            }
            Ok(outcome)
        }

        ExecutionMode::SafeOnly => {
            let outcome = execution_helpers::apply_safe_rendered_steps(
                &rendered,
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
            Ok(outcome)
        }

        ExecutionMode::RequireApproval => {
            let has_destructive = rendered.iter().any(|s| s.safety == Safety::Destructive);

            if has_destructive {
                println!("\n⚠️  Destructive operations detected:");
                for step in rendered.iter().filter(|s| s.safety == Safety::Destructive) {
                    let preview = step.sql.lines().next().unwrap_or("");
                    println!("   • {}", preview);
                }
                println!("\nRun with --force to apply, or resolve the schema changes.");
                Ok(ApplyOutcome::DestructiveRequired)
            } else {
                // All safe, apply them
                if verbose {
                    println!("🚀 All operations are safe - applying automatically...");
                }
                let outcome = execution_helpers::apply_all_rendered_steps(
                    &rendered,
                    dev_pool,
                    expected_catalog,
                    config,
                    verbose,
                )
                .await?;
                if !verbose {
                    println!("\n✅ Applied {} changes", steps.len());
                }
                Ok(outcome)
            }
        }

        ExecutionMode::Interactive => {
            // Check if all operations are safe
            let all_safe = rendered.iter().all(|s| s.safety == Safety::Safe);

            if all_safe {
                // Auto-apply when all operations are safe
                if verbose {
                    println!("🚀 All operations are safe - applying automatically...");
                }
                let outcome = execution_helpers::apply_all_rendered_steps(
                    &rendered,
                    dev_pool,
                    expected_catalog,
                    config,
                    verbose,
                )
                .await?;
                if !verbose {
                    println!("\n✅ Applied {} changes", steps.len());
                }
                Ok(outcome)
            } else {
                // Prompt when any destructive operations are present
                user_interaction::execute_with_user_control(
                    &rendered,
                    steps,
                    dev_pool,
                    expected_catalog,
                    config,
                    verbose,
                )
                .await
            }
        }
    }
}

/// Print the plan header line: "📋 N changes" or "📋 N changes (X safe, Y destructive)"
pub fn print_plan_header(steps: &[MigrationStep]) {
    let total = steps.len();
    let destructive = steps.iter().filter(|s| s.has_destructive_sql()).count();

    if destructive > 0 {
        let safe = total - destructive;
        println!(
            "\n📋 {} change{} ({} safe, {} destructive)",
            total,
            if total == 1 { "" } else { "s" },
            safe,
            destructive,
        );
    } else {
        println!(
            "\n📋 {} change{}",
            total,
            if total == 1 { "" } else { "s" },
        );
    }
}

/// Print a concise one-line-per-step plan with grants collapsed
pub fn print_concise_plan(steps: &[MigrationStep]) {
    let non_grants: Vec<_> = steps.iter().filter(|s| !s.is_grant()).collect();
    let grant_count = steps.iter().filter(|s| s.is_grant()).count();

    for step in &non_grants {
        let icon = if step.has_destructive_sql() {
            "  ⚠"
        } else {
            "  ✓"
        };
        println!("{} {}", icon, step.summary());
    }
    if grant_count > 0 {
        println!(
            "  + {} grant change{}",
            grant_count,
            if grant_count == 1 { "" } else { "s" }
        );
    }
}

/// Print detailed migration summary (verbose mode)
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
