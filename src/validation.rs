use crate::catalog::Catalog;
use crate::config::{Config, ObjectFilter};
use crate::diff::operations::{MigrationStep, SqlRenderer};
use crate::diff::{cascade, diff_all, diff_order};
use crate::schema_ops::apply_current_schema_to_shadow;
use anyhow::{Result, anyhow};
use std::path::Path;

/// Validation configuration
#[derive(Debug, Clone)]
pub struct ValidationConfig {
    /// Whether to show detailed differences on validation failure
    pub show_differences: bool,
    /// Whether to apply object filtering during validation
    pub apply_object_filter: bool,
    /// Whether to provide verbose output during validation
    pub verbose: bool,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            show_differences: true,
            apply_object_filter: true,
            verbose: true,
        }
    }
}

/// Result of schema validation
#[derive(Debug)]
pub struct ValidationResult {
    /// Whether the validation passed (no differences found)
    pub passed: bool,
    /// List of migration steps needed to bring schemas in sync
    pub differences: Vec<MigrationStep>,
    /// Summary message
    pub message: String,
}

/// Validate that a database matches the expected schema from files
pub async fn validate_database_against_schema_files(
    dev_catalog: &Catalog,
    config: &Config,
    root_dir: &Path,
    validation_config: &ValidationConfig,
) -> Result<ValidationResult> {
    if validation_config.verbose {
        println!("🔍 Validating database against schema files...");
    }

    let expected_catalog = apply_current_schema_to_shadow(config, root_dir).await?;

    validate_catalogs(dev_catalog, &expected_catalog, config, validation_config)
}

/// Compare two catalogs and return validation result
pub fn validate_catalogs(
    actual_catalog: &Catalog,
    expected_catalog: &Catalog,
    config: &Config,
    validation_config: &ValidationConfig,
) -> Result<ValidationResult> {
    let (actual, expected) = if validation_config.apply_object_filter {
        let filter = ObjectFilter::new(&config.objects, &config.migration.tracking_table);
        (
            filter.filter_catalog(actual_catalog.clone()),
            filter.filter_catalog(expected_catalog.clone()),
        )
    } else {
        (actual_catalog.clone(), expected_catalog.clone())
    };

    if validation_config.verbose {
        println!("🔍 Comparing schemas...");
    }

    let steps = diff_all(&actual, &expected);
    let expanded_steps = cascade::expand(steps, &actual, &expected);
    let ordered_steps = diff_order(expanded_steps, &actual, &expected)?;

    if ordered_steps.is_empty() {
        Ok(ValidationResult {
            passed: true,
            differences: ordered_steps,
            message: "Schema validation passed! Database matches expected schema.".to_string(),
        })
    } else {
        let message = if validation_config.show_differences {
            format_validation_failure(&ordered_steps)
        } else {
            format!(
                "Schema validation failed! Found {} differences.",
                ordered_steps.len()
            )
        };

        Ok(ValidationResult {
            passed: false,
            differences: ordered_steps,
            message,
        })
    }
}

/// Format a detailed validation failure message
fn format_validation_failure(differences: &[MigrationStep]) -> String {
    let mut message = format!(
        "Schema validation failed! Found {} differences:\n",
        differences.len()
    );
    message.push_str("\nRequired changes to bring database in sync:\n");
    message.push_str(&"=".repeat(50));
    message.push('\n');

    for (i, step) in differences.iter().enumerate() {
        message.push_str(&format!("{}. {:?}\n", i + 1, step.id()));
        for rendered in step.to_sql() {
            message.push_str(&format!("   {}\n", rendered.sql));
        }
        message.push('\n');
    }

    message.push_str("💡 To fix these issues:\n");
    message.push_str("   1. Update your schema files to match the database, OR\n");
    message.push_str("   2. Run 'pgmt apply' to apply schema files to the database\n");

    message
}

// Removed unused validate_tracking_table_exists function

/// Enhanced validation with optional file dependency suggestions
pub fn validate_baseline_consistency_with_suggestions(
    baseline_catalog: &Catalog,
    expected_catalog: &Catalog,
    suggest_file_dependencies: bool,
) -> Result<()> {
    let config = Config::default(); // Use default config for baseline validation
    let validation_config = ValidationConfig {
        show_differences: false, // Don't show detailed diffs for baseline validation
        apply_object_filter: false, // Don't filter for baseline validation
        verbose: false,
    };

    let result = validate_catalogs(
        baseline_catalog,
        expected_catalog,
        &config,
        &validation_config,
    )?;

    if result.passed {
        Ok(())
    } else {
        let mut error_msg = format!(
            "Baseline validation failed: baseline does not match expected schema. {} differences found.",
            result.differences.len()
        );

        if suggest_file_dependencies {
            error_msg.push_str("\n\n💡 Possible solutions:");
            error_msg
                .push_str("\n   1. Check if your schema files have correct `-- require:` headers");
            error_msg.push_str("\n   2. Enable file dependency augmentation with: augment_dependencies_from_files: true");
            error_msg.push_str("\n   3. Re-create the baseline with: pgmt baseline create");
            error_msg.push_str("\n   4. Add missing `-- require:` dependencies based on actual object relationships");

            // Analyze missing dependencies and provide specific suggestions
            if let Some(suggestions) = analyze_missing_dependencies(&result.differences) {
                error_msg.push_str("\n\n🔍 Dependency analysis suggests:");
                error_msg.push_str(&suggestions);
            }
        }

        Err(anyhow!(error_msg))
    }
}

/// Analyze migration differences to suggest missing file dependencies
fn analyze_missing_dependencies(
    differences: &[crate::diff::operations::MigrationStep],
) -> Option<String> {
    use crate::diff::operations::MigrationStep;

    let mut suggestions = Vec::new();
    let mut dependency_issues = Vec::new();

    for step in differences {
        match step {
            MigrationStep::Table(_) => {
                dependency_issues.push("Tables may depend on schemas or custom types");
            }
            MigrationStep::View(_) => {
                dependency_issues.push("Views may depend on tables, other views, or functions");
            }
            MigrationStep::Function(_) => {
                dependency_issues.push("Functions may depend on custom types or other functions");
            }
            MigrationStep::Index(_) => {
                dependency_issues
                    .push("Indexes may depend on tables and functions (for expression indexes)");
            }
            MigrationStep::Constraint(_) => {
                dependency_issues.push("Constraints may depend on tables and custom types");
            }
            MigrationStep::Trigger(_) => {
                dependency_issues.push("Triggers may depend on tables and functions");
            }
            _ => {} // Other objects have fewer common dependency issues
        }
    }

    if !dependency_issues.is_empty() {
        suggestions.push(
            "\n   - Review object creation order and add appropriate `-- require:` headers"
                .to_string(),
        );
        suggestions.push("\n   - Common dependency patterns:".to_string());
        for (_i, issue) in dependency_issues.iter().enumerate().take(5) {
            suggestions.push(format!("\n     • {}", issue));
        }
        if dependency_issues.len() > 5 {
            suggestions.push(format!(
                "\n     • ... and {} more dependency patterns",
                dependency_issues.len() - 5
            ));
        }
    }

    if suggestions.is_empty() {
        None
    } else {
        Some(suggestions.join(""))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn test_validation_config_default() {
        let config = ValidationConfig::default();
        assert!(config.show_differences);
        assert!(config.apply_object_filter);
        assert!(config.verbose);
    }

    #[test]
    fn test_validate_catalogs_same() {
        let catalog = Catalog::empty();
        let config = Config::default();
        let validation_config = ValidationConfig::default();

        let result = validate_catalogs(&catalog, &catalog, &config, &validation_config).unwrap();
        assert!(result.passed);
        assert!(result.differences.is_empty());
    }

    #[test]
    fn test_format_validation_failure() {
        let differences = vec![]; // Empty for test
        let message = format_validation_failure(&differences);
        assert!(message.contains("Schema validation failed"));
        assert!(message.contains("To fix these issues"));
    }
}
