use crate::catalog::Catalog;
use crate::config::{ColumnOrderMode, Config, ObjectFilter};
use crate::diff::operations::{MigrationStep, SqlRenderer};
use crate::diff::{cascade, diff_all, diff_order};
use crate::schema_ops::apply_current_schema_to_shadow;
use anyhow::Result;
use std::collections::HashSet;
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

/// Baseline validation result with detailed differences
#[derive(Debug)]
pub struct BaselineValidationError {
    pub differences: Vec<MigrationStep>,
}

impl std::error::Error for BaselineValidationError {}

impl std::fmt::Display for BaselineValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Baseline validation found {} unexpected difference(s):\n",
            self.differences.len()
        )?;

        for (i, step) in self.differences.iter().enumerate() {
            writeln!(f, "  {}. {:?}", i + 1, step.id())?;
            for rendered in step.to_sql() {
                // Truncate long SQL for readability
                let sql = if rendered.sql.len() > 100 {
                    format!("{}...", &rendered.sql[..100])
                } else {
                    rendered.sql.clone()
                };
                writeln!(f, "     {}", sql)?;
            }
        }

        Ok(())
    }
}

/// Enhanced validation with optional file dependency suggestions
pub fn validate_baseline_consistency_with_suggestions(
    baseline_catalog: &Catalog,
    expected_catalog: &Catalog,
    _suggest_file_dependencies: bool,
) -> Result<(), BaselineValidationError> {
    let config = Config::default();
    let validation_config = ValidationConfig {
        show_differences: false,
        apply_object_filter: false,
        verbose: false,
    };

    let result = validate_catalogs(
        baseline_catalog,
        expected_catalog,
        &config,
        &validation_config,
    )
    .map_err(|_| BaselineValidationError {
        differences: vec![],
    })?;

    if result.passed {
        Ok(())
    } else {
        Err(BaselineValidationError {
            differences: result.differences,
        })
    }
}

/// Represents a column order violation
#[derive(Debug, Clone)]
pub struct ColumnOrderViolation {
    pub schema: String,
    pub table: String,
    pub new_column: String,
    pub old_column_after: String,
}

impl std::fmt::Display for ColumnOrderViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Table {}.{}: new column '{}' must come after existing column '{}'",
            self.schema, self.table, self.new_column, self.old_column_after
        )
    }
}

/// Validate column ordering for all tables.
///
/// PostgreSQL's `ALTER TABLE ADD COLUMN` always appends columns to the end.
/// This function checks that new columns in the target schema are placed at
/// the end of table definitions, so the schema files match physical column order.
pub fn validate_column_order(
    old_catalog: &Catalog,
    new_catalog: &Catalog,
) -> Vec<ColumnOrderViolation> {
    let mut violations = Vec::new();

    // Build a lookup of old tables by (schema, name)
    let old_tables: std::collections::HashMap<(&str, &str), &crate::catalog::table::Table> =
        old_catalog
            .tables
            .iter()
            .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
            .collect();

    for new_table in &new_catalog.tables {
        // Skip new tables (they don't have existing columns to validate against)
        let Some(old_table) = old_tables.get(&(new_table.schema.as_str(), new_table.name.as_str()))
        else {
            continue;
        };

        // Build set of column names from old table
        let old_columns: HashSet<&str> =
            old_table.columns.iter().map(|c| c.name.as_str()).collect();

        // Track when we first see a "new" column (not in old set)
        let mut seen_new_column: Option<&str> = None;

        for column in &new_table.columns {
            let is_old_column = old_columns.contains(column.name.as_str());

            if !is_old_column {
                // This is a new column
                if seen_new_column.is_none() {
                    seen_new_column = Some(&column.name);
                }
            } else {
                // This is an old column
                // If we've already seen a new column, this is a violation
                if let Some(new_col_name) = seen_new_column {
                    violations.push(ColumnOrderViolation {
                        schema: new_table.schema.clone(),
                        table: new_table.name.clone(),
                        new_column: new_col_name.to_string(),
                        old_column_after: column.name.clone(),
                    });
                    // Only report one violation per table to keep error message concise
                    break;
                }
            }
        }
    }

    violations
}

/// Apply column order validation based on mode.
///
/// Returns an error if validation fails in strict mode.
pub fn apply_column_order_validation(
    old_catalog: &Catalog,
    new_catalog: &Catalog,
    mode: ColumnOrderMode,
) -> Result<()> {
    if mode == ColumnOrderMode::Relaxed {
        return Ok(());
    }

    let violations = validate_column_order(old_catalog, new_catalog);

    if violations.is_empty() {
        return Ok(());
    }

    match mode {
        ColumnOrderMode::Strict => {
            let mut message = String::from("Column order validation failed.\n\n");
            for violation in &violations {
                message.push_str(&format!("{}\n", violation));
            }
            message.push_str("\nTo fix: Move new columns to the end of your table definition.\n");
            message.push_str(
                "To disable this check: Set `migration.column_order: relaxed` in pgmt.yaml",
            );
            Err(anyhow::anyhow!("{}", message))
        }
        ColumnOrderMode::Warn => {
            eprintln!("Warning: Column order validation issues detected:\n");
            for violation in &violations {
                eprintln!("  {}", violation);
            }
            eprintln!(
                "\nNew columns should be placed at the end of table definitions to match physical column order."
            );
            Ok(())
        }
        ColumnOrderMode::Relaxed => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::id::DbObjectId;
    use crate::catalog::table::{Column, Table};
    use crate::config::Config;

    fn make_test_table(schema: &str, name: &str, columns: Vec<&str>) -> Table {
        let columns = columns
            .into_iter()
            .map(|col_name| Column {
                name: col_name.to_string(),
                data_type: "text".to_string(),
                default: None,
                generated: None,
                comment: None,
                depends_on: vec![],
                not_null: false,
            })
            .collect();

        Table::new(
            schema.to_string(),
            name.to_string(),
            columns,
            None,
            None,
            vec![DbObjectId::Schema {
                name: schema.to_string(),
            }],
        )
    }

    fn make_catalog_with_table(table: Table) -> Catalog {
        let mut catalog = Catalog::empty();
        catalog.tables.push(table);
        catalog
    }

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

    #[test]
    fn test_new_column_at_end_passes() {
        let old_table = make_test_table("public", "users", vec!["id", "name"]);
        let new_table = make_test_table("public", "users", vec!["id", "name", "email"]);

        let old_catalog = make_catalog_with_table(old_table);
        let new_catalog = make_catalog_with_table(new_table);

        let violations = validate_column_order(&old_catalog, &new_catalog);
        assert!(violations.is_empty());
    }

    #[test]
    fn test_new_column_in_middle_fails() {
        let old_table = make_test_table("public", "users", vec!["id", "name"]);
        let new_table = make_test_table("public", "users", vec!["id", "email", "name"]);

        let old_catalog = make_catalog_with_table(old_table);
        let new_catalog = make_catalog_with_table(new_table);

        let violations = validate_column_order(&old_catalog, &new_catalog);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].schema, "public");
        assert_eq!(violations[0].table, "users");
        assert_eq!(violations[0].new_column, "email");
        assert_eq!(violations[0].old_column_after, "name");
    }

    #[test]
    fn test_new_column_at_start_fails() {
        let old_table = make_test_table("public", "users", vec!["id", "name"]);
        let new_table = make_test_table("public", "users", vec!["email", "id", "name"]);

        let old_catalog = make_catalog_with_table(old_table);
        let new_catalog = make_catalog_with_table(new_table);

        let violations = validate_column_order(&old_catalog, &new_catalog);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].new_column, "email");
        assert_eq!(violations[0].old_column_after, "id");
    }

    #[test]
    fn test_multiple_new_columns_at_end_passes() {
        let old_table = make_test_table("public", "users", vec!["id", "name"]);
        let new_table = make_test_table("public", "users", vec!["id", "name", "email", "phone"]);

        let old_catalog = make_catalog_with_table(old_table);
        let new_catalog = make_catalog_with_table(new_table);

        let violations = validate_column_order(&old_catalog, &new_catalog);
        assert!(violations.is_empty());
    }

    #[test]
    fn test_new_table_no_validation() {
        // New tables don't need validation - all columns are "new"
        let old_catalog = Catalog::empty();
        let new_table = make_test_table("public", "users", vec!["id", "name", "email"]);
        let new_catalog = make_catalog_with_table(new_table);

        let violations = validate_column_order(&old_catalog, &new_catalog);
        assert!(violations.is_empty());
    }

    #[test]
    fn test_dropped_columns_ignored() {
        // Dropped columns shouldn't affect validation
        let old_table = make_test_table("public", "users", vec!["id", "legacy", "name"]);
        let new_table = make_test_table("public", "users", vec!["id", "name", "email"]);

        let old_catalog = make_catalog_with_table(old_table);
        let new_catalog = make_catalog_with_table(new_table);

        let violations = validate_column_order(&old_catalog, &new_catalog);
        assert!(violations.is_empty());
    }

    #[test]
    fn test_relaxed_mode_allows_violations() {
        let old_table = make_test_table("public", "users", vec!["id", "name"]);
        let new_table = make_test_table("public", "users", vec!["id", "email", "name"]);

        let old_catalog = make_catalog_with_table(old_table);
        let new_catalog = make_catalog_with_table(new_table);

        let result =
            apply_column_order_validation(&old_catalog, &new_catalog, ColumnOrderMode::Relaxed);
        assert!(result.is_ok());
    }

    #[test]
    fn test_strict_mode_rejects_violations() {
        let old_table = make_test_table("public", "users", vec!["id", "name"]);
        let new_table = make_test_table("public", "users", vec!["id", "email", "name"]);

        let old_catalog = make_catalog_with_table(old_table);
        let new_catalog = make_catalog_with_table(new_table);

        let result =
            apply_column_order_validation(&old_catalog, &new_catalog, ColumnOrderMode::Strict);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Column order validation failed"));
        assert!(err.contains("email"));
    }

    #[test]
    fn test_warn_mode_allows_violations() {
        let old_table = make_test_table("public", "users", vec!["id", "name"]);
        let new_table = make_test_table("public", "users", vec!["id", "email", "name"]);

        let old_catalog = make_catalog_with_table(old_table);
        let new_catalog = make_catalog_with_table(new_table);

        let result =
            apply_column_order_validation(&old_catalog, &new_catalog, ColumnOrderMode::Warn);
        assert!(result.is_ok());
    }

    #[test]
    fn test_column_order_violation_display() {
        let violation = ColumnOrderViolation {
            schema: "public".to_string(),
            table: "users".to_string(),
            new_column: "email".to_string(),
            old_column_after: "name".to_string(),
        };
        let display = format!("{}", violation);
        assert!(display.contains("public.users"));
        assert!(display.contains("email"));
        assert!(display.contains("name"));
    }
}
