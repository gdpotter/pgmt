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

    // Check column order mismatches (respecting ColumnOrderMode)
    let column_order_mismatches = if config.migration.column_order != ColumnOrderMode::Relaxed {
        find_column_order_mismatches(&actual, &expected)
    } else {
        Vec::new()
    };

    // Warn about column order mismatches if in warn mode
    if config.migration.column_order == ColumnOrderMode::Warn && !column_order_mismatches.is_empty()
    {
        eprintln!("Warning: Column order mismatches detected:\n");
        for mismatch in &column_order_mismatches {
            eprintln!("  {}", mismatch);
        }
        eprintln!("\nSchema files have columns in a different order than the database.");
    }

    let has_column_order_errors = config.migration.column_order == ColumnOrderMode::Strict
        && !column_order_mismatches.is_empty();

    if ordered_steps.is_empty() && !has_column_order_errors {
        Ok(ValidationResult {
            passed: true,
            differences: ordered_steps,
            message: "Schema validation passed! Database matches expected schema.".to_string(),
        })
    } else {
        let message = if validation_config.show_differences {
            format_validation_failure(
                &ordered_steps,
                &column_order_mismatches,
                has_column_order_errors,
            )
        } else {
            let total = ordered_steps.len()
                + if has_column_order_errors {
                    column_order_mismatches.len()
                } else {
                    0
                };
            format!("Schema validation failed! Found {} differences.", total)
        };

        Ok(ValidationResult {
            passed: false,
            differences: ordered_steps,
            message,
        })
    }
}

/// Format a detailed validation failure message
fn format_validation_failure(
    differences: &[MigrationStep],
    column_order_mismatches: &[ColumnOrderMismatch],
    include_column_order_errors: bool,
) -> String {
    let total_issues = differences.len()
        + if include_column_order_errors {
            column_order_mismatches.len()
        } else {
            0
        };
    let mut message = format!(
        "Schema validation failed! Found {} issue(s):\n",
        total_issues
    );

    if !differences.is_empty() {
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
    }

    if include_column_order_errors && !column_order_mismatches.is_empty() {
        message.push_str("\nColumn order mismatches:\n");
        message.push_str(&"=".repeat(50));
        message.push('\n');

        for mismatch in column_order_mismatches {
            message.push_str(&format!("{}\n\n", mismatch));
        }
    }

    message.push_str("💡 To fix these issues:\n");
    if !differences.is_empty() {
        message.push_str("   1. Update your schema files to match the database, OR\n");
        message.push_str("   2. Run 'pgmt apply' to apply schema files to the database\n");
    }
    if include_column_order_errors && !column_order_mismatches.is_empty() {
        message.push_str("   - For column order: Update schema files to match the physical column order in the database\n");
        message.push_str("   - To disable column order checks: Set `migration.column_order: relaxed` in pgmt.yaml\n");
    }

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

/// Represents a column order violation (new column not at end)
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

/// Represents a column order mismatch between actual and expected catalogs
#[derive(Debug, Clone)]
pub struct ColumnOrderMismatch {
    pub schema: String,
    pub table: String,
    pub expected_order: Vec<String>,
    pub actual_order: Vec<String>,
}

impl std::fmt::Display for ColumnOrderMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Table {}.{}: column order mismatch\n  Expected: [{}]\n  Actual:   [{}]",
            self.schema,
            self.table,
            self.expected_order.join(", "),
            self.actual_order.join(", ")
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

/// Find tables where column order differs between actual and expected catalogs.
///
/// This detects when schema files have columns in a different order than the
/// actual database, which can happen when someone hand-writes migrations and
/// places columns in the wrong position in schema files.
pub fn find_column_order_mismatches(
    actual_catalog: &Catalog,
    expected_catalog: &Catalog,
) -> Vec<ColumnOrderMismatch> {
    let mut mismatches = Vec::new();

    // Build a lookup of actual tables by (schema, name)
    let actual_tables: std::collections::HashMap<(&str, &str), &crate::catalog::table::Table> =
        actual_catalog
            .tables
            .iter()
            .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
            .collect();

    for expected_table in &expected_catalog.tables {
        let Some(actual_table) =
            actual_tables.get(&(expected_table.schema.as_str(), expected_table.name.as_str()))
        else {
            continue;
        };

        // Get column names in order
        let expected_columns: Vec<&str> = expected_table
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        let actual_columns: Vec<&str> = actual_table
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();

        // Only compare if both tables have the same set of columns
        let expected_set: HashSet<&str> = expected_columns.iter().copied().collect();
        let actual_set: HashSet<&str> = actual_columns.iter().copied().collect();

        if expected_set != actual_set {
            // Different columns - the regular diff will catch this
            continue;
        }

        // Same columns, check if order matches
        if expected_columns != actual_columns {
            mismatches.push(ColumnOrderMismatch {
                schema: expected_table.schema.clone(),
                table: expected_table.name.clone(),
                expected_order: expected_columns.iter().map(|s| s.to_string()).collect(),
                actual_order: actual_columns.iter().map(|s| s.to_string()).collect(),
            });
        }
    }

    mismatches
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
        let column_order_mismatches = vec![];
        let message = format_validation_failure(&differences, &column_order_mismatches, false);
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

    #[test]
    fn test_find_column_order_mismatches_same_order() {
        let table1 = make_test_table("public", "users", vec!["id", "name", "email"]);
        let table2 = make_test_table("public", "users", vec!["id", "name", "email"]);

        let catalog1 = make_catalog_with_table(table1);
        let catalog2 = make_catalog_with_table(table2);

        let mismatches = find_column_order_mismatches(&catalog1, &catalog2);
        assert!(mismatches.is_empty());
    }

    #[test]
    fn test_find_column_order_mismatches_different_order() {
        // Actual DB has columns in one order
        let actual_table = make_test_table("public", "users", vec!["id", "name", "email"]);
        // Schema file has them in different order
        let expected_table = make_test_table("public", "users", vec!["id", "email", "name"]);

        let actual_catalog = make_catalog_with_table(actual_table);
        let expected_catalog = make_catalog_with_table(expected_table);

        let mismatches = find_column_order_mismatches(&actual_catalog, &expected_catalog);
        assert_eq!(mismatches.len(), 1);
        assert_eq!(mismatches[0].schema, "public");
        assert_eq!(mismatches[0].table, "users");
        assert_eq!(mismatches[0].actual_order, vec!["id", "name", "email"]);
        assert_eq!(mismatches[0].expected_order, vec!["id", "email", "name"]);
    }

    #[test]
    fn test_find_column_order_mismatches_different_columns_ignored() {
        // If catalogs have different columns, diff_all handles it
        let actual_table = make_test_table("public", "users", vec!["id", "name"]);
        let expected_table = make_test_table("public", "users", vec!["id", "name", "email"]);

        let actual_catalog = make_catalog_with_table(actual_table);
        let expected_catalog = make_catalog_with_table(expected_table);

        let mismatches = find_column_order_mismatches(&actual_catalog, &expected_catalog);
        assert!(mismatches.is_empty()); // Different column sets - not a mismatch, it's a diff
    }

    #[test]
    fn test_find_column_order_mismatches_new_table_ignored() {
        // Table only in expected - not a mismatch
        let expected_table = make_test_table("public", "users", vec!["id", "name"]);
        let actual_catalog = Catalog::empty();
        let expected_catalog = make_catalog_with_table(expected_table);

        let mismatches = find_column_order_mismatches(&actual_catalog, &expected_catalog);
        assert!(mismatches.is_empty());
    }

    #[test]
    fn test_column_order_mismatch_display() {
        let mismatch = ColumnOrderMismatch {
            schema: "public".to_string(),
            table: "users".to_string(),
            expected_order: vec!["id".to_string(), "email".to_string(), "name".to_string()],
            actual_order: vec!["id".to_string(), "name".to_string(), "email".to_string()],
        };
        let display = format!("{}", mismatch);
        assert!(display.contains("public.users"));
        assert!(display.contains("column order mismatch"));
        assert!(display.contains("id, email, name"));
        assert!(display.contains("id, name, email"));
    }
}
