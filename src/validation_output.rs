use crate::validation::ValidationResult as CoreValidationResult;
use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Options for validation output formatting
#[derive(Debug, Clone)]
pub struct ValidationOutputOptions {
    /// Output format: "human" or "json"
    pub format: String,
    /// Suppress verbose output (useful with JSON)
    pub quiet: bool,
    /// Show detailed validation information
    pub verbose: bool,
    /// Ignore specific migrations during validation
    #[allow(dead_code)]
    pub ignore_migrations: Vec<String>,
}

impl Default for ValidationOutputOptions {
    fn default() -> Self {
        Self {
            format: "human".to_string(),
            quiet: false,
            verbose: false,
            ignore_migrations: vec![],
        }
    }
}

/// JSON output structure for validation results (CI integration)
#[derive(Debug, Serialize, Deserialize)]
pub struct ValidationJsonOutput {
    /// Validation result: "success" or "conflict"
    pub status: String,
    /// Exit code for CI systems
    pub exit_code: i32,
    /// Baseline information
    pub baseline: Option<BaselineInfo>,
    /// List of applied migrations
    pub applied_migrations: Vec<u64>,
    /// List of unapplied migrations
    pub unapplied_migrations: Vec<u64>,
    /// Details about conflicts found
    pub conflicts: Vec<ConflictInfo>,
    /// Suggested actions for resolving conflicts
    pub suggested_actions: Vec<SuggestedAction>,
    /// Human-readable summary message
    pub message: String,
}

/// Information about the baseline used for validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaselineInfo {
    /// Baseline version timestamp
    pub version: u64,
    /// Number of objects in baseline
    pub object_count: usize,
    /// Baseline description
    pub description: String,
}

/// Information about a specific conflict
#[derive(Debug, Serialize, Deserialize)]
pub struct ConflictInfo {
    /// Type of database object (table, view, function, etc.)
    pub object_type: String,
    /// Name of the conflicting object
    pub object_name: String,
    /// Type of conflict (unexpected_existence, missing_object, modified_definition, etc.)
    pub conflict_type: String,
    /// Likely source of the conflict (migration version, manual change, etc.)
    pub likely_source: Option<String>,
    /// Detailed description of the conflict
    pub details: String,
}

/// Suggested action for resolving conflicts
#[derive(Debug, Serialize, Deserialize)]
pub struct SuggestedAction {
    /// Type of action (rebase_migration, apply_migration, manual_fix, etc.)
    pub action: String,
    /// Target for the action (migration version, file, etc.)
    pub target: Option<String>,
    /// Command to execute
    pub command: Option<String>,
    /// Human-readable description
    pub description: String,
}

/// Format and output validation results based on options
pub fn format_validation_output(
    result: &CoreValidationResult,
    options: &ValidationOutputOptions,
    applied_migrations: &[u64],
    unapplied_migrations: &[u64],
    baseline_info: Option<&BaselineInfo>,
) -> Result<String> {
    match options.format.as_str() {
        "json" => {
            let json_output = create_json_output(
                result,
                applied_migrations,
                unapplied_migrations,
                baseline_info,
            )?;
            Ok(serde_json::to_string_pretty(&json_output)?)
        }
        "human" => Ok(format_human_output(result, options)),
        _ => Ok(format_human_output(result, options)),
    }
}

/// Create JSON output structure from validation result
fn create_json_output(
    result: &CoreValidationResult,
    applied_migrations: &[u64],
    unapplied_migrations: &[u64],
    baseline_info: Option<&BaselineInfo>,
) -> Result<ValidationJsonOutput> {
    let status = if result.passed { "success" } else { "conflict" };
    let exit_code = if result.passed { 0 } else { 1 };

    // Extract conflict information from validation result
    let conflicts = extract_conflicts_from_result(result);
    let suggested_actions = generate_suggested_actions(&conflicts);

    Ok(ValidationJsonOutput {
        status: status.to_string(),
        exit_code,
        baseline: baseline_info.cloned(),
        applied_migrations: applied_migrations.to_vec(),
        unapplied_migrations: unapplied_migrations.to_vec(),
        conflicts,
        suggested_actions,
        message: result.message.clone(),
    })
}

/// Format human-readable output
fn format_human_output(result: &CoreValidationResult, options: &ValidationOutputOptions) -> String {
    if result.passed {
        let mut output = String::new();

        if !options.quiet {
            output.push_str("✅ Migration consistency validation passed!\n");
            output.push_str("   Schema files match expected state from baseline + migrations.\n");

            if options.verbose {
                // CoreValidationResult doesn't have baseline_info, applied_migrations fields
                // These will be passed separately to the format_validation_output function
                output.push_str("📊 Detailed validation information available in verbose mode\n");
            }
        }

        output
    } else {
        let mut output = String::new();

        if !options.quiet {
            output.push_str("❌ Migration consistency validation failed!\n\n");
            output.push_str("🔍 Expected state: baseline + applied migrations\n");
            output.push_str("📄 Current state: schema files as they exist now\n\n");
        }

        output.push_str(&result.message);
        output.push('\n');

        if !options.quiet {
            output.push_str("\n💡 This typically means:\n");
            output.push_str(
                "   1. Other migrations were merged that conflict with your local changes\n",
            );
            output.push_str(
                "   2. You need to update your migration to account for the new base state\n",
            );
            output.push_str(
                "   3. Schema files have been modified without updating the migration\n\n",
            );
            output.push_str("🔧 Suggested actions:\n");
            output.push_str("   1. Run: git pull origin main  # Get latest migrations\n");
            output.push_str(
                "   2. Run: pgmt migrate update   # Regenerate migration from current state\n",
            );
            output
                .push_str("   3. Or: manually resolve conflicts and update your migration file\n");
        }

        output
    }
}

/// Extract conflict information from validation result differences
fn extract_conflicts_from_result(result: &CoreValidationResult) -> Vec<ConflictInfo> {
    let mut conflicts = Vec::new();

    if !result.passed {
        // Analyze each migration step to classify conflicts
        for step in &result.differences {
            let conflict_info = classify_migration_step_conflict(step);
            conflicts.push(conflict_info);
        }

        // If no specific conflicts found but validation failed, add generic conflict
        if conflicts.is_empty() {
            conflicts.push(ConflictInfo {
                object_type: "unknown".to_string(),
                object_name: "detected from diff".to_string(),
                conflict_type: "schema_mismatch".to_string(),
                likely_source: Some("unapplied_migration".to_string()),
                details: "Schema files contain changes not reflected in applied migrations"
                    .to_string(),
            });
        }
    }

    conflicts
}

/// Classify a migration step into a conflict type
fn classify_migration_step_conflict(step: &crate::diff::operations::MigrationStep) -> ConflictInfo {
    use crate::diff::operations::MigrationStep;

    match step {
        MigrationStep::Table(table_op) => {
            use crate::diff::operations::TableOperation;
            match table_op {
                TableOperation::Create { name, .. } => ConflictInfo {
                    object_type: "table".to_string(),
                    object_name: name.clone(),
                    conflict_type: "unexpected_existence".to_string(),
                    likely_source: Some("unapplied_migration".to_string()),
                    details: format!(
                        "Table '{}' exists in current schema but not in expected state",
                        name
                    ),
                },
                TableOperation::Drop { name, .. } => ConflictInfo {
                    object_type: "table".to_string(),
                    object_name: name.clone(),
                    conflict_type: "missing_object".to_string(),
                    likely_source: Some("manual_change".to_string()),
                    details: format!(
                        "Table '{}' expected in applied migrations but missing from current schema",
                        name
                    ),
                },
                TableOperation::Alter { name, actions, .. } => {
                    // For ALTER operations, analyze the specific actions
                    let action_summary = format!("{} column actions", actions.len());
                    ConflictInfo {
                        object_type: "table".to_string(),
                        object_name: name.clone(),
                        conflict_type: "modified_definition".to_string(),
                        likely_source: Some("unapplied_migration".to_string()),
                        details: format!(
                            "Table '{}' has {} that differ from expected state",
                            name, action_summary
                        ),
                    }
                }
                _ => ConflictInfo {
                    object_type: "table".to_string(),
                    object_name: "unknown".to_string(),
                    conflict_type: "modified_definition".to_string(),
                    likely_source: Some("unapplied_migration".to_string()),
                    details: "Table definition differs from expected state".to_string(),
                },
            }
        }
        MigrationStep::View(view_op) => {
            use crate::diff::operations::ViewOperation;
            match view_op {
                ViewOperation::Create { name, .. } | ViewOperation::Replace { name, .. } => {
                    ConflictInfo {
                        object_type: "view".to_string(),
                        object_name: name.clone(),
                        conflict_type: "unexpected_existence".to_string(),
                        likely_source: Some("unapplied_migration".to_string()),
                        details: format!(
                            "View '{}' exists in current schema but not in expected state",
                            name
                        ),
                    }
                }
                ViewOperation::Drop { name, .. } => ConflictInfo {
                    object_type: "view".to_string(),
                    object_name: name.clone(),
                    conflict_type: "missing_object".to_string(),
                    likely_source: Some("manual_change".to_string()),
                    details: format!(
                        "View '{}' expected in applied migrations but missing from current schema",
                        name
                    ),
                },
                _ => ConflictInfo {
                    object_type: "view".to_string(),
                    object_name: "unknown".to_string(),
                    conflict_type: "modified_definition".to_string(),
                    likely_source: Some("unapplied_migration".to_string()),
                    details: "View definition differs from expected state".to_string(),
                },
            }
        }
        MigrationStep::Function(func_op) => {
            use crate::diff::operations::FunctionOperation;
            match func_op {
                FunctionOperation::Create { name, .. }
                | FunctionOperation::Replace { name, .. } => ConflictInfo {
                    object_type: "function".to_string(),
                    object_name: name.clone(),
                    conflict_type: "unexpected_existence".to_string(),
                    likely_source: Some("unapplied_migration".to_string()),
                    details: format!(
                        "Function '{}' exists in current schema but not in expected state",
                        name
                    ),
                },
                FunctionOperation::Drop { name, .. } => ConflictInfo {
                    object_type: "function".to_string(),
                    object_name: name.clone(),
                    conflict_type: "missing_object".to_string(),
                    likely_source: Some("manual_change".to_string()),
                    details: format!(
                        "Function '{}' expected in applied migrations but missing from current schema",
                        name
                    ),
                },
                _ => ConflictInfo {
                    object_type: "function".to_string(),
                    object_name: "unknown".to_string(),
                    conflict_type: "modified_definition".to_string(),
                    likely_source: Some("unapplied_migration".to_string()),
                    details: "Function definition differs from expected state".to_string(),
                },
            }
        }
        MigrationStep::Index(index_op) => {
            use crate::diff::operations::IndexOperation;
            match index_op {
                IndexOperation::Create(index) => ConflictInfo {
                    object_type: "index".to_string(),
                    object_name: index.name.clone(),
                    conflict_type: "unexpected_existence".to_string(),
                    likely_source: Some("unapplied_migration".to_string()),
                    details: format!(
                        "Index '{}' exists in current schema but not in expected state",
                        index.name
                    ),
                },
                IndexOperation::Drop { name, .. } => ConflictInfo {
                    object_type: "index".to_string(),
                    object_name: name.clone(),
                    conflict_type: "missing_object".to_string(),
                    likely_source: Some("manual_change".to_string()),
                    details: format!(
                        "Index '{}' expected in applied migrations but missing from current schema",
                        name
                    ),
                },
                _ => ConflictInfo {
                    object_type: "index".to_string(),
                    object_name: "unknown".to_string(),
                    conflict_type: "modified_definition".to_string(),
                    likely_source: Some("unapplied_migration".to_string()),
                    details: "Index definition differs from expected state".to_string(),
                },
            }
        }
        _ => ConflictInfo {
            object_type: "unknown".to_string(),
            object_name: "unknown".to_string(),
            conflict_type: "schema_mismatch".to_string(),
            likely_source: Some("unapplied_migration".to_string()),
            details: "Unclassified schema difference detected".to_string(),
        },
    }
}

/// Generate suggested actions based on conflicts
fn generate_suggested_actions(conflicts: &[ConflictInfo]) -> Vec<SuggestedAction> {
    let mut actions = Vec::new();

    if !conflicts.is_empty() {
        // Check if conflicts suggest unapplied migrations
        let has_unexpected_existence = conflicts
            .iter()
            .any(|c| c.conflict_type == "unexpected_existence");

        let has_missing_objects = conflicts
            .iter()
            .any(|c| c.conflict_type == "missing_object");

        if has_unexpected_existence {
            // Current schema has objects that applied migrations don't have
            // This typically means there are unapplied local migrations
            actions.push(SuggestedAction {
                action: "pull_and_rebase".to_string(),
                target: None,
                command: Some("git pull origin main && pgmt migrate validate".to_string()),
                description: "Pull latest changes from main branch and check for conflicts"
                    .to_string(),
            });

            actions.push(SuggestedAction {
                action: "rebase_migration".to_string(),
                target: None,
                command: Some("pgmt migrate rebase".to_string()),
                description: "Rebase local migrations against current main branch state"
                    .to_string(),
            });
        }

        if has_missing_objects {
            // Applied migrations expect objects that aren't in current schema
            // This suggests manual changes or corrupted state
            actions.push(SuggestedAction {
                action: "apply_migrations".to_string(),
                target: None,
                command: Some("pgmt migrate apply".to_string()),
                description: "Apply any pending migrations to bring schema up to date".to_string(),
            });

            actions.push(SuggestedAction {
                action: "check_manual_changes".to_string(),
                target: None,
                command: None,
                description: "Check if objects were manually deleted from database".to_string(),
            });
        }

        // Always provide generic fallback options
        actions.push(SuggestedAction {
            action: "validate_verbose".to_string(),
            target: None,
            command: Some("pgmt migrate validate --verbose".to_string()),
            description: "Get detailed information about schema differences".to_string(),
        });

        actions.push(SuggestedAction {
            action: "update_migration".to_string(),
            target: None,
            command: Some("pgmt migrate update".to_string()),
            description: "Regenerate latest migration from current schema state".to_string(),
        });
    }

    actions
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_output_success() -> Result<()> {
        let result = CoreValidationResult {
            passed: true,
            differences: vec![],
            message: "All good".to_string(),
        };

        let applied_migrations = vec![1000, 2000];
        let unapplied_migrations = vec![];
        let baseline_info = Some(BaselineInfo {
            version: 1234567890,
            object_count: 5,
            description: "test baseline".to_string(),
        });

        let options = ValidationOutputOptions {
            format: "json".to_string(),
            quiet: true,
            verbose: false,
            ignore_migrations: vec![],
        };

        let output = format_validation_output(
            &result,
            &options,
            &applied_migrations,
            &unapplied_migrations,
            baseline_info.as_ref(),
        )?;

        // Should be valid JSON
        let json: ValidationJsonOutput = serde_json::from_str(&output)?;
        assert_eq!(json.status, "success");
        assert_eq!(json.exit_code, 0);

        Ok(())
    }

    #[test]
    fn test_json_output_conflict() -> Result<()> {
        let result = CoreValidationResult {
            passed: false,
            differences: vec![], // MigrationStep objects would be here in real usage
            message: "Conflicts detected".to_string(),
        };

        let applied_migrations = vec![1000];
        let unapplied_migrations = vec![2000];
        let baseline_info = Some(BaselineInfo {
            version: 1234567890,
            object_count: 3,
            description: "test baseline".to_string(),
        });

        let options = ValidationOutputOptions {
            format: "json".to_string(),
            quiet: true,
            verbose: false,
            ignore_migrations: vec![],
        };

        let output = format_validation_output(
            &result,
            &options,
            &applied_migrations,
            &unapplied_migrations,
            baseline_info.as_ref(),
        )?;

        // Should be valid JSON
        let json: ValidationJsonOutput = serde_json::from_str(&output)?;
        assert_eq!(json.status, "conflict");
        assert_eq!(json.exit_code, 1);
        assert!(!json.conflicts.is_empty());
        assert!(!json.suggested_actions.is_empty());

        Ok(())
    }

    #[test]
    fn test_human_output_quiet() -> Result<()> {
        let result = CoreValidationResult {
            passed: true,
            differences: vec![],
            message: "Success".to_string(),
        };

        let applied_migrations = vec![];
        let unapplied_migrations = vec![];
        let baseline_info: Option<BaselineInfo> = None;

        let options = ValidationOutputOptions {
            format: "human".to_string(),
            quiet: true,
            verbose: false,
            ignore_migrations: vec![],
        };

        let output = format_validation_output(
            &result,
            &options,
            &applied_migrations,
            &unapplied_migrations,
            baseline_info.as_ref(),
        )?;

        // Quiet mode should produce minimal output
        assert!(!output.contains("✅"));

        Ok(())
    }
}
