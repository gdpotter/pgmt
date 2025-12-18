use crate::catalog::Catalog;
use crate::constants::BASELINE_FILENAME_PREFIX;
use crate::diff::operations::MigrationStep;
use crate::migrate::generation::{MigrationGenerationInput, generate_migration};
use anyhow::{Result, anyhow};
use std::fs;
use std::path::PathBuf;

/// Request for creating a baseline
#[derive(Debug, Clone)]
pub struct BaselineCreationRequest {
    /// Source catalog to create baseline from
    pub catalog: Catalog,
    /// Version timestamp for the baseline
    pub version: u64,
    /// Description for the baseline
    pub description: String,
    /// Directory to store the baseline file
    pub baselines_dir: PathBuf,
    /// Whether to provide verbose output
    pub verbose: bool,
}

/// Result of baseline creation
#[derive(Debug)]
pub struct BaselineCreationResult {
    /// Path to the created baseline file
    pub path: PathBuf,
    /// The baseline SQL content
    pub baseline_sql: String,
    /// Migration steps that were included in the baseline
    pub steps: Vec<MigrationStep>,
    /// Version of the created baseline
    pub version: u64,
}

/// Create a baseline from the given request
pub async fn create_baseline(request: BaselineCreationRequest) -> Result<BaselineCreationResult> {
    if request.verbose {
        println!("📸 Creating baseline schema snapshot...");
    }

    // Generate baseline SQL from catalog
    if request.verbose {
        println!("🔄 Generating baseline SQL...");
    }

    let generation_input = MigrationGenerationInput {
        old_catalog: Catalog::empty(),
        new_catalog: request.catalog,
        description: request.description.clone(),
        version: request.version,
    };

    let generation_result = generate_migration(generation_input)?;

    // Ensure baselines directory exists
    if !request.baselines_dir.exists() {
        fs::create_dir_all(&request.baselines_dir)
            .map_err(|e| anyhow!("Failed to create baselines directory: {}", e))?;
    }

    // Write baseline file
    let baseline_filename = format!("{}{}.sql", BASELINE_FILENAME_PREFIX, request.version);
    let baseline_path = request.baselines_dir.join(&baseline_filename);

    if request.verbose {
        println!("📁 Writing baseline to: {}", baseline_path.display());
    }

    fs::write(&baseline_path, &generation_result.migration_sql)
        .map_err(|e| anyhow!("Failed to write baseline file: {}", e))?;

    Ok(BaselineCreationResult {
        path: baseline_path,
        baseline_sql: generation_result.migration_sql,
        steps: generation_result.steps,
        version: request.version,
    })
}

/// Display a summary of the baseline creation result
pub fn display_baseline_summary(result: &BaselineCreationResult) {
    println!("✅ Baseline created successfully!");
    println!(
        "📁 File: {}",
        result.path.file_name().unwrap().to_str().unwrap()
    );
    println!("🔢 Version: V{}", result.version);
    println!("📊 Contains {} migration steps", result.steps.len());

    if result.steps.is_empty() {
        println!("   (Empty schema - no objects found)");
    } else {
        // Count different types of objects
        let mut object_counts = std::collections::BTreeMap::new();
        for step in &result.steps {
            let step_type = match step {
                MigrationStep::Schema(_) => "Schema",
                MigrationStep::Table(_) => "Table",
                MigrationStep::View(_) => "View",
                MigrationStep::Type(_) => "Type",
                MigrationStep::Domain(_) => "Domain",
                MigrationStep::Sequence(_) => "Sequence",
                MigrationStep::Function(_) => "Function",
                MigrationStep::Aggregate(_) => "Aggregate",
                MigrationStep::Index(_) => "Index",
                MigrationStep::Constraint(_) => "Constraint",
                MigrationStep::Trigger(_) => "Trigger",
                MigrationStep::Policy(_) => "Policy",
                MigrationStep::Extension(_) => "Extension",
                MigrationStep::Grant(_) => "Grant",
            };
            *object_counts.entry(step_type).or_insert(0) += 1;
        }

        println!("   Objects captured:");
        for (obj_type, count) in object_counts {
            println!("   - {}: {}", obj_type, count);
        }
    }
}

/// Display usage information for the baseline
pub fn display_baseline_usage_info() {
    println!();
    println!("💡 This baseline can be used for:");
    println!("   - Recreating the database schema from scratch");
    println!("   - Comparing against future schema changes");
    println!("   - Migration validation and consistency checks");
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_create_baseline_empty_catalog() -> Result<()> {
        let temp_dir = tempdir()?;
        let baselines_dir = temp_dir.path().to_path_buf();

        let request = BaselineCreationRequest {
            catalog: Catalog::empty(),
            version: 1234567890,
            description: "test_baseline".to_string(),
            baselines_dir,
            verbose: false,
        };

        let result = create_baseline(request).await?;

        // Verify the result
        assert_eq!(result.version, 1234567890);
        assert!(result.path.exists());
        assert_eq!(result.steps.len(), 0); // Empty catalog should have no steps

        // Verify filename format
        let filename = result.path.file_name().unwrap().to_str().unwrap();
        assert!(filename.starts_with(BASELINE_FILENAME_PREFIX));
        assert!(filename.ends_with(".sql"));

        Ok(())
    }

    #[test]
    fn test_display_baseline_summary() {
        let temp_dir = tempdir().unwrap();
        let baseline_path = temp_dir.path().join("baseline_123.sql");

        let result = BaselineCreationResult {
            path: baseline_path,
            baseline_sql: "CREATE TABLE test (id INT);".to_string(),
            steps: vec![], // Empty for this test
            version: 123,
        };

        // Just verify this doesn't panic
        display_baseline_summary(&result);
        display_baseline_usage_info();
    }
}
