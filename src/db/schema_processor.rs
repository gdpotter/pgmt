//! Unified schema processor that combines loading, execution, and dependency tracking
//!
//! This module provides a single entry point for processing schema directories,
//! handling file loading, execution, dependency tracking, and catalog generation.

use anyhow::{Context, Result};
use sqlx::PgPool;
use std::path::Path;
use tracing::{debug, info};

use crate::catalog::Catalog;
use crate::catalog::file_dependencies::{
    FileDependencyAugmentation, FileToObjectMapping, create_dependency_augmentation,
};
use crate::catalog::identity::{self, CatalogIdentity};
use crate::db::cleaner;
use crate::db::schema_executor::SchemaFileExecutor;
use crate::schema_loader::{SchemaLoader, SchemaLoaderConfig};
use std::collections::BTreeMap;

/// Configuration for schema processing behavior
#[derive(Debug, Clone)]
pub struct SchemaProcessorConfig {
    /// Whether to show verbose output during processing
    pub verbose: bool,
    /// Whether to clean the database before applying schema
    pub clean_before_apply: bool,
}

impl Default for SchemaProcessorConfig {
    fn default() -> Self {
        Self {
            verbose: false,
            clean_before_apply: true,
        }
    }
}

/// Result of processing a schema directory
#[derive(Debug)]
pub struct ProcessedSchema {
    /// The catalog without file-based dependency augmentation
    pub catalog: Catalog,
    /// File-based dependency augmentation data
    pub augmentation: FileDependencyAugmentation,
    /// Mapping of files to the objects they create
    pub file_mapping: FileToObjectMapping,
    /// File-to-file dependencies from `-- require:` headers
    pub file_dependencies: BTreeMap<String, Vec<String>>,
}

impl ProcessedSchema {
    /// Get a catalog with file dependencies applied
    pub fn with_file_dependencies_applied(self) -> Catalog {
        self.catalog
            .with_file_dependencies_augmented(self.augmentation)
    }
}

/// Unified schema processor that handles the complete schema processing workflow
pub struct SchemaProcessor {
    pool: PgPool,
    config: SchemaProcessorConfig,
}

impl SchemaProcessor {
    /// Create a new schema processor with the given pool and configuration
    pub fn new(pool: PgPool, config: SchemaProcessorConfig) -> Self {
        Self { pool, config }
    }

    /// Process a schema directory, returning a catalog with file dependencies tracked
    ///
    /// This method:
    /// 1. Optionally cleans the database
    /// 2. Loads and orders schema files based on dependencies
    /// 3. Applies each file incrementally while tracking created objects
    /// 4. Builds file-to-object mappings
    /// 5. Creates dependency augmentation from file relationships
    /// 6. Returns an augmented catalog with complete dependency information
    pub async fn process_schema_directory(&self, schema_dir: &Path) -> Result<ProcessedSchema> {
        // Step 1: Clean database if requested
        if self.config.clean_before_apply {
            debug!("🧹 Cleaning database before applying schema...");
            cleaner::clean_shadow_db(&self.pool)
                .await
                .context("Failed to clean database before applying schema")?;
        }

        // Step 2: Load and order schema files
        debug!("📁 Loading schema files from: {}", schema_dir.display());
        let loader = SchemaLoader::new(SchemaLoaderConfig::new(schema_dir.to_path_buf()));
        let schema_files = loader.load_ordered_schema_files().with_context(|| {
            format!(
                "Failed to load and order schema files from directory: {}\n\n\
                    Common causes:\n\
                    • Schema directory doesn't exist or is empty\n\
                    • Circular dependencies between files (A requires B, B requires A)\n\
                    • Missing dependency files referenced in '-- require:' headers\n\
                    • Invalid file paths in dependency declarations",
                schema_dir.display()
            )
        })?;

        debug!(
            "📁 Loaded {} schema files with dependency information",
            schema_files.len()
        );

        // Step 3: Create executor for applying files
        let executor = SchemaFileExecutor::new(self.pool.clone(), self.config.verbose);

        // Step 4: Process files incrementally, tracking what each file creates
        // Use lightweight CatalogIdentity for fast per-file tracking (single query vs 50+)
        let mut file_mapping = FileToObjectMapping::new();
        let mut previous_identity = CatalogIdentity::load(&self.pool)
            .await
            .context("Failed to load initial catalog identity")?;

        debug!(
            "Creating file-to-object mappings by applying {} schema files incrementally",
            schema_files.len()
        );

        for (index, file) in schema_files.iter().enumerate() {
            debug!(
                "  [{}/{}] Applying {} and taking snapshot",
                index + 1,
                schema_files.len(),
                file.relative_path
            );

            // Execute the schema file (SchemaFileExecutor provides detailed error messages)
            executor.execute_schema_file(file).await?;

            // Load lightweight identity after applying this file (single UNION ALL query)
            let current_identity = CatalogIdentity::load(&self.pool).await.with_context(|| {
                format!(
                    "Failed to load catalog identity after applying {}",
                    file.relative_path
                )
            })?;

            // Find objects created by this file
            let new_objects = identity::find_new_objects(&previous_identity, &current_identity);

            // Track the mapping
            for object_id in new_objects {
                file_mapping.add_object(file.relative_path.clone(), object_id);
            }

            // Update previous identity for next iteration
            previous_identity = current_identity;
        }

        debug!(
            "File-to-object mapping complete: {} files mapped to {} total objects",
            file_mapping.file_objects.len(),
            file_mapping.object_files.len()
        );

        // Step 5: Load full catalog once at the end for diff operations
        debug!("Loading full catalog for diff operations");
        let final_catalog = Catalog::load(&self.pool)
            .await
            .context("Failed to load final catalog")?;

        // Step 6: Create file-based dependency augmentation
        debug!("Creating file-based dependency augmentation");
        let augmentation = create_dependency_augmentation(&file_mapping, &schema_files)
            .context("Failed to create dependency augmentation from file mappings")?;

        // Step 7: Extract file-to-file dependencies before schema_files goes out of scope
        let file_dependencies: BTreeMap<String, Vec<String>> = schema_files
            .iter()
            .filter(|f| !f.dependencies.is_empty())
            .map(|f| (f.relative_path.clone(), f.dependencies.clone()))
            .collect();

        // Step 8: Return catalog and augmentation separately
        info!(
            "✅ Schema processing complete: {} files processed",
            schema_files.len()
        );

        Ok(ProcessedSchema {
            catalog: final_catalog,
            augmentation,
            file_mapping,
            file_dependencies,
        })
    }
}
