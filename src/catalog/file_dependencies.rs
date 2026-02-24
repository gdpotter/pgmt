//! File-based dependency augmentation system
//!
//! This module provides functionality to augment PostgreSQL's introspection-based dependency
//! tracking with file-level `-- require:` dependencies. When a schema file A requires file B,
//! all database objects created in file A are considered to depend on all objects in file B.

use crate::catalog::id::DbObjectId;
use crate::schema_loader::SchemaFile;
use anyhow::Result;
use std::collections::BTreeMap;
use tracing::info;

/// Maps file paths to the database objects they create
#[derive(Debug, Clone)]
pub struct FileToObjectMapping {
    /// Map from file path to objects created by that file
    pub file_objects: BTreeMap<String, Vec<DbObjectId>>,
    /// Map from object ID to the file that created it
    pub object_files: BTreeMap<DbObjectId, String>,
}

impl Default for FileToObjectMapping {
    fn default() -> Self {
        Self::new()
    }
}

impl FileToObjectMapping {
    pub fn new() -> Self {
        Self {
            file_objects: BTreeMap::new(),
            object_files: BTreeMap::new(),
        }
    }

    /// Add an object as being created by a specific file
    pub fn add_object(&mut self, file_path: String, object_id: DbObjectId) {
        self.file_objects
            .entry(file_path.clone())
            .or_default()
            .push(object_id.clone());
        self.object_files.insert(object_id, file_path);
    }

    /// Get all objects created by a file
    pub fn get_objects_for_file(&self, file_path: &str) -> Vec<DbObjectId> {
        self.file_objects
            .get(file_path)
            .cloned()
            .unwrap_or_default()
    }
}

/// Additional file-based dependencies to augment catalog dependencies
#[derive(Debug, Clone)]
pub struct FileDependencyAugmentation {
    /// Additional dependencies from file-level requires
    pub additional_dependencies: BTreeMap<DbObjectId, Vec<DbObjectId>>,
}

impl Default for FileDependencyAugmentation {
    fn default() -> Self {
        Self::new()
    }
}

impl FileDependencyAugmentation {
    pub fn new() -> Self {
        Self {
            additional_dependencies: BTreeMap::new(),
        }
    }

    /// Add a dependency from one object to another
    pub fn add_dependency(&mut self, from: DbObjectId, to: DbObjectId) {
        self.additional_dependencies
            .entry(from)
            .or_default()
            .push(to);
    }
}

/// Create file-based dependency augmentation from file mappings and schema file dependencies
pub fn create_dependency_augmentation(
    mapping: &FileToObjectMapping,
    schema_files: &[SchemaFile],
) -> Result<FileDependencyAugmentation> {
    let mut augmentation = FileDependencyAugmentation::new();

    info!("Creating file-based dependency augmentation");

    // For each schema file, if it has dependencies (require: headers),
    // make all objects in this file depend on all objects in required files
    for schema_file in schema_files {
        let objects_in_file = mapping.get_objects_for_file(&schema_file.relative_path);

        if objects_in_file.is_empty() {
            continue; // Skip files that don't create any objects
        }

        for required_file in &schema_file.dependencies {
            let objects_in_required_file = mapping.get_objects_for_file(required_file);

            if objects_in_required_file.is_empty() {
                info!(
                    "  Warning: File {} requires {}, but {} creates no trackable objects",
                    schema_file.relative_path, required_file, required_file
                );
                continue;
            }

            // Create dependencies: all objects in current file depend on all objects in required file
            for object_in_file in &objects_in_file {
                for object_in_required_file in &objects_in_required_file {
                    augmentation
                        .add_dependency(object_in_file.clone(), object_in_required_file.clone());
                }
            }

            info!(
                "  Added {} -> {} dependencies from file {} requiring {}",
                objects_in_file.len(),
                objects_in_required_file.len(),
                schema_file.relative_path,
                required_file
            );
        }
    }

    let total_additional_deps: usize = augmentation
        .additional_dependencies
        .values()
        .map(|deps| deps.len())
        .sum();
    info!(
        "File-based dependency augmentation complete: {} additional dependencies for {} objects",
        total_additional_deps,
        augmentation.additional_dependencies.len()
    );

    Ok(augmentation)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_to_object_mapping() {
        let mut mapping = FileToObjectMapping::new();

        let file1 = "schemas.sql".to_string();
        let file2 = "tables.sql".to_string();
        let obj1 = DbObjectId::Schema {
            name: "app".to_string(),
        };
        let obj2 = DbObjectId::Table {
            schema: "app".to_string(),
            name: "users".to_string(),
        };

        mapping.add_object(file1.clone(), obj1.clone());
        mapping.add_object(file2.clone(), obj2.clone());

        assert_eq!(mapping.get_objects_for_file(&file1), vec![obj1.clone()]);
        assert_eq!(mapping.get_objects_for_file(&file2), vec![obj2.clone()]);
    }

    #[test]
    fn test_dependency_augmentation() {
        let mut augmentation = FileDependencyAugmentation::new();

        let obj1 = DbObjectId::Schema {
            name: "app".to_string(),
        };
        let obj2 = DbObjectId::Table {
            schema: "app".to_string(),
            name: "users".to_string(),
        };

        augmentation.add_dependency(obj2.clone(), obj1.clone());

        let deps = augmentation
            .additional_dependencies
            .get(&obj2)
            .cloned()
            .unwrap_or_default();
        assert_eq!(deps, vec![obj1]);
    }
}
