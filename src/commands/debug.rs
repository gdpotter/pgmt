//! Debug commands for troubleshooting schema dependencies
//!
//! This module provides commands to inspect and debug dependency tracking,
//! particularly useful when `-- require:` file dependencies aren't ordering
//! things as expected.

use anyhow::Result;
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::Path;
use tracing::debug;

use crate::catalog::id::DbObjectId;
use crate::config::Config;
use crate::db::cleaner;
use crate::db::connection::connect_with_retry;
use crate::db::schema_processor::{SchemaProcessor, SchemaProcessorConfig};
use crate::schema_ops::apply_roles_file;

/// Output format for debug commands
#[derive(Debug, Clone, PartialEq)]
pub enum OutputFormat {
    Json,
    Text,
}

/// JSON-serializable representation of a DbObjectId
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
pub enum ObjectIdJson {
    Schema {
        name: String,
    },
    Table {
        schema: String,
        name: String,
    },
    View {
        schema: String,
        name: String,
    },
    Type {
        schema: String,
        name: String,
    },
    Domain {
        schema: String,
        name: String,
    },
    Function {
        schema: String,
        name: String,
        arguments: String,
    },
    Sequence {
        schema: String,
        name: String,
    },
    Index {
        schema: String,
        name: String,
    },
    Constraint {
        schema: String,
        table: String,
        name: String,
    },
    Trigger {
        schema: String,
        table: String,
        name: String,
    },
    Policy {
        schema: String,
        table: String,
        name: String,
    },
    Extension {
        name: String,
    },
    Aggregate {
        schema: String,
        name: String,
        arguments: String,
    },
    Grant {
        id: String,
    },
    Comment {
        object_id: Box<ObjectIdJson>,
    },
    Column {
        schema: String,
        table: String,
        column: String,
    },
}

impl From<&DbObjectId> for ObjectIdJson {
    fn from(id: &DbObjectId) -> Self {
        match id {
            DbObjectId::Schema { name } => ObjectIdJson::Schema { name: name.clone() },
            DbObjectId::Table { schema, name } => ObjectIdJson::Table {
                schema: schema.clone(),
                name: name.clone(),
            },
            DbObjectId::View { schema, name } => ObjectIdJson::View {
                schema: schema.clone(),
                name: name.clone(),
            },
            DbObjectId::Type { schema, name } => ObjectIdJson::Type {
                schema: schema.clone(),
                name: name.clone(),
            },
            DbObjectId::Domain { schema, name } => ObjectIdJson::Domain {
                schema: schema.clone(),
                name: name.clone(),
            },
            DbObjectId::Function {
                schema,
                name,
                arguments,
            } => ObjectIdJson::Function {
                schema: schema.clone(),
                name: name.clone(),
                arguments: arguments.clone(),
            },
            DbObjectId::Sequence { schema, name } => ObjectIdJson::Sequence {
                schema: schema.clone(),
                name: name.clone(),
            },
            DbObjectId::Index { schema, name } => ObjectIdJson::Index {
                schema: schema.clone(),
                name: name.clone(),
            },
            DbObjectId::Constraint {
                schema,
                table,
                name,
            } => ObjectIdJson::Constraint {
                schema: schema.clone(),
                table: table.clone(),
                name: name.clone(),
            },
            DbObjectId::Trigger {
                schema,
                table,
                name,
            } => ObjectIdJson::Trigger {
                schema: schema.clone(),
                table: table.clone(),
                name: name.clone(),
            },
            DbObjectId::Policy {
                schema,
                table,
                name,
            } => ObjectIdJson::Policy {
                schema: schema.clone(),
                table: table.clone(),
                name: name.clone(),
            },
            DbObjectId::Extension { name } => ObjectIdJson::Extension { name: name.clone() },
            DbObjectId::Aggregate {
                schema,
                name,
                arguments,
            } => ObjectIdJson::Aggregate {
                schema: schema.clone(),
                name: name.clone(),
                arguments: arguments.clone(),
            },
            DbObjectId::Grant { id } => ObjectIdJson::Grant { id: id.clone() },
            DbObjectId::Comment { object_id } => ObjectIdJson::Comment {
                object_id: Box::new(ObjectIdJson::from(object_id.as_ref())),
            },
            DbObjectId::Column {
                schema,
                table,
                column,
            } => ObjectIdJson::Column {
                schema: schema.clone(),
                table: table.clone(),
                column: column.clone(),
            },
        }
    }
}

/// Information about a single object's dependencies
#[derive(Debug, Serialize)]
pub struct ObjectDependencyInfo {
    pub id: ObjectIdJson,
    pub source_file: Option<String>,
    pub intrinsic_dependencies: Vec<ObjectIdJson>,
    pub augmented_dependencies: Vec<ObjectIdJson>,
    pub all_dependencies: Vec<ObjectIdJson>,
}

/// Complete dependency report
#[derive(Debug, Serialize)]
pub struct DependencyReport {
    pub objects: Vec<ObjectDependencyInfo>,
    pub file_mappings: BTreeMap<String, Vec<ObjectIdJson>>,
    pub file_dependencies: BTreeMap<String, Vec<String>>,
}

/// Debug dependencies command - shows intrinsic vs augmented dependencies
pub async fn cmd_debug_dependencies(
    config: &Config,
    root_dir: &Path,
    format: OutputFormat,
    object_filter: Option<&str>,
) -> Result<()> {
    debug!("Loading schema files into shadow database for dependency analysis");

    // Connect to shadow database
    let shadow_url = config.databases.shadow.get_connection_string().await?;
    let shadow_pool = connect_with_retry(&shadow_url).await?;

    // Clean the database first
    debug!("Cleaning shadow database");
    cleaner::clean_shadow_db(&shadow_pool, &config.objects).await?;

    // Apply roles file before schema (if it exists)
    let roles_file = root_dir.join(&config.directories.roles);
    apply_roles_file(&shadow_pool, &roles_file).await?;

    // Process schema with file dependency tracking (skip cleaning since we already did it)
    let processor = SchemaProcessor::new(
        shadow_pool,
        SchemaProcessorConfig {
            verbose: false,
            clean_before_apply: false,
            objects: config.objects.clone(),
        },
    );

    let schema_dir = root_dir.join(&config.directories.schema);
    let processed = processor.process_schema_directory(&schema_dir).await?;

    // Build the dependency report
    let mut objects = Vec::new();

    // Get intrinsic dependencies from catalog.forward_deps
    // Get augmented dependencies from augmentation.additional_dependencies
    for (object_id, intrinsic_deps) in &processed.catalog.forward_deps {
        // Apply object filter if specified
        if let Some(filter) = object_filter
            && !object_matches_filter(object_id, filter)
        {
            continue;
        }

        let source_file = processed.file_mapping.object_files.get(object_id).cloned();

        let augmented_deps = processed
            .augmentation
            .additional_dependencies
            .get(object_id)
            .cloned()
            .unwrap_or_default();

        // Combine intrinsic + augmented for all_dependencies
        let mut all_deps: Vec<&DbObjectId> = intrinsic_deps.iter().collect();
        for aug in &augmented_deps {
            if !all_deps.contains(&aug) {
                all_deps.push(aug);
            }
        }

        objects.push(ObjectDependencyInfo {
            id: ObjectIdJson::from(object_id),
            source_file,
            intrinsic_dependencies: intrinsic_deps.iter().map(ObjectIdJson::from).collect(),
            augmented_dependencies: augmented_deps.iter().map(ObjectIdJson::from).collect(),
            all_dependencies: all_deps.iter().map(|d| ObjectIdJson::from(*d)).collect(),
        });
    }

    // Also include objects that have no intrinsic deps but are in the file mapping
    for (object_id, file_path) in &processed.file_mapping.object_files {
        if processed.catalog.forward_deps.contains_key(object_id) {
            continue; // Already included above
        }

        // Apply object filter if specified
        if let Some(filter) = object_filter
            && !object_matches_filter(object_id, filter)
        {
            continue;
        }

        let augmented_deps = processed
            .augmentation
            .additional_dependencies
            .get(object_id)
            .cloned()
            .unwrap_or_default();

        objects.push(ObjectDependencyInfo {
            id: ObjectIdJson::from(object_id),
            source_file: Some(file_path.clone()),
            intrinsic_dependencies: vec![],
            augmented_dependencies: augmented_deps.iter().map(ObjectIdJson::from).collect(),
            all_dependencies: augmented_deps.iter().map(ObjectIdJson::from).collect(),
        });
    }

    // Build file mappings
    let file_mappings: BTreeMap<String, Vec<ObjectIdJson>> = processed
        .file_mapping
        .file_objects
        .iter()
        .map(|(path, objs)| (path.clone(), objs.iter().map(ObjectIdJson::from).collect()))
        .collect();

    let report = DependencyReport {
        objects,
        file_mappings,
        file_dependencies: processed.file_dependencies,
    };

    // Output in requested format
    match format {
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(&report)?;
            println!("{}", json);
        }
        OutputFormat::Text => {
            print_text_report(&report);
        }
    }

    Ok(())
}

/// Check if an object matches a filter pattern
fn object_matches_filter(object_id: &DbObjectId, filter: &str) -> bool {
    let object_str = format!("{:?}", object_id);
    object_str.to_lowercase().contains(&filter.to_lowercase())
}

/// Print the report in human-readable text format
fn print_text_report(report: &DependencyReport) {
    println!("=== Dependency Report ===\n");

    // File Dependencies (-- require: relationships)
    if !report.file_dependencies.is_empty() {
        println!("📁 File Dependencies (-- require:)");
        println!("{}", "─".repeat(50));
        for (file, deps) in &report.file_dependencies {
            println!("  {} requires:", file);
            for dep in deps {
                println!("    → {}", dep);
            }
        }
        println!();
    }

    // File Mappings (which objects each file creates)
    println!("📦 File → Object Mappings");
    println!("{}", "─".repeat(50));
    for (file, objs) in &report.file_mappings {
        println!("  {}:", file);
        for obj in objs {
            println!("    - {}", format_object_id(obj));
        }
    }
    println!();

    // Object Dependencies
    println!("🔗 Object Dependencies");
    println!("{}", "─".repeat(50));
    for obj_info in &report.objects {
        println!("  {}", format_object_id(&obj_info.id));
        if let Some(ref file) = obj_info.source_file {
            println!("    Source: {}", file);
        }

        if !obj_info.intrinsic_dependencies.is_empty() {
            println!("    Intrinsic (from PostgreSQL):");
            for dep in &obj_info.intrinsic_dependencies {
                println!("      → {}", format_object_id(dep));
            }
        }

        if !obj_info.augmented_dependencies.is_empty() {
            println!("    Augmented (from -- require:):");
            for dep in &obj_info.augmented_dependencies {
                println!("      → {}", format_object_id(dep));
            }
        }

        if obj_info.intrinsic_dependencies.is_empty() && obj_info.augmented_dependencies.is_empty()
        {
            println!("    (no dependencies)");
        }
        println!();
    }
}

/// Format an ObjectIdJson for display
fn format_object_id(obj: &ObjectIdJson) -> String {
    match obj {
        ObjectIdJson::Schema { name } => format!("Schema: {}", name),
        ObjectIdJson::Table { schema, name } => format!("Table: {}.{}", schema, name),
        ObjectIdJson::View { schema, name } => format!("View: {}.{}", schema, name),
        ObjectIdJson::Type { schema, name } => format!("Type: {}.{}", schema, name),
        ObjectIdJson::Domain { schema, name } => format!("Domain: {}.{}", schema, name),
        ObjectIdJson::Function {
            schema,
            name,
            arguments,
        } => {
            format!("Function: {}.{}({})", schema, name, arguments)
        }
        ObjectIdJson::Sequence { schema, name } => format!("Sequence: {}.{}", schema, name),
        ObjectIdJson::Index { schema, name } => format!("Index: {}.{}", schema, name),
        ObjectIdJson::Constraint {
            schema,
            table,
            name,
        } => {
            format!("Constraint: {}.{}.{}", schema, table, name)
        }
        ObjectIdJson::Trigger {
            schema,
            table,
            name,
        } => {
            format!("Trigger: {}.{}.{}", schema, table, name)
        }
        ObjectIdJson::Policy {
            schema,
            table,
            name,
        } => {
            format!("Policy: {}.{}.{}", schema, table, name)
        }
        ObjectIdJson::Extension { name } => format!("Extension: {}", name),
        ObjectIdJson::Aggregate {
            schema,
            name,
            arguments,
        } => {
            format!("Aggregate: {}.{}({})", schema, name, arguments)
        }
        ObjectIdJson::Grant { id } => format!("Grant: {}", id),
        ObjectIdJson::Comment { object_id } => {
            format!("Comment on {}", format_object_id(object_id))
        }
        ObjectIdJson::Column {
            schema,
            table,
            column,
        } => {
            format!("Column: {}.{}.{}", schema, table, column)
        }
    }
}
