use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

use crate::catalog::Catalog;
use crate::catalog::id::DbObjectId;
use crate::diff::operations::{MigrationStep, SqlRenderer};
use crate::diff::{diff_all, diff_order};

#[derive(Debug, Clone)]
pub struct SchemaGeneratorConfig {
    pub include_comments: bool,
    pub include_grants: bool,
    pub include_triggers: bool,
    pub include_extensions: bool,
}

impl Default for SchemaGeneratorConfig {
    fn default() -> Self {
        Self {
            include_comments: true,
            include_grants: true,
            include_triggers: true,
            include_extensions: true,
        }
    }
}

#[derive(Debug, Clone)]
struct FileContent {
    path: PathBuf,
    dependencies: Vec<String>,
    sql_statements: Vec<String>,
}

pub struct SchemaGenerator {
    catalog: Catalog,
    output_dir: PathBuf,
    config: SchemaGeneratorConfig,
}

impl SchemaGenerator {
    pub fn new(catalog: Catalog, output_dir: PathBuf, config: SchemaGeneratorConfig) -> Self {
        Self {
            catalog,
            output_dir,
            config,
        }
    }

    /// Generate all schema files using the diffing pipeline
    pub fn generate_files(&self) -> Result<()> {
        self.create_directory_structure()?;

        let empty_catalog = Catalog::empty();
        let steps = diff_all(&empty_catalog, &self.catalog);
        let ordered_steps = diff_order(steps, &empty_catalog, &self.catalog)?;
        let filtered_steps = self.filter_steps_by_config(ordered_steps);
        let organized_files = self.organize_steps_into_files(filtered_steps)?;
        self.write_organized_files(organized_files)?;

        Ok(())
    }

    /// Create the directory structure
    fn create_directory_structure(&self) -> Result<()> {
        fs::create_dir_all(&self.output_dir)?;
        fs::create_dir_all(self.output_dir.join("tables"))?;
        fs::create_dir_all(self.output_dir.join("views"))?;
        fs::create_dir_all(self.output_dir.join("functions"))?;
        Ok(())
    }

    /// Filter migration steps based on configuration
    fn filter_steps_by_config(&self, steps: Vec<MigrationStep>) -> Vec<MigrationStep> {
        steps
            .into_iter()
            .filter(|step| match step {
                MigrationStep::Grant(_) => self.config.include_grants,
                MigrationStep::Trigger(_) => self.config.include_triggers,
                MigrationStep::Extension(_) => self.config.include_extensions,
                _ => {
                    if let DbObjectId::Comment { .. } = step.id() {
                        self.config.include_comments
                    } else {
                        true
                    }
                }
            })
            .collect()
    }

    /// Organize migration steps into individual object files
    fn organize_steps_into_files(
        &self,
        steps: Vec<MigrationStep>,
    ) -> Result<BTreeMap<String, FileContent>> {
        let mut files: BTreeMap<String, FileContent> = BTreeMap::new();

        let mut steps_by_file: BTreeMap<String, Vec<MigrationStep>> = BTreeMap::new();

        for step in steps {
            let file_key = self.determine_file_for_step(&step);
            steps_by_file.entry(file_key).or_default().push(step);
        }

        for (file_key, file_steps) in steps_by_file {
            let file_content = self.create_file_content(file_key, file_steps)?;
            files.insert(
                file_content.path.to_string_lossy().to_string(),
                file_content,
            );
        }

        Ok(files)
    }

    /// Determine which file a migration step should go into
    fn determine_file_for_step(&self, step: &MigrationStep) -> String {
        match step {
            MigrationStep::Schema(_) => "schemas.sql".to_string(),
            MigrationStep::Extension(_) => "extensions.sql".to_string(),
            MigrationStep::Type(_) => "types.sql".to_string(),

            MigrationStep::Table(op) => {
                let table_name = self.extract_table_name_from_operation(op);
                format!("tables/{}.sql", table_name)
            }

            MigrationStep::View(op) => {
                let view_name = self.extract_view_name_from_operation(op);
                format!("views/{}.sql", view_name)
            }

            MigrationStep::Function(op) => {
                let function_name = self.extract_function_name_from_operation(op);
                format!("functions/{}.sql", function_name)
            }

            MigrationStep::Sequence(op) => {
                let sequence_name = self.extract_sequence_name_from_operation(op);

                if let Some(table_name) = self.find_owning_table_for_sequence(&sequence_name) {
                    format!("tables/{}.sql", table_name)
                } else {
                    "sequences.sql".to_string()
                }
            }

            MigrationStep::Index(op) => {
                let table_name = self.extract_table_name_from_index_operation(op);
                format!("tables/{}.sql", table_name)
            }

            MigrationStep::Constraint(op) => {
                let table_name = self.extract_table_name_from_constraint_operation(op);
                format!("tables/{}.sql", table_name)
            }

            MigrationStep::Trigger(op) => {
                let table_name = self.extract_table_name_from_trigger_operation(op);
                format!("tables/{}.sql", table_name)
            }

            MigrationStep::Grant(op) => match self.extract_grant_target(op) {
                GrantTarget::Table(table_name) => format!("tables/{}.sql", table_name),
                GrantTarget::View(view_name) => format!("views/{}.sql", view_name),
                GrantTarget::Function(function_name) => format!("functions/{}.sql", function_name),
                GrantTarget::Schema => "schemas.sql".to_string(),
                GrantTarget::Type => "types.sql".to_string(),
                GrantTarget::Sequence(sequence_name) => {
                    if let Some(table_name) = self.find_owning_table_for_sequence(&sequence_name) {
                        format!("tables/{}.sql", table_name)
                    } else {
                        "sequences.sql".to_string()
                    }
                }
            },
        }
    }

    /// Create file content for a group of migration steps
    fn create_file_content(
        &self,
        file_key: String,
        steps: Vec<MigrationStep>,
    ) -> Result<FileContent> {
        let file_path = self.output_dir.join(&file_key);

        // Calculate dependencies for this file
        let dependencies = self.calculate_file_dependencies(&steps);

        // Convert steps to SQL statements
        let mut sql_statements = Vec::new();
        for step in steps {
            let rendered_sqls = step.to_sql();
            for rendered_sql in rendered_sqls {
                sql_statements.push(rendered_sql.sql);
            }
        }

        Ok(FileContent {
            path: file_path,
            dependencies,
            sql_statements,
        })
    }

    /// Calculate file dependencies from migration steps
    fn calculate_file_dependencies(&self, steps: &[MigrationStep]) -> Vec<String> {
        let mut dependencies = BTreeSet::new();

        let current_file_path = if let Some(first_step) = steps.first() {
            self.determine_file_for_step(first_step)
        } else {
            return vec![];
        };

        for step in steps {
            let step_deps = self.get_step_dependencies(step);
            for dep in step_deps {
                if let Some(file_path) = self.object_id_to_file_path(&dep)
                    && file_path != current_file_path
                {
                    dependencies.insert(file_path);
                }
            }
        }

        dependencies.into_iter().collect()
    }

    /// Get dependencies for a migration step
    fn get_step_dependencies(&self, step: &MigrationStep) -> Vec<DbObjectId> {
        let step_id = step.id();

        self.catalog
            .forward_deps
            .get(&step_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Convert a DbObjectId to a file path for dependencies
    fn object_id_to_file_path(&self, object_id: &DbObjectId) -> Option<String> {
        match object_id {
            DbObjectId::Schema { name } => {
                if name != "public" {
                    Some("schemas.sql".to_string())
                } else {
                    None
                }
            }
            DbObjectId::Table { name, .. } => Some(format!("tables/{}.sql", name)),
            DbObjectId::View { name, .. } => Some(format!("views/{}.sql", name)),
            DbObjectId::Function { name, .. } => Some(format!("functions/{}.sql", name)),
            DbObjectId::Type { .. } => Some("types.sql".to_string()),
            DbObjectId::Sequence { name, .. } => {
                if let Some(table_name) = self.find_owning_table_for_sequence(name) {
                    Some(format!("tables/{}.sql", table_name))
                } else {
                    Some("sequences.sql".to_string())
                }
            }
            DbObjectId::Extension { .. } => Some("extensions.sql".to_string()),
            _ => None,
        }
    }

    /// Write organized files to disk
    fn write_organized_files(&self, files: BTreeMap<String, FileContent>) -> Result<()> {
        for (_, file_content) in files {
            let mut content = String::new();

            if !file_content.dependencies.is_empty() {
                content.push_str(&format!(
                    "-- require: {}\n\n",
                    file_content.dependencies.join(", ")
                ));
            }

            for (i, sql) in file_content.sql_statements.iter().enumerate() {
                if i > 0 {
                    content.push('\n');
                }
                content.push_str(sql);
                if !sql.ends_with(';') {
                    content.push(';');
                }
                content.push('\n');
            }

            // Only write file if it has content
            if !content.trim().is_empty() {
                // Ensure parent directory exists
                if let Some(parent) = file_content.path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(&file_content.path, content)?;
            }
        }

        Ok(())
    }

    // Helper methods for extracting names from operations
    fn extract_table_name_from_operation(
        &self,
        op: &crate::diff::operations::TableOperation,
    ) -> String {
        use crate::diff::operations::TableOperation;
        match op {
            TableOperation::Create { name, .. } => name.clone(),
            TableOperation::Drop { name, .. } => name.clone(),
            TableOperation::Alter { name, .. } => name.clone(),
            TableOperation::Comment(comment_op) => match comment_op {
                crate::diff::operations::CommentOperation::Set { target, .. } => {
                    target.table.clone()
                }
                crate::diff::operations::CommentOperation::Drop { target } => target.table.clone(),
            },
        }
    }

    fn extract_view_name_from_operation(
        &self,
        op: &crate::diff::operations::ViewOperation,
    ) -> String {
        use crate::diff::operations::ViewOperation;
        match op {
            ViewOperation::Create { name, .. } => name.clone(),
            ViewOperation::Drop { name, .. } => name.clone(),
            ViewOperation::Replace { name, .. } => name.clone(),
            ViewOperation::Comment(comment_op) => match comment_op {
                crate::diff::operations::CommentOperation::Set { target, .. } => {
                    target.name.clone()
                }
                crate::diff::operations::CommentOperation::Drop { target } => target.name.clone(),
            },
        }
    }

    fn extract_function_name_from_operation(
        &self,
        op: &crate::diff::operations::FunctionOperation,
    ) -> String {
        use crate::diff::operations::FunctionOperation;
        match op {
            FunctionOperation::Create { name, .. } => name.clone(),
            FunctionOperation::Drop { name, .. } => name.clone(),
            FunctionOperation::Replace { name, .. } => name.clone(),
            FunctionOperation::Comment(comment_op) => match comment_op {
                crate::diff::operations::CommentOperation::Set { target, .. } => {
                    target.name.clone()
                }
                crate::diff::operations::CommentOperation::Drop { target } => target.name.clone(),
            },
        }
    }

    fn extract_sequence_name_from_operation(
        &self,
        op: &crate::diff::operations::SequenceOperation,
    ) -> String {
        use crate::diff::operations::SequenceOperation;
        match op {
            SequenceOperation::Create { name, .. } => name.clone(),
            SequenceOperation::Drop { name, .. } => name.clone(),
            SequenceOperation::AlterOwnership { name, .. } => name.clone(),
            SequenceOperation::Comment(comment_op) => match comment_op {
                crate::diff::operations::CommentOperation::Set { target, .. } => {
                    target.name.clone()
                }
                crate::diff::operations::CommentOperation::Drop { target } => target.name.clone(),
            },
        }
    }

    fn extract_table_name_from_index_operation(
        &self,
        op: &crate::diff::operations::IndexOperation,
    ) -> String {
        use crate::diff::operations::IndexOperation;
        match op {
            IndexOperation::Create(index) => index.table_name.clone(),
            IndexOperation::Drop { name, .. } => {
                for index in &self.catalog.indexes {
                    if index.name == *name {
                        return index.table_name.clone();
                    }
                }
                "unknown".to_string()
            }
            IndexOperation::Comment(_comment_op) => {
                // Similar issue with comment operations
                "unknown".to_string()
            }
            IndexOperation::Cluster { table_name, .. } => table_name.clone(),
            IndexOperation::SetWithoutCluster { name, .. } => name.clone(),
            IndexOperation::Reindex { name, .. } => {
                // Find the table name from the index in the catalog
                for index in &self.catalog.indexes {
                    if index.name == *name {
                        return index.table_name.clone();
                    }
                }
                "unknown".to_string()
            }
        }
    }

    fn extract_table_name_from_constraint_operation(
        &self,
        op: &crate::diff::operations::ConstraintOperation,
    ) -> String {
        use crate::diff::operations::ConstraintOperation;
        match op {
            ConstraintOperation::Create(constraint) => constraint.table.clone(),
            ConstraintOperation::Drop(constraint_id) => constraint_id.table.clone(),
            ConstraintOperation::Comment(comment_op) => match comment_op {
                crate::diff::operations::CommentOperation::Set { target, .. } => {
                    target.table.clone()
                }
                crate::diff::operations::CommentOperation::Drop { target } => target.table.clone(),
            },
        }
    }

    fn extract_table_name_from_trigger_operation(
        &self,
        op: &crate::diff::operations::TriggerOperation,
    ) -> String {
        use crate::diff::operations::TriggerOperation;
        match op {
            TriggerOperation::Create { trigger } => trigger.table_name.clone(),
            TriggerOperation::Drop { identifier } => identifier.table.clone(),
            TriggerOperation::Replace { new_trigger, .. } => new_trigger.table_name.clone(),
            TriggerOperation::Comment(comment_op) => match comment_op {
                crate::diff::operations::CommentOperation::Set { target, .. } => {
                    target.table.clone()
                }
                crate::diff::operations::CommentOperation::Drop { target } => target.table.clone(),
            },
        }
    }

    fn extract_grant_target(&self, op: &crate::diff::operations::GrantOperation) -> GrantTarget {
        use crate::catalog::grant::ObjectType;
        use crate::diff::operations::GrantOperation;

        let object_type = match op {
            GrantOperation::Grant { grant } => &grant.object,
            GrantOperation::Revoke { grant } => &grant.object,
        };

        match object_type {
            ObjectType::Table { name, .. } => GrantTarget::Table(name.clone()),
            ObjectType::View { name, .. } => GrantTarget::View(name.clone()),
            ObjectType::Function { name, .. } => GrantTarget::Function(name.clone()),
            ObjectType::Schema { .. } => GrantTarget::Schema,
            ObjectType::Type { .. } => GrantTarget::Type,
            ObjectType::Sequence { name, .. } => GrantTarget::Sequence(name.clone()),
        }
    }

    fn find_owning_table_for_sequence(&self, sequence_name: &str) -> Option<String> {
        // Look through catalog sequences to find if this sequence is owned by a table
        for sequence in &self.catalog.sequences {
            if sequence.name == sequence_name {
                if let Some(ref owned_by) = sequence.owned_by {
                    // owned_by format is usually "schema.table.column"
                    let parts: Vec<&str> = owned_by.split('.').collect();
                    if parts.len() >= 2 {
                        return Some(parts[1].to_string()); // Return table name
                    }
                }
                break;
            }
        }
        None
    }
}

#[derive(Debug, Clone)]
enum GrantTarget {
    Table(String),
    View(String),
    Function(String),
    Schema,
    Type,
    Sequence(String),
}
