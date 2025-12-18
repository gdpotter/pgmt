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

    /// Check if the catalog has schemas other than "public"
    /// When true, files will be organized into per-schema directories
    fn has_multiple_schemas(&self) -> bool {
        self.catalog.schemas.iter().any(|s| s.name != "public")
    }

    /// Get the file path prefix for a schema (empty string for flat structure, "schema/" for multi-schema)
    fn schema_path_prefix(&self, schema: &str) -> String {
        if self.has_multiple_schemas() {
            format!("{}/", schema)
        } else {
            String::new()
        }
    }

    /// Generate all schema files using the diffing pipeline
    pub fn generate_files(&self) -> Result<()> {
        self.create_directory_structure()?;

        let empty_catalog = Catalog::empty();
        let steps = diff_all(&empty_catalog, &self.catalog);

        // diff_all includes grant diffing which now handles REVOKE generation
        // for missing default privileges. Step-level dependencies are used
        // by diff_order for proper ordering.
        let ordered_steps = diff_order(steps, &empty_catalog, &self.catalog)?;
        let filtered_steps = self.filter_steps_by_config(ordered_steps);
        let organized_files = self.organize_steps_into_files(filtered_steps)?;
        self.write_organized_files(organized_files)?;

        Ok(())
    }

    /// Create the directory structure
    fn create_directory_structure(&self) -> Result<()> {
        fs::create_dir_all(&self.output_dir)?;

        if self.has_multiple_schemas() {
            // Create per-schema directories
            for schema in &self.catalog.schemas {
                let schema_dir = self.output_dir.join(&schema.name);
                fs::create_dir_all(schema_dir.join("tables"))?;
                fs::create_dir_all(schema_dir.join("views"))?;
                fs::create_dir_all(schema_dir.join("functions"))?;
                fs::create_dir_all(schema_dir.join("types"))?;
                fs::create_dir_all(schema_dir.join("aggregates"))?;
                fs::create_dir_all(schema_dir.join("sequences"))?;
            }
        } else {
            // Flat structure for single schema
            fs::create_dir_all(self.output_dir.join("tables"))?;
            fs::create_dir_all(self.output_dir.join("views"))?;
            fs::create_dir_all(self.output_dir.join("functions"))?;
            fs::create_dir_all(self.output_dir.join("types"))?;
            fs::create_dir_all(self.output_dir.join("aggregates"))?;
            fs::create_dir_all(self.output_dir.join("sequences"))?;
        }
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
        // Phase 1: Assign steps to files and build object-to-file mapping
        let mut steps_by_file: BTreeMap<String, Vec<MigrationStep>> = BTreeMap::new();
        let mut object_to_file: BTreeMap<DbObjectId, String> = BTreeMap::new();

        for step in steps {
            let file_key = self.determine_file_for_step(&step);
            let object_id = step.id();

            // Track which file contains this object
            object_to_file.insert(object_id, file_key.clone());
            steps_by_file.entry(file_key).or_default().push(step);
        }

        // Phase 2: Create file content with dependencies resolved via mapping
        let mut files: BTreeMap<String, FileContent> = BTreeMap::new();
        for (file_key, file_steps) in steps_by_file {
            let file_content = self.create_file_content(file_key, file_steps, &object_to_file)?;
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

            MigrationStep::Type(op) => {
                let (schema, name) = self.extract_type_info_from_operation(op);
                let prefix = self.schema_path_prefix(&schema);
                format!("{}types/{}.sql", prefix, name)
            }

            MigrationStep::Domain(op) => {
                let (schema, name) = self.extract_domain_info_from_operation(op);
                let prefix = self.schema_path_prefix(&schema);
                format!("{}domains/{}.sql", prefix, name)
            }

            MigrationStep::Table(op) => {
                let (schema, name) = self.extract_table_info_from_operation(op);
                let prefix = self.schema_path_prefix(&schema);
                format!("{}tables/{}.sql", prefix, name)
            }

            MigrationStep::View(op) => {
                let (schema, name) = self.extract_view_info_from_operation(op);
                let prefix = self.schema_path_prefix(&schema);
                format!("{}views/{}.sql", prefix, name)
            }

            MigrationStep::Function(op) => {
                let (schema, name) = self.extract_function_info_from_operation(op);
                let prefix = self.schema_path_prefix(&schema);
                format!("{}functions/{}.sql", prefix, name)
            }

            MigrationStep::Aggregate(op) => {
                let (schema, name) = self.extract_aggregate_info_from_operation(op);
                let prefix = self.schema_path_prefix(&schema);
                format!("{}aggregates/{}.sql", prefix, name)
            }

            MigrationStep::Sequence(op) => {
                let (schema, name) = self.extract_sequence_info_from_operation(op);

                if let Some((table_schema, table_name)) =
                    self.find_owning_table_for_sequence(&schema, &name)
                {
                    let prefix = self.schema_path_prefix(&table_schema);
                    format!("{}tables/{}.sql", prefix, table_name)
                } else {
                    let prefix = self.schema_path_prefix(&schema);
                    format!("{}sequences/{}.sql", prefix, name)
                }
            }

            MigrationStep::Index(op) => {
                let (schema, table_name) = self.extract_table_info_from_index_operation(op);
                let prefix = self.schema_path_prefix(&schema);
                format!("{}tables/{}.sql", prefix, table_name)
            }

            MigrationStep::Constraint(op) => {
                let (schema, table_name) = self.extract_table_info_from_constraint_operation(op);
                let prefix = self.schema_path_prefix(&schema);
                format!("{}tables/{}.sql", prefix, table_name)
            }

            MigrationStep::Trigger(op) => {
                let (schema, table_name) = self.extract_table_info_from_trigger_operation(op);
                let prefix = self.schema_path_prefix(&schema);
                format!("{}tables/{}.sql", prefix, table_name)
            }

            MigrationStep::Policy(op) => {
                let (schema, table_name) = self.extract_table_info_from_policy_operation(op);
                let prefix = self.schema_path_prefix(&schema);
                format!("{}tables/{}.sql", prefix, table_name)
            }

            MigrationStep::Grant(op) => match self.extract_grant_target(op) {
                GrantTarget::Table { schema, name } => {
                    let prefix = self.schema_path_prefix(&schema);
                    format!("{}tables/{}.sql", prefix, name)
                }
                GrantTarget::View { schema, name } => {
                    let prefix = self.schema_path_prefix(&schema);
                    format!("{}views/{}.sql", prefix, name)
                }
                GrantTarget::Function { schema, name } => {
                    let prefix = self.schema_path_prefix(&schema);
                    format!("{}functions/{}.sql", prefix, name)
                }
                GrantTarget::Procedure { schema, name } => {
                    let prefix = self.schema_path_prefix(&schema);
                    format!("{}functions/{}.sql", prefix, name)
                }
                GrantTarget::Aggregate { schema, name } => {
                    let prefix = self.schema_path_prefix(&schema);
                    format!("{}aggregates/{}.sql", prefix, name)
                }
                GrantTarget::Schema => "schemas.sql".to_string(),
                GrantTarget::Type { schema } => {
                    let prefix = self.schema_path_prefix(&schema);
                    format!("{}types.sql", prefix)
                }
                GrantTarget::Domain { schema } => {
                    let prefix = self.schema_path_prefix(&schema);
                    format!("{}domains.sql", prefix)
                }
                GrantTarget::Sequence { schema, name } => {
                    if let Some((table_schema, table_name)) =
                        self.find_owning_table_for_sequence(&schema, &name)
                    {
                        let prefix = self.schema_path_prefix(&table_schema);
                        format!("{}tables/{}.sql", prefix, table_name)
                    } else {
                        let prefix = self.schema_path_prefix(&schema);
                        format!("{}sequences/{}.sql", prefix, name)
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
        object_to_file: &BTreeMap<DbObjectId, String>,
    ) -> Result<FileContent> {
        let file_path = self.output_dir.join(&file_key);

        // Calculate dependencies for this file
        let dependencies = self.calculate_file_dependencies(&steps, object_to_file);

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

    /// Calculate file dependencies from migration steps using the object-to-file mapping
    fn calculate_file_dependencies(
        &self,
        steps: &[MigrationStep],
        object_to_file: &BTreeMap<DbObjectId, String>,
    ) -> Vec<String> {
        let mut dependencies = BTreeSet::new();

        let current_file_path = if let Some(first_step) = steps.first() {
            self.determine_file_for_step(first_step)
        } else {
            return vec![];
        };

        for step in steps {
            let step_deps = self.get_step_dependencies(step);
            for dep in step_deps {
                // Use the mapping to find where the dependency object is written
                if let Some(file_path) = object_to_file.get(&dep)
                    && *file_path != current_file_path
                {
                    dependencies.insert(file_path.clone());
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

    /// Write organized files to disk
    fn write_organized_files(&self, files: BTreeMap<String, FileContent>) -> Result<()> {
        for (_, file_content) in files {
            let mut content = String::new();

            // Write each dependency on its own line for readability
            if !file_content.dependencies.is_empty() {
                for dep in &file_content.dependencies {
                    content.push_str(&format!("-- require: {}\n", dep));
                }
                content.push('\n');
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

    // Helper methods for extracting schema and name from operations
    fn extract_table_info_from_operation(
        &self,
        op: &crate::diff::operations::TableOperation,
    ) -> (String, String) {
        use crate::diff::operations::TableOperation;
        match op {
            TableOperation::Create { schema, name, .. } => (schema.clone(), name.clone()),
            TableOperation::Drop { schema, name } => (schema.clone(), name.clone()),
            TableOperation::Alter { schema, name, .. } => (schema.clone(), name.clone()),
            TableOperation::Comment(comment_op) => match comment_op {
                crate::diff::operations::CommentOperation::Set { target, .. } => {
                    (target.schema.clone(), target.table.clone())
                }
                crate::diff::operations::CommentOperation::Drop { target } => {
                    (target.schema.clone(), target.table.clone())
                }
            },
        }
    }

    fn extract_view_info_from_operation(
        &self,
        op: &crate::diff::operations::ViewOperation,
    ) -> (String, String) {
        use crate::diff::operations::ViewOperation;
        match op {
            ViewOperation::Create { schema, name, .. } => (schema.clone(), name.clone()),
            ViewOperation::Drop { schema, name } => (schema.clone(), name.clone()),
            ViewOperation::Replace { schema, name, .. } => (schema.clone(), name.clone()),
            ViewOperation::SetOption { schema, name, .. } => (schema.clone(), name.clone()),
            ViewOperation::Comment(comment_op) => match comment_op {
                crate::diff::operations::CommentOperation::Set { target, .. } => {
                    (target.schema.clone(), target.name.clone())
                }
                crate::diff::operations::CommentOperation::Drop { target } => {
                    (target.schema.clone(), target.name.clone())
                }
            },
        }
    }

    fn extract_function_info_from_operation(
        &self,
        op: &crate::diff::operations::FunctionOperation,
    ) -> (String, String) {
        use crate::diff::operations::FunctionOperation;
        match op {
            FunctionOperation::Create { schema, name, .. } => (schema.clone(), name.clone()),
            FunctionOperation::Drop { schema, name, .. } => (schema.clone(), name.clone()),
            FunctionOperation::Replace { schema, name, .. } => (schema.clone(), name.clone()),
            FunctionOperation::Comment(comment_op) => match comment_op {
                crate::diff::operations::CommentOperation::Set { target, .. } => {
                    (target.schema.clone(), target.name.clone())
                }
                crate::diff::operations::CommentOperation::Drop { target } => {
                    (target.schema.clone(), target.name.clone())
                }
            },
        }
    }

    fn extract_aggregate_info_from_operation(
        &self,
        op: &crate::diff::operations::AggregateOperation,
    ) -> (String, String) {
        use crate::diff::operations::AggregateOperation;
        match op {
            AggregateOperation::Create { aggregate, .. } => {
                (aggregate.schema.clone(), aggregate.name.clone())
            }
            AggregateOperation::Drop { identifier, .. } => {
                (identifier.schema.clone(), identifier.name.clone())
            }
            AggregateOperation::Replace { new_aggregate, .. } => {
                (new_aggregate.schema.clone(), new_aggregate.name.clone())
            }
            AggregateOperation::Comment(comment_op) => match comment_op {
                crate::diff::operations::CommentOperation::Set { target, .. } => {
                    (target.schema.clone(), target.name.clone())
                }
                crate::diff::operations::CommentOperation::Drop { target } => {
                    (target.schema.clone(), target.name.clone())
                }
            },
        }
    }

    fn extract_sequence_info_from_operation(
        &self,
        op: &crate::diff::operations::SequenceOperation,
    ) -> (String, String) {
        use crate::diff::operations::SequenceOperation;
        match op {
            SequenceOperation::Create { schema, name, .. } => (schema.clone(), name.clone()),
            SequenceOperation::Drop { schema, name } => (schema.clone(), name.clone()),
            SequenceOperation::AlterOwnership { schema, name, .. } => {
                (schema.clone(), name.clone())
            }
            SequenceOperation::Comment(comment_op) => match comment_op {
                crate::diff::operations::CommentOperation::Set { target, .. } => {
                    (target.schema.clone(), target.name.clone())
                }
                crate::diff::operations::CommentOperation::Drop { target } => {
                    (target.schema.clone(), target.name.clone())
                }
            },
        }
    }

    fn extract_type_info_from_operation(
        &self,
        op: &crate::diff::operations::TypeOperation,
    ) -> (String, String) {
        use crate::diff::operations::TypeOperation;
        match op {
            TypeOperation::Create { schema, name, .. } => (schema.clone(), name.clone()),
            TypeOperation::Drop { schema, name } => (schema.clone(), name.clone()),
            TypeOperation::Alter { schema, name, .. } => (schema.clone(), name.clone()),
            TypeOperation::Comment(comment_op) => match comment_op {
                crate::diff::operations::CommentOperation::Set { target, .. } => {
                    (target.schema.clone(), target.name.clone())
                }
                crate::diff::operations::CommentOperation::Drop { target } => {
                    (target.schema.clone(), target.name.clone())
                }
            },
        }
    }

    fn extract_domain_info_from_operation(
        &self,
        op: &crate::diff::operations::DomainOperation,
    ) -> (String, String) {
        use crate::diff::operations::DomainOperation;
        match op {
            DomainOperation::Create { schema, name, .. }
            | DomainOperation::Drop { schema, name }
            | DomainOperation::AlterSetNotNull { schema, name }
            | DomainOperation::AlterDropNotNull { schema, name }
            | DomainOperation::AlterSetDefault { schema, name, .. }
            | DomainOperation::AlterDropDefault { schema, name }
            | DomainOperation::AddConstraint { schema, name, .. }
            | DomainOperation::DropConstraint { schema, name, .. } => {
                (schema.clone(), name.clone())
            }
            DomainOperation::Comment(comment_op) => match comment_op {
                crate::diff::operations::CommentOperation::Set { target, .. } => {
                    (target.schema.clone(), target.name.clone())
                }
                crate::diff::operations::CommentOperation::Drop { target } => {
                    (target.schema.clone(), target.name.clone())
                }
            },
        }
    }

    fn extract_table_info_from_index_operation(
        &self,
        op: &crate::diff::operations::IndexOperation,
    ) -> (String, String) {
        use crate::diff::operations::IndexOperation;
        match op {
            IndexOperation::Create(index) => (index.table_schema.clone(), index.table_name.clone()),
            IndexOperation::Drop { schema, name, .. } => {
                for index in &self.catalog.indexes {
                    if index.schema == *schema && index.name == *name {
                        return (index.table_schema.clone(), index.table_name.clone());
                    }
                }
                (schema.clone(), "unknown".to_string())
            }
            IndexOperation::Comment(comment_op) => match comment_op {
                crate::diff::operations::CommentOperation::Set { target, .. } => {
                    // Look up the index to find its table
                    for index in &self.catalog.indexes {
                        if index.schema == target.schema && index.name == target.name {
                            return (index.table_schema.clone(), index.table_name.clone());
                        }
                    }
                    (target.schema.clone(), "unknown".to_string())
                }
                crate::diff::operations::CommentOperation::Drop { target } => {
                    for index in &self.catalog.indexes {
                        if index.schema == target.schema && index.name == target.name {
                            return (index.table_schema.clone(), index.table_name.clone());
                        }
                    }
                    (target.schema.clone(), "unknown".to_string())
                }
            },
            IndexOperation::Cluster {
                table_schema,
                table_name,
                ..
            } => (table_schema.clone(), table_name.clone()),
            IndexOperation::SetWithoutCluster { schema, name, .. } => {
                for index in &self.catalog.indexes {
                    if index.schema == *schema && index.name == *name {
                        return (index.table_schema.clone(), index.table_name.clone());
                    }
                }
                (schema.clone(), name.clone())
            }
            IndexOperation::Reindex { schema, name, .. } => {
                for index in &self.catalog.indexes {
                    if index.schema == *schema && index.name == *name {
                        return (index.table_schema.clone(), index.table_name.clone());
                    }
                }
                (schema.clone(), "unknown".to_string())
            }
        }
    }

    fn extract_table_info_from_constraint_operation(
        &self,
        op: &crate::diff::operations::ConstraintOperation,
    ) -> (String, String) {
        use crate::diff::operations::ConstraintOperation;
        match op {
            ConstraintOperation::Create(constraint) => {
                (constraint.schema.clone(), constraint.table.clone())
            }
            ConstraintOperation::Drop(constraint_id) => {
                (constraint_id.schema.clone(), constraint_id.table.clone())
            }
            ConstraintOperation::Comment(comment_op) => match comment_op {
                crate::diff::operations::CommentOperation::Set { target, .. } => {
                    (target.schema.clone(), target.table.clone())
                }
                crate::diff::operations::CommentOperation::Drop { target } => {
                    (target.schema.clone(), target.table.clone())
                }
            },
        }
    }

    fn extract_table_info_from_trigger_operation(
        &self,
        op: &crate::diff::operations::TriggerOperation,
    ) -> (String, String) {
        use crate::diff::operations::TriggerOperation;
        match op {
            TriggerOperation::Create { trigger } => {
                // Trigger's schema field IS the table's schema
                (trigger.schema.clone(), trigger.table_name.clone())
            }
            TriggerOperation::Drop { identifier } => {
                (identifier.schema.clone(), identifier.table.clone())
            }
            TriggerOperation::Replace { new_trigger, .. } => {
                (new_trigger.schema.clone(), new_trigger.table_name.clone())
            }
            TriggerOperation::Comment(comment_op) => match comment_op {
                crate::diff::operations::CommentOperation::Set { target, .. } => {
                    (target.schema.clone(), target.table.clone())
                }
                crate::diff::operations::CommentOperation::Drop { target } => {
                    (target.schema.clone(), target.table.clone())
                }
            },
        }
    }

    fn extract_table_info_from_policy_operation(
        &self,
        op: &crate::diff::operations::PolicyOperation,
    ) -> (String, String) {
        use crate::diff::operations::PolicyOperation;
        match op {
            PolicyOperation::Create { policy } => {
                (policy.schema.clone(), policy.table_name.clone())
            }
            PolicyOperation::Drop { identifier } => {
                (identifier.schema.clone(), identifier.table.clone())
            }
            PolicyOperation::Alter { identifier, .. } => {
                (identifier.schema.clone(), identifier.table.clone())
            }
            PolicyOperation::Replace { new_policy, .. } => {
                (new_policy.schema.clone(), new_policy.table_name.clone())
            }
            PolicyOperation::Comment(comment_op) => match comment_op {
                crate::diff::operations::CommentOperation::Set { target, .. } => {
                    (target.schema.clone(), target.table.clone())
                }
                crate::diff::operations::CommentOperation::Drop { target } => {
                    (target.schema.clone(), target.table.clone())
                }
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
            ObjectType::Table { schema, name } => GrantTarget::Table {
                schema: schema.clone(),
                name: name.clone(),
            },
            ObjectType::View { schema, name } => GrantTarget::View {
                schema: schema.clone(),
                name: name.clone(),
            },
            ObjectType::Function { schema, name, .. } => GrantTarget::Function {
                schema: schema.clone(),
                name: name.clone(),
            },
            ObjectType::Procedure { schema, name, .. } => GrantTarget::Procedure {
                schema: schema.clone(),
                name: name.clone(),
            },
            ObjectType::Aggregate { schema, name, .. } => GrantTarget::Aggregate {
                schema: schema.clone(),
                name: name.clone(),
            },
            ObjectType::Schema { .. } => GrantTarget::Schema,
            ObjectType::Type { schema, .. } => GrantTarget::Type {
                schema: schema.clone(),
            },
            ObjectType::Domain { schema, .. } => GrantTarget::Domain {
                schema: schema.clone(),
            },
            ObjectType::Sequence { schema, name } => GrantTarget::Sequence {
                schema: schema.clone(),
                name: name.clone(),
            },
        }
    }

    /// Find the owning table for a sequence, returns (schema, table_name) if found
    fn find_owning_table_for_sequence(
        &self,
        seq_schema: &str,
        seq_name: &str,
    ) -> Option<(String, String)> {
        // Look through catalog sequences to find if this sequence is owned by a table
        for sequence in &self.catalog.sequences {
            if sequence.schema == seq_schema && sequence.name == seq_name {
                if let Some(ref owned_by) = sequence.owned_by {
                    // owned_by format is usually "schema.table.column"
                    let parts: Vec<&str> = owned_by.split('.').collect();
                    if parts.len() >= 3 {
                        return Some((parts[0].to_string(), parts[1].to_string()));
                    }
                }
                break;
            }
        }
        None
    }
}

/// Target of a grant operation with schema information
#[derive(Debug, Clone)]
enum GrantTarget {
    Table { schema: String, name: String },
    View { schema: String, name: String },
    Function { schema: String, name: String },
    Procedure { schema: String, name: String },
    Aggregate { schema: String, name: String },
    Schema,
    Type { schema: String },
    Domain { schema: String },
    Sequence { schema: String, name: String },
}
