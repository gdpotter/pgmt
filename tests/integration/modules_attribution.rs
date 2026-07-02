//! End-to-end object→module attribution (Phase 1): apply a real schema
//! directory through the SchemaProcessor, then resolve each catalog object's
//! module via the file that created it.

use crate::helpers::harness::with_test_db;
use anyhow::Result;
use pgmt::catalog::id::DbObjectId;
use pgmt::config::builder::ConfigBuilder;
use pgmt::config::types::ConfigInput;
use pgmt::db::schema_processor::{SchemaProcessor, SchemaProcessorConfig};
use pgmt::modules::{ModulePartition, validate_module_references};
use std::fs;
use tempfile::TempDir;

fn write_schema_files(root: &std::path::Path) -> Result<()> {
    let schema = root.join("schema");
    fs::create_dir_all(schema.join("core"))?;
    fs::create_dir_all(schema.join("billing"))?;
    fs::write(
        schema.join("core/users.sql"),
        "CREATE TABLE users (id SERIAL PRIMARY KEY);",
    )?;
    fs::write(
        schema.join("billing/invoices.sql"),
        "-- require: core/users.sql\n\
         CREATE TABLE invoices (id SERIAL PRIMARY KEY, user_id INT REFERENCES users(id));",
    )?;
    // Unmoduled file → the base.
    fs::write(
        schema.join("audit_log.sql"),
        "CREATE TABLE audit_log (id SERIAL PRIMARY KEY);",
    )?;
    Ok(())
}

fn modules_config(yaml_modules: &str) -> Result<pgmt::config::Config> {
    let yaml = format!("directories:\n  schema_dir: schema\n{}", yaml_modules);
    let input: ConfigInput = serde_yaml::from_str(&yaml)?;
    ConfigBuilder::new().with_file(input).resolve()
}

fn table(name: &str) -> DbObjectId {
    DbObjectId::Table {
        schema: "public".to_string(),
        name: name.to_string(),
    }
}

#[tokio::test]
async fn test_object_module_attribution_through_schema_apply() -> Result<()> {
    with_test_db(async |db| {
        let project = TempDir::new()?;
        write_schema_files(project.path())?;

        let config = modules_config(
            r#"
modules:
  core:
    paths: ["schema/core/**"]
  billing:
    paths: ["schema/billing/**"]
    depends_on: [core]
"#,
        )?;

        let processor = SchemaProcessor::new(
            db.pool().clone(),
            SchemaProcessorConfig {
                verbose: false,
                clean_before_apply: false,
                objects: config.objects.clone(),
            },
        );
        let processed = processor
            .process_schema_directory(&project.path().join("schema"))
            .await?;

        let partition = ModulePartition::from_config(&config)?;

        // Objects resolve to the module of their defining file.
        assert_eq!(
            partition.module_for_object(&table("users"), &processed.file_mapping)?,
            Some("core")
        );
        assert_eq!(
            partition.module_for_object(&table("invoices"), &processed.file_mapping)?,
            Some("billing")
        );
        // Unmoduled file → the base (None).
        assert_eq!(
            partition.module_for_object(&table("audit_log"), &processed.file_mapping)?,
            None
        );

        // The billing→core FK reference is covered by the declared dependency.
        let report = validate_module_references(
            &processed.catalog,
            &processed.file_mapping,
            &partition,
            &config,
        )?;
        assert!(
            report.is_clean(),
            "declared billing→core dep should validate cleanly: {:?}",
            report
        );

        Ok(())
    })
    .await
}

/// The same schema WITHOUT the declared dependency: the billing→core FK is
/// flagged as an undeclared cross-module reference (warning, §5), and a base
/// file referencing a module's object is a hard error (§6).
#[tokio::test]
async fn test_undeclared_and_base_references_are_flagged() -> Result<()> {
    with_test_db(async |db| {
        let project = TempDir::new()?;
        write_schema_files(project.path())?;
        // Base file referencing billing's table: §6 error.
        fs::write(
            project.path().join("schema/report.sql"),
            "-- require: billing/invoices.sql\n\
             CREATE VIEW invoice_report AS SELECT id FROM invoices;",
        )?;

        // billing deliberately does NOT declare depends_on: [core].
        let config = modules_config(
            r#"
modules:
  core:
    paths: ["schema/core/**"]
  billing:
    paths: ["schema/billing/**"]
"#,
        )?;

        let processor = SchemaProcessor::new(
            db.pool().clone(),
            SchemaProcessorConfig {
                verbose: false,
                clean_before_apply: false,
                objects: config.objects.clone(),
            },
        );
        let processed = processor
            .process_schema_directory(&project.path().join("schema"))
            .await?;

        let partition = ModulePartition::from_config(&config)?;
        let report = validate_module_references(
            &processed.catalog,
            &processed.file_mapping,
            &partition,
            &config,
        )?;

        assert!(
            report
                .warnings
                .iter()
                .any(|w| w.contains("billing") && w.contains("core")),
            "undeclared billing→core reference should warn: {:?}",
            report
        );
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.contains("unmoduled object") && e.contains("billing")),
            "base→billing reference should be a hard error: {:?}",
            report
        );

        Ok(())
    })
    .await
}
