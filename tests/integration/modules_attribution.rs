//! End-to-end object→module attribution: apply a real schema
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

fn policy(table: &str, name: &str) -> DbObjectId {
    DbObjectId::Policy {
        schema: "public".to_string(),
        table: table.to_string(),
        name: name.to_string(),
    }
}

/// Apply a schema directory and return the processed result.
async fn process(
    db: &crate::helpers::harness::TestDatabase,
    config: &pgmt::config::Config,
    root: &std::path::Path,
) -> Result<pgmt::db::schema_processor::ProcessedSchema> {
    let processor = SchemaProcessor::new(
        db.pool().clone(),
        SchemaProcessorConfig {
            verbose: false,
            clean_before_apply: false,
            objects: config.objects.clone(),
        },
    );
    processor
        .process_schema_directory(&root.join("schema"))
        .await
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

        let processed = process(db, &config, project.path()).await?;

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
/// flagged as an undeclared cross-module reference (a warning), and a base
/// file referencing a module's object is a hard error.
#[tokio::test]
async fn test_undeclared_and_base_references_are_flagged() -> Result<()> {
    with_test_db(async |db| {
        let project = TempDir::new()?;
        write_schema_files(project.path())?;
        // Base file referencing a module's table: a hard error.
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

        let processed = process(db, &config, project.path()).await?;

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

/// An RLS policy created alongside its table in a module's file is attributed
/// to that module. Regression: policies were absent from the identity snapshot,
/// so no file claimed them; they fell to the base and their (mandatory)
/// reference to the module's table was reported as a base→module violation.
#[tokio::test]
async fn test_policy_attributed_to_its_own_module() -> Result<()> {
    with_test_db(async |db| {
        let project = TempDir::new()?;
        let schema = project.path().join("schema");
        fs::create_dir_all(schema.join("core"))?;
        fs::write(
            schema.join("core/users.sql"),
            "CREATE TABLE users (id SERIAL PRIMARY KEY);\n\
             ALTER TABLE users ENABLE ROW LEVEL SECURITY;\n\
             CREATE POLICY users_self ON users USING (true);",
        )?;

        let config = modules_config(
            r#"
modules:
  core:
    paths: ["schema/core/**"]
"#,
        )?;
        let processed = process(db, &config, project.path()).await?;
        let partition = ModulePartition::from_config(&config)?;

        assert_eq!(
            partition.module_for_object(&policy("users", "users_self"), &processed.file_mapping)?,
            Some("core"),
            "a policy defined in core's file belongs to core"
        );

        let report = validate_module_references(
            &processed.catalog,
            &processed.file_mapping,
            &partition,
            &config,
        )?;
        assert!(
            report.is_clean(),
            "a policy on its own module's table is intra-module: {:?}",
            report
        );

        Ok(())
    })
    .await
}

/// A policy on another module's table keeps the module of the file that
/// defines it: with `depends_on` declared the cross-module reference validates
/// cleanly, and without it the same layout only warns.
#[tokio::test]
async fn test_policy_in_dependent_module_keeps_its_file_module() -> Result<()> {
    with_test_db(async |db| {
        let project = TempDir::new()?;
        let schema = project.path().join("schema");
        fs::create_dir_all(schema.join("core"))?;
        fs::create_dir_all(schema.join("policies"))?;
        fs::write(
            schema.join("core/users.sql"),
            "CREATE TABLE users (id SERIAL PRIMARY KEY);",
        )?;
        fs::write(
            schema.join("policies/users_rls.sql"),
            "-- require: core/users.sql\n\
             ALTER TABLE users ENABLE ROW LEVEL SECURITY;\n\
             CREATE POLICY users_self ON users USING (true);",
        )?;

        let with_dep = modules_config(
            r#"
modules:
  core:
    paths: ["schema/core/**"]
  policies:
    paths: ["schema/policies/**"]
    depends_on: [core]
"#,
        )?;
        let processed = process(db, &with_dep, project.path()).await?;
        let partition = ModulePartition::from_config(&with_dep)?;

        assert_eq!(
            partition.module_for_object(&policy("users", "users_self"), &processed.file_mapping)?,
            Some("policies"),
            "explicit file placement wins over the parent-table fallback"
        );
        assert_eq!(
            partition.module_for_object(&table("users"), &processed.file_mapping)?,
            Some("core")
        );

        let report = validate_module_references(
            &processed.catalog,
            &processed.file_mapping,
            &partition,
            &with_dep,
        )?;
        assert!(
            report.is_clean(),
            "declared policies→core dep should validate cleanly: {:?}",
            report
        );

        // The same layout without the declared dependency: a warning, not an
        // error (both sides are modules).
        let without_dep = modules_config(
            r#"
modules:
  core:
    paths: ["schema/core/**"]
  policies:
    paths: ["schema/policies/**"]
"#,
        )?;
        let partition = ModulePartition::from_config(&without_dep)?;
        let report = validate_module_references(
            &processed.catalog,
            &processed.file_mapping,
            &partition,
            &without_dep,
        )?;
        assert!(report.errors.is_empty(), "{:?}", report);
        assert!(
            report
                .warnings
                .iter()
                .any(|w| w.contains("policies") && w.contains("core")),
            "undeclared policies→core reference should warn: {:?}",
            report
        );

        Ok(())
    })
    .await
}

/// A unique index that a later foreign key references keeps the module of the
/// file that created it. Regression: the identity snapshot discarded any index
/// some constraint pointed at via `conindid`, and a foreign key's `conindid` is
/// the *referenced* table's index — so creating an FK against a uniquely
/// indexed column made the index invisible to attribution, leaving it claimed
/// by no file and falling out of its module.
#[tokio::test]
async fn test_fk_referenced_index_keeps_its_module() -> Result<()> {
    with_test_db(async |db| {
        let project = TempDir::new()?;
        let schema = project.path().join("schema");
        fs::create_dir_all(schema.join("core"))?;
        fs::create_dir_all(schema.join("billing"))?;
        fs::write(
            schema.join("core/users.sql"),
            "CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT NOT NULL);\n\
             CREATE UNIQUE INDEX users_email_idx ON users (email);",
        )?;
        fs::write(
            schema.join("billing/invoices.sql"),
            "-- require: core/users.sql\n\
             CREATE TABLE invoices (id SERIAL PRIMARY KEY, user_email TEXT REFERENCES users (email));",
        )?;

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
        let processed = process(db, &config, project.path()).await?;
        let partition = ModulePartition::from_config(&config)?;

        let index = DbObjectId::Index {
            schema: "public".to_string(),
            name: "users_email_idx".to_string(),
        };
        assert_eq!(
            partition.module_for_object(&index, &processed.file_mapping)?,
            Some("core"),
            "the index belongs to the module of the file that created it"
        );

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
