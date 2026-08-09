//! Tests for file-based dependency augmentation system

use crate::helpers::harness::with_test_db;
use pgmt::catalog::Catalog;
use pgmt::catalog::file_dependencies::{
    FileDependencyAugmentation, FileToObjectMapping, create_dependency_augmentation,
};
use pgmt::catalog::id::DbObjectId;
use pgmt::schema_loader::SchemaFile;

#[test]
fn test_file_to_object_mapping() {
    let mut mapping = FileToObjectMapping::new();

    let file1 = "01_schemas/app.sql".to_string();
    let file2 = "02_tables/users.sql".to_string();
    let schema_id = DbObjectId::Schema {
        name: "app".to_string(),
    };
    let table_id = DbObjectId::Table {
        schema: "app".to_string(),
        name: "users".to_string(),
    };

    mapping.add_object(file1.clone(), schema_id.clone());
    mapping.add_object(file2.clone(), table_id.clone());

    assert_eq!(
        mapping.get_objects_for_file(&file1),
        vec![schema_id.clone()]
    );
    assert_eq!(mapping.get_objects_for_file(&file2), vec![table_id.clone()]);
}

#[test]
fn test_dependency_augmentation() {
    let mut augmentation = FileDependencyAugmentation::new();

    let table_id = DbObjectId::Table {
        schema: "app".to_string(),
        name: "users".to_string(),
    };
    let schema_id = DbObjectId::Schema {
        name: "app".to_string(),
    };

    augmentation.add_dependency(table_id.clone(), schema_id.clone());

    let deps = augmentation
        .additional_dependencies
        .get(&table_id)
        .cloned()
        .unwrap_or_default();
    assert_eq!(deps, vec![schema_id]);

    // Test empty dependencies
    let other_table = DbObjectId::Table {
        schema: "app".to_string(),
        name: "posts".to_string(),
    };
    let empty_deps = augmentation
        .additional_dependencies
        .get(&other_table)
        .cloned()
        .unwrap_or_default();
    assert_eq!(empty_deps, Vec::<DbObjectId>::new());
}

#[tokio::test]
async fn test_augmented_catalog_loading() {
    with_test_db(async |db| {
        // Set up test schema
        db.execute("CREATE SCHEMA app").await;
        db.execute("CREATE TABLE app.users (id SERIAL PRIMARY KEY, name VARCHAR(100))")
            .await;

        // Create a dummy augmentation
        let mut augmentation = FileDependencyAugmentation::new();
        let table_id = DbObjectId::Table {
            schema: "app".to_string(),
            name: "users".to_string(),
        };
        let schema_id = DbObjectId::Schema {
            name: "app".to_string(),
        };
        augmentation.add_dependency(table_id.clone(), schema_id.clone());

        // Load catalog with augmentation
        let catalog = Catalog::load_with_file_dependencies(db.pool(), Some(&augmentation))
            .await
            .unwrap();

        // Verify that the augmentation was applied
        let table_deps = catalog.forward_deps.get(&table_id);
        assert!(table_deps.is_some());

        let deps = table_deps.unwrap();
        assert!(
            deps.contains(&schema_id),
            "Table should have file-based dependency on schema"
        );

        // Verify reverse dependencies
        let schema_reverse_deps = catalog.reverse_deps.get(&schema_id);
        assert!(schema_reverse_deps.is_some());
        let reverse_deps = schema_reverse_deps.unwrap();
        assert!(
            reverse_deps.contains(&table_id),
            "Schema should be in reverse dependencies of table"
        );
    })
    .await;
}

#[test]
fn test_complex_dependency_chain() {
    let schema_files = vec![
        SchemaFile {
            relative_path: "01_schemas.sql".to_string(),
            content: "CREATE SCHEMA app;".to_string(),
            dependencies: vec![],
        },
        SchemaFile {
            relative_path: "02_types.sql".to_string(),
            content: "CREATE TYPE app.status AS ENUM ('active', 'inactive');".to_string(),
            dependencies: vec!["01_schemas.sql".to_string()],
        },
        SchemaFile {
            relative_path: "03_tables.sql".to_string(),
            content: "CREATE TABLE app.users (id SERIAL, status app.status);".to_string(),
            dependencies: vec!["01_schemas.sql".to_string(), "02_types.sql".to_string()],
        },
        SchemaFile {
            relative_path: "04_views.sql".to_string(),
            content:
                "CREATE VIEW app.active_users AS SELECT * FROM app.users WHERE status = 'active';"
                    .to_string(),
            dependencies: vec!["03_tables.sql".to_string()],
        },
    ];

    // Create mock file-to-object mappings
    let mut mapping = FileToObjectMapping::new();
    mapping.add_object(
        "01_schemas.sql".to_string(),
        DbObjectId::Schema {
            name: "app".to_string(),
        },
    );
    mapping.add_object(
        "02_types.sql".to_string(),
        DbObjectId::Type {
            schema: "app".to_string(),
            name: "status".to_string(),
        },
    );
    mapping.add_object(
        "03_tables.sql".to_string(),
        DbObjectId::Table {
            schema: "app".to_string(),
            name: "users".to_string(),
        },
    );
    mapping.add_object(
        "04_views.sql".to_string(),
        DbObjectId::View {
            schema: "app".to_string(),
            name: "active_users".to_string(),
        },
    );

    let augmentation = create_dependency_augmentation(&mapping, &schema_files).unwrap();

    // Verify complex dependency relationships
    let view_id = DbObjectId::View {
        schema: "app".to_string(),
        name: "active_users".to_string(),
    };
    let table_id = DbObjectId::Table {
        schema: "app".to_string(),
        name: "users".to_string(),
    };
    let type_id = DbObjectId::Type {
        schema: "app".to_string(),
        name: "status".to_string(),
    };
    let schema_id = DbObjectId::Schema {
        name: "app".to_string(),
    };

    // View should depend on table (04_views.sql requires 03_tables.sql)
    let view_deps = augmentation.additional_dependencies.get(&view_id).unwrap();
    assert!(view_deps.contains(&table_id));

    // Table should depend on schema and type (03_tables.sql requires 01_schemas.sql and 02_types.sql)
    let table_deps = augmentation.additional_dependencies.get(&table_id).unwrap();
    assert!(table_deps.contains(&schema_id));
    assert!(table_deps.contains(&type_id));

    // Type should depend on schema (02_types.sql requires 01_schemas.sql)
    let type_deps = augmentation.additional_dependencies.get(&type_id).unwrap();
    assert!(type_deps.contains(&schema_id));
}

/// Every object a schema file creates is attributed to that file, for every
/// object kind the identity snapshot reports.
///
/// Attribution is derived from OID boundaries recorded between files and a
/// single snapshot at the end, so a kind whose catalog is missing from the
/// boundary query would have its objects allocated past the last boundary and
/// attributed to no file at all — which on a module project silently lands them
/// in the unmoduled base. Asserting per-kind ownership is what catches that.
#[tokio::test]
async fn test_each_file_owns_the_objects_it_creates() -> anyhow::Result<()> {
    use pgmt::db::schema_processor::{SchemaProcessor, SchemaProcessorConfig};
    use std::fs;

    with_test_db(async |db| {
        let project = tempfile::TempDir::new()?;
        let schema = project.path().join("schema");
        fs::create_dir_all(&schema)?;

        let files: [(&str, &str); 6] = [
            ("01_schema.sql", "CREATE SCHEMA app;"),
            (
                "02_types.sql",
                "-- require: 01_schema.sql\n\
                 CREATE TYPE app.status AS ENUM ('on', 'off');\n\
                 CREATE DOMAIN app.email AS text;\n\
                 CREATE SEQUENCE app.counter;",
            ),
            (
                "03_tables.sql",
                "-- require: 02_types.sql\n\
                 CREATE TABLE app.users (\n\
                   id integer PRIMARY KEY,\n\
                   email app.email,\n\
                   state app.status,\n\
                   CONSTRAINT users_id_positive CHECK (id > 0)\n\
                 );",
            ),
            (
                "04_indexes.sql",
                "-- require: 03_tables.sql\n\
                 CREATE INDEX users_state_idx ON app.users (state);",
            ),
            (
                "05_functions.sql",
                "-- require: 03_tables.sql\n\
                 CREATE FUNCTION app.touch() RETURNS trigger AS $$\n\
                 BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;",
            ),
            (
                "06_attached.sql",
                "-- require: 05_functions.sql\n\
                 CREATE TRIGGER users_touch BEFORE UPDATE ON app.users\n\
                   FOR EACH ROW EXECUTE FUNCTION app.touch();\n\
                 ALTER TABLE app.users ENABLE ROW LEVEL SECURITY;\n\
                 CREATE POLICY users_self ON app.users USING (id > 0);\n\
                 CREATE VIEW app.active AS SELECT id FROM app.users;",
            ),
        ];
        for (name, body) in files {
            fs::write(schema.join(name), body)?;
        }

        let processor = SchemaProcessor::new(
            db.pool().clone(),
            SchemaProcessorConfig {
                verbose: false,
                clean_before_apply: false,
                objects: Default::default(),
            },
        );
        let processed = processor.process_schema_directory(&schema).await?;
        let owner = &processed.file_mapping.object_files;

        let expected: Vec<(DbObjectId, &str)> = vec![
            (
                DbObjectId::Schema {
                    name: "app".to_string(),
                },
                "01_schema.sql",
            ),
            (
                DbObjectId::Type {
                    schema: "app".to_string(),
                    name: "status".to_string(),
                },
                "02_types.sql",
            ),
            (
                DbObjectId::Domain {
                    schema: "app".to_string(),
                    name: "email".to_string(),
                },
                "02_types.sql",
            ),
            (
                DbObjectId::Sequence {
                    schema: "app".to_string(),
                    name: "counter".to_string(),
                },
                "02_types.sql",
            ),
            (
                DbObjectId::Table {
                    schema: "app".to_string(),
                    name: "users".to_string(),
                },
                "03_tables.sql",
            ),
            (
                DbObjectId::Constraint {
                    schema: "app".to_string(),
                    table: "users".to_string(),
                    name: "users_id_positive".to_string(),
                },
                "03_tables.sql",
            ),
            (
                DbObjectId::Index {
                    schema: "app".to_string(),
                    name: "users_state_idx".to_string(),
                },
                "04_indexes.sql",
            ),
            (
                DbObjectId::Function {
                    schema: "app".to_string(),
                    name: "touch".to_string(),
                    arguments: String::new(),
                },
                "05_functions.sql",
            ),
            (
                DbObjectId::Trigger {
                    schema: "app".to_string(),
                    table: "users".to_string(),
                    name: "users_touch".to_string(),
                },
                "06_attached.sql",
            ),
            (
                DbObjectId::Policy {
                    schema: "app".to_string(),
                    table: "users".to_string(),
                    name: "users_self".to_string(),
                },
                "06_attached.sql",
            ),
            (
                DbObjectId::View {
                    schema: "app".to_string(),
                    name: "active".to_string(),
                },
                "06_attached.sql",
            ),
        ];

        for (id, file) in expected {
            assert_eq!(
                owner.get(&id).map(String::as_str),
                Some(file),
                "{id:?} should be owned by {file}, mapping has {:?}",
                owner.get(&id)
            );
        }

        Ok(())
    })
    .await
}
