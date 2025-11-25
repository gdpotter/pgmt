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

    let deps = augmentation.get_additional_dependencies(&table_id);
    assert_eq!(deps, vec![schema_id]);

    // Test empty dependencies
    let other_table = DbObjectId::Table {
        schema: "app".to_string(),
        name: "posts".to_string(),
    };
    let empty_deps = augmentation.get_additional_dependencies(&other_table);
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
    let view_deps = augmentation.get_additional_dependencies(&view_id);
    assert!(view_deps.contains(&table_id));

    // Table should depend on schema and type (03_tables.sql requires 01_schemas.sql and 02_types.sql)
    let table_deps = augmentation.get_additional_dependencies(&table_id);
    assert!(table_deps.contains(&schema_id));
    assert!(table_deps.contains(&type_id));

    // Type should depend on schema (02_types.sql requires 01_schemas.sql)
    let type_deps = augmentation.get_additional_dependencies(&type_id);
    assert!(type_deps.contains(&schema_id));
}
