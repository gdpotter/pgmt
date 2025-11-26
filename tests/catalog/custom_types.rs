use crate::helpers::harness::with_test_db;

use pgmt::catalog::custom_type::{TypeKind, fetch};
use pgmt::catalog::id::{DbObjectId, DependsOn};

#[tokio::test]
async fn test_fetch_enum_type() {
    with_test_db(async |db| {
        // Create an enum type
        db.execute("CREATE TYPE status AS ENUM ('active', 'inactive', 'pending')")
            .await;

        let types = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(types.len(), 1);
        let type_ = &types[0];

        assert_eq!(type_.schema, "public");
        assert_eq!(type_.name, "status");
        assert_eq!(type_.kind, TypeKind::Enum);
        assert_eq!(type_.enum_values.len(), 3);
        assert!(type_.composite_attributes.is_empty());

        // Check enum values in order
        assert_eq!(type_.enum_values[0].name, "active");
        assert_eq!(type_.enum_values[1].name, "inactive");
        assert_eq!(type_.enum_values[2].name, "pending");

        // Check dependencies
        assert_eq!(
            type_.depends_on,
            vec![DbObjectId::Schema {
                name: "public".to_string()
            }]
        );
    })
    .await;
}

#[tokio::test]
async fn test_fetch_composite_type() {
    with_test_db(async |db| {
        // Create a composite type
        db.execute(
            "CREATE TYPE address AS (
                street TEXT,
                city TEXT,
                postal_code VARCHAR(10),
                country TEXT
            )",
        )
        .await;

        let types = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(types.len(), 1);
        let type_ = &types[0];

        assert_eq!(type_.schema, "public");
        assert_eq!(type_.name, "address");
        assert_eq!(type_.kind, TypeKind::Composite);
        assert!(type_.enum_values.is_empty());
        assert_eq!(type_.composite_attributes.len(), 4);

        // Check composite attributes in order
        assert_eq!(type_.composite_attributes[0].name, "street");
        assert_eq!(type_.composite_attributes[0].type_name, "text");

        assert_eq!(type_.composite_attributes[1].name, "city");
        assert_eq!(type_.composite_attributes[1].type_name, "text");

        assert_eq!(type_.composite_attributes[2].name, "postal_code");
        assert_eq!(
            type_.composite_attributes[2].type_name,
            "character varying(10)"
        );

        assert_eq!(type_.composite_attributes[3].name, "country");
        assert_eq!(type_.composite_attributes[3].type_name, "text");

        // Check dependencies
        assert_eq!(
            type_.depends_on,
            vec![DbObjectId::Schema {
                name: "public".to_string()
            }]
        );
    })
    .await;
}

// Domain tests have been moved to tests/catalog/domains.rs

#[tokio::test]
async fn test_fetch_multiple_types_different_schemas() {
    with_test_db(async |db| {
        // Create schemas
        db.execute("CREATE SCHEMA app").await;
        db.execute("CREATE SCHEMA common").await;

        // Create types in different schemas
        db.execute("CREATE TYPE public.user_role AS ENUM ('admin', 'user', 'guest')")
            .await;

        db.execute("CREATE TYPE app.product_status AS ENUM ('draft', 'published', 'archived')")
            .await;

        db.execute(
            "CREATE TYPE common.coordinates AS (
                latitude DECIMAL(9,6),
                longitude DECIMAL(9,6)
            )",
        )
        .await;

        let mut types = fetch(&mut *db.conn().await).await.unwrap();
        types.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));

        assert_eq!(types.len(), 3);

        // Check app.product_status
        let app_type = types
            .iter()
            .find(|t| t.schema == "app" && t.name == "product_status")
            .unwrap();
        assert_eq!(app_type.kind, TypeKind::Enum);
        assert_eq!(app_type.enum_values.len(), 3);
        assert_eq!(
            app_type.depends_on,
            vec![DbObjectId::Schema {
                name: "app".to_string()
            }]
        );

        // Check common.coordinates
        let common_type = types
            .iter()
            .find(|t| t.schema == "common" && t.name == "coordinates")
            .unwrap();
        assert_eq!(common_type.kind, TypeKind::Composite);
        assert_eq!(common_type.composite_attributes.len(), 2);

        // Check public.user_role
        let public_type = types
            .iter()
            .find(|t| t.schema == "public" && t.name == "user_role")
            .unwrap();
        assert_eq!(public_type.kind, TypeKind::Enum);
        assert_eq!(public_type.enum_values.len(), 3);
    })
    .await;
}

#[tokio::test]
async fn test_type_id_and_dependencies() {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA test_schema").await;
        db.execute("CREATE TYPE test_schema.test_type AS ENUM ('value1', 'value2')")
            .await;

        let types = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(types.len(), 1);
        let type_ = &types[0];

        // Test type.id() method
        assert_eq!(
            type_.id(),
            DbObjectId::Type {
                schema: "test_schema".to_string(),
                name: "test_type".to_string()
            }
        );

        // Test type.depends_on() method
        let deps = type_.depends_on();
        assert_eq!(deps.len(), 1);
        assert_eq!(
            deps[0],
            DbObjectId::Schema {
                name: "test_schema".to_string()
            }
        );
    })
    .await;
}

#[tokio::test]
async fn test_enum_values_ordering() {
    with_test_db(async |db| {
        // Create enum with explicit ordering
        db.execute("CREATE TYPE priority AS ENUM ('low', 'medium', 'high', 'critical')")
            .await;

        let types = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(types.len(), 1);
        let type_ = &types[0];

        assert_eq!(type_.enum_values.len(), 4);

        // Check that values are in the order they were defined
        assert_eq!(type_.enum_values[0].name, "low");
        assert_eq!(type_.enum_values[1].name, "medium");
        assert_eq!(type_.enum_values[2].name, "high");
        assert_eq!(type_.enum_values[3].name, "critical");

        // Verify sort_order values are increasing
        assert!(type_.enum_values[0].sort_order < type_.enum_values[1].sort_order);
        assert!(type_.enum_values[1].sort_order < type_.enum_values[2].sort_order);
        assert!(type_.enum_values[2].sort_order < type_.enum_values[3].sort_order);
    })
    .await;
}

#[tokio::test]
async fn test_composite_attributes_ordering() {
    with_test_db(async |db| {
        // Create composite with specific field ordering
        db.execute(
            "CREATE TYPE person AS (
                z_last_name TEXT,
                a_first_name TEXT,
                middle_initial CHAR(1),
                birth_year INTEGER
            )",
        )
        .await;

        let types = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(types.len(), 1);
        let type_ = &types[0];

        assert_eq!(type_.composite_attributes.len(), 4);

        // Verify attributes are in definition order, not alphabetical
        assert_eq!(type_.composite_attributes[0].name, "z_last_name");

        assert_eq!(type_.composite_attributes[1].name, "a_first_name");

        assert_eq!(type_.composite_attributes[2].name, "middle_initial");

        assert_eq!(type_.composite_attributes[3].name, "birth_year");
    })
    .await;
}

#[tokio::test]
async fn test_exclude_table_row_types() {
    with_test_db(async |db| {
        // Create a table (which automatically creates a row type)
        db.execute(
            "CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL
            )",
        )
        .await;

        // Create a standalone custom type
        db.execute("CREATE TYPE status AS ENUM ('active', 'inactive')")
            .await;

        let types = fetch(&mut *db.conn().await).await.unwrap();

        // Should only find the standalone enum, not the table row type
        assert_eq!(types.len(), 1);
        assert_eq!(types[0].name, "status");
        assert_eq!(types[0].kind, TypeKind::Enum);
    })
    .await;
}

#[tokio::test]
async fn test_fetch_range_type() {
    with_test_db(async |db| {
        // Create a range type
        db.execute(
            "CREATE TYPE float_range AS RANGE (
                subtype = FLOAT8,
                subtype_diff = float8mi
            )",
        )
        .await;

        let types = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(types.len(), 1);
        let type_ = &types[0];

        assert_eq!(type_.schema, "public");
        assert_eq!(type_.name, "float_range");
        assert_eq!(type_.kind, TypeKind::Range);
        assert!(type_.enum_values.is_empty());
        assert!(type_.composite_attributes.is_empty());

        // Check dependencies
        assert_eq!(
            type_.depends_on,
            vec![DbObjectId::Schema {
                name: "public".to_string()
            }]
        );
    })
    .await;
}

#[tokio::test]
async fn test_fetch_enum_type_with_comment() {
    with_test_db(async |db| {
        // Create enum type with comment
        db.execute("CREATE TYPE priority AS ENUM ('low', 'medium', 'high')")
            .await;
        db.execute("COMMENT ON TYPE priority IS 'Task priority levels'")
            .await;

        let types = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(types.len(), 1);
        let type_ = &types[0];

        assert_eq!(type_.schema, "public");
        assert_eq!(type_.name, "priority");
        assert_eq!(type_.kind, TypeKind::Enum);
        assert_eq!(type_.comment, Some("Task priority levels".to_string()));
    })
    .await;
}

// test_fetch_domain_type_with_comment has been moved to tests/catalog/domains.rs

#[tokio::test]
async fn test_fetch_composite_type_with_comment() {
    with_test_db(async |db| {
        // Create composite type with comment
        db.execute(
            "CREATE TYPE address AS (
                street TEXT,
                city TEXT,
                state TEXT,
                zip_code TEXT
            )",
        )
        .await;
        db.execute("COMMENT ON TYPE address IS 'Postal address structure'")
            .await;

        let types = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(types.len(), 1);
        let type_ = &types[0];

        assert_eq!(type_.schema, "public");
        assert_eq!(type_.name, "address");
        assert_eq!(type_.kind, TypeKind::Composite);
        assert_eq!(type_.comment, Some("Postal address structure".to_string()));
    })
    .await;
}

#[tokio::test]
async fn test_composite_type_custom_dependency() {
    with_test_db(async |db| {
        // Create an enum type
        db.execute("CREATE TYPE custom_type AS ENUM ('value1', 'value2', 'value3')")
            .await;

        // Create a composite type that references the enum
        db.execute(
            "CREATE TYPE another_custom_type AS (
                field1 text,
                special_field custom_type
            )",
        )
        .await;

        let types = fetch(&mut *db.conn().await).await.unwrap();

        // Sort by name for predictable order
        let mut types = types;
        types.sort_by(|a, b| a.name.cmp(&b.name));

        assert_eq!(types.len(), 2);

        // Find the composite type (another_custom_type)
        let composite_type = types
            .iter()
            .find(|t| t.name == "another_custom_type")
            .expect("composite type should exist");

        assert_eq!(composite_type.kind, TypeKind::Composite);

        // Verify dependency on the custom enum
        assert!(
            composite_type.depends_on().contains(&DbObjectId::Type {
                schema: "public".to_string(),
                name: "custom_type".to_string()
            }),
            "Composite type should depend on the custom enum it references"
        );
    })
    .await;
}

#[tokio::test]
async fn test_composite_type_custom_array_dependency() {
    with_test_db(async |db| {
        // Create an enum type
        db.execute("CREATE TYPE priority AS ENUM ('low', 'medium', 'high')")
            .await;

        // Create a composite type with an array of the custom type
        db.execute(
            "CREATE TYPE task_info AS (
                id integer,
                priorities priority[]
            )",
        )
        .await;

        let types = fetch(&mut *db.conn().await).await.unwrap();

        // Sort by name for predictable order
        let mut types = types;
        types.sort_by(|a, b| a.name.cmp(&b.name));

        assert_eq!(types.len(), 2);

        // Find the composite type (task_info)
        let composite_type = types
            .iter()
            .find(|t| t.name == "task_info")
            .expect("composite type should exist");

        assert_eq!(composite_type.kind, TypeKind::Composite);

        // Verify dependency on the base custom enum (priority, not _priority)
        assert!(
            composite_type.depends_on().contains(&DbObjectId::Type {
                schema: "public".to_string(),
                name: "priority".to_string()
            }),
            "Composite type should depend on the base custom enum (priority, not _priority)"
        );

        // Verify we don't have the incorrect "_priority" dependency
        assert!(
            !composite_type.depends_on().contains(&DbObjectId::Type {
                schema: "public".to_string(),
                name: "_priority".to_string()
            }),
            "Should not depend on the internal array type name"
        );
    })
    .await;
}

#[tokio::test]
async fn test_composite_type_cross_schema_dependency() {
    with_test_db(async |db| {
        // Create schemas
        db.execute("CREATE SCHEMA app").await;
        db.execute("CREATE SCHEMA common").await;

        // Create an enum type in one schema
        db.execute("CREATE TYPE common.status AS ENUM ('active', 'inactive')")
            .await;

        // Create a composite type in another schema that references it
        db.execute(
            "CREATE TYPE app.user_info AS (
                name text,
                user_status common.status
            )",
        )
        .await;

        let types = fetch(&mut *db.conn().await).await.unwrap();

        // Sort by schema and name for predictable order
        let mut types = types;
        types.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));

        assert_eq!(types.len(), 2);

        // Find the composite type
        let composite_type = types
            .iter()
            .find(|t| t.schema == "app" && t.name == "user_info")
            .expect("composite type should exist");

        assert_eq!(composite_type.kind, TypeKind::Composite);

        // Should depend on the enum from the other schema
        assert!(
            composite_type.depends_on().contains(&DbObjectId::Type {
                schema: "common".to_string(),
                name: "status".to_string()
            }),
            "Composite type should depend on cross-schema custom type"
        );

        // Should also depend on its own schema
        assert!(
            composite_type.depends_on().contains(&DbObjectId::Schema {
                name: "app".to_string()
            }),
            "Composite type should depend on its own schema"
        );
    })
    .await;
}

#[tokio::test]
async fn test_composite_type_multilevel_dependency() {
    with_test_db(async |db| {
        // Create enum type (bottom level)
        db.execute("CREATE TYPE priority AS ENUM ('low', 'medium', 'high')")
            .await;

        // Create first composite type (middle level)
        db.execute(
            "CREATE TYPE task AS (
                name text,
                task_priority priority
            )",
        )
        .await;

        // Create second composite type (top level) that references the first composite
        db.execute(
            "CREATE TYPE project AS (
                title text,
                main_task task
            )",
        )
        .await;

        let types = fetch(&mut *db.conn().await).await.unwrap();

        // Sort by name for predictable order
        let mut types = types;
        types.sort_by(|a, b| a.name.cmp(&b.name));

        assert_eq!(types.len(), 3);

        // Find the types
        let priority_type = types.iter().find(|t| t.name == "priority").unwrap();
        let task_type = types.iter().find(|t| t.name == "task").unwrap();
        let project_type = types.iter().find(|t| t.name == "project").unwrap();

        // Verify dependency chain: project -> task -> priority

        // task should depend on priority
        assert!(
            task_type.depends_on().contains(&DbObjectId::Type {
                schema: "public".to_string(),
                name: "priority".to_string()
            }),
            "task should depend on priority enum"
        );

        // project should depend on task
        assert!(
            project_type.depends_on().contains(&DbObjectId::Type {
                schema: "public".to_string(),
                name: "task".to_string()
            }),
            "project should depend on task composite"
        );

        // priority should not depend on anything custom (just schema)
        assert_eq!(priority_type.depends_on().len(), 1);
        assert!(priority_type.depends_on().contains(&DbObjectId::Schema {
            name: "public".to_string()
        }));
    })
    .await;
}
