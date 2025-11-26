use crate::helpers::harness::with_test_db;
use crate::helpers::migration::MigrationTestHelper;

use anyhow::Result;

use pgmt::catalog::custom_type::{TypeKind, fetch};
use pgmt::diff::custom_types::diff;
use pgmt::diff::operations::{CommentOperation, MigrationStep, SqlRenderer, TypeOperation};

#[tokio::test]
async fn test_create_enum_migration() -> Result<()> {
    with_test_db(async |source_db| {
        with_test_db(async |target_db| {
            // Create enum in source database
            source_db
                .execute("CREATE TYPE status AS ENUM ('active', 'inactive', 'pending')")
                .await;

            // Get types from source database
            let source_types = fetch(&mut *source_db.conn().await).await?;

            // Generate diff
            let steps = diff(None, source_types.first());
            assert_eq!(steps.len(), 1);

            // Verify the migration step
            match &steps[0] {
                MigrationStep::Type(TypeOperation::Create {
                    schema,
                    name,
                    kind,
                    definition,
                }) => {
                    assert_eq!(schema, "public");
                    assert_eq!(name, "status");
                    assert_eq!(kind, "ENUM");
                    assert_eq!(definition, "('active', 'inactive', 'pending')");
                }
                _ => panic!("Expected Type(Create) step"),
            }

            // Apply migration to target
            let sql_statements = steps[0].to_sql();
            assert_eq!(sql_statements.len(), 1);

            target_db.execute(&sql_statements[0].sql).await;

            // Verify final state
            let final_types = fetch(&mut *target_db.conn().await).await?;
            assert_eq!(final_types.len(), 1);

            let created_type = &final_types[0];
            assert_eq!(created_type.schema, "public");
            assert_eq!(created_type.name, "status");
            assert_eq!(created_type.kind, TypeKind::Enum);
            assert_eq!(created_type.enum_values.len(), 3);
            assert_eq!(created_type.enum_values[0].name, "active");
            assert_eq!(created_type.enum_values[1].name, "inactive");
            assert_eq!(created_type.enum_values[2].name, "pending");

            Ok(())
        })
        .await
    })
    .await
}

#[tokio::test]
async fn test_create_composite_migration() -> Result<()> {
    with_test_db(async |source_db| {
        with_test_db(async |target_db| {
            // Create composite type in source database
            source_db
                .execute(
                    "CREATE TYPE address AS (
                        street TEXT,
                        city TEXT,
                        postal_code VARCHAR(10)
                    )",
                )
                .await;

            // Generate and apply migration
            let source_types = fetch(&mut *source_db.conn().await).await?;
            let steps = diff(None, source_types.first());

            assert_eq!(steps.len(), 1);
            match &steps[0] {
                MigrationStep::Type(TypeOperation::Create {
                    schema,
                    name,
                    kind,
                    definition,
                }) => {
                    assert_eq!(schema, "public");
                    assert_eq!(name, "address");
                    assert_eq!(kind, "COMPOSITE");
                    assert_eq!(
                        definition,
                        "(street text, city text, postal_code character varying(10))"
                    );
                }
                _ => panic!("Expected CreateType step"),
            }

            let sql_statements = steps[0].to_sql();
            target_db.execute(&sql_statements[0].sql).await;

            // Verify final state
            let final_types = fetch(&mut *target_db.conn().await).await?;
            assert_eq!(final_types.len(), 1);

            let created_type = &final_types[0];
            assert_eq!(created_type.kind, TypeKind::Composite);
            assert_eq!(created_type.composite_attributes.len(), 3);
            assert_eq!(created_type.composite_attributes[0].name, "street");
            assert_eq!(created_type.composite_attributes[1].name, "city");
            assert_eq!(created_type.composite_attributes[2].name, "postal_code");

            Ok(())
        })
        .await
    })
    .await
}

#[tokio::test]
async fn test_drop_type_migration() -> Result<()> {
    with_test_db(async |target_db| {
        // Create type in target database that we'll drop
        target_db
            .execute("CREATE TYPE status AS ENUM ('active', 'inactive')")
            .await;

        // Verify type exists
        let target_types = fetch(&mut *target_db.conn().await).await?;
        assert_eq!(target_types.len(), 1);

        // Generate drop migration (source has no types, target has one)
        let steps = diff(target_types.first(), None);

        assert_eq!(steps.len(), 1);
        match &steps[0] {
            MigrationStep::Type(TypeOperation::Drop { schema, name }) => {
                assert_eq!(schema, "public");
                assert_eq!(name, "status");
            }
            _ => panic!("Expected DropType step"),
        }

        // Apply migration
        let sql_statements = steps[0].to_sql();
        target_db.execute(&sql_statements[0].sql).await;

        // Verify type was dropped
        let final_types = fetch(&mut *target_db.conn().await).await?;
        assert_eq!(final_types.len(), 0);

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_add_enum_values_migration() -> Result<()> {
    with_test_db(async |source_db| {
        with_test_db(async |target_db| {
            // Create base enum in both databases
            let base_enum_sql = "CREATE TYPE priority AS ENUM ('low', 'medium', 'high')";
            source_db.execute(base_enum_sql).await;
            target_db.execute(base_enum_sql).await;

            // Add new values to source
            source_db
                .execute("ALTER TYPE priority ADD VALUE 'critical' AFTER 'high'")
                .await;
            source_db
                .execute("ALTER TYPE priority ADD VALUE 'urgent' AFTER 'critical'")
                .await;

            // Generate migration
            let source_types = fetch(&mut *source_db.conn().await).await?;
            let target_types = fetch(&mut *target_db.conn().await).await?;

            let steps = diff(target_types.first(), source_types.first());
            assert_eq!(steps.len(), 2); // Now generates separate steps for each value

            // Check first ADD VALUE step
            match &steps[0] {
                MigrationStep::Type(TypeOperation::Alter {
                    schema,
                    name,
                    action,
                    definition,
                }) => {
                    assert_eq!(schema, "public");
                    assert_eq!(name, "priority");
                    assert_eq!(action, "ADD VALUE");
                    assert_eq!(definition, "'critical' AFTER 'high'");
                }
                _ => panic!("Expected AlterType step for first value"),
            }

            // Check second ADD VALUE step
            match &steps[1] {
                MigrationStep::Type(TypeOperation::Alter {
                    schema,
                    name,
                    action,
                    definition,
                }) => {
                    assert_eq!(schema, "public");
                    assert_eq!(name, "priority");
                    assert_eq!(action, "ADD VALUE");
                    assert_eq!(definition, "'urgent' AFTER 'critical'");
                }
                _ => panic!("Expected AlterType step for second value"),
            }

            // Apply migration - all steps
            for step in &steps {
                let sql_statements = step.to_sql();
                for stmt in sql_statements {
                    target_db.execute(&stmt.sql).await;
                }
            }

            // Verify final state
            let final_types = fetch(&mut *target_db.conn().await).await?;
            assert_eq!(final_types.len(), 1);

            let updated_type = &final_types[0];
            assert_eq!(updated_type.enum_values.len(), 5);

            let value_names: Vec<&str> = updated_type
                .enum_values
                .iter()
                .map(|v| v.name.as_str())
                .collect();
            assert!(value_names.contains(&"low"));
            assert!(value_names.contains(&"medium"));
            assert!(value_names.contains(&"high"));
            assert!(value_names.contains(&"critical"));
            assert!(value_names.contains(&"urgent"));

            Ok(())
        })
        .await
    })
    .await
}

#[tokio::test]
async fn test_enum_drop_and_recreate_when_values_removed() -> Result<()> {
    with_test_db(async |source_db| {
        with_test_db(async |target_db| {
            // Create enum with more values in target
            target_db
                .execute("CREATE TYPE status AS ENUM ('draft', 'active', 'inactive', 'archived')")
                .await;

            // Create enum with fewer values in source (values removed)
            source_db
                .execute("CREATE TYPE status AS ENUM ('active', 'inactive')")
                .await;

            // Generate migration
            let source_types = fetch(&mut *source_db.conn().await).await?;
            let target_types = fetch(&mut *target_db.conn().await).await?;

            let steps = diff(target_types.first(), source_types.first());

            // Should be drop + recreate since values were removed
            assert_eq!(steps.len(), 2);

            match &steps[0] {
                MigrationStep::Type(TypeOperation::Drop { schema, name }) => {
                    assert_eq!(schema, "public");
                    assert_eq!(name, "status");
                }
                _ => panic!("Expected DropType step first"),
            }

            match &steps[1] {
                MigrationStep::Type(TypeOperation::Create {
                    schema,
                    name,
                    kind,
                    definition,
                }) => {
                    assert_eq!(schema, "public");
                    assert_eq!(name, "status");
                    assert_eq!(kind, "ENUM");
                    assert_eq!(definition, "('active', 'inactive')");
                }
                _ => panic!("Expected CreateType step second"),
            }

            // Apply migration
            for step in &steps {
                let sql_statements = step.to_sql();
                for stmt in sql_statements {
                    target_db.execute(&stmt.sql).await;
                }
            }

            // Verify final state
            let final_types = fetch(&mut *target_db.conn().await).await?;
            assert_eq!(final_types.len(), 1);

            let recreated_type = &final_types[0];
            assert_eq!(recreated_type.enum_values.len(), 2);
            assert_eq!(recreated_type.enum_values[0].name, "active");
            assert_eq!(recreated_type.enum_values[1].name, "inactive");

            Ok(())
        })
        .await
    })
    .await
}

#[tokio::test]
async fn test_composite_attributes_change_drop_recreate() -> Result<()> {
    with_test_db(async |source_db| {
        with_test_db(async |target_db| {
            // Create composite with 3 attributes in target
            target_db
                .execute(
                    "CREATE TYPE person AS (
                        first_name TEXT,
                        last_name TEXT,
                        age INTEGER
                    )",
                )
                .await;

            // Create composite with different attributes in source
            source_db
                .execute(
                    "CREATE TYPE person AS (
                        full_name TEXT,
                        birth_year INTEGER,
                        email TEXT
                    )",
                )
                .await;

            // Generate migration
            let source_types = fetch(&mut *source_db.conn().await).await?;
            let target_types = fetch(&mut *target_db.conn().await).await?;

            let steps = diff(target_types.first(), source_types.first());

            // Should be drop + recreate since attributes changed
            assert_eq!(steps.len(), 2);

            match &steps[0] {
                MigrationStep::Type(TypeOperation::Drop { .. }) => {}
                _ => panic!("Expected DropType step first"),
            }

            match &steps[1] {
                MigrationStep::Type(TypeOperation::Create {
                    schema,
                    name,
                    kind,
                    definition,
                }) => {
                    assert_eq!(schema, "public");
                    assert_eq!(name, "person");
                    assert_eq!(kind, "COMPOSITE");
                    assert_eq!(
                        definition,
                        "(full_name text, birth_year integer, email text)"
                    );
                }
                _ => panic!("Expected CreateType step second"),
            }

            // Apply migration
            for step in &steps {
                let sql_statements = step.to_sql();
                for stmt in sql_statements {
                    target_db.execute(&stmt.sql).await;
                }
            }

            // Verify final state
            let final_types = fetch(&mut *target_db.conn().await).await?;
            assert_eq!(final_types.len(), 1);

            let recreated_type = &final_types[0];
            assert_eq!(recreated_type.composite_attributes.len(), 3);
            assert_eq!(recreated_type.composite_attributes[0].name, "full_name");
            assert_eq!(recreated_type.composite_attributes[1].name, "birth_year");
            assert_eq!(recreated_type.composite_attributes[2].name, "email");

            Ok(())
        })
        .await
    })
    .await
}

#[tokio::test]
async fn test_multiple_schema_type_migrations() -> Result<()> {
    with_test_db(async |source_db| {
        with_test_db(async |target_db| {
            // Set up schemas
            for db in [&source_db, &target_db] {
                db.execute("CREATE SCHEMA app").await;
                db.execute("CREATE SCHEMA common").await;
            }

            // Create types in source across multiple schemas
            source_db
                .execute("CREATE TYPE public.user_status AS ENUM ('active', 'suspended')")
                .await;
            source_db
                .execute("CREATE TYPE app.product_type AS ENUM ('digital', 'physical')")
                .await;
            source_db
                .execute("CREATE TYPE common.coordinates AS (lat DECIMAL, lng DECIMAL)")
                .await;

            // Target has no types, so this should create all three
            let source_types = fetch(&mut *source_db.conn().await).await?;

            // Generate migrations for each type
            let mut all_steps = Vec::new();
            for source_type in &source_types {
                let steps = diff(None, Some(source_type));
                all_steps.extend(steps);
            }

            assert_eq!(all_steps.len(), 3);

            // Apply all migrations
            for step in &all_steps {
                let sql_statements = step.to_sql();
                for stmt in sql_statements {
                    target_db.execute(&stmt.sql).await;
                }
            }

            // Verify final state
            let final_types = fetch(&mut *target_db.conn().await).await?;
            assert_eq!(final_types.len(), 3);

            // Check that we have types in all schemas
            let schemas: std::collections::HashSet<&str> =
                final_types.iter().map(|t| t.schema.as_str()).collect();
            assert!(schemas.contains("public"));
            assert!(schemas.contains("app"));
            assert!(schemas.contains("common"));

            Ok(())
        })
        .await
    })
    .await
}

#[tokio::test]
async fn test_type_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: schema and enum type
        &[
            "CREATE SCHEMA test_schema",
            "CREATE TYPE test_schema.priority AS ENUM ('low', 'medium', 'high')"
        ],
        // Initial DB only: nothing extra (no comment)
        &[],
        // Target DB only: add comment
        &["COMMENT ON TYPE test_schema.priority IS 'Priority levels for tasks'"],
        // Verification closure
        |steps, final_catalog| {
            // Should have SET TYPE COMMENT step
            assert!(!steps.is_empty());

            let comment_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::Type(TypeOperation::Comment(CommentOperation::Set { target, comment }))
                    if target.schema == "test_schema" && target.name == "priority" && comment == "Priority levels for tasks")
            }).expect("Should have SetTypeComment step");

            match comment_step {
                MigrationStep::Type(TypeOperation::Comment(CommentOperation::Set { target, comment })) => {
                    assert_eq!(target.schema, "test_schema");
                    assert_eq!(target.name, "priority");
                    assert_eq!(comment, "Priority levels for tasks");
                }
                _ => panic!("Expected SetTypeComment step"),
            }

            // Verify final state
            assert_eq!(final_catalog.types.len(), 1);

            let commented_type = &final_catalog.types[0];
            assert_eq!(commented_type.schema, "test_schema");
            assert_eq!(commented_type.name, "priority");
            assert_eq!(commented_type.comment, Some("Priority levels for tasks".to_string()));

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_drop_type_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: schema and enum type
        &[
            "CREATE SCHEMA test_schema",
            "CREATE TYPE test_schema.priority AS ENUM ('low', 'medium', 'high')"
        ],
        // Initial DB only: has comment
        &["COMMENT ON TYPE test_schema.priority IS 'Priority levels for tasks'"],
        // Target DB only: nothing extra (no comment)
        &[],
        // Verification closure
        |steps, final_catalog| {
            // Should have DROP TYPE COMMENT step
            assert!(!steps.is_empty());

            let comment_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::Type(TypeOperation::Comment(CommentOperation::Drop { target }))
                    if target.schema == "test_schema" && target.name == "priority")
            }).expect("Should have DropTypeComment step");

            match comment_step {
                MigrationStep::Type(TypeOperation::Comment(CommentOperation::Drop { target })) => {
                    assert_eq!(target.schema, "test_schema");
                    assert_eq!(target.name, "priority");
                }
                _ => panic!("Expected DropTypeComment step"),
            }

            // Verify final state
            assert_eq!(final_catalog.types.len(), 1);

            let uncommented_type = &final_catalog.types[0];
            assert_eq!(uncommented_type.schema, "test_schema");
            assert_eq!(uncommented_type.name, "priority");
            assert_eq!(uncommented_type.comment, None);

            Ok(())
        }
    ).await?;

    Ok(())
}
