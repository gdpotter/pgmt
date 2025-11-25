use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::diff::operations::{
    CommentOperation, FunctionOperation, MigrationStep, SchemaOperation, TableOperation,
    TypeOperation,
};

#[tokio::test]
async fn test_create_function_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: schema
        &["CREATE SCHEMA test_schema"],
        // Initial DB only: nothing extra (no function)
        &[],
        // Target DB only: add function
        &["CREATE OR REPLACE FUNCTION test_schema.add_numbers(a INTEGER, b INTEGER) RETURNS INTEGER AS $$ BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql"],
        // Verification closure
        |steps, final_catalog| {
            // Should have CREATE FUNCTION step
            assert!(!steps.is_empty());
            let create_step = steps
                .iter()
                .find(|s| {
                    matches!(s, MigrationStep::Function(FunctionOperation::Create { schema, name, .. })
                    if schema == "test_schema" && name == "add_numbers")
                })
                .expect("Should have CreateFunction step");

            match create_step {
                MigrationStep::Function(FunctionOperation::Create {
                    schema,
                    name,
                    definition,
                    ..
                }) => {
                    assert_eq!(schema, "test_schema");
                    assert_eq!(name, "add_numbers");
                    assert!(definition.contains("integer")); // PostgreSQL normalizes to lowercase
                    assert!(definition.contains("plpgsql"));
                }
                _ => panic!("Expected CreateFunction step"),
            }

            // Verify final state exactly
            assert_eq!(final_catalog.functions.len(), 1);

            let function = &final_catalog.functions[0];
            assert_eq!(function.schema, "test_schema");
            assert_eq!(function.name, "add_numbers");
            assert!(function.definition.contains("RETURNS integer"));

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_drop_function_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: schema
        &["CREATE SCHEMA test_schema"],
        // Initial DB only: has function
        &["CREATE OR REPLACE FUNCTION test_schema.multiply_numbers(a INTEGER, b INTEGER) RETURNS INTEGER AS $$ BEGIN RETURN a * b; END; $$ LANGUAGE plpgsql"],
        // Target DB only: nothing extra (no function)
        &[],
        // Verification closure
        |steps, final_catalog| {
            // Should have DROP FUNCTION step
            assert!(!steps.is_empty());
            let drop_step = steps
                .iter()
                .find(|s| {
                    matches!(s, MigrationStep::Function(FunctionOperation::Drop { schema, name, .. })
                    if schema == "test_schema" && name == "multiply_numbers")
                })
                .expect("Should have DropFunction step");

            match drop_step {
                MigrationStep::Function(FunctionOperation::Drop {
                    schema,
                    name,
                    parameter_types,
                    ..
                }) => {
                    assert_eq!(schema, "test_schema");
                    assert_eq!(name, "multiply_numbers");
                    assert!(parameter_types.contains("integer"));
                }
                _ => panic!("Expected DropFunction step"),
            }

            // Verify final state exactly
            assert!(final_catalog.functions.is_empty());

            // Verify schema still exists but function is gone
            assert!(!final_catalog.schemas.is_empty());
            let test_schema = final_catalog
                .schemas
                .iter()
                .find(|s| s.name == "test_schema")
                .expect("Should have test_schema");
            assert_eq!(test_schema.name, "test_schema");

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_replace_function_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: schema
        &["CREATE SCHEMA test_schema"],
        // Initial DB only: function with one implementation
        &["CREATE OR REPLACE FUNCTION test_schema.calculate(x INTEGER) RETURNS INTEGER AS $$ BEGIN RETURN x * 2; END; $$ LANGUAGE plpgsql"],
        // Target DB only: function with different implementation
        &["CREATE OR REPLACE FUNCTION test_schema.calculate(x INTEGER) RETURNS INTEGER AS $$ BEGIN RETURN x * 3; END; $$ LANGUAGE plpgsql"],
        // Verification closure
        |steps, final_catalog| {
            // Should have REPLACE FUNCTION step
            assert!(!steps.is_empty());
            let replace_step = steps
                .iter()
                .find(|s| {
                    matches!(s, MigrationStep::Function(FunctionOperation::Replace { schema, name, .. })
                    if schema == "test_schema" && name == "calculate")
                })
                .expect("Should have ReplaceFunction step");

            match replace_step {
                MigrationStep::Function(FunctionOperation::Replace {
                    schema,
                    name,
                    definition,
                    ..
                }) => {
                    assert_eq!(schema, "test_schema");
                    assert_eq!(name, "calculate");
                    assert!(definition.contains("x * 3"));
                }
                _ => panic!("Expected ReplaceFunction step"),
            }

            // Verify final state exactly
            assert_eq!(final_catalog.functions.len(), 1);

            let function = &final_catalog.functions[0];
            assert_eq!(function.schema, "test_schema");
            assert_eq!(function.name, "calculate");
            assert!(function.definition.contains("x * 3"));

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_function_dependency_on_custom_type() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: nothing (empty initial state)
        &[],
        // Initial DB only: nothing extra
        &[],
        // Target DB only: custom type and function that uses it
        &[
            "CREATE SCHEMA test_schema",
            "CREATE TYPE test_schema.priority AS ENUM ('low', 'medium', 'high')",
            "CREATE OR REPLACE FUNCTION test_schema.get_priority_level(p test_schema.priority) RETURNS TEXT AS $$ BEGIN RETURN p::text; END; $$ LANGUAGE plpgsql"
        ],
        // Verification closure
        |steps, final_catalog| {
            // Should have both CREATE SCHEMA, CREATE TYPE and CREATE FUNCTION steps
            let create_schema = steps.iter().any(|s| {
                matches!(s, MigrationStep::Schema(SchemaOperation::Create { name })
                    if name == "test_schema")
            });
            assert!(create_schema);

            let create_type = steps.iter().any(|s| {
                matches!(s, MigrationStep::Type(TypeOperation::Create { schema, name, .. })
                    if schema == "test_schema" && name == "priority")
            });
            assert!(create_type);

            let create_function = steps.iter().any(|s| {
                matches!(s, MigrationStep::Function(FunctionOperation::Create { schema, name, .. })
                    if schema == "test_schema" && name == "get_priority_level")
            });
            assert!(create_function);

            // Verify ordering: schema -> type -> function
            let create_schema_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::Schema(SchemaOperation::Create { name })
                    if name == "test_schema")
                })
                .expect("Should have CreateSchema step");

            let create_type_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::Type(TypeOperation::Create { schema, name, .. })
                    if schema == "test_schema" && name == "priority")
                })
                .expect("Should have CreateType step");

            let create_function_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::Function(FunctionOperation::Create { schema, name, .. })
                    if schema == "test_schema" && name == "get_priority_level")
                })
                .expect("Should have CreateFunction step");

            assert!(
                create_schema_pos < create_type_pos,
                "Schema should be created before type"
            );
            assert!(
                create_type_pos < create_function_pos,
                "Type should be created before function"
            );

            // Verify final state exactly
            assert_eq!(final_catalog.types.len(), 1);
            assert_eq!(final_catalog.functions.len(), 1);

            let function = &final_catalog.functions[0];
            assert_eq!(function.schema, "test_schema");
            assert_eq!(function.name, "get_priority_level");

            let custom_type = &final_catalog.types[0];
            assert_eq!(custom_type.schema, "test_schema");
            assert_eq!(custom_type.name, "priority");

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_function_dependency_on_table() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: nothing (empty initial state)
        &[],
        // Initial DB only: nothing extra
        &[],
        // Target DB only: table and function that operates on it
        &[
            "CREATE SCHEMA test_schema",
            "CREATE TABLE test_schema.users (id INTEGER, name TEXT, active BOOLEAN)",
            "CREATE OR REPLACE FUNCTION test_schema.count_active_users() RETURNS INTEGER AS $$ BEGIN RETURN (SELECT COUNT(*) FROM test_schema.users WHERE active = true); END; $$ LANGUAGE plpgsql"
        ],
        // Verification closure
        |steps, final_catalog| {
            // Should have both CREATE TABLE and CREATE FUNCTION steps
            let create_table = steps.iter().any(|s| {
                matches!(s, MigrationStep::Table(TableOperation::Create { schema, name, .. })
                    if schema == "test_schema" && name == "users")
            });
            assert!(create_table);

            let create_function = steps.iter().any(|s| {
                matches!(s, MigrationStep::Function(FunctionOperation::Create { schema, name, .. })
            if schema == "test_schema" && name == "count_active_users")
            });
            assert!(create_function);

            // NOTE: Functions only track signature-level dependencies (parameter/return types)
            // PostgreSQL does NOT record table/view references in pg_depend for function bodies.
            // This is a fundamental PostgreSQL limitation that affects all procedural languages.
            // Workaround: Use file-based dependencies via `-- require:` comments in schema files.
            // For now, just verify that both steps exist

            // Verify final state exactly
            assert_eq!(final_catalog.tables.len(), 1);
            assert_eq!(final_catalog.functions.len(), 1);

            let function = &final_catalog.functions[0];
            assert_eq!(function.schema, "test_schema");
            assert_eq!(function.name, "count_active_users");

            let table = &final_catalog.tables[0];
            assert_eq!(table.schema, "test_schema");
            assert_eq!(table.name, "users");

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_cross_schema_function_dependencies() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: nothing (empty initial state)
        &[],
        // Initial DB only: nothing extra
        &[],
        // Target DB only: table in one schema, function in another that references it
        &[
            "CREATE SCHEMA data_schema",
            "CREATE SCHEMA api_schema",
            "CREATE TABLE data_schema.products (id INTEGER, name TEXT, price DECIMAL)",
            "CREATE OR REPLACE FUNCTION api_schema.get_product_count() RETURNS INTEGER AS $$ BEGIN RETURN (SELECT COUNT(*) FROM data_schema.products); END; $$ LANGUAGE plpgsql"
        ],
        // Verification closure
        |steps, final_catalog| {
            // Should have CREATE SCHEMA, CREATE TABLE and CREATE FUNCTION steps
            let create_data_schema = steps.iter().any(|s| {
                matches!(s, MigrationStep::Schema(SchemaOperation::Create { name })
            if name == "data_schema")
            });
            assert!(create_data_schema);

            let create_api_schema = steps.iter().any(|s| {
                matches!(s, MigrationStep::Schema(SchemaOperation::Create { name })
            if name == "api_schema")
            });
            assert!(create_api_schema);

            let create_table = steps.iter().any(|s| {
                matches!(s, MigrationStep::Table(TableOperation::Create { schema, name, .. })
            if schema == "data_schema" && name == "products")
            });
            assert!(create_table);

            let create_function = steps.iter().any(|s| {
                matches!(s, MigrationStep::Function(FunctionOperation::Create { schema, name, .. })
            if schema == "api_schema" && name == "get_product_count")
            });
            assert!(create_function);

            // Verify ordering: schemas -> table -> function
            let create_data_schema_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::Schema(SchemaOperation::Create { name })
            if name == "data_schema")
                })
                .expect("Should have CreateSchema step for data_schema");

            let create_api_schema_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::Schema(SchemaOperation::Create { name })
            if name == "api_schema")
                })
                .expect("Should have CreateSchema step for api_schema");

            let create_table_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::Table(TableOperation::Create { schema, name, .. })
            if schema == "data_schema" && name == "products")
                })
                .expect("Should have CreateTable step");

            let create_function_pos = steps
                .iter()
                .position(|s| {
                    matches!(s, MigrationStep::Function(FunctionOperation::Create { schema, name, .. })
            if schema == "api_schema" && name == "get_product_count")
                })
                .expect("Should have CreateFunction step");

            assert!(
                create_data_schema_pos < create_table_pos,
                "Data schema should be created before table"
            );
            assert!(
                create_api_schema_pos < create_function_pos,
                "API schema should be created before function"
            );
            assert!(
                create_table_pos < create_function_pos,
                "Table should be created before function that references it"
            );

            // Verify final state exactly
            assert!(final_catalog.schemas.len() >= 2);
            assert_eq!(final_catalog.tables.len(), 1);
            assert_eq!(final_catalog.functions.len(), 1);

            // Verify both schemas exist
            let data_schema = final_catalog
                .schemas
                .iter()
                .find(|s| s.name == "data_schema")
                .expect("Should have data_schema");
            assert_eq!(data_schema.name, "data_schema");

            let api_schema = final_catalog
                .schemas
                .iter()
                .find(|s| s.name == "api_schema")
                .expect("Should have api_schema");
            assert_eq!(api_schema.name, "api_schema");

            // Verify table and function
            let table = &final_catalog.tables[0];
            assert_eq!(table.schema, "data_schema");
            assert_eq!(table.name, "products");

            let function = &final_catalog.functions[0];
            assert_eq!(function.schema, "api_schema");
            assert_eq!(function.name, "get_product_count");

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_function_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: schema and function
        &[
            "CREATE SCHEMA test_schema",
            "CREATE OR REPLACE FUNCTION test_schema.calculate_total(price DECIMAL, tax_rate DECIMAL) RETURNS DECIMAL AS $$ BEGIN RETURN price * (1 + tax_rate); END; $$ LANGUAGE plpgsql"
        ],
        // Initial DB only: nothing extra (no comment)
        &[],
        // Target DB only: add comment
        &["COMMENT ON FUNCTION test_schema.calculate_total(DECIMAL, DECIMAL) IS 'Calculates total price including tax'"],
        // Verification closure
        |steps, final_catalog| {
            // Should have SET FUNCTION COMMENT step
            assert!(!steps.is_empty());
            let comment_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::Function(FunctionOperation::Comment(CommentOperation::Set { target, comment }))
                    if target.schema == "test_schema" && target.name == "calculate_total" && comment == "Calculates total price including tax")
            }).expect("Should have SetFunctionComment step");

            match comment_step {
                MigrationStep::Function(FunctionOperation::Comment(CommentOperation::Set { target, comment })) => {
                    assert_eq!(target.schema, "test_schema");
                    assert_eq!(target.name, "calculate_total");
                    assert_eq!(comment, "Calculates total price including tax");
                }
                _ => panic!("Expected SetFunctionComment step"),
            }

            // Verify final state
            assert_eq!(final_catalog.functions.len(), 1);

            let commented_function = &final_catalog.functions[0];
            assert_eq!(commented_function.schema, "test_schema");
            assert_eq!(commented_function.name, "calculate_total");
            assert_eq!(commented_function.comment, Some("Calculates total price including tax".to_string()));

            Ok(())
        }
    ).await?;

    Ok(())
}

#[tokio::test]
async fn test_drop_function_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Both DBs: schema and function
        &[
            "CREATE SCHEMA test_schema",
            "CREATE OR REPLACE FUNCTION test_schema.calculate_total(price DECIMAL, tax_rate DECIMAL) RETURNS DECIMAL AS $$ BEGIN RETURN price * (1 + tax_rate); END; $$ LANGUAGE plpgsql"
        ],
        // Initial DB only: has comment
        &["COMMENT ON FUNCTION test_schema.calculate_total(DECIMAL, DECIMAL) IS 'Calculates total price including tax'"],
        // Target DB only: nothing extra (no comment)
        &[],
        // Verification closure
        |steps, final_catalog| {
            // Should have DROP FUNCTION COMMENT step
            assert!(!steps.is_empty());
            let comment_step = steps.iter().find(|s| {
                matches!(s, MigrationStep::Function(FunctionOperation::Comment(CommentOperation::Drop { target }))
                    if target.schema == "test_schema" && target.name == "calculate_total")
            }).expect("Should have DropFunctionComment step");

            match comment_step {
                MigrationStep::Function(FunctionOperation::Comment(CommentOperation::Drop { target })) => {
                    assert_eq!(target.schema, "test_schema");
                    assert_eq!(target.name, "calculate_total");
                }
                _ => panic!("Expected DropFunctionComment step"),
            }

            // Verify final state
            assert_eq!(final_catalog.functions.len(), 1);

            let uncommented_function = &final_catalog.functions[0];
            assert_eq!(uncommented_function.schema, "test_schema");
            assert_eq!(uncommented_function.name, "calculate_total");
            assert_eq!(uncommented_function.comment, None);

            Ok(())
        }
    ).await?;

    Ok(())
}
