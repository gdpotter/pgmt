use crate::helpers::harness::with_test_db;

use pgmt::catalog::function::{FunctionKind, fetch};
use pgmt::catalog::id::{DbObjectId, DependsOn};

#[tokio::test]
async fn test_fetch_basic_function() {
    with_test_db(async |db| {
        db.execute(
            "CREATE OR REPLACE FUNCTION add_numbers(a INTEGER, b INTEGER)
             RETURNS INTEGER AS $$
             BEGIN
                 RETURN a + b;
             END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        let functions = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(functions.len(), 1);
        let func = &functions[0];

        assert_eq!(func.schema, "public");
        assert_eq!(func.name, "add_numbers");
        assert_eq!(func.kind, FunctionKind::Function);
        assert_eq!(func.parameters.len(), 2);
        assert_eq!(func.return_type, Some("integer".to_string()));
        assert_eq!(func.language, "plpgsql");
        assert_eq!(func.volatility, "VOLATILE");
        assert!(!func.is_strict);
        assert_eq!(func.security_type, "INVOKER");

        assert_eq!(func.parameters[0].name, Some("a".to_string()));
        assert_eq!(func.parameters[0].data_type, "integer");
        assert_eq!(func.parameters[0].mode, None);

        assert_eq!(func.parameters[1].name, Some("b".to_string()));
        assert_eq!(func.parameters[1].data_type, "integer");
        assert_eq!(func.parameters[1].mode, None);

        assert_eq!(
            func.depends_on,
            vec![DbObjectId::Schema {
                name: "public".to_string()
            }]
        );
    })
    .await;
}

#[tokio::test]
async fn test_fetch_function_with_custom_types() {
    with_test_db(async |db| {
        db.execute("CREATE TYPE status AS ENUM ('active', 'inactive')")
            .await;

        db.execute(
            "CREATE OR REPLACE FUNCTION get_status_message(user_status public.status)
             RETURNS TEXT AS $$
             BEGIN
                 CASE user_status
                     WHEN 'active' THEN RETURN 'User is active';
                     WHEN 'inactive' THEN RETURN 'User is inactive';
                 END CASE;
             END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        let functions = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(functions.len(), 1);
        let func = &functions[0];

        assert_eq!(func.schema, "public");
        assert_eq!(func.name, "get_status_message");
        assert_eq!(func.parameters.len(), 1);
        assert_eq!(func.parameters[0].data_type, "public.status");

        assert_eq!(func.depends_on().len(), 2);
        assert!(func.depends_on().contains(&DbObjectId::Schema {
            name: "public".to_string()
        }));
        assert!(func.depends_on().contains(&DbObjectId::Type {
            schema: "public".to_string(),
            name: "status".to_string()
        }));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_function_with_extension_type_dependency() -> anyhow::Result<()> {
    with_test_db(async |db| {
        // Create citext extension
        db.execute("CREATE EXTENSION citext").await;

        // Create function that uses citext type
        db.execute(
            "CREATE FUNCTION normalize_email(email citext) RETURNS citext AS $$
                SELECT lower(email)
            $$ LANGUAGE SQL",
        )
        .await;

        let functions = fetch(&mut *db.conn().await).await.unwrap();
        let func = functions
            .iter()
            .find(|f| f.name == "normalize_email")
            .expect("function should exist");

        // Should depend on Extension, NOT Type
        let deps = func.depends_on();
        assert!(
            deps.contains(&DbObjectId::Extension {
                name: "citext".to_string(),
            }),
            "Function should depend on the extension providing the citext type"
        );
        // Should NOT contain Type variant for citext
        assert!(
            !deps
                .iter()
                .any(|d| matches!(d, DbObjectId::Type { name, .. } if name == "citext")),
            "Extension type should not be recorded as Type"
        );

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_fetch_procedure() {
    with_test_db(async |db| {
        db.execute(
            "CREATE OR REPLACE PROCEDURE update_user_count()
             LANGUAGE plpgsql AS $$
             BEGIN
                 -- This would update some counter table
                 RAISE NOTICE 'User count updated';
             END;
             $$",
        )
        .await;

        let functions = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(functions.len(), 1);
        let proc = &functions[0];

        assert_eq!(proc.schema, "public");
        assert_eq!(proc.name, "update_user_count");
        assert_eq!(proc.kind, FunctionKind::Procedure);
        assert_eq!(proc.parameters.len(), 0);
        assert_eq!(proc.return_type, None); // Procedures don't have return types
        assert_eq!(proc.language, "plpgsql");
    })
    .await;
}

#[tokio::test]
async fn test_fetch_function_with_multiple_parameters() {
    with_test_db(async |db| {
        db.execute(
            "CREATE OR REPLACE FUNCTION calculate(
                 x INTEGER,
                 y INTEGER,
                 operation TEXT
             ) RETURNS INTEGER AS $$
             BEGIN
                 CASE operation
                     WHEN 'add' THEN RETURN x + y;
                     WHEN 'multiply' THEN RETURN x * y;
                     ELSE RETURN 0;
                 END CASE;
             END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        let functions = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(functions.len(), 1);
        let func = &functions[0];

        assert_eq!(func.parameters.len(), 3);

        assert_eq!(func.parameters[0].name, Some("x".to_string()));
        assert_eq!(func.parameters[0].data_type, "integer");
        assert_eq!(func.parameters[0].mode, None); // IN is default

        assert_eq!(func.parameters[1].name, Some("y".to_string()));
        assert_eq!(func.parameters[1].data_type, "integer");

        assert_eq!(func.parameters[2].name, Some("operation".to_string()));
        assert_eq!(func.parameters[2].data_type, "text");
    })
    .await;
}

#[tokio::test]
async fn test_fetch_immutable_function() {
    with_test_db(async |db| {
        db.execute(
            "CREATE OR REPLACE FUNCTION calculate_circle_area(radius DECIMAL)
             RETURNS DECIMAL AS $$
             BEGIN
                 RETURN 3.14159 * radius * radius;
             END;
             $$ LANGUAGE plpgsql IMMUTABLE",
        )
        .await;

        let functions = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(functions.len(), 1);
        let func = &functions[0];

        assert_eq!(func.name, "calculate_circle_area");
        assert_eq!(func.volatility, "IMMUTABLE");
    })
    .await;
}

#[tokio::test]
async fn test_fetch_strict_security_definer_function() {
    with_test_db(async |db| {
        db.execute(
            "CREATE OR REPLACE FUNCTION secure_function(input TEXT)
             RETURNS TEXT AS $$
             BEGIN
                 RETURN UPPER(input);
             END;
             $$ LANGUAGE plpgsql STRICT SECURITY DEFINER",
        )
        .await;

        let functions = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(functions.len(), 1);
        let func = &functions[0];

        assert_eq!(func.name, "secure_function");
        assert!(func.is_strict);
        assert_eq!(func.security_type, "DEFINER");
    })
    .await;
}

#[tokio::test]
async fn test_fetch_functions_different_schemas() {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA utils").await;
        db.execute("CREATE SCHEMA reporting").await;

        db.execute(
            "CREATE OR REPLACE FUNCTION public.public_func()
             RETURNS TEXT AS $$
             BEGIN
                 RETURN 'public';
             END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        db.execute(
            "CREATE OR REPLACE FUNCTION utils.helper_func(x INTEGER)
             RETURNS INTEGER AS $$
             BEGIN
                 RETURN x * 2;
             END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        db.execute(
            "CREATE OR REPLACE FUNCTION reporting.generate_report()
             RETURNS TEXT AS $$
             BEGIN
                 RETURN 'report content';
             END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        let mut functions = fetch(&mut *db.conn().await).await.unwrap();
        functions.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));

        assert_eq!(functions.len(), 3);

        let public_func = functions
            .iter()
            .find(|f| f.schema == "public" && f.name == "public_func")
            .unwrap();
        assert_eq!(public_func.parameters.len(), 0);
        assert_eq!(
            public_func.depends_on,
            vec![DbObjectId::Schema {
                name: "public".to_string()
            }]
        );

        let utils_func = functions
            .iter()
            .find(|f| f.schema == "utils" && f.name == "helper_func")
            .unwrap();
        assert_eq!(utils_func.parameters.len(), 1);
        assert_eq!(
            utils_func.depends_on,
            vec![DbObjectId::Schema {
                name: "utils".to_string()
            }]
        );

        let reporting_func = functions
            .iter()
            .find(|f| f.schema == "reporting" && f.name == "generate_report")
            .unwrap();
        assert_eq!(reporting_func.parameters.len(), 0);
    })
    .await;
}

#[tokio::test]
async fn test_function_id_and_dependencies() {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA test_schema").await;
        db.execute(
            "CREATE OR REPLACE FUNCTION test_schema.test_function()
             RETURNS INTEGER AS $$
             BEGIN
                 RETURN 42;
             END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        let functions = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(functions.len(), 1);
        let func = &functions[0];

        assert_eq!(
            func.id(),
            DbObjectId::Function {
                schema: "test_schema".to_string(),
                name: "test_function".to_string(),
                arguments: "".to_string(),
            }
        );

        let deps = func.depends_on();
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
async fn test_fetch_function_no_parameters() {
    with_test_db(async |db| {
        db.execute(
            "CREATE OR REPLACE FUNCTION get_current_timestamp()
             RETURNS TIMESTAMP AS $$
             BEGIN
                 RETURN NOW();
             END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        let functions = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(functions.len(), 1);
        let func = &functions[0];

        assert_eq!(func.name, "get_current_timestamp");
        assert_eq!(func.parameters.len(), 0);
        assert_eq!(
            func.return_type,
            Some("timestamp without time zone".to_string())
        );
    })
    .await;
}

#[tokio::test]
async fn test_fetch_sql_function() {
    with_test_db(async |db| {
        db.execute(
            "CREATE OR REPLACE FUNCTION double_value(x INTEGER)
             RETURNS INTEGER AS 'SELECT $1 * 2'
             LANGUAGE SQL IMMUTABLE",
        )
        .await;

        let functions = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(functions.len(), 1);
        let func = &functions[0];

        assert_eq!(func.name, "double_value");
        assert_eq!(func.language, "sql");
        assert_eq!(func.volatility, "IMMUTABLE");
        assert_eq!(func.parameters.len(), 1);
        assert_eq!(func.parameters[0].name, Some("x".to_string()));
        assert_eq!(func.parameters[0].data_type, "integer");
    })
    .await;
}

#[tokio::test]
async fn test_fetch_overloaded_functions() {
    with_test_db(async |db| {
        db.execute(
            "CREATE OR REPLACE FUNCTION format_value(val INTEGER)
             RETURNS TEXT AS $$
             BEGIN
                 RETURN val::TEXT;
             END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        db.execute(
            "CREATE OR REPLACE FUNCTION format_value(val FLOAT)
             RETURNS TEXT AS $$
             BEGIN
                 RETURN ROUND(val, 2)::TEXT;
             END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        db.execute(
            "CREATE OR REPLACE FUNCTION format_value(val TEXT, prefix TEXT)
             RETURNS TEXT AS $$
             BEGIN
                 RETURN prefix || ': ' || val;
             END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        let mut functions = fetch(&mut *db.conn().await).await.unwrap();
        functions.sort_by(|a, b| {
            // Sort by parameter count first, then by parameter type for deterministic ordering
            let count_cmp = a.parameters.len().cmp(&b.parameters.len());
            if count_cmp != std::cmp::Ordering::Equal {
                return count_cmp;
            }

            // For functions with same parameter count, sort by first parameter type
            if !a.parameters.is_empty() && !b.parameters.is_empty() {
                return a.parameters[0].data_type.cmp(&b.parameters[0].data_type);
            }

            std::cmp::Ordering::Equal
        });

        assert_eq!(functions.len(), 3);

        assert!(functions.iter().all(|f| f.name == "format_value"));
        assert!(functions.iter().all(|f| f.schema == "public"));

        let integer_func = functions
            .iter()
            .find(|f| f.parameters.len() == 1 && f.parameters[0].data_type == "integer")
            .expect("Should have integer function");

        let float_func = functions
            .iter()
            .find(|f| {
                f.parameters.len() == 1
                    && (f.parameters[0].data_type.contains("double")
                        || f.parameters[0].data_type.contains("float"))
            })
            .expect("Should have float/double function");

        let text_func = functions
            .iter()
            .find(|f| {
                f.parameters.len() == 2
                    && f.parameters[0].data_type == "text"
                    && f.parameters[1].data_type == "text"
            })
            .expect("Should have text function");

        assert_eq!(integer_func.parameters.len(), 1);
        assert_eq!(integer_func.parameters[0].data_type, "integer");

        assert_eq!(float_func.parameters.len(), 1);
        assert!(
            float_func.parameters[0].data_type.contains("double")
                || float_func.parameters[0].data_type.contains("float")
        );

        assert_eq!(text_func.parameters.len(), 2);
        assert_eq!(text_func.parameters[0].data_type, "text");
        assert_eq!(text_func.parameters[1].data_type, "text");
    })
    .await;
}

#[tokio::test]
async fn test_fetch_function_with_custom_return_type() {
    with_test_db(async |db| {
        db.execute("CREATE TYPE priority AS ENUM ('low', 'medium', 'high')")
            .await;

        db.execute(
            "CREATE OR REPLACE FUNCTION get_default_priority()
             RETURNS public.priority AS $$
             BEGIN
                 RETURN 'medium'::priority;
             END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        let functions = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(functions.len(), 1);
        let func = &functions[0];

        assert_eq!(func.schema, "public");
        assert_eq!(func.name, "get_default_priority");
        assert_eq!(func.parameters.len(), 0);

        assert!(func.return_type.is_some());
        let return_type = func.return_type.as_ref().unwrap();
        assert!(return_type.contains("priority")); // May be "priority" or "public.priority"

        assert_eq!(func.depends_on().len(), 2);
        assert!(func.depends_on().contains(&DbObjectId::Schema {
            name: "public".to_string()
        }));
        assert!(func.depends_on().contains(&DbObjectId::Type {
            schema: "public".to_string(),
            name: "priority".to_string()
        }));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_stable_function() {
    with_test_db(async |db| {
        db.execute(
            "CREATE OR REPLACE FUNCTION get_current_date_stable()
             RETURNS DATE AS $$
             BEGIN
                 RETURN CURRENT_DATE;
             END;
             $$ LANGUAGE plpgsql STABLE",
        )
        .await;

        let functions = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(functions.len(), 1);
        let func = &functions[0];

        assert_eq!(func.schema, "public");
        assert_eq!(func.name, "get_current_date_stable");
        assert_eq!(func.kind, FunctionKind::Function);
        assert_eq!(func.volatility, "STABLE");
        assert_eq!(func.parameters.len(), 0);
    })
    .await;
}

#[tokio::test]
async fn test_fetch_security_invoker_function() {
    with_test_db(async |db| {
        db.execute(
            "CREATE OR REPLACE FUNCTION get_user_name()
             RETURNS TEXT AS $$
             BEGIN
                 RETURN USER;
             END;
             $$ LANGUAGE plpgsql SECURITY INVOKER",
        )
        .await;

        let functions = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(functions.len(), 1);
        let func = &functions[0];

        assert_eq!(func.schema, "public");
        assert_eq!(func.name, "get_user_name");
        assert_eq!(func.security_type, "INVOKER");
    })
    .await;
}

#[tokio::test]
async fn test_fetch_non_strict_function() {
    with_test_db(async |db| {
        db.execute(
            "CREATE OR REPLACE FUNCTION handle_null_input(val INTEGER)
             RETURNS INTEGER AS $$
             BEGIN
                 IF val IS NULL THEN
                     RETURN 0;
                 ELSE
                     RETURN val * 2;
                 END IF;
             END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        let functions = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(functions.len(), 1);
        let func = &functions[0];

        assert_eq!(func.schema, "public");
        assert_eq!(func.name, "handle_null_input");
        assert!(!func.is_strict);
    })
    .await;
}

#[tokio::test]
async fn test_fetch_function_with_comment() {
    with_test_db(async |db| {
        db.execute(
            "CREATE OR REPLACE FUNCTION calculate_discount(price DECIMAL, discount_rate DECIMAL)
             RETURNS DECIMAL AS $$
             BEGIN
                 RETURN price * (1 - discount_rate);
             END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        db.execute("COMMENT ON FUNCTION calculate_discount(DECIMAL, DECIMAL) IS 'Calculates the discounted price given a price and discount rate'")
            .await;

        let functions = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(functions.len(), 1);
        let func = &functions[0];

        assert_eq!(func.schema, "public");
        assert_eq!(func.name, "calculate_discount");
        assert_eq!(func.parameters.len(), 2);
        assert_eq!(
            func.comment,
            Some("Calculates the discounted price given a price and discount rate".to_string())
        );
    })
    .await;
}

#[tokio::test]
async fn test_fetch_function_with_domain_parameter() {
    with_test_db(async |db| {
        db.execute("CREATE DOMAIN positive_int AS INTEGER CHECK (VALUE > 0)")
            .await;

        db.execute(
            "CREATE OR REPLACE FUNCTION check_positive(val public.positive_int)
             RETURNS BOOLEAN AS $$
             BEGIN
                 RETURN val IS NOT NULL;
             END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        let functions = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(functions.len(), 1);
        let func = &functions[0];

        assert_eq!(func.schema, "public");
        assert_eq!(func.name, "check_positive");
        assert_eq!(func.parameters.len(), 1);
        assert_eq!(func.parameters[0].data_type, "public.positive_int");

        // Should contain DbObjectId::Domain, not DbObjectId::Type
        assert_eq!(func.depends_on().len(), 2);
        assert!(func.depends_on().contains(&DbObjectId::Schema {
            name: "public".to_string()
        }));
        assert!(func.depends_on().contains(&DbObjectId::Domain {
            schema: "public".to_string(),
            name: "positive_int".to_string()
        }));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_function_with_domain_return_type() {
    with_test_db(async |db| {
        db.execute("CREATE DOMAIN positive_int AS INTEGER CHECK (VALUE > 0)")
            .await;

        db.execute(
            "CREATE OR REPLACE FUNCTION get_positive()
             RETURNS public.positive_int AS $$
             BEGIN
                 RETURN 42;
             END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        let functions = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(functions.len(), 1);
        let func = &functions[0];

        assert_eq!(func.schema, "public");
        assert_eq!(func.name, "get_positive");

        // Should contain DbObjectId::Domain, not DbObjectId::Type
        assert!(func.depends_on().contains(&DbObjectId::Domain {
            schema: "public".to_string(),
            name: "positive_int".to_string()
        }));
    })
    .await;
}

/// Test that functions with array parameters of custom types have correct dependencies
/// on the base type, not the internal array type name (e.g., "status" not "_status")
#[tokio::test]
async fn test_fetch_function_with_custom_type_array_parameter() {
    with_test_db(async |db| {
        db.execute("CREATE TYPE item_status AS ENUM ('pending', 'active', 'completed')")
            .await;

        db.execute(
            "CREATE OR REPLACE FUNCTION process_items(statuses item_status[])
             RETURNS INTEGER AS $$
             BEGIN
                 RETURN array_length(statuses, 1);
             END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        let functions = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(functions.len(), 1);
        let func = &functions[0];

        assert_eq!(func.schema, "public");
        assert_eq!(func.name, "process_items");
        assert_eq!(func.parameters.len(), 1);
        // The parameter type should include the array brackets
        assert!(
            func.parameters[0].data_type.contains("item_status"),
            "Parameter type should contain 'item_status', got: {}",
            func.parameters[0].data_type
        );

        // Critical: Should depend on "item_status", NOT "_item_status"
        // Before the fix, pg_depend returns the internal array type name (_typename)
        assert!(
            func.depends_on().contains(&DbObjectId::Type {
                schema: "public".to_string(),
                name: "item_status".to_string()
            }),
            "Function should depend on base type 'item_status', not '_item_status'. Got: {:?}",
            func.depends_on()
        );

        // Should NOT depend on the internal array type name
        assert!(
            !func.depends_on().contains(&DbObjectId::Type {
                schema: "public".to_string(),
                name: "_item_status".to_string()
            }),
            "Function should NOT depend on internal array type '_item_status'. Got: {:?}",
            func.depends_on()
        );
    })
    .await;
}

#[tokio::test]
async fn test_function_returns_table_type() {
    with_test_db(async |db| {
        // Create a table
        db.execute(
            "CREATE TABLE policies (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL
             )",
        )
        .await;

        // Create a function that returns the table type
        db.execute(
            "CREATE OR REPLACE FUNCTION get_policy(p_id INTEGER)
             RETURNS policies AS $$
             BEGIN
                 RETURN (SELECT p FROM policies p WHERE p.id = p_id);
             END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        let functions = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(functions.len(), 1);
        let func = &functions[0];

        assert_eq!(func.schema, "public");
        assert_eq!(func.name, "get_policy");
        assert_eq!(func.return_type, Some("policies".to_string()));

        assert!(
            func.depends_on().contains(&DbObjectId::Table {
                schema: "public".to_string(),
                name: "policies".to_string()
            }),
            "Function should depend on Table 'policies', not Type. Got: {:?}",
            func.depends_on()
        );

        assert!(
            !func.depends_on().contains(&DbObjectId::Type {
                schema: "public".to_string(),
                name: "policies".to_string()
            }),
            "Function should NOT depend on Type 'policies'. Got: {:?}",
            func.depends_on()
        );
    })
    .await;
}

#[tokio::test]
async fn test_function_returns_view_type() {
    with_test_db(async |db| {
        // Create a table and view
        db.execute(
            "CREATE TABLE items (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL
             )",
        )
        .await;

        db.execute(
            "CREATE VIEW active_items AS
             SELECT * FROM items WHERE id > 0",
        )
        .await;

        // Create a function that returns the view type
        db.execute(
            "CREATE OR REPLACE FUNCTION get_active_item(p_id INTEGER)
             RETURNS active_items AS $$
             BEGIN
                 RETURN (SELECT v FROM active_items v WHERE v.id = p_id);
             END;
             $$ LANGUAGE plpgsql",
        )
        .await;

        let functions = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(functions.len(), 1);
        let func = &functions[0];

        assert_eq!(func.schema, "public");
        assert_eq!(func.name, "get_active_item");
        assert_eq!(func.return_type, Some("active_items".to_string()));

        // Critical: Should depend on the View, NOT a Type
        assert!(
            func.depends_on().contains(&DbObjectId::View {
                schema: "public".to_string(),
                name: "active_items".to_string()
            }),
            "Function should depend on View 'active_items', not Type. Got: {:?}",
            func.depends_on()
        );

        // Should NOT depend on a Type with the same name
        assert!(
            !func.depends_on().contains(&DbObjectId::Type {
                schema: "public".to_string(),
                name: "active_items".to_string()
            }),
            "Function should NOT depend on Type 'active_items'. Got: {:?}",
            func.depends_on()
        );
    })
    .await;
}
