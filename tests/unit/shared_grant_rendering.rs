//! Tests for shared grant rendering consistency between schema generator and diff operations

use anyhow::Result;
use pgmt::catalog::grant::{Grant, GranteeType, ObjectType};
use pgmt::diff::operations::SqlRenderer;
use pgmt::diff::operations::grant::GrantOperation;
use pgmt::render::sql::render_grant_statement;

/// Test that schema generator and diff operations produce identical GRANT SQL for views
#[test]
fn test_shared_grant_rendering_view_no_view_keyword() -> Result<()> {
    // This specifically tests the original issue: GRANT on VIEW should not include VIEW keyword
    let grant = Grant {
        object: ObjectType::View {
            schema: "public".to_string(),
            name: "current_subscriptions".to_string(),
        },
        grantee: GranteeType::Role("postgres".to_string()),
        privileges: vec![
            "DELETE".to_string(),
            "INSERT".to_string(),
            "REFERENCES".to_string(),
            "SELECT".to_string(),
            "TRIGGER".to_string(),
            "TRUNCATE".to_string(),
            "UPDATE".to_string(),
        ],
        with_grant_option: false,
        depends_on: vec![],
        object_owner: "postgres".to_string(),
    };

    // Get SQL from shared rendering function (used by schema generator)
    let schema_generator_sql = render_grant_statement(&grant);

    // Get SQL from diff operations
    let grant_operation = GrantOperation::Grant {
        grant: grant.clone(),
    };
    let diff_operation_sqls = grant_operation.to_sql();
    assert_eq!(diff_operation_sqls.len(), 1);
    let diff_operation_sql = &diff_operation_sqls[0].sql;

    // Verify they produce identical SQL
    assert_eq!(
        schema_generator_sql, *diff_operation_sql,
        "Schema generator and diff operations must produce identical GRANT SQL"
    );

    // Most importantly: verify NO VIEW keyword is present
    assert!(
        !schema_generator_sql.contains("VIEW"),
        "GRANT statement should NOT contain VIEW keyword, but got: {}",
        schema_generator_sql
    );

    // Verify the correct format
    assert_eq!(
        schema_generator_sql,
        "GRANT DELETE, INSERT, REFERENCES, SELECT, TRIGGER, TRUNCATE, UPDATE ON \"public\".\"current_subscriptions\" TO \"postgres\";"
    );

    Ok(())
}

/// Test GRANT consistency on tables
#[test]
fn test_shared_grant_rendering_table_consistency() -> Result<()> {
    let grant = Grant {
        object: ObjectType::Table {
            schema: "public".to_string(),
            name: "users".to_string(),
        },
        grantee: GranteeType::Role("app_user".to_string()),
        privileges: vec![
            "SELECT".to_string(),
            "INSERT".to_string(),
            "UPDATE".to_string(),
        ],
        with_grant_option: false,
        depends_on: vec![],
        object_owner: "postgres".to_string(),
    };

    // Get SQL from both sources
    let schema_generator_sql = render_grant_statement(&grant);

    let grant_operation = GrantOperation::Grant {
        grant: grant.clone(),
    };
    let diff_operation_sql = &grant_operation.to_sql()[0].sql;

    // Verify identical output
    assert_eq!(schema_generator_sql, *diff_operation_sql);

    // Verify no TABLE keyword (tables and views use same syntax)
    assert!(!schema_generator_sql.contains("TABLE"));
    assert_eq!(
        schema_generator_sql,
        "GRANT SELECT, INSERT, UPDATE ON \"public\".\"users\" TO \"app_user\";"
    );

    Ok(())
}

/// Test GRANT WITH GRANT OPTION consistency
#[test]
fn test_shared_grant_rendering_with_grant_option() -> Result<()> {
    let grant = Grant {
        object: ObjectType::Table {
            schema: "public".to_string(),
            name: "orders".to_string(),
        },
        grantee: GranteeType::Role("manager".to_string()),
        privileges: vec!["ALL".to_string()],
        with_grant_option: true,
        depends_on: vec![],
        object_owner: "postgres".to_string(),
    };

    // Get SQL from both sources
    let schema_generator_sql = render_grant_statement(&grant);

    let grant_operation = GrantOperation::Grant {
        grant: grant.clone(),
    };
    let diff_operation_sql = &grant_operation.to_sql()[0].sql;

    // Verify identical output
    assert_eq!(schema_generator_sql, *diff_operation_sql);

    // Verify WITH GRANT OPTION is included
    assert!(schema_generator_sql.contains("WITH GRANT OPTION"));
    assert_eq!(
        schema_generator_sql,
        "GRANT ALL ON \"public\".\"orders\" TO \"manager\" WITH GRANT OPTION;"
    );

    Ok(())
}

/// Test GRANT TO PUBLIC consistency
#[test]
fn test_shared_grant_rendering_public_grantee() -> Result<()> {
    let grant = Grant {
        object: ObjectType::View {
            schema: "public".to_string(),
            name: "public_stats".to_string(),
        },
        grantee: GranteeType::Public,
        privileges: vec!["SELECT".to_string()],
        with_grant_option: false,
        depends_on: vec![],
        object_owner: "postgres".to_string(),
    };

    // Get SQL from both sources
    let schema_generator_sql = render_grant_statement(&grant);

    let grant_operation = GrantOperation::Grant {
        grant: grant.clone(),
    };
    let diff_operation_sql = &grant_operation.to_sql()[0].sql;

    // Verify identical output
    assert_eq!(schema_generator_sql, *diff_operation_sql);

    // Verify PUBLIC grantee (no quotes)
    assert!(schema_generator_sql.contains("TO PUBLIC"));
    assert_eq!(
        schema_generator_sql,
        "GRANT SELECT ON \"public\".\"public_stats\" TO PUBLIC;"
    );

    Ok(())
}

/// Test GRANT on schema (should include SCHEMA keyword)
#[test]
fn test_shared_grant_rendering_schema_with_keyword() -> Result<()> {
    let grant = Grant {
        object: ObjectType::Schema {
            name: "analytics".to_string(),
        },
        grantee: GranteeType::Role("data_analyst".to_string()),
        privileges: vec!["USAGE".to_string(), "CREATE".to_string()],
        with_grant_option: false,
        depends_on: vec![],
        object_owner: "postgres".to_string(),
    };

    // Get SQL from both sources
    let schema_generator_sql = render_grant_statement(&grant);

    let grant_operation = GrantOperation::Grant {
        grant: grant.clone(),
    };
    let diff_operation_sql = &grant_operation.to_sql()[0].sql;

    // Verify identical output
    assert_eq!(schema_generator_sql, *diff_operation_sql);

    // Verify SCHEMA keyword IS included (schemas require it)
    assert!(schema_generator_sql.contains("SCHEMA"));
    assert_eq!(
        schema_generator_sql,
        "GRANT USAGE, CREATE ON SCHEMA \"analytics\" TO \"data_analyst\";"
    );

    Ok(())
}

/// Test GRANT on function (should include FUNCTION keyword)
#[test]
fn test_shared_grant_rendering_function_with_keyword() -> Result<()> {
    let grant = Grant {
        object: ObjectType::Function {
            schema: "public".to_string(),
            name: "calculate_total".to_string(),
        },
        grantee: GranteeType::Role("app_user".to_string()),
        privileges: vec!["EXECUTE".to_string()],
        with_grant_option: false,
        depends_on: vec![],
        object_owner: "postgres".to_string(),
    };

    // Get SQL from both sources
    let schema_generator_sql = render_grant_statement(&grant);

    let grant_operation = GrantOperation::Grant {
        grant: grant.clone(),
    };
    let diff_operation_sql = &grant_operation.to_sql()[0].sql;

    // Verify identical output
    assert_eq!(schema_generator_sql, *diff_operation_sql);

    // Verify FUNCTION keyword IS included (functions require it)
    assert!(schema_generator_sql.contains("FUNCTION"));
    assert_eq!(
        schema_generator_sql,
        "GRANT EXECUTE ON FUNCTION \"public\".\"calculate_total\" TO \"app_user\";"
    );

    Ok(())
}

/// Test REVOKE statement consistency
#[test]
fn test_shared_revoke_rendering_consistency() -> Result<()> {
    let grant = Grant {
        object: ObjectType::Table {
            schema: "public".to_string(),
            name: "sensitive_data".to_string(),
        },
        grantee: GranteeType::Role("temp_user".to_string()),
        privileges: vec!["SELECT".to_string(), "INSERT".to_string()],
        with_grant_option: false,
        depends_on: vec![],
        object_owner: "postgres".to_string(),
    };

    // Schema generator doesn't generate REVOKE, so just test diff operations produce correct SQL
    let grant_operation = GrantOperation::Revoke {
        grant: grant.clone(),
    };
    let diff_operation_sqls = grant_operation.to_sql();
    assert_eq!(diff_operation_sqls.len(), 1);
    let diff_operation_sql = &diff_operation_sqls[0].sql;

    // Verify REVOKE format
    assert_eq!(
        *diff_operation_sql,
        "REVOKE SELECT, INSERT ON \"public\".\"sensitive_data\" FROM \"temp_user\";"
    );

    Ok(())
}
