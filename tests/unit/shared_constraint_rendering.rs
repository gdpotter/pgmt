//! Tests for shared constraint rendering consistency between schema generator and diff operations

use anyhow::Result;
use pgmt::catalog::constraint::{Constraint, ConstraintType};
use pgmt::diff::operations::SqlRenderer;
use pgmt::diff::operations::constraint::ConstraintOperation;
use pgmt::render::sql::render_create_constraint;

/// Test that schema generator and diff operations produce identical CREATE CONSTRAINT SQL
#[test]
fn test_shared_constraint_rendering_check_consistency() -> Result<()> {
    // Test CHECK constraint with simple expression (should be wrapped in CHECK)
    let constraint = Constraint {
        schema: "public".to_string(),
        table: "users".to_string(),
        name: "users_age_check".to_string(),
        constraint_type: ConstraintType::Check {
            expression: "age >= 0 AND age <= 150".to_string(),
        },
        comment: None,
        depends_on: vec![],
    };

    // Get SQL from shared rendering function (used by schema generator)
    let schema_generator_sql = render_create_constraint(&constraint);

    // Get SQL from diff operations
    let constraint_operation = ConstraintOperation::Create(constraint.clone());
    let diff_operation_sqls = constraint_operation.to_sql();
    assert_eq!(diff_operation_sqls.len(), 1);
    let diff_operation_sql = &diff_operation_sqls[0].sql;

    // Verify they produce identical SQL
    assert_eq!(
        schema_generator_sql, *diff_operation_sql,
        "Schema generator and diff operations must produce identical CREATE CONSTRAINT SQL"
    );

    // Verify expected CHECK wrapping
    assert!(schema_generator_sql.contains("CHECK (age >= 0 AND age <= 150)"));

    Ok(())
}

/// Test CHECK constraint that already includes CHECK keyword (should not double wrap)
#[test]
fn test_shared_constraint_rendering_check_keyword_handling() -> Result<()> {
    // Test CHECK constraint with expression that already includes CHECK keyword
    let constraint = Constraint {
        schema: "public".to_string(),
        table: "products".to_string(),
        name: "products_price_check".to_string(),
        constraint_type: ConstraintType::Check {
            expression: "CHECK (price > 0::numeric)".to_string(),
        },
        comment: None,
        depends_on: vec![],
    };

    // Get SQL from both sources
    let schema_generator_sql = render_create_constraint(&constraint);

    let constraint_operation = ConstraintOperation::Create(constraint.clone());
    let diff_operation_sql = &constraint_operation.to_sql()[0].sql;

    // Verify identical output
    assert_eq!(schema_generator_sql, *diff_operation_sql);

    // Verify no double CHECK keyword
    assert!(schema_generator_sql.contains("CHECK (price > 0::numeric)"));
    assert!(!schema_generator_sql.contains("CHECK (CHECK"));

    Ok(())
}

/// Test foreign key constraint consistency
#[test]
fn test_shared_constraint_rendering_foreign_key_consistency() -> Result<()> {
    let constraint = Constraint {
        schema: "public".to_string(),
        table: "posts".to_string(),
        name: "posts_user_id_fkey".to_string(),
        constraint_type: ConstraintType::ForeignKey {
            columns: vec!["user_id".to_string()],
            referenced_schema: "public".to_string(),
            referenced_table: "users".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: Some("CASCADE".to_string()),
            on_update: Some("RESTRICT".to_string()),
            deferrable: true,
            initially_deferred: false,
        },
        comment: None,
        depends_on: vec![],
    };

    // Get SQL from both sources
    let schema_generator_sql = render_create_constraint(&constraint);

    let constraint_operation = ConstraintOperation::Create(constraint.clone());
    let diff_operation_sql = &constraint_operation.to_sql()[0].sql;

    // Verify identical output
    assert_eq!(schema_generator_sql, *diff_operation_sql);

    // Verify foreign key features
    let expected_features = vec![
        "FOREIGN KEY (\"user_id\")",
        "REFERENCES \"public\".\"users\" (\"id\")",
        "ON DELETE CASCADE",
        "ON UPDATE RESTRICT",
        "DEFERRABLE",
    ];

    for feature in expected_features {
        assert!(
            schema_generator_sql.contains(feature),
            "SQL should contain '{}', but got: {}",
            feature,
            schema_generator_sql
        );
    }

    // Should not contain INITIALLY DEFERRED since initially_deferred is false
    assert!(!schema_generator_sql.contains("INITIALLY DEFERRED"));

    Ok(())
}

/// Test unique constraint consistency
#[test]
fn test_shared_constraint_rendering_unique_consistency() -> Result<()> {
    let constraint = Constraint {
        schema: "public".to_string(),
        table: "users".to_string(),
        name: "users_email_unique".to_string(),
        constraint_type: ConstraintType::Unique {
            columns: vec!["email".to_string(), "tenant_id".to_string()],
        },
        comment: None,
        depends_on: vec![],
    };

    // Get SQL from both sources
    let schema_generator_sql = render_create_constraint(&constraint);

    let constraint_operation = ConstraintOperation::Create(constraint.clone());
    let diff_operation_sql = &constraint_operation.to_sql()[0].sql;

    // Verify identical output
    assert_eq!(schema_generator_sql, *diff_operation_sql);

    // Verify unique constraint structure
    assert!(schema_generator_sql.contains("UNIQUE (\"email\", \"tenant_id\")"));

    Ok(())
}

/// Test exclusion constraint consistency
#[test]
fn test_shared_constraint_rendering_exclusion_consistency() -> Result<()> {
    let constraint = Constraint {
        schema: "public".to_string(),
        table: "reservations".to_string(),
        name: "reservations_overlap_excl".to_string(),
        constraint_type: ConstraintType::Exclusion {
            elements: vec![
                "room_id".to_string(),
                "tsrange(start_time, end_time)".to_string(),
            ],
            operator_classes: vec!["int4_ops".to_string(), "range_ops".to_string()],
            operators: vec!["=".to_string(), "&&".to_string()],
            index_method: "gist".to_string(),
            predicate: Some("status = 'active'".to_string()),
        },
        comment: None,
        depends_on: vec![],
    };

    // Get SQL from both sources
    let schema_generator_sql = render_create_constraint(&constraint);

    let constraint_operation = ConstraintOperation::Create(constraint.clone());
    let diff_operation_sql = &constraint_operation.to_sql()[0].sql;

    // Verify identical output
    assert_eq!(schema_generator_sql, *diff_operation_sql);

    // Verify exclusion constraint features
    let expected_features = vec![
        "EXCLUDE USING gist",
        "room_id WITH =",
        "tsrange(start_time, end_time) WITH &&",
        "WHERE status = 'active'",
    ];

    for feature in expected_features {
        assert!(
            schema_generator_sql.contains(feature),
            "SQL should contain '{}', but got: {}",
            feature,
            schema_generator_sql
        );
    }

    Ok(())
}
