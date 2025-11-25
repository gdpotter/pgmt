// Tests for SQL injection protection in migration tracking

use anyhow::Result;
use pgmt::config::types::TrackingTable;
use pgmt::migration_tracking::{format_tracking_table_name, version_from_db, version_to_db};

#[test]
fn test_format_tracking_table_name_valid_identifiers() -> Result<()> {
    let valid_table = TrackingTable {
        schema: "public".to_string(),
        name: "pgmt_migrations".to_string(),
    };

    let formatted = format_tracking_table_name(&valid_table)?;
    assert_eq!(formatted, r#""public"."pgmt_migrations""#);
    Ok(())
}

#[test]
fn test_format_tracking_table_name_valid_with_underscores() -> Result<()> {
    let valid_table = TrackingTable {
        schema: "my_schema".to_string(),
        name: "migration_history".to_string(),
    };

    let formatted = format_tracking_table_name(&valid_table)?;
    assert_eq!(formatted, r#""my_schema"."migration_history""#);
    Ok(())
}

#[test]
fn test_format_tracking_table_name_valid_with_numbers() -> Result<()> {
    let valid_table = TrackingTable {
        schema: "schema123".to_string(),
        name: "table456".to_string(),
    };

    let formatted = format_tracking_table_name(&valid_table)?;
    assert_eq!(formatted, r#""schema123"."table456""#);
    Ok(())
}

#[test]
fn test_format_tracking_table_name_rejects_empty_schema() {
    let invalid_table = TrackingTable {
        schema: "".to_string(),
        name: "valid_name".to_string(),
    };

    let result = format_tracking_table_name(&invalid_table);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Invalid schema name")
    );
}

#[test]
fn test_format_tracking_table_name_rejects_empty_table() {
    let invalid_table = TrackingTable {
        schema: "valid_schema".to_string(),
        name: "".to_string(),
    };

    let result = format_tracking_table_name(&invalid_table);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Invalid table name")
    );
}

#[test]
fn test_format_tracking_table_name_rejects_sql_injection() {
    // Test case: schema name with SQL injection attempt
    let malicious_table = TrackingTable {
        schema: "public; DROP TABLE users; --".to_string(),
        name: "migrations".to_string(),
    };

    let result = format_tracking_table_name(&malicious_table);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Invalid schema name")
    );
}

#[test]
fn test_format_tracking_table_name_rejects_special_characters() {
    // Test case: table name with special characters
    let malicious_table = TrackingTable {
        schema: "public".to_string(),
        name: "migrations' OR '1'='1".to_string(),
    };

    let result = format_tracking_table_name(&malicious_table);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Invalid table name")
    );
}

#[test]
fn test_format_tracking_table_name_rejects_starting_with_number() {
    let invalid_table = TrackingTable {
        schema: "123invalid".to_string(),
        name: "migrations".to_string(),
    };

    let result = format_tracking_table_name(&invalid_table);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Invalid schema name")
    );
}

#[test]
fn test_format_tracking_table_name_allows_starting_with_underscore() -> Result<()> {
    let valid_table = TrackingTable {
        schema: "_private".to_string(),
        name: "_internal_migrations".to_string(),
    };

    let formatted = format_tracking_table_name(&valid_table)?;
    assert_eq!(formatted, r#""_private"."_internal_migrations""#);
    Ok(())
}

#[test]
fn test_format_tracking_table_name_allows_dollar_sign() -> Result<()> {
    let valid_table = TrackingTable {
        schema: "schema$1".to_string(),
        name: "table$2".to_string(),
    };

    let formatted = format_tracking_table_name(&valid_table)?;
    assert_eq!(formatted, r#""schema$1"."table$2""#);
    Ok(())
}

// Tests for integer overflow protection

#[test]
fn test_version_to_db_valid_conversion() -> Result<()> {
    let version = 1734567890u64; // Normal Unix timestamp
    let db_version = version_to_db(version)?;
    assert_eq!(db_version, 1734567890i64);
    Ok(())
}

#[test]
fn test_version_to_db_max_safe_value() -> Result<()> {
    let version = i64::MAX as u64; // Maximum safe value
    let db_version = version_to_db(version)?;
    assert_eq!(db_version, i64::MAX);
    Ok(())
}

#[test]
fn test_version_to_db_overflow_protection() {
    let version = u64::MAX; // This will overflow i64
    let result = version_to_db(version);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.to_string().contains("too large for database storage"));
}

#[test]
fn test_version_from_db_positive_values() {
    let db_version = 1734567890i64;
    let version = version_from_db(db_version);
    assert_eq!(version, 1734567890u64);
}

#[test]
fn test_version_from_db_zero() {
    let db_version = 0i64;
    let version = version_from_db(db_version);
    assert_eq!(version, 0u64);
}

#[test]
fn test_version_from_db_negative_protection() {
    let db_version = -1i64; // Invalid negative version
    let version = version_from_db(db_version);
    assert_eq!(version, 0u64); // Should return 0 for invalid negative values
}

#[test]
fn test_version_from_db_large_negative_protection() {
    let db_version = i64::MIN; // Most negative value
    let version = version_from_db(db_version);
    assert_eq!(version, 0u64); // Should return 0 for invalid negative values
}

#[test]
fn test_version_round_trip() -> Result<()> {
    let original = 1734567890u64;
    let db_version = version_to_db(original)?;
    let recovered = version_from_db(db_version);
    assert_eq!(original, recovered);
    Ok(())
}
