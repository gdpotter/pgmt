use super::section_parser::{MigrationSection, TransactionMode};
use anyhow::{Result, anyhow};
use std::collections::HashSet;

/// Validate all sections in a migration
pub fn validate_sections(sections: &[MigrationSection]) -> Result<()> {
    // Check for duplicate section names
    validate_unique_names(sections)?;

    // Validate each section individually
    for section in sections {
        validate_section(section)?;
    }

    Ok(())
}

/// Ensure section names are unique
fn validate_unique_names(sections: &[MigrationSection]) -> Result<()> {
    let mut seen = HashSet::new();

    for section in sections {
        if !seen.insert(&section.name) {
            return Err(anyhow!(
                "Duplicate section name '{}' at line {}",
                section.name,
                section.start_line
            ));
        }
    }

    Ok(())
}

/// Validate individual section configuration
fn validate_section(section: &MigrationSection) -> Result<()> {
    // Validate timeout
    if section.timeout.as_secs() == 0 && section.timeout.subsec_millis() == 0 {
        return Err(anyhow!(
            "Section '{}' (line {}) has zero timeout (must be positive)",
            section.name,
            section.start_line
        ));
    }

    // Validate retry config
    if let Some(retry) = &section.retry_config {
        if retry.attempts == 0 {
            return Err(anyhow!(
                "Section '{}' (line {}) has zero retry attempts",
                section.name,
                section.start_line
            ));
        }

        if retry.attempts > 100 {
            return Err(anyhow!(
                "Section '{}' (line {}) has excessive retry attempts: {} (max: 100)",
                section.name,
                section.start_line,
                retry.attempts
            ));
        }
    }

    // Validate batch config
    if let Some(batch) = &section.batch_config {
        if batch.size == 0 {
            return Err(anyhow!(
                "Section '{}' (line {}) has zero batch size",
                section.name,
                section.start_line
            ));
        }

        if batch.size > 1_000_000 {
            return Err(anyhow!(
                "Section '{}' (line {}) has excessive batch size: {} (max: 1,000,000)",
                section.name,
                section.start_line,
                batch.size
            ));
        }

        // Batch processing requires autocommit mode
        if section.mode != TransactionMode::Autocommit {
            return Err(anyhow!(
                "Section '{}' (line {}) has batch_size but mode is not 'autocommit'. \
                 Batch processing requires autocommit mode to commit each batch independently.",
                section.name,
                section.start_line
            ));
        }
    }

    // Validate SQL is not empty
    if section.sql.trim().is_empty() {
        return Err(anyhow!(
            "Section '{}' (line {}) has no SQL statements",
            section.name,
            section.start_line
        ));
    }

    // Validate CONCURRENTLY operations are non-transactional
    if section.sql.to_uppercase().contains("CONCURRENTLY")
        && section.mode == TransactionMode::Transactional
    {
        return Err(anyhow!(
            "Section '{}' (line {}) uses CONCURRENTLY but mode is 'transactional'. \
             CONCURRENTLY operations cannot run in transactions. \
             Use mode='non-transactional' instead.",
            section.name,
            section.start_line
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migration::section_parser::parse_migration_sections;
    use std::path::Path;

    #[test]
    fn test_validate_unique_names() {
        let sql = r#"
-- pgmt:section name="test"
-- pgmt:  mode="transactional"
-- pgmt:  timeout="30s"
ALTER TABLE users ADD COLUMN test TEXT;

-- pgmt:section name="test"
-- pgmt:  mode="transactional"
-- pgmt:  timeout="30s"
ALTER TABLE users ADD COLUMN test2 TEXT;
"#;

        let sections = parse_migration_sections(Path::new("test.sql"), sql).unwrap();
        let result = validate_sections(&sections);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Duplicate"));
    }

    #[test]
    fn test_validate_concurrently_with_transactional() {
        let sql = r#"
-- pgmt:section name="test"
-- pgmt:  mode="transactional"
-- pgmt:  timeout="30s"
CREATE INDEX CONCURRENTLY idx_test ON users(email);
"#;

        let sections = parse_migration_sections(Path::new("test.sql"), sql).unwrap();
        let result = validate_sections(&sections);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("CONCURRENTLY"));
        assert!(err_msg.contains("non-transactional"));
    }

    #[test]
    fn test_validate_batch_without_autocommit() {
        let sql = r#"
-- pgmt:section name="test"
-- pgmt:  mode="transactional"
-- pgmt:  timeout="30s"
-- pgmt:  batch_size="5000"
UPDATE users SET status = 'active';
"#;

        let sections = parse_migration_sections(Path::new("test.sql"), sql).unwrap();
        let result = validate_sections(&sections);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("autocommit"));
    }

    #[test]
    fn test_validate_excessive_retry_attempts() {
        let sql = r#"
-- pgmt:section name="test"
-- pgmt:  mode="non-transactional"
-- pgmt:  timeout="2s"
-- pgmt:  retry_attempts="150"
CREATE INDEX CONCURRENTLY idx_test ON users(email);
"#;

        let sections = parse_migration_sections(Path::new("test.sql"), sql).unwrap();
        let result = validate_sections(&sections);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("excessive"));
    }

    #[test]
    fn test_validate_valid_sections() {
        let sql = r#"
-- pgmt:section name="add_column"
-- pgmt:  mode="transactional"
-- pgmt:  timeout="30s"
ALTER TABLE users ADD COLUMN status TEXT;

-- pgmt:section name="create_index"
-- pgmt:  mode="non-transactional"
-- pgmt:  timeout="2s"
-- pgmt:  retry_attempts="10"
-- pgmt:  retry_delay="5s"
CREATE INDEX CONCURRENTLY idx_users_status ON users(status);

-- pgmt:section name="add_constraint"
-- pgmt:  mode="transactional"
-- pgmt:  timeout="10s"
ALTER TABLE users ALTER COLUMN status SET NOT NULL;
"#;

        let sections = parse_migration_sections(Path::new("test.sql"), sql).unwrap();
        let result = validate_sections(&sections);

        assert!(result.is_ok());
    }
}
