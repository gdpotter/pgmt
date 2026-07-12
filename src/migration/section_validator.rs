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

    // Lint (warning, not error): pgmt resumes at section granularity, and a
    // non-transactional/autocommit section is the only place partial
    // intra-section failure exists — one recorded status covers every
    // statement in it, so a resume re-touches statements that already
    // succeeded. CONCURRENTLY statements are the canonical case: each
    // physically commits on its own, so packing several into one section means
    // a mid-section failure can't resume precisely.
    //
    // We do NOT parse SQL — pgmt never parses user SQL. This is a deliberately
    // crude heuristic: a case-insensitive substring count of "CONCURRENTLY".
    // Overcounting (e.g. the word inside a comment) at worst produces a
    // spurious warning, which is acceptable for a lint.
    if matches!(
        section.mode,
        TransactionMode::NonTransactional | TransactionMode::Autocommit
    ) {
        let concurrently_count = count_concurrently(&section.sql);
        if concurrently_count > 1 {
            eprintln!(
                "Warning: section '{}' (line {}) contains {} CONCURRENTLY statements; \
                 consider one non-transactional statement per section so a failure can \
                 resume precisely (each section is tracked and resumed independently).",
                section.name, section.start_line, concurrently_count
            );
        }
    }

    Ok(())
}

/// Count case-insensitive occurrences of the literal "CONCURRENTLY" in `sql`.
/// A lint heuristic only (see the call site): pgmt never parses user SQL, so
/// this is a plain substring count, not a statement count.
fn count_concurrently(sql: &str) -> usize {
    sql.to_uppercase().matches("CONCURRENTLY").count()
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
    fn test_count_concurrently_case_insensitive() {
        assert_eq!(count_concurrently("CREATE INDEX foo ON t(a);"), 0);
        assert_eq!(
            count_concurrently("CREATE INDEX CONCURRENTLY foo ON t(a);"),
            1
        );
        assert_eq!(
            count_concurrently(
                "CREATE INDEX concurrently a ON t(x);\nCREATE INDEX CONCURRENTLY b ON t(y);"
            ),
            2
        );
    }

    /// Two CONCURRENTLY statements in one non-transactional section is a lint
    /// (warning), not an error: validation still succeeds so apply proceeds.
    #[test]
    fn test_two_concurrently_in_one_section_warns_not_errors() {
        let sql = r#"
-- pgmt:section name="indexes"
-- pgmt:  mode="non-transactional"
-- pgmt:  timeout="2s"
CREATE INDEX CONCURRENTLY idx_a ON users(a);
CREATE INDEX CONCURRENTLY idx_b ON users(b);
"#;

        let sections = parse_migration_sections(Path::new("test.sql"), sql).unwrap();
        assert_eq!(count_concurrently(&sections[0].sql), 2);
        // Warning is emitted (stderr), but validation succeeds.
        assert!(validate_sections(&sections).is_ok());
    }

    /// A single CONCURRENTLY statement per non-transactional section is the
    /// recommended shape and must not warn (count == 1).
    #[test]
    fn test_single_concurrently_per_section_does_not_warn() {
        let sql = r#"
-- pgmt:section name="idx_a"
-- pgmt:  mode="non-transactional"
-- pgmt:  timeout="2s"
CREATE INDEX CONCURRENTLY idx_a ON users(a);

-- pgmt:section name="idx_b"
-- pgmt:  mode="non-transactional"
-- pgmt:  timeout="2s"
CREATE INDEX CONCURRENTLY idx_b ON users(b);
"#;

        let sections = parse_migration_sections(Path::new("test.sql"), sql).unwrap();
        assert_eq!(count_concurrently(&sections[0].sql), 1);
        assert_eq!(count_concurrently(&sections[1].sql), 1);
        assert!(validate_sections(&sections).is_ok());
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
