use crate::catalog::Catalog;
use crate::constants::MIGRATION_FILENAME_PREFIX;
use crate::diff::operations::{MigrationStep, SqlRenderer};
use crate::diff::{cascade, diff_all, diff_order};
use anyhow::Result;

/// Input for migration generation - all pure data, no side effects
#[derive(Debug, Clone)]
pub struct MigrationGenerationInput {
    pub old_catalog: Catalog,
    pub new_catalog: Catalog,
    pub description: String,
    pub version: u64,
}

/// Result of migration generation - all pure data
#[derive(Debug, Clone)]
pub struct MigrationGenerationResult {
    pub migration_sql: String,
    pub migration_filename: String,
    pub steps: Vec<MigrationStep>,
    pub has_changes: bool,
}

/// Pure function to generate migration from two catalogs
/// No side effects - just transformation of data
pub fn generate_migration(input: MigrationGenerationInput) -> Result<MigrationGenerationResult> {
    let steps = diff_all(&input.old_catalog, &input.new_catalog);
    let expanded_steps = cascade::expand(steps, &input.old_catalog, &input.new_catalog);
    let ordered_steps = diff_order(expanded_steps, &input.old_catalog, &input.new_catalog)?;

    let has_changes = !ordered_steps.is_empty();

    let migration_sql = if has_changes {
        render_migration_steps(&ordered_steps)?
    } else {
        "-- No changes detected\n".to_string()
    };

    let sanitized_description = sanitize_description(&input.description);
    let migration_filename = format!(
        "{}{}_{}.sql",
        MIGRATION_FILENAME_PREFIX, input.version, sanitized_description
    );

    Ok(MigrationGenerationResult {
        migration_sql,
        migration_filename,
        steps: ordered_steps,
        has_changes,
    })
}

/// Pure function to render migration steps into SQL
fn render_migration_steps(steps: &[MigrationStep]) -> Result<String> {
    let mut sql_parts = Vec::new();

    for step in steps {
        let rendered_sqls = step.to_sql();
        for rendered in rendered_sqls {
            sql_parts.push(rendered.sql);
        }
    }

    Ok(sql_parts.join("\n\n"))
}

/// Pure function to sanitize migration description for filename
fn sanitize_description(description: &str) -> String {
    let mut result = String::new();
    let mut last_was_underscore = false;

    for c in description.chars() {
        if c.is_alphanumeric() {
            result.push(c);
            last_was_underscore = false;
        } else if !last_was_underscore {
            result.push('_');
            last_was_underscore = true;
        }
    }

    result.trim_matches('_').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::id::DbObjectId;
    use crate::catalog::{
        schema::Schema,
        table::{Column, Table},
    };

    #[test]
    fn test_sanitize_description() {
        assert_eq!(sanitize_description("add_user_table"), "add_user_table");
        assert_eq!(sanitize_description("add user table!"), "add_user_table");
        assert_eq!(sanitize_description("hello-world@2024"), "hello_world_2024");
        assert_eq!(sanitize_description("___test___"), "test");
        assert_eq!(
            sanitize_description("add-user@email.feature!!! (with validation)"),
            "add_user_email_feature_with_validation"
        );
    }

    #[test]
    fn test_generate_migration_no_changes() {
        let catalog = Catalog::empty();

        let input = MigrationGenerationInput {
            old_catalog: catalog.clone(),
            new_catalog: catalog,
            description: "no_changes".to_string(),
            version: 123456789,
        };

        let result = generate_migration(input).unwrap();

        assert!(!result.has_changes);
        assert_eq!(result.migration_filename, "V123456789_no_changes.sql");
        assert!(result.migration_sql.contains("No changes detected"));
        assert!(result.steps.is_empty());
    }

    #[test]
    fn test_generate_migration_with_schema_changes() {
        let old_catalog = Catalog::empty();
        let mut new_catalog = Catalog::empty();
        new_catalog.schemas.push(Schema {
            name: "test_schema".to_string(),
            comment: None,
        });

        let input = MigrationGenerationInput {
            old_catalog,
            new_catalog,
            description: "add_schema".to_string(),
            version: 987654321,
        };

        let result = generate_migration(input).unwrap();

        assert!(result.has_changes);
        assert_eq!(result.migration_filename, "V987654321_add_schema.sql");
        assert!(result.migration_sql.contains("CREATE SCHEMA"));
        assert!(!result.steps.is_empty());
    }

    #[test]
    fn test_generate_migration_with_table_and_comments() {
        let old_catalog = Catalog::empty();
        let mut new_catalog = Catalog::empty();

        let mut table = Table::new(
            "public".to_string(),
            "users".to_string(),
            vec![
                Column {
                    name: "id".to_string(),
                    data_type: "integer".to_string(),
                    default: None,
                    not_null: true,
                    generated: None,
                    comment: Some("Primary key".to_string()),
                    depends_on: vec![],
                },
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    default: None,
                    not_null: false,
                    generated: None,
                    comment: None,
                    depends_on: vec![],
                },
            ],
            None,
            Some("User accounts".to_string()),
            vec![DbObjectId::Schema {
                name: "public".to_string(),
            }],
        );
        table.update_all_dependencies();
        new_catalog.tables.push(table);

        let result = generate_migration(MigrationGenerationInput {
            old_catalog,
            new_catalog,
            description: "add_users_table".to_string(),
            version: 9876543210,
        })
        .unwrap();

        assert!(result.has_changes);
        assert_eq!(result.migration_filename, "V9876543210_add_users_table.sql");

        assert_eq!(result.steps.len(), 3);

        assert!(result.migration_sql.contains("CREATE TABLE"));
        assert!(result.migration_sql.contains("COMMENT ON TABLE"));
        assert!(result.migration_sql.contains("COMMENT ON COLUMN"));
    }

    #[test]
    fn test_migration_result_structure() {
        let old_catalog = Catalog::empty();
        let mut new_catalog = Catalog::empty();
        new_catalog.schemas.push(Schema {
            name: "test".to_string(),
            comment: None,
        });

        let result = generate_migration(MigrationGenerationInput {
            old_catalog,
            new_catalog,
            description: "test_description".to_string(),
            version: 123,
        })
        .unwrap();

        assert!(!result.migration_sql.is_empty());
        assert_eq!(result.migration_filename, "V123_test_description.sql");
        assert!(!result.steps.is_empty());
        assert!(result.has_changes);
    }
}
