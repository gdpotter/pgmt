//! SQL rendering for schema operations

use crate::catalog::id::DbObjectId;
use crate::diff::operations::SchemaOperation;
use crate::render::{RenderedSql, Safety, SqlRenderer, quote_ident};

impl SqlRenderer for SchemaOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            SchemaOperation::Create { name } => vec![RenderedSql {
                sql: format!("CREATE SCHEMA {};", quote_ident(name)),
                safety: Safety::Safe,
            }],
            SchemaOperation::Drop { name } => vec![RenderedSql {
                sql: format!("DROP SCHEMA {};", quote_ident(name)),
                safety: Safety::Destructive,
            }],
            SchemaOperation::Comment(op) => op.to_sql(),
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            SchemaOperation::Create { name } | SchemaOperation::Drop { name } => {
                DbObjectId::Schema { name: name.clone() }
            }
            SchemaOperation::Comment(op) => op.db_object_id(),
        }
    }

    fn is_destructive(&self) -> bool {
        matches!(self, SchemaOperation::Drop { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_create_schema() {
        let op = SchemaOperation::Create {
            name: "app".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert_eq!(rendered[0].sql, "CREATE SCHEMA \"app\";");
        assert_eq!(rendered[0].safety, Safety::Safe);
    }

    #[test]
    fn test_render_create_schema_with_special_chars() {
        let op = SchemaOperation::Create {
            name: "my-schema".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(rendered[0].sql, "CREATE SCHEMA \"my-schema\";");
    }

    #[test]
    fn test_render_drop_schema() {
        let op = SchemaOperation::Drop {
            name: "old_schema".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert_eq!(rendered[0].sql, "DROP SCHEMA \"old_schema\";");
        assert_eq!(rendered[0].safety, Safety::Destructive);
    }

    #[test]
    fn test_is_destructive() {
        let create = SchemaOperation::Create {
            name: "test".to_string(),
        };
        let drop = SchemaOperation::Drop {
            name: "test".to_string(),
        };

        assert!(!create.is_destructive());
        assert!(drop.is_destructive());
    }

    #[test]
    fn test_db_object_id() {
        let op = SchemaOperation::Create {
            name: "myschema".to_string(),
        };
        assert_eq!(
            op.db_object_id(),
            DbObjectId::Schema {
                name: "myschema".to_string()
            }
        );
    }
}
