//! SQL rendering for extension operations

use crate::catalog::extension::Extension;
use crate::catalog::id::DbObjectId;
use crate::diff::operations::{CommentOperation, ExtensionIdentifier, ExtensionOperation};
use crate::render::{RenderedSql, SqlRenderer};

impl SqlRenderer for ExtensionOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            ExtensionOperation::Create { extension } => {
                vec![render_create_extension(extension)]
            }
            ExtensionOperation::Drop { identifier } => {
                vec![render_drop_extension(identifier)]
            }
            ExtensionOperation::Comment(comment_op) => comment_op.to_sql(),
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            ExtensionOperation::Create { extension } => DbObjectId::Extension {
                name: extension.name.clone(),
            },
            ExtensionOperation::Drop { identifier } => DbObjectId::Extension {
                name: identifier.name.clone(),
            },
            ExtensionOperation::Comment(comment_op) => match comment_op {
                CommentOperation::Set { target, .. } | CommentOperation::Drop { target } => {
                    DbObjectId::Extension {
                        name: target.name.clone(),
                    }
                }
            },
        }
    }

    fn is_destructive(&self) -> bool {
        matches!(self, ExtensionOperation::Drop { .. })
    }
}

fn render_create_extension(extension: &Extension) -> RenderedSql {
    let mut sql = format!("CREATE EXTENSION IF NOT EXISTS \"{}\"", extension.name);

    // Add schema if not the default
    if extension.schema != "public" {
        sql.push_str(&format!(" SCHEMA \"{}\"", extension.schema));
    }

    sql.push(';');

    // Note: We don't include VERSION in the rendered SQL to avoid conflicts
    // with existing installations. PostgreSQL will use the default/installed version.

    RenderedSql::new(sql)
}

fn render_drop_extension(identifier: &ExtensionIdentifier) -> RenderedSql {
    let sql = format!("DROP EXTENSION IF EXISTS \"{}\";", identifier.name);
    RenderedSql::destructive(sql)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render::Safety;

    fn create_test_extension() -> Extension {
        Extension {
            name: "uuid-ossp".to_string(),
            schema: "public".to_string(),
            version: "1.1".to_string(),
            relocatable: true,
            comment: None,
            depends_on: vec![],
        }
    }

    #[test]
    fn test_render_create_extension() {
        let extension = create_test_extension();
        let rendered = render_create_extension(&extension);

        assert_eq!(
            rendered.sql,
            "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";"
        );
        assert_eq!(rendered.safety, Safety::Safe);
    }

    #[test]
    fn test_render_create_extension_with_custom_schema() {
        let mut extension = create_test_extension();
        extension.schema = "utils".to_string();

        let rendered = render_create_extension(&extension);
        assert!(rendered.sql.contains("SCHEMA \"utils\""));
    }

    #[test]
    fn test_render_drop_extension() {
        let identifier = ExtensionIdentifier::new("uuid-ossp".to_string());
        let rendered = render_drop_extension(&identifier);

        assert_eq!(rendered.sql, "DROP EXTENSION IF EXISTS \"uuid-ossp\";");
        assert_eq!(rendered.safety, Safety::Destructive);
    }

    #[test]
    fn test_render_create_operation() {
        let extension = create_test_extension();
        let operation = ExtensionOperation::Create {
            extension: extension.clone(),
        };

        let rendered_list = operation.to_sql();
        assert_eq!(rendered_list.len(), 1);
        assert!(
            rendered_list[0]
                .sql
                .contains("CREATE EXTENSION IF NOT EXISTS")
        );
        assert!(rendered_list[0].sql.contains("uuid-ossp"));
    }

    #[test]
    fn test_render_drop_operation() {
        let identifier = ExtensionIdentifier::new("uuid-ossp".to_string());
        let operation = ExtensionOperation::Drop { identifier };

        let rendered_list = operation.to_sql();
        assert_eq!(rendered_list.len(), 1);
        assert_eq!(
            rendered_list[0].sql,
            "DROP EXTENSION IF EXISTS \"uuid-ossp\";"
        );
    }
}
