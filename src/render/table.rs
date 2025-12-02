//! SQL rendering for table operations

use crate::catalog::id::DbObjectId;
use crate::diff::operations::{ColumnAction, TableOperation};
use crate::render::{RenderedSql, Safety, SqlRenderer, quote_ident};

impl SqlRenderer for TableOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            TableOperation::Create {
                schema,
                name,
                columns,
                primary_key,
            } => {
                // Create a temporary Table struct to use with shared rendering
                let table = crate::catalog::table::Table::new(
                    schema.clone(),
                    name.clone(),
                    columns.clone(),
                    primary_key.clone(),
                    None,   // comment
                    vec![], // dependencies
                );

                vec![RenderedSql {
                    sql: crate::render::sql::render_create_table(&table),
                    safety: Safety::Safe,
                }]
            }
            TableOperation::Drop { schema, name } => vec![RenderedSql {
                sql: format!("DROP TABLE {}.{};", quote_ident(schema), quote_ident(name)),
                safety: Safety::Destructive,
            }],
            TableOperation::Alter {
                schema,
                name,
                actions,
            } => actions
                .iter()
                .map(|action| render_column_action(action, schema, name))
                .collect(),
            TableOperation::Comment(op) => op.to_sql(),
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            TableOperation::Create { schema, name, .. }
            | TableOperation::Drop { schema, name }
            | TableOperation::Alter { schema, name, .. } => DbObjectId::Table {
                schema: schema.clone(),
                name: name.clone(),
            },
            TableOperation::Comment(op) => op.db_object_id(),
        }
    }

    fn is_destructive(&self) -> bool {
        matches!(self, TableOperation::Drop { .. })
    }
}

fn render_column_action(action: &ColumnAction, schema: &str, table: &str) -> RenderedSql {
    match action {
        ColumnAction::Add { column } => {
            let default_clause = match &column.default {
                Some(default) => format!(" DEFAULT {}", default),
                None => String::new(),
            };

            let not_null_clause = if column.not_null { " NOT NULL" } else { "" };

            let generated_clause = match &column.generated {
                Some(expr) => format!(" GENERATED ALWAYS AS ({}) STORED", expr),
                None => String::new(),
            };

            RenderedSql {
                sql: format!(
                    "ALTER TABLE {}.{} ADD COLUMN {} {}{}{}{};",
                    quote_ident(schema),
                    quote_ident(table),
                    quote_ident(&column.name),
                    column.data_type,
                    generated_clause,
                    default_clause,
                    not_null_clause
                ),
                safety: Safety::Safe,
            }
        }
        ColumnAction::Drop { name } => RenderedSql {
            sql: format!(
                "ALTER TABLE {}.{} DROP COLUMN IF EXISTS {};",
                quote_ident(schema),
                quote_ident(table),
                quote_ident(name)
            ),
            safety: Safety::Destructive,
        },
        ColumnAction::SetNotNull { name } => RenderedSql {
            sql: format!(
                "ALTER TABLE {}.{} ALTER COLUMN {} SET NOT NULL;",
                quote_ident(schema),
                quote_ident(table),
                quote_ident(name)
            ),
            safety: Safety::Safe,
        },
        ColumnAction::DropNotNull { name } => RenderedSql {
            sql: format!(
                "ALTER TABLE {}.{} ALTER COLUMN {} DROP NOT NULL;",
                quote_ident(schema),
                quote_ident(table),
                quote_ident(name)
            ),
            safety: Safety::Safe,
        },
        ColumnAction::SetDefault { name, default } => RenderedSql {
            sql: format!(
                "ALTER TABLE {}.{} ALTER COLUMN {} SET DEFAULT {};",
                quote_ident(schema),
                quote_ident(table),
                quote_ident(name),
                default
            ),
            safety: Safety::Safe,
        },
        ColumnAction::DropDefault { name } => RenderedSql {
            sql: format!(
                "ALTER TABLE {}.{} ALTER COLUMN {} DROP DEFAULT;",
                quote_ident(schema),
                quote_ident(table),
                quote_ident(name)
            ),
            safety: Safety::Safe,
        },
        ColumnAction::DropGenerated { name } => RenderedSql {
            sql: format!(
                "ALTER TABLE {}.{} ALTER COLUMN {} DROP EXPRESSION;",
                quote_ident(schema),
                quote_ident(table),
                quote_ident(name)
            ),
            safety: Safety::Destructive,
        },
        ColumnAction::AlterType { name, new_type } => RenderedSql {
            sql: format!(
                "ALTER TABLE {}.{} ALTER COLUMN {} TYPE {};",
                quote_ident(schema),
                quote_ident(table),
                quote_ident(name),
                new_type
            ),
            safety: Safety::Destructive,
        },
        ColumnAction::AddPrimaryKey { constraint } => {
            let pk_cols = constraint
                .columns
                .iter()
                .map(|col| quote_ident(col))
                .collect::<Vec<_>>()
                .join(", ");

            RenderedSql {
                sql: format!(
                    "ALTER TABLE {}.{} ADD CONSTRAINT {} PRIMARY KEY ({});",
                    quote_ident(schema),
                    quote_ident(table),
                    quote_ident(&constraint.name),
                    pk_cols
                ),
                safety: Safety::Safe,
            }
        }
        ColumnAction::DropPrimaryKey { name } => RenderedSql {
            sql: format!(
                "ALTER TABLE {}.{} DROP CONSTRAINT {};",
                quote_ident(schema),
                quote_ident(table),
                quote_ident(name)
            ),
            safety: Safety::Destructive,
        },
        ColumnAction::Comment(op) => op.to_sql()[0].clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::table::{Column, PrimaryKey};

    fn create_simple_column() -> Column {
        Column {
            name: "email".to_string(),
            data_type: "text".to_string(),
            default: None,
            not_null: true,
            generated: None,
            comment: None,
            depends_on: vec![],
        }
    }

    #[test]
    fn test_render_create_table() {
        let columns = vec![
            Column {
                name: "id".to_string(),
                data_type: "integer".to_string(),
                default: None,
                not_null: true,
                generated: None,
                comment: None,
                depends_on: vec![],
            },
            create_simple_column(),
        ];

        let op = TableOperation::Create {
            schema: "public".to_string(),
            name: "users".to_string(),
            columns,
            primary_key: Some(PrimaryKey {
                name: "users_pkey".to_string(),
                columns: vec!["id".to_string()],
            }),
        };
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert!(rendered[0].sql.contains("CREATE TABLE"));
        assert!(rendered[0].sql.contains("\"public\".\"users\""));
        assert!(rendered[0].sql.contains("PRIMARY KEY"));
        assert_eq!(rendered[0].safety, Safety::Safe);
    }

    #[test]
    fn test_render_drop_table() {
        let op = TableOperation::Drop {
            schema: "public".to_string(),
            name: "old_table".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert_eq!(rendered[0].sql, "DROP TABLE \"public\".\"old_table\";");
        assert_eq!(rendered[0].safety, Safety::Destructive);
    }

    #[test]
    fn test_render_alter_add_column() {
        let op = TableOperation::Alter {
            schema: "public".to_string(),
            name: "users".to_string(),
            actions: vec![ColumnAction::Add {
                column: create_simple_column(),
            }],
        };
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert!(rendered[0].sql.contains("ADD COLUMN"));
        assert!(rendered[0].sql.contains("\"email\""));
        assert!(rendered[0].sql.contains("NOT NULL"));
    }

    #[test]
    fn test_render_alter_add_column_with_default() {
        let column = Column {
            name: "status".to_string(),
            data_type: "text".to_string(),
            default: Some("'active'".to_string()),
            not_null: false,
            generated: None,
            comment: None,
            depends_on: vec![],
        };
        let op = TableOperation::Alter {
            schema: "app".to_string(),
            name: "items".to_string(),
            actions: vec![ColumnAction::Add { column }],
        };
        let rendered = op.to_sql();
        assert!(rendered[0].sql.contains("DEFAULT 'active'"));
    }

    #[test]
    fn test_render_alter_drop_column() {
        let op = TableOperation::Alter {
            schema: "public".to_string(),
            name: "users".to_string(),
            actions: vec![ColumnAction::Drop {
                name: "old_col".to_string(),
            }],
        };
        let rendered = op.to_sql();
        assert!(rendered[0].sql.contains("DROP COLUMN IF EXISTS"));
        assert_eq!(rendered[0].safety, Safety::Destructive);
    }

    #[test]
    fn test_render_alter_set_not_null() {
        let op = TableOperation::Alter {
            schema: "public".to_string(),
            name: "users".to_string(),
            actions: vec![ColumnAction::SetNotNull {
                name: "email".to_string(),
            }],
        };
        let rendered = op.to_sql();
        assert_eq!(
            rendered[0].sql,
            "ALTER TABLE \"public\".\"users\" ALTER COLUMN \"email\" SET NOT NULL;"
        );
    }

    #[test]
    fn test_render_alter_drop_not_null() {
        let op = TableOperation::Alter {
            schema: "public".to_string(),
            name: "users".to_string(),
            actions: vec![ColumnAction::DropNotNull {
                name: "optional_field".to_string(),
            }],
        };
        let rendered = op.to_sql();
        assert!(rendered[0].sql.contains("DROP NOT NULL"));
    }

    #[test]
    fn test_render_alter_set_default() {
        let op = TableOperation::Alter {
            schema: "public".to_string(),
            name: "users".to_string(),
            actions: vec![ColumnAction::SetDefault {
                name: "created_at".to_string(),
                default: "NOW()".to_string(),
            }],
        };
        let rendered = op.to_sql();
        assert!(rendered[0].sql.contains("SET DEFAULT NOW()"));
    }

    #[test]
    fn test_render_alter_drop_default() {
        let op = TableOperation::Alter {
            schema: "public".to_string(),
            name: "users".to_string(),
            actions: vec![ColumnAction::DropDefault {
                name: "status".to_string(),
            }],
        };
        let rendered = op.to_sql();
        assert!(rendered[0].sql.contains("DROP DEFAULT"));
    }

    #[test]
    fn test_render_alter_type() {
        let op = TableOperation::Alter {
            schema: "public".to_string(),
            name: "users".to_string(),
            actions: vec![ColumnAction::AlterType {
                name: "count".to_string(),
                new_type: "bigint".to_string(),
            }],
        };
        let rendered = op.to_sql();
        assert!(rendered[0].sql.contains("TYPE bigint"));
        assert_eq!(rendered[0].safety, Safety::Destructive);
    }

    #[test]
    fn test_render_add_primary_key() {
        let op = TableOperation::Alter {
            schema: "public".to_string(),
            name: "users".to_string(),
            actions: vec![ColumnAction::AddPrimaryKey {
                constraint: PrimaryKey {
                    name: "users_pkey".to_string(),
                    columns: vec!["id".to_string()],
                },
            }],
        };
        let rendered = op.to_sql();
        assert!(rendered[0].sql.contains("ADD CONSTRAINT"));
        assert!(rendered[0].sql.contains("PRIMARY KEY"));
    }

    #[test]
    fn test_render_drop_primary_key() {
        let op = TableOperation::Alter {
            schema: "public".to_string(),
            name: "users".to_string(),
            actions: vec![ColumnAction::DropPrimaryKey {
                name: "users_pkey".to_string(),
            }],
        };
        let rendered = op.to_sql();
        assert!(rendered[0].sql.contains("DROP CONSTRAINT"));
        assert_eq!(rendered[0].safety, Safety::Destructive);
    }

    #[test]
    fn test_render_multiple_alter_actions() {
        let op = TableOperation::Alter {
            schema: "public".to_string(),
            name: "users".to_string(),
            actions: vec![
                ColumnAction::Add {
                    column: create_simple_column(),
                },
                ColumnAction::SetNotNull {
                    name: "name".to_string(),
                },
            ],
        };
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 2);
        assert!(rendered[0].sql.contains("ADD COLUMN"));
        assert!(rendered[1].sql.contains("SET NOT NULL"));
    }

    #[test]
    fn test_is_destructive() {
        let create = TableOperation::Create {
            schema: "s".to_string(),
            name: "t".to_string(),
            columns: vec![],
            primary_key: None,
        };
        let drop = TableOperation::Drop {
            schema: "s".to_string(),
            name: "t".to_string(),
        };
        let alter = TableOperation::Alter {
            schema: "s".to_string(),
            name: "t".to_string(),
            actions: vec![],
        };

        assert!(!create.is_destructive());
        assert!(drop.is_destructive());
        assert!(!alter.is_destructive());
    }

    #[test]
    fn test_db_object_id() {
        let op = TableOperation::Create {
            schema: "app".to_string(),
            name: "mytable".to_string(),
            columns: vec![],
            primary_key: None,
        };
        assert_eq!(
            op.db_object_id(),
            DbObjectId::Table {
                schema: "app".to_string(),
                name: "mytable".to_string()
            }
        );
    }
}
