//! SQL rendering for view operations

use crate::catalog::id::DbObjectId;
use crate::diff::operations::ViewOperation;
use crate::render::{RenderedSql, Safety, SqlRenderer, quote_ident};

impl SqlRenderer for ViewOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            ViewOperation::Create {
                schema,
                name,
                definition,
                security_invoker,
                security_barrier,
            } => {
                let mut with_options = Vec::new();
                if *security_invoker {
                    with_options.push("security_invoker = true");
                }
                if *security_barrier {
                    with_options.push("security_barrier = true");
                }

                let with_clause = if !with_options.is_empty() {
                    format!(" WITH ({})", with_options.join(", "))
                } else {
                    String::new()
                };

                vec![RenderedSql {
                    sql: format!(
                        "CREATE VIEW {}.{}{} AS\n{};",
                        quote_ident(schema),
                        quote_ident(name),
                        with_clause,
                        definition.trim_end_matches(';'),
                    ),
                    safety: Safety::Safe,
                }]
            }
            ViewOperation::Drop { schema, name } => vec![RenderedSql {
                sql: format!("DROP VIEW {}.{};", quote_ident(schema), quote_ident(name)),
                safety: Safety::Safe,
            }],
            ViewOperation::Replace {
                schema,
                name,
                definition,
            } => vec![RenderedSql {
                sql: format!(
                    "CREATE OR REPLACE VIEW {}.{} AS\n{};",
                    quote_ident(schema),
                    quote_ident(name),
                    definition.trim_end_matches(';'),
                ),
                safety: Safety::Safe,
            }],
            ViewOperation::SetOption {
                schema,
                name,
                option,
                enabled,
            } => {
                let sql = if *enabled {
                    format!(
                        "ALTER VIEW {}.{} SET ({} = on);",
                        quote_ident(schema),
                        quote_ident(name),
                        option.as_str()
                    )
                } else {
                    format!(
                        "ALTER VIEW {}.{} RESET ({});",
                        quote_ident(schema),
                        quote_ident(name),
                        option.as_str()
                    )
                };
                vec![RenderedSql {
                    sql,
                    safety: Safety::Safe,
                }]
            }
            ViewOperation::Comment(op) => op.to_sql(),
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            ViewOperation::Create { schema, name, .. }
            | ViewOperation::Drop { schema, name }
            | ViewOperation::Replace { schema, name, .. }
            | ViewOperation::SetOption { schema, name, .. } => DbObjectId::View {
                schema: schema.clone(),
                name: name.clone(),
            },
            ViewOperation::Comment(op) => op.db_object_id(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_create_view() {
        let op = ViewOperation::Create {
            schema: "public".to_string(),
            name: "active_users".to_string(),
            definition: "SELECT * FROM users WHERE active = true".to_string(),
            security_invoker: false,
            security_barrier: false,
        };
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert_eq!(
            rendered[0].sql,
            "CREATE VIEW \"public\".\"active_users\" AS\nSELECT * FROM users WHERE active = true;"
        );
        assert_eq!(rendered[0].safety, Safety::Safe);
    }

    #[test]
    fn test_render_create_view_strips_trailing_semicolon() {
        let op = ViewOperation::Create {
            schema: "app".to_string(),
            name: "v".to_string(),
            definition: "SELECT 1;".to_string(),
            security_invoker: false,
            security_barrier: false,
        };
        let rendered = op.to_sql();
        assert_eq!(rendered[0].sql, "CREATE VIEW \"app\".\"v\" AS\nSELECT 1;");
    }

    #[test]
    fn test_render_drop_view() {
        let op = ViewOperation::Drop {
            schema: "public".to_string(),
            name: "old_view".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert_eq!(rendered[0].sql, "DROP VIEW \"public\".\"old_view\";");
        assert_eq!(rendered[0].safety, Safety::Safe);
    }

    #[test]
    fn test_render_replace_view() {
        let op = ViewOperation::Replace {
            schema: "public".to_string(),
            name: "user_summary".to_string(),
            definition: "SELECT id, name FROM users".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert_eq!(
            rendered[0].sql,
            "CREATE OR REPLACE VIEW \"public\".\"user_summary\" AS\nSELECT id, name FROM users;"
        );
        assert_eq!(rendered[0].safety, Safety::Safe);
    }

    #[test]
    fn test_has_destructive_sql() {
        let create = ViewOperation::Create {
            schema: "s".to_string(),
            name: "v".to_string(),
            definition: "SELECT 1".to_string(),
            security_invoker: false,
            security_barrier: false,
        };
        let drop = ViewOperation::Drop {
            schema: "s".to_string(),
            name: "v".to_string(),
        };
        let replace = ViewOperation::Replace {
            schema: "s".to_string(),
            name: "v".to_string(),
            definition: "SELECT 2".to_string(),
        };

        // Views can be recreated from schema, so DROP VIEW is not destructive
        assert!(
            !create
                .to_sql()
                .iter()
                .any(|s| s.safety == Safety::Destructive)
        );
        assert!(
            !drop
                .to_sql()
                .iter()
                .any(|s| s.safety == Safety::Destructive)
        );
        assert!(
            !replace
                .to_sql()
                .iter()
                .any(|s| s.safety == Safety::Destructive)
        );
    }

    #[test]
    fn test_db_object_id() {
        let op = ViewOperation::Create {
            schema: "app".to_string(),
            name: "myview".to_string(),
            definition: "SELECT 1".to_string(),
            security_invoker: false,
            security_barrier: false,
        };
        assert_eq!(
            op.db_object_id(),
            DbObjectId::View {
                schema: "app".to_string(),
                name: "myview".to_string()
            }
        );
    }
}
