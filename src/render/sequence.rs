//! SQL rendering for sequence operations

use crate::catalog::id::DbObjectId;
use crate::diff::operations::SequenceOperation;
use crate::render::{RenderedSql, Safety, SqlRenderer, quote_ident};

impl SqlRenderer for SequenceOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            SequenceOperation::Create {
                schema,
                name,
                data_type,
                start_value,
                min_value,
                max_value,
                increment,
                cycle,
            } => vec![RenderedSql {
                sql: format!(
                    "CREATE SEQUENCE {}.{} AS {} START {} MINVALUE {} MAXVALUE {} INCREMENT {}{};",
                    quote_ident(schema),
                    quote_ident(name),
                    data_type,
                    start_value,
                    min_value,
                    max_value,
                    increment,
                    if *cycle { " CYCLE" } else { " NO CYCLE" }
                ),
                safety: Safety::Safe,
            }],
            SequenceOperation::Drop { schema, name } => vec![RenderedSql {
                sql: format!(
                    "DROP SEQUENCE {}.{};",
                    quote_ident(schema),
                    quote_ident(name)
                ),
                safety: Safety::Destructive,
            }],
            SequenceOperation::AlterOwnership {
                schema,
                name,
                owned_by,
            } => vec![RenderedSql {
                sql: if owned_by == "NONE" {
                    format!(
                        "ALTER SEQUENCE {}.{} OWNED BY NONE;",
                        quote_ident(schema),
                        quote_ident(name)
                    )
                } else {
                    let parts: Vec<&str> = owned_by.split('.').collect();
                    if parts.len() == 3 {
                        format!(
                            "ALTER SEQUENCE {}.{} OWNED BY {}.{}.{};",
                            quote_ident(schema),
                            quote_ident(name),
                            quote_ident(parts[0]),
                            quote_ident(parts[1]),
                            quote_ident(parts[2])
                        )
                    } else {
                        format!(
                            "ALTER SEQUENCE {}.{} OWNED BY {};",
                            quote_ident(schema),
                            quote_ident(name),
                            owned_by
                        )
                    }
                },
                safety: Safety::Safe,
            }],
            SequenceOperation::Comment(op) => op.to_sql(),
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            SequenceOperation::Create { schema, name, .. }
            | SequenceOperation::Drop { schema, name }
            | SequenceOperation::AlterOwnership { schema, name, .. } => DbObjectId::Sequence {
                schema: schema.clone(),
                name: name.clone(),
            },
            SequenceOperation::Comment(op) => op.db_object_id(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_create_sequence() {
        let op = SequenceOperation::Create {
            schema: "public".to_string(),
            name: "user_id_seq".to_string(),
            data_type: "bigint".to_string(),
            start_value: 1,
            min_value: 1,
            max_value: 9223372036854775807,
            increment: 1,
            cycle: false,
        };
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert_eq!(
            rendered[0].sql,
            "CREATE SEQUENCE \"public\".\"user_id_seq\" AS bigint START 1 MINVALUE 1 MAXVALUE 9223372036854775807 INCREMENT 1 NO CYCLE;"
        );
        assert_eq!(rendered[0].safety, Safety::Safe);
    }

    #[test]
    fn test_render_create_sequence_with_cycle() {
        let op = SequenceOperation::Create {
            schema: "app".to_string(),
            name: "counter_seq".to_string(),
            data_type: "integer".to_string(),
            start_value: 100,
            min_value: 1,
            max_value: 1000,
            increment: 10,
            cycle: true,
        };
        let rendered = op.to_sql();
        assert!(rendered[0].sql.ends_with(" CYCLE;"));
    }

    #[test]
    fn test_render_drop_sequence() {
        let op = SequenceOperation::Drop {
            schema: "public".to_string(),
            name: "old_seq".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert_eq!(rendered[0].sql, "DROP SEQUENCE \"public\".\"old_seq\";");
        assert_eq!(rendered[0].safety, Safety::Destructive);
    }

    #[test]
    fn test_render_alter_ownership_none() {
        let op = SequenceOperation::AlterOwnership {
            schema: "public".to_string(),
            name: "orphan_seq".to_string(),
            owned_by: "NONE".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(
            rendered[0].sql,
            "ALTER SEQUENCE \"public\".\"orphan_seq\" OWNED BY NONE;"
        );
    }

    #[test]
    fn test_render_alter_ownership_table_column() {
        let op = SequenceOperation::AlterOwnership {
            schema: "public".to_string(),
            name: "users_id_seq".to_string(),
            owned_by: "public.users.id".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(
            rendered[0].sql,
            "ALTER SEQUENCE \"public\".\"users_id_seq\" OWNED BY \"public\".\"users\".\"id\";"
        );
        assert_eq!(rendered[0].safety, Safety::Safe);
    }

    #[test]
    fn test_has_destructive_sql() {
        let create = SequenceOperation::Create {
            schema: "s".to_string(),
            name: "seq".to_string(),
            data_type: "integer".to_string(),
            start_value: 1,
            min_value: 1,
            max_value: 2147483647,
            increment: 1,
            cycle: false,
        };
        let drop = SequenceOperation::Drop {
            schema: "s".to_string(),
            name: "seq".to_string(),
        };
        let alter = SequenceOperation::AlterOwnership {
            schema: "s".to_string(),
            name: "seq".to_string(),
            owned_by: "NONE".to_string(),
        };

        // DROP SEQUENCE loses the current sequence value, so it's destructive
        assert!(
            !create
                .to_sql()
                .iter()
                .any(|s| s.safety == Safety::Destructive)
        );
        assert!(
            drop.to_sql()
                .iter()
                .any(|s| s.safety == Safety::Destructive)
        );
        assert!(
            !alter
                .to_sql()
                .iter()
                .any(|s| s.safety == Safety::Destructive)
        );
    }

    #[test]
    fn test_db_object_id() {
        let op = SequenceOperation::Create {
            schema: "app".to_string(),
            name: "myseq".to_string(),
            data_type: "bigint".to_string(),
            start_value: 1,
            min_value: 1,
            max_value: 9223372036854775807,
            increment: 1,
            cycle: false,
        };
        assert_eq!(
            op.db_object_id(),
            DbObjectId::Sequence {
                schema: "app".to_string(),
                name: "myseq".to_string()
            }
        );
    }
}
