//! SQL rendering for comment operations

use crate::catalog::id::DbObjectId;
use crate::catalog::target::{AttrTarget, SubObject};
use crate::diff::operations::CommentOperation;
use crate::render::{RenderedSql, SqlRenderer, quote_ident, render_comment_sql};

impl SqlRenderer for CommentOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            CommentOperation::Set { target, comment } => {
                vec![render_comment_sql(
                    comment_keyword(target),
                    &comment_reference(target),
                    Some(comment),
                )]
            }
            CommentOperation::Drop { target } => {
                vec![render_comment_sql(
                    comment_keyword(target),
                    &comment_reference(target),
                    None,
                )]
            }
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            CommentOperation::Set { target, .. } | CommentOperation::Drop { target } => {
                DbObjectId::Comment {
                    object_id: Box::new(target.db_object_id()),
                }
            }
        }
    }
}

/// The `COMMENT ON <keyword>` keyword for a target. A column sub-object is always
/// `COLUMN`; otherwise it derives from the object kind.
fn comment_keyword(target: &AttrTarget) -> &'static str {
    if target.sub.is_some() {
        return "COLUMN";
    }
    match &target.object {
        DbObjectId::Schema { .. } => "SCHEMA",
        DbObjectId::Table { .. } => "TABLE",
        DbObjectId::View { .. } => "VIEW",
        DbObjectId::Type { .. } => "TYPE",
        DbObjectId::Domain { .. } => "DOMAIN",
        DbObjectId::Function { .. } => "FUNCTION",
        DbObjectId::Procedure { .. } => "PROCEDURE",
        DbObjectId::Aggregate { .. } => "AGGREGATE",
        DbObjectId::Sequence { .. } => "SEQUENCE",
        DbObjectId::Index { .. } => "INDEX",
        DbObjectId::Constraint { .. } => "CONSTRAINT",
        DbObjectId::Trigger { .. } => "TRIGGER",
        DbObjectId::Policy { .. } => "POLICY",
        DbObjectId::Extension { .. } => "EXTENSION",
        // Not used as comment targets; fall back to a neutral keyword.
        DbObjectId::Grant { .. } | DbObjectId::Comment { .. } | DbObjectId::Column { .. } => "",
    }
}

/// The object reference that follows the keyword, e.g. `"s"."t"`,
/// `"s"."t"."col"`, `"name" ON "s"."t"`, or `"s"."f"(args)`.
fn comment_reference(target: &AttrTarget) -> String {
    if let Some(SubObject::Column { name }) = &target.sub {
        // COLUMN relation.column — the parent is a table, view, or composite type.
        if let Some((schema, relation)) = relation_parts(&target.object) {
            return format!(
                "{}.{}.{}",
                quote_ident(schema),
                quote_ident(relation),
                quote_ident(name)
            );
        }
    }

    match &target.object {
        DbObjectId::Schema { name } | DbObjectId::Extension { name } => quote_ident(name),
        DbObjectId::Table { schema, name }
        | DbObjectId::View { schema, name }
        | DbObjectId::Type { schema, name }
        | DbObjectId::Domain { schema, name }
        | DbObjectId::Sequence { schema, name }
        | DbObjectId::Index { schema, name } => {
            format!("{}.{}", quote_ident(schema), quote_ident(name))
        }
        DbObjectId::Function {
            schema,
            name,
            arguments,
        }
        | DbObjectId::Procedure {
            schema,
            name,
            arguments,
        }
        | DbObjectId::Aggregate {
            schema,
            name,
            arguments,
        } => format!(
            "{}.{}({})",
            quote_ident(schema),
            quote_ident(name),
            arguments
        ),
        DbObjectId::Constraint {
            schema,
            table,
            name,
        }
        | DbObjectId::Trigger {
            schema,
            table,
            name,
        }
        | DbObjectId::Policy {
            schema,
            table,
            name,
        } => format!(
            "{} ON {}.{}",
            quote_ident(name),
            quote_ident(schema),
            quote_ident(table)
        ),
        DbObjectId::Grant { .. } | DbObjectId::Comment { .. } | DbObjectId::Column { .. } => {
            String::new()
        }
    }
}

/// Extract (schema, relation_name) from an object that can own columns.
fn relation_parts(object: &DbObjectId) -> Option<(&str, &str)> {
    match object {
        DbObjectId::Table { schema, name }
        | DbObjectId::View { schema, name }
        | DbObjectId::Type { schema, name } => Some((schema, name)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render::Safety;

    fn schema_target(name: &str) -> AttrTarget {
        AttrTarget::object(DbObjectId::Schema {
            name: name.to_string(),
        })
    }

    #[test]
    fn test_render_set_comment() {
        let op = CommentOperation::Set {
            target: schema_target("app"),
            comment: "Application schema".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert_eq!(
            rendered[0].sql,
            "COMMENT ON SCHEMA \"app\" IS 'Application schema';"
        );
    }

    #[test]
    fn test_render_set_comment_with_quotes() {
        let op = CommentOperation::Set {
            target: schema_target("test"),
            comment: "Schema for 'testing' purposes".to_string(),
        };
        let rendered = op.to_sql();
        assert!(rendered[0].sql.contains("''testing''"));
    }

    #[test]
    fn test_render_drop_comment() {
        let op = CommentOperation::Drop {
            target: schema_target("old_schema"),
        };
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert_eq!(rendered[0].sql, "COMMENT ON SCHEMA \"old_schema\" IS NULL;");
    }

    #[test]
    fn test_render_column_comment() {
        let op = CommentOperation::Set {
            target: AttrTarget::column(
                DbObjectId::Table {
                    schema: "public".to_string(),
                    name: "users".to_string(),
                },
                "email",
            ),
            comment: "the email".to_string(),
        };
        assert_eq!(
            op.to_sql()[0].sql,
            "COMMENT ON COLUMN \"public\".\"users\".\"email\" IS 'the email';"
        );
    }

    #[test]
    fn test_render_constraint_comment_two_part() {
        let op = CommentOperation::Set {
            target: AttrTarget::object(DbObjectId::Constraint {
                schema: "public".to_string(),
                table: "orders".to_string(),
                name: "orders_pk".to_string(),
            }),
            comment: "pk".to_string(),
        };
        assert_eq!(
            op.to_sql()[0].sql,
            "COMMENT ON CONSTRAINT \"orders_pk\" ON \"public\".\"orders\" IS 'pk';"
        );
    }

    #[test]
    fn test_render_procedure_comment() {
        let op = CommentOperation::Set {
            target: AttrTarget::object(DbObjectId::Procedure {
                schema: "public".to_string(),
                name: "do_thing".to_string(),
                arguments: "integer".to_string(),
            }),
            comment: "runs".to_string(),
        };
        assert_eq!(
            op.to_sql()[0].sql,
            "COMMENT ON PROCEDURE \"public\".\"do_thing\"(integer) IS 'runs';"
        );
    }

    #[test]
    fn test_has_destructive_sql() {
        let set_op = CommentOperation::Set {
            target: schema_target("test"),
            comment: "Test".to_string(),
        };
        let drop_op = CommentOperation::Drop {
            target: schema_target("test"),
        };
        assert!(
            !set_op
                .to_sql()
                .iter()
                .any(|s| s.safety == Safety::Destructive)
        );
        assert!(
            !drop_op
                .to_sql()
                .iter()
                .any(|s| s.safety == Safety::Destructive)
        );
    }

    #[test]
    fn test_db_object_id() {
        let op = CommentOperation::Set {
            target: schema_target("myschema"),
            comment: "Test".to_string(),
        };
        assert_eq!(
            op.db_object_id(),
            DbObjectId::Comment {
                object_id: Box::new(DbObjectId::Schema {
                    name: "myschema".to_string()
                })
            }
        );
    }
}
