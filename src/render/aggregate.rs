//! SQL rendering for aggregate operations

use crate::catalog::aggregate::Aggregate;
use crate::catalog::id::DbObjectId;
use crate::diff::operations::{AggregateIdentifier, AggregateOperation, CommentOperation};
use crate::render::{RenderedSql, SqlRenderer};

impl SqlRenderer for AggregateOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            AggregateOperation::Create { aggregate } => {
                vec![render_create_aggregate(aggregate)]
            }
            AggregateOperation::Drop { identifier } => {
                vec![render_drop_aggregate(identifier)]
            }
            AggregateOperation::Replace { new_aggregate, .. } => {
                // For replace, we drop and recreate
                vec![
                    render_drop_aggregate(&AggregateIdentifier::from_aggregate(new_aggregate)),
                    render_create_aggregate(new_aggregate),
                ]
            }
            AggregateOperation::Comment(comment_op) => comment_op.to_sql(),
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            AggregateOperation::Create { aggregate } => DbObjectId::Aggregate {
                schema: aggregate.schema.clone(),
                name: aggregate.name.clone(),
                arguments: aggregate.arguments.clone(),
            },
            AggregateOperation::Drop { identifier } => DbObjectId::Aggregate {
                schema: identifier.schema.clone(),
                name: identifier.name.clone(),
                arguments: identifier.arguments.clone(),
            },
            AggregateOperation::Replace { new_aggregate, .. } => DbObjectId::Aggregate {
                schema: new_aggregate.schema.clone(),
                name: new_aggregate.name.clone(),
                arguments: new_aggregate.arguments.clone(),
            },
            AggregateOperation::Comment(comment_op) => match comment_op {
                CommentOperation::Set { target, .. } | CommentOperation::Drop { target } => {
                    DbObjectId::Aggregate {
                        schema: target.schema.clone(),
                        name: target.name.clone(),
                        arguments: target.arguments.clone(),
                    }
                }
            },
        }
    }

    fn is_destructive(&self) -> bool {
        matches!(self, AggregateOperation::Drop { .. })
    }
}

fn render_create_aggregate(aggregate: &Aggregate) -> RenderedSql {
    // Use the reconstructed definition
    let sql = format!("{};", aggregate.definition);
    RenderedSql::new(sql)
}

fn render_drop_aggregate(identifier: &AggregateIdentifier) -> RenderedSql {
    let sql = format!(
        "DROP AGGREGATE \"{}\".\"{}\"({})",
        identifier.schema, identifier.name, identifier.arguments
    );
    RenderedSql::destructive(sql)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render::Safety;

    fn create_test_aggregate() -> Aggregate {
        Aggregate {
            schema: "public".to_string(),
            name: "group_concat".to_string(),
            arguments: "text".to_string(),
            state_type: "text".to_string(),
            state_type_schema: "pg_catalog".to_string(),
            state_func: "group_concat_state".to_string(),
            state_func_schema: "public".to_string(),
            final_func: None,
            final_func_schema: None,
            combine_func: None,
            combine_func_schema: None,
            initial_value: Some("".to_string()),
            definition: "CREATE AGGREGATE public.group_concat(text) (\n    SFUNC = public.group_concat_state,\n    STYPE = text,\n    INITCOND = ''\n)".to_string(),
            comment: None,
            depends_on: vec![
                DbObjectId::Schema {
                    name: "public".to_string(),
                },
                DbObjectId::Function {
                    schema: "public".to_string(),
                    name: "group_concat_state".to_string(),
                    arguments: "text, text".to_string(),
                },
            ],
        }
    }

    #[test]
    fn test_render_create_aggregate() {
        let aggregate = create_test_aggregate();
        let rendered = render_create_aggregate(&aggregate);

        assert!(
            rendered
                .sql
                .contains("CREATE AGGREGATE public.group_concat")
        );
        assert!(rendered.sql.contains("SFUNC"));
        assert!(rendered.sql.contains("STYPE"));
        assert!(rendered.sql.ends_with(';'));
    }

    #[test]
    fn test_render_drop_aggregate() {
        let identifier = AggregateIdentifier::new(
            "public".to_string(),
            "group_concat".to_string(),
            "text".to_string(),
        );

        let rendered = render_drop_aggregate(&identifier);
        assert_eq!(
            rendered.sql,
            "DROP AGGREGATE \"public\".\"group_concat\"(text)"
        );
        assert_eq!(rendered.safety, Safety::Destructive);
    }

    #[test]
    fn test_render_create_operation() {
        let aggregate = create_test_aggregate();
        let operation = AggregateOperation::Create {
            aggregate: Box::new(aggregate.clone()),
        };

        let rendered_list = operation.to_sql();
        assert_eq!(rendered_list.len(), 1);
        assert!(rendered_list[0].sql.contains("CREATE AGGREGATE"));
    }

    #[test]
    fn test_render_drop_operation() {
        let identifier = AggregateIdentifier::new(
            "public".to_string(),
            "group_concat".to_string(),
            "text".to_string(),
        );
        let operation = AggregateOperation::Drop { identifier };

        let rendered_list = operation.to_sql();
        assert_eq!(rendered_list.len(), 1);
        assert!(rendered_list[0].sql.contains("DROP AGGREGATE"));
    }

    #[test]
    fn test_render_replace_operation() {
        let old_aggregate = create_test_aggregate();
        let mut new_aggregate = create_test_aggregate();
        new_aggregate.initial_value = Some("N/A".to_string());
        new_aggregate.definition = "CREATE AGGREGATE public.group_concat(text) (\n    SFUNC = public.group_concat_state,\n    STYPE = text,\n    INITCOND = 'N/A'\n)".to_string();

        let operation = AggregateOperation::Replace {
            old_aggregate: Box::new(old_aggregate),
            new_aggregate: Box::new(new_aggregate),
        };

        let rendered_list = operation.to_sql();
        assert_eq!(rendered_list.len(), 2);
        assert!(rendered_list[0].sql.contains("DROP AGGREGATE"));
        assert!(rendered_list[1].sql.contains("CREATE AGGREGATE"));
    }
}
