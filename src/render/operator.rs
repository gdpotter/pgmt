//! SQL rendering for operator operations

use crate::catalog::id::DbObjectId;
use crate::catalog::operator::Operator;
use crate::diff::operations::{OperatorIdentifier, OperatorOperation};
use crate::render::{RenderedSql, SqlRenderer, quote_ident};

impl SqlRenderer for OperatorOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            OperatorOperation::Create { operator } => vec![render_create_operator(operator)],
            OperatorOperation::Drop { identifier } => vec![render_drop_operator(identifier)],
            OperatorOperation::Replace { new_operator, .. } => vec![
                render_drop_operator(&OperatorIdentifier::from_operator(new_operator)),
                render_create_operator(new_operator),
            ],
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            OperatorOperation::Create { operator } => operator.id(),
            OperatorOperation::Drop { identifier } => DbObjectId::Operator {
                schema: identifier.schema.clone(),
                name: identifier.name.clone(),
                arguments: identifier.arguments.clone(),
            },
            OperatorOperation::Replace { new_operator, .. } => new_operator.id(),
        }
    }
}

fn render_create_operator(operator: &Operator) -> RenderedSql {
    RenderedSql::new(format!("{};", operator.definition))
}

fn render_drop_operator(identifier: &OperatorIdentifier) -> RenderedSql {
    // The operator symbol is not a quotable identifier; the schema is.
    RenderedSql::new(format!(
        "DROP OPERATOR {}.{} ({})",
        quote_ident(&identifier.schema),
        identifier.name,
        identifier.arguments
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render::Safety;

    fn test_operator() -> Operator {
        Operator {
            schema: "public".to_string(),
            name: "===".to_string(),
            arguments: "integer, integer".to_string(),
            definition: "CREATE OPERATOR public.=== (\n    FUNCTION = public.my_eq,\n    LEFTARG = integer,\n    RIGHTARG = integer\n)".to_string(),
            comment: None,
            depends_on: vec![],
        }
    }

    #[test]
    fn test_render_create_operator() {
        let rendered = render_create_operator(&test_operator());
        assert!(rendered.sql.contains("CREATE OPERATOR public.==="));
        assert!(rendered.sql.contains("FUNCTION = public.my_eq"));
        assert!(rendered.sql.ends_with(';'));
        assert_eq!(rendered.safety, Safety::Safe);
    }

    #[test]
    fn test_render_drop_operator() {
        let identifier = OperatorIdentifier::from_operator(&test_operator());
        let rendered = render_drop_operator(&identifier);
        assert_eq!(
            rendered.sql,
            "DROP OPERATOR \"public\".=== (integer, integer)"
        );
    }

    #[test]
    fn test_render_drop_prefix_operator_uses_none() {
        let identifier = OperatorIdentifier {
            schema: "public".to_string(),
            name: "@@".to_string(),
            arguments: "NONE, integer".to_string(),
        };
        let rendered = render_drop_operator(&identifier);
        assert_eq!(rendered.sql, "DROP OPERATOR \"public\".@@ (NONE, integer)");
    }

    #[test]
    fn test_render_replace_drops_then_creates() {
        let op = test_operator();
        let operation = OperatorOperation::Replace {
            old_operator: Box::new(op.clone()),
            new_operator: Box::new(op),
        };
        let rendered = operation.to_sql();
        assert_eq!(rendered.len(), 2);
        assert!(rendered[0].sql.contains("DROP OPERATOR"));
        assert!(rendered[1].sql.contains("CREATE OPERATOR"));
    }
}
