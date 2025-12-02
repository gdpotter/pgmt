//! SQL rendering for function operations

use crate::catalog::id::DbObjectId;
use crate::diff::operations::FunctionOperation;
use crate::render::{RenderedSql, Safety, SqlRenderer, quote_ident};

impl SqlRenderer for FunctionOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            FunctionOperation::Create { definition, .. } => {
                // definition contains the complete CREATE OR REPLACE FUNCTION statement from pg_get_functiondef()
                vec![RenderedSql {
                    sql: if definition.trim_end().ends_with(';') {
                        definition.clone()
                    } else {
                        format!("{};", definition.trim_end())
                    },
                    safety: Safety::Safe,
                }]
            }
            FunctionOperation::Replace { definition, .. } => {
                // definition contains the complete CREATE OR REPLACE FUNCTION statement from pg_get_functiondef()
                vec![RenderedSql {
                    sql: if definition.trim_end().ends_with(';') {
                        definition.clone()
                    } else {
                        format!("{};", definition.trim_end())
                    },
                    safety: Safety::Safe,
                }]
            }
            FunctionOperation::Drop {
                schema,
                name,
                arguments: _,
                kind,
                parameter_types,
            } => vec![RenderedSql {
                sql: format!(
                    "DROP {} {}.{}({});",
                    kind,
                    quote_ident(schema),
                    quote_ident(name),
                    parameter_types
                ),
                safety: Safety::Destructive,
            }],
            FunctionOperation::Comment(op) => op.to_sql(),
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            FunctionOperation::Create {
                schema,
                name,
                arguments,
                ..
            }
            | FunctionOperation::Replace {
                schema,
                name,
                arguments,
                ..
            }
            | FunctionOperation::Drop {
                schema,
                name,
                arguments,
                ..
            } => DbObjectId::Function {
                schema: schema.clone(),
                name: name.clone(),
                arguments: arguments.clone(),
            },
            FunctionOperation::Comment(op) => op.db_object_id(),
        }
    }

    fn is_destructive(&self) -> bool {
        matches!(self, FunctionOperation::Drop { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_create_function() {
        let op = FunctionOperation::Create {
            schema: "public".to_string(),
            name: "add_numbers".to_string(),
            arguments: "a integer, b integer".to_string(),
            kind: "FUNCTION".to_string(),
            parameters: "a integer, b integer".to_string(),
            returns: "integer".to_string(),
            attributes: "LANGUAGE sql".to_string(),
            definition: "CREATE OR REPLACE FUNCTION public.add_numbers(a integer, b integer)\nRETURNS integer\nLANGUAGE sql\nAS $function$SELECT a + b$function$".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert!(rendered[0].sql.starts_with("CREATE OR REPLACE FUNCTION"));
        assert!(rendered[0].sql.ends_with(';'));
        assert_eq!(rendered[0].safety, Safety::Safe);
    }

    #[test]
    fn test_render_create_function_with_semicolon() {
        let op = FunctionOperation::Create {
            schema: "public".to_string(),
            name: "test_fn".to_string(),
            arguments: "".to_string(),
            kind: "FUNCTION".to_string(),
            parameters: "".to_string(),
            returns: "void".to_string(),
            attributes: "".to_string(),
            definition: "CREATE FUNCTION public.test_fn() RETURNS void AS $$ $$ LANGUAGE sql;"
                .to_string(),
        };
        let rendered = op.to_sql();
        // Should not add double semicolon
        assert!(rendered[0].sql.ends_with(';'));
        assert!(!rendered[0].sql.ends_with(";;"));
    }

    #[test]
    fn test_render_replace_function() {
        let op = FunctionOperation::Replace {
            schema: "public".to_string(),
            name: "greet".to_string(),
            arguments: "name text".to_string(),
            kind: "FUNCTION".to_string(),
            parameters: "name text".to_string(),
            returns: "text".to_string(),
            attributes: "".to_string(),
            definition: "CREATE OR REPLACE FUNCTION public.greet(name text)\nRETURNS text\nAS $$SELECT 'Hello, ' || name$$\nLANGUAGE sql".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert!(rendered[0].sql.contains("CREATE OR REPLACE FUNCTION"));
        assert_eq!(rendered[0].safety, Safety::Safe);
    }

    #[test]
    fn test_render_drop_function() {
        let op = FunctionOperation::Drop {
            schema: "public".to_string(),
            name: "old_func".to_string(),
            arguments: "x integer".to_string(),
            kind: "FUNCTION".to_string(),
            parameter_types: "integer".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert_eq!(
            rendered[0].sql,
            "DROP FUNCTION \"public\".\"old_func\"(integer);"
        );
        assert_eq!(rendered[0].safety, Safety::Destructive);
    }

    #[test]
    fn test_render_drop_procedure() {
        let op = FunctionOperation::Drop {
            schema: "public".to_string(),
            name: "do_something".to_string(),
            arguments: "".to_string(),
            kind: "PROCEDURE".to_string(),
            parameter_types: "".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(
            rendered[0].sql,
            "DROP PROCEDURE \"public\".\"do_something\"();"
        );
    }

    #[test]
    fn test_is_destructive() {
        let create = FunctionOperation::Create {
            schema: "s".to_string(),
            name: "f".to_string(),
            arguments: "".to_string(),
            kind: "FUNCTION".to_string(),
            parameters: "".to_string(),
            returns: "void".to_string(),
            attributes: "".to_string(),
            definition: "CREATE FUNCTION f() RETURNS void AS $$ $$ LANGUAGE sql".to_string(),
        };
        let replace = FunctionOperation::Replace {
            schema: "s".to_string(),
            name: "f".to_string(),
            arguments: "".to_string(),
            kind: "FUNCTION".to_string(),
            parameters: "".to_string(),
            returns: "void".to_string(),
            attributes: "".to_string(),
            definition: "CREATE OR REPLACE FUNCTION f() RETURNS void AS $$ $$ LANGUAGE sql"
                .to_string(),
        };
        let drop = FunctionOperation::Drop {
            schema: "s".to_string(),
            name: "f".to_string(),
            arguments: "".to_string(),
            kind: "FUNCTION".to_string(),
            parameter_types: "".to_string(),
        };

        assert!(!create.is_destructive());
        assert!(!replace.is_destructive());
        assert!(drop.is_destructive());
    }

    #[test]
    fn test_db_object_id() {
        let op = FunctionOperation::Create {
            schema: "app".to_string(),
            name: "myfunc".to_string(),
            arguments: "x integer, y text".to_string(),
            kind: "FUNCTION".to_string(),
            parameters: "x integer, y text".to_string(),
            returns: "boolean".to_string(),
            attributes: "".to_string(),
            definition: "CREATE FUNCTION...".to_string(),
        };
        assert_eq!(
            op.db_object_id(),
            DbObjectId::Function {
                schema: "app".to_string(),
                name: "myfunc".to_string(),
                arguments: "x integer, y text".to_string()
            }
        );
    }
}
