use super::id::{DbObjectId, DependsOn};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FunctionKind {
    Function,
    Procedure,
}

#[derive(Debug, Clone)]
pub struct FunctionParam {
    pub name: Option<String>,
    pub data_type: String,
    pub mode: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Function {
    pub schema: String,
    pub name: String,
    pub kind: FunctionKind,
    pub arguments: String,
    pub parameters: Vec<FunctionParam>,
    pub return_type: Option<String>,
    pub language: String,
    pub definition: String,
    pub volatility: String,
    pub is_strict: bool,
    pub security_type: String,
    pub comment: Option<String>,
    pub depends_on: Vec<DbObjectId>,
}

impl Function {
    pub fn id(&self) -> DbObjectId {
        // Procedures are a distinct DbObjectId variant (like aggregates); this
        // catalog fetches both functions (prokind 'f') and procedures ('p').
        match self.kind {
            FunctionKind::Procedure => DbObjectId::Procedure {
                schema: self.schema.clone(),
                name: self.name.clone(),
                arguments: self.arguments.clone(),
            },
            _ => DbObjectId::Function {
                schema: self.schema.clone(),
                name: self.name.clone(),
                arguments: self.arguments.clone(),
            },
        }
    }
}

impl DependsOn for Function {
    fn id(&self) -> DbObjectId {
        self.id()
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
mod tests {
    use super::*;
    use crate::diff::functions::diff;
    use crate::diff::operations::{FunctionOperation, MigrationStep};

    fn make_function(
        schema: &str,
        name: &str,
        params: Vec<(Option<&str>, &str, Option<&str>)>, // (name, type, mode)
        return_type: Option<&str>,
        language: &str,
        volatility: &str,
        is_strict: bool,
        security_type: &str,
        definition: &str,
    ) -> Function {
        // Build arguments string from parameter types (mimics pg_get_function_arguments)
        let arguments = params
            .iter()
            .map(|(_, data_type, _)| *data_type)
            .collect::<Vec<_>>()
            .join(", ");

        let parameters = params
            .into_iter()
            .map(|(name, data_type, mode)| FunctionParam {
                name: name.map(|s| s.to_string()),
                data_type: data_type.to_string(),
                mode: mode.map(|s| s.to_string()),
            })
            .collect();

        Function {
            schema: schema.to_string(),
            name: name.to_string(),
            kind: FunctionKind::Function,
            arguments,
            parameters,
            return_type: return_type.map(|s| s.to_string()),
            language: language.to_string(),
            definition: definition.to_string(),
            volatility: volatility.to_string(),
            is_strict,
            security_type: security_type.to_string(),
            comment: None,
            depends_on: vec![],
        }
    }

    fn make_procedure(
        schema: &str,
        name: &str,
        params: Vec<(Option<&str>, &str, Option<&str>)>, // (name, type, mode)
        language: &str,
        security_type: &str,
        definition: &str,
    ) -> Function {
        // Build arguments string from parameter types (mimics pg_get_function_arguments)
        let arguments = params
            .iter()
            .map(|(_, data_type, _)| *data_type)
            .collect::<Vec<_>>()
            .join(", ");

        let parameters = params
            .into_iter()
            .map(|(name, data_type, mode)| FunctionParam {
                name: name.map(|s| s.to_string()),
                data_type: data_type.to_string(),
                mode: mode.map(|s| s.to_string()),
            })
            .collect();

        Function {
            schema: schema.to_string(),
            name: name.to_string(),
            kind: FunctionKind::Procedure,
            arguments,
            parameters,
            return_type: None,
            language: language.to_string(),
            definition: definition.to_string(),
            volatility: "VOLATILE".to_string(), // Not used for procedures
            is_strict: false,                   // Not used for procedures
            security_type: security_type.to_string(),
            comment: None,
            depends_on: vec![],
        }
    }

    #[test]
    fn test_create_function() {
        let new_func = make_function(
            "public",
            "add_numbers",
            vec![(Some("a"), "integer", None), (Some("b"), "integer", None)],
            Some("integer"),
            "plpgsql",
            "IMMUTABLE",
            true,
            "INVOKER",
            "AS $$ BEGIN RETURN a + b; END; $$;",
        );

        let steps = diff(None, Some(&new_func));

        assert_eq!(steps.len(), 1);
        match &steps[0] {
            MigrationStep::Function(FunctionOperation::Create {
                schema,
                name,
                kind,
                parameters,
                returns,
                attributes,
                definition,
                ..
            }) => {
                assert_eq!(schema, "public");
                assert_eq!(name, "add_numbers");
                assert_eq!(kind, "FUNCTION");
                assert!(parameters.contains("a integer"));
                assert!(parameters.contains("b integer"));
                assert_eq!(returns, " RETURNS integer");
                assert!(attributes.contains("LANGUAGE plpgsql"));
                assert!(attributes.contains("IMMUTABLE"));
                assert!(attributes.contains("STRICT"));
                assert!(attributes.contains("SECURITY INVOKER"));
                assert_eq!(definition, "AS $$ BEGIN RETURN a + b; END; $$;");
            }
            _ => panic!("Expected CreateFunction step"),
        }
    }

    #[test]
    fn test_drop_function() {
        let old_func = make_function(
            "public",
            "add_numbers",
            vec![(Some("a"), "integer", None), (Some("b"), "integer", None)],
            Some("integer"),
            "plpgsql",
            "IMMUTABLE",
            true,
            "INVOKER",
            "AS $$ BEGIN RETURN a + b; END; $$;",
        );

        let steps = diff(Some(&old_func), None);

        assert_eq!(steps.len(), 1);
        match &steps[0] {
            MigrationStep::Function(FunctionOperation::Drop {
                schema,
                name,
                kind,
                parameter_types,
                ..
            }) => {
                assert_eq!(schema, "public");
                assert_eq!(name, "add_numbers");
                assert_eq!(kind, "FUNCTION");
                assert_eq!(parameter_types, "integer, integer");
            }
            _ => panic!("Expected DropFunction step"),
        }
    }

    #[test]
    fn test_replace_function() {
        let old_func = make_function(
            "public",
            "add_numbers",
            vec![(Some("a"), "integer", None), (Some("b"), "integer", None)],
            Some("integer"),
            "plpgsql",
            "IMMUTABLE",
            true,
            "INVOKER",
            "AS $$ BEGIN RETURN a + b; END; $$;",
        );

        let new_func = make_function(
            "public",
            "add_numbers",
            vec![(Some("a"), "integer", None), (Some("b"), "integer", None)],
            Some("integer"),
            "plpgsql",
            "IMMUTABLE",
            true,
            "INVOKER",
            "AS $$ BEGIN RETURN a + b + 1; END; $$;", // Changed definition
        );

        let steps = diff(Some(&old_func), Some(&new_func));

        assert_eq!(steps.len(), 1);
        match &steps[0] {
            MigrationStep::Function(FunctionOperation::Replace {
                schema,
                name,
                kind,
                parameters,
                returns,
                attributes,
                definition,
                ..
            }) => {
                assert_eq!(schema, "public");
                assert_eq!(name, "add_numbers");
                assert_eq!(kind, "FUNCTION");
                assert!(parameters.contains("a integer"));
                assert!(parameters.contains("b integer"));
                assert_eq!(returns, " RETURNS integer");
                assert!(attributes.contains("LANGUAGE plpgsql"));
                assert!(attributes.contains("IMMUTABLE"));
                assert!(attributes.contains("STRICT"));
                assert!(attributes.contains("SECURITY INVOKER"));
                assert_eq!(definition, "AS $$ BEGIN RETURN a + b + 1; END; $$;");
            }
            _ => panic!("Expected ReplaceFunction step"),
        }
    }

    #[test]
    fn test_change_function_signature() {
        let old_func = make_function(
            "public",
            "add_numbers",
            vec![(Some("a"), "integer", None), (Some("b"), "integer", None)],
            Some("integer"),
            "plpgsql",
            "IMMUTABLE",
            true,
            "INVOKER",
            "AS $$ BEGIN RETURN a + b; END; $$;",
        );

        let new_func = make_function(
            "public",
            "add_numbers",
            vec![
                (Some("a"), "integer", None),
                (Some("b"), "integer", None),
                (Some("c"), "integer", None), // Added parameter
            ],
            Some("integer"),
            "plpgsql",
            "IMMUTABLE",
            true,
            "INVOKER",
            "AS $$ BEGIN RETURN a + b + c; END; $$;",
        );

        let steps = diff(Some(&old_func), Some(&new_func));

        // Should drop and recreate since signature changed
        assert_eq!(steps.len(), 2);

        match &steps[0] {
            MigrationStep::Function(FunctionOperation::Drop { schema, name, .. }) => {
                assert_eq!(schema, "public");
                assert_eq!(name, "add_numbers");
            }
            _ => panic!("Expected DropFunction step"),
        }

        match &steps[1] {
            MigrationStep::Function(FunctionOperation::Create {
                schema,
                name,
                parameters,
                ..
            }) => {
                assert_eq!(schema, "public");
                assert_eq!(name, "add_numbers");
                assert!(parameters.contains("c integer"));
            }
            _ => panic!("Expected CreateFunction step"),
        }
    }

    #[test]
    fn test_create_procedure() {
        let new_proc = make_procedure(
            "public",
            "update_data",
            vec![
                (Some("id"), "integer", Some("IN")),
                (Some("new_value"), "text", Some("IN")),
            ],
            "plpgsql",
            "INVOKER",
            "AS $$ BEGIN UPDATE data SET value = new_value WHERE data_id = id; END; $$;",
        );

        let steps = diff(None, Some(&new_proc));

        assert_eq!(steps.len(), 1);
        match &steps[0] {
            MigrationStep::Function(FunctionOperation::Create {
                schema,
                name,
                kind,
                parameters,
                returns,
                attributes,
                definition,
                ..
            }) => {
                assert_eq!(schema, "public");
                assert_eq!(name, "update_data");
                assert_eq!(kind, "PROCEDURE");
                assert!(parameters.contains("IN id integer"));
                assert!(parameters.contains("IN new_value text"));
                assert_eq!(returns, ""); // Procedures don't have return types
                assert!(attributes.contains("LANGUAGE plpgsql"));
                assert!(attributes.contains("SECURITY INVOKER"));
                assert_eq!(
                    definition,
                    "AS $$ BEGIN UPDATE data SET value = new_value WHERE data_id = id; END; $$;"
                );
            }
            _ => panic!("Expected CreateFunction step"),
        }
    }
}
