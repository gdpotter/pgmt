use anyhow::Result;
use sqlx::PgPool;
use sqlx::postgres::types::Oid;
use std::collections::HashMap;

use super::comments::Commentable;
use super::id::{DbObjectId, DependsOn};
use super::utils::{DependencyBuilder, is_system_schema};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FunctionKind {
    Function,
    Procedure,
    Aggregate,
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
        DbObjectId::Function {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }
}

impl DependsOn for Function {
    fn id(&self) -> DbObjectId {
        DbObjectId::Function {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}

impl Commentable for Function {
    fn comment(&self) -> &Option<String> {
        &self.comment
    }
}

/// Populate function dependencies using pg_depend.
///
/// **Important limitation**: PostgreSQL does NOT record table/view/sequence references
/// from function bodies in pg_depend. This is a fundamental PostgreSQL limitation that
/// affects all procedural languages (SQL, PL/pgSQL, etc.).
///
/// This function currently only captures:
/// - Type dependencies from function parameters and return types
/// - Dependencies that PostgreSQL explicitly records in pg_depend
///
/// For function body dependencies (tables/views/sequences referenced in the code),
/// use file-based dependencies via `-- require:` comments in schema files.
async fn populate_function_dependencies(functions: &mut [Function], pool: &PgPool) -> Result<()> {
    // Query dependencies for all functions in our list by schema/name
    // This is less efficient than querying by OID, but simpler for now
    for function in functions.iter_mut() {
        let deps = sqlx::query!(
            r#"
            SELECT DISTINCT
                d.refclassid,
                d.refobjid,

                -- Table or view reference
                cls.relkind::text AS "cls_relkind?",
                cls_n.nspname AS "cls_schema?",
                cls.relname AS "cls_name?",

                -- Type reference
                typ.typname AS "typ_name?",
                typ_n.nspname AS "typ_schema?",

                -- Function reference
                proc.proname AS "proc_name?",
                proc_n.nspname AS "proc_schema?",

                -- Sequence reference
                seq_cls.relname AS "seq_name?",
                seq_n.nspname AS "seq_schema?"

            FROM pg_proc p
            JOIN pg_depend d ON d.objid = p.oid

            -- Table/view reference
            LEFT JOIN pg_class cls
                ON d.refclassid = 'pg_class'::regclass::oid
                AND d.refobjid = cls.oid
                AND cls.relkind IN ('r', 'v', 'm', 'p') -- tables, views, materialized views, partitioned tables
            LEFT JOIN pg_namespace cls_n ON cls.relnamespace = cls_n.oid

            -- Type reference
            LEFT JOIN pg_type typ
                ON d.refclassid = 'pg_type'::regclass::oid
                AND d.refobjid = typ.oid
            LEFT JOIN pg_namespace typ_n ON typ.typnamespace = typ_n.oid

            -- Function reference
            LEFT JOIN pg_proc proc
                ON d.refclassid = 'pg_proc'::regclass::oid
                AND d.refobjid = proc.oid
                AND proc.oid != p.oid  -- Don't include self-references
            LEFT JOIN pg_namespace proc_n ON proc.pronamespace = proc_n.oid

            -- Sequence reference
            LEFT JOIN pg_class seq_cls
                ON d.refclassid = 'pg_class'::regclass::oid
                AND d.refobjid = seq_cls.oid
                AND seq_cls.relkind = 'S' -- sequences
            LEFT JOIN pg_namespace seq_n ON seq_cls.relnamespace = seq_n.oid

            WHERE p.pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)
                AND p.proname = $2
                AND d.deptype = 'n'  -- normal dependencies
                AND d.refclassid IN (
                    'pg_class'::regclass::oid,    -- tables, views, sequences
                    'pg_type'::regclass::oid,     -- types
                    'pg_proc'::regclass::oid      -- functions
                )
            "#,
            function.schema,
            function.name
        )
        .fetch_all(pool)
        .await?;

        for dep in deps {
            let function_id = function.id();

            // Table or view dependency
            if let Some(relkind) = dep.cls_relkind.as_deref() {
                if let (Some(schema), Some(name)) = (dep.cls_schema, dep.cls_name) {
                    // Skip system schemas
                    if is_system_schema(&schema) {
                        continue;
                    }

                    let dep_id = match relkind {
                        "r" | "p" => DbObjectId::Table { schema, name },
                        "v" | "m" => DbObjectId::View { schema, name },
                        "S" => DbObjectId::Sequence { schema, name },
                        _ => continue,
                    };

                    if dep_id != function_id && !function.depends_on.contains(&dep_id) {
                        function.depends_on.push(dep_id);
                    }
                }
                continue;
            }

            // Type dependency (beyond what DependencyBuilder already added)
            if let (Some(typ_schema), Some(typ_name)) = (dep.typ_schema, dep.typ_name) {
                if !is_system_schema(&typ_schema) {
                    let dep_id = DbObjectId::Type {
                        schema: typ_schema,
                        name: typ_name,
                    };
                    if !function.depends_on.contains(&dep_id) {
                        function.depends_on.push(dep_id);
                    }
                }
                continue;
            }

            // Function dependency
            if let (Some(proc_schema), Some(proc_name)) = (dep.proc_schema, dep.proc_name) {
                if !is_system_schema(&proc_schema) {
                    let dep_id = DbObjectId::Function {
                        schema: proc_schema,
                        name: proc_name,
                    };
                    if dep_id != function_id && !function.depends_on.contains(&dep_id) {
                        function.depends_on.push(dep_id);
                    }
                }
                continue;
            }

            // Sequence dependency
            if let (Some(seq_schema), Some(seq_name)) = (dep.seq_schema, dep.seq_name)
                && !is_system_schema(&seq_schema)
            {
                let dep_id = DbObjectId::Sequence {
                    schema: seq_schema,
                    name: seq_name,
                };
                if !function.depends_on.contains(&dep_id) {
                    function.depends_on.push(dep_id);
                }
            }
        }
    }

    Ok(())
}

pub async fn fetch(pool: &PgPool) -> Result<Vec<Function>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            n.nspname AS schema,
            p.proname AS name,
            p.oid AS "func_oid!",
            p.prokind::text AS "kind!",
            pg_catalog.pg_get_function_result(p.oid) AS return_type,
            pg_catalog.pg_get_function_arguments(p.oid) AS "arguments!",
            pg_catalog.pg_get_functiondef(p.oid) AS "definition!",
            l.lanname AS language,
            p.provolatile::text AS "volatility!",
            p.proisstrict AS is_strict,
            p.prosecdef AS security_definer,
            p.proretset AS returns_set,
            p.pronargs AS num_args,
            ret_type.typname AS "return_type_name?",
            ret_ns.nspname AS "return_type_schema?",
            d.description AS "comment?"
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        JOIN pg_language l ON p.prolang = l.oid
        LEFT JOIN pg_type ret_type ON p.prorettype = ret_type.oid
        LEFT JOIN pg_namespace ret_ns ON ret_type.typnamespace = ret_ns.oid
        LEFT JOIN pg_description d ON d.objoid = p.oid AND d.objsubid = 0
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        -- Exclude functions that belong to extensions
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend dep
            WHERE dep.objid = p.oid
            AND dep.deptype = 'e'
        )
        ORDER BY n.nspname, p.proname
        "#
    )
    .fetch_all(pool)
    .await?;

    // Query function parameter types with proper schema information
    let param_rows = sqlx::query!(
        r#"
        SELECT
            p.oid AS "func_oid!",
            t.typname AS "type_name!",
            tn.nspname AS "type_schema!",
            pg_catalog.format_type(t.oid, NULL) AS "formatted_type!",
            COALESCE(p.proargnames[param_num], '') AS "param_name!",
            p.proargmodes[param_num - 1] AS "param_mode"
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        CROSS JOIN LATERAL unnest(p.proargtypes) WITH ORDINALITY AS param_types(type_oid, param_num)
        JOIN pg_type t ON param_types.type_oid = t.oid
        JOIN pg_namespace tn ON t.typnamespace = tn.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        -- Exclude functions that belong to extensions
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend dep
            WHERE dep.objid = p.oid
            AND dep.deptype = 'e'
        )
        ORDER BY p.oid, param_types.param_num
        "#
    )
    .fetch_all(pool)
    .await?;

    // Group parameters by function OID
    let mut params_by_function: HashMap<Oid, Vec<FunctionParam>> = HashMap::new();
    for param in param_rows {
        let param_name = if param.param_name.is_empty() {
            None
        } else {
            Some(param.param_name)
        };

        // Build qualified type name for custom types
        let data_type = if is_system_schema(&param.type_schema) {
            param.formatted_type
        } else {
            format!("{}.{}", param.type_schema, param.type_name)
        };

        params_by_function
            .entry(param.func_oid)
            .or_default()
            .push(FunctionParam {
                name: param_name,
                data_type,
                mode: param.param_mode.map(|m| match m as u8 as char {
                    'i' => "IN".to_string(),
                    'o' => "OUT".to_string(),
                    'b' => "INOUT".to_string(),
                    'v' => "VARIADIC".to_string(),
                    't' => "TABLE".to_string(),
                    _ => "IN".to_string(),
                }),
            });
    }

    // Process results into functions
    let mut functions = Vec::new();
    for row in rows {
        // Get parameters for this function
        let parameters = params_by_function.remove(&row.func_oid).unwrap_or_default();

        // Check if this function has OUT/INOUT parameters that we don't support yet
        if parameters.len() as i16 != row.num_args {
            return Err(anyhow::anyhow!(
                "Function {}.{} has OUT/INOUT parameters which are not yet supported. \
                 Found {} IN parameters but function has {} total parameters.",
                row.schema,
                row.name,
                parameters.len(),
                row.num_args
            ));
        }

        // Determine function kind
        let kind = match row.kind.as_str() {
            "p" => FunctionKind::Procedure,
            "a" => FunctionKind::Aggregate,
            _ => FunctionKind::Function,
        };

        // Parse the return type
        let return_type = if kind == FunctionKind::Procedure {
            None
        } else {
            row.return_type.clone()
        };

        // Build basic dependencies using DependencyBuilder (schema + custom types)
        let mut builder = DependencyBuilder::new(row.schema.clone());

        // Add dependencies for parameter types that are custom types
        for param in &parameters {
            if let Some((param_schema, param_name)) = param.data_type.split_once('.')
                && !is_system_schema(param_schema)
            {
                builder
                    .add_custom_type(Some(param_schema.to_string()), Some(param_name.to_string()));
            }
        }

        // Add dependency for return type if it's a custom type
        if let (Some(ret_name), Some(ret_schema)) = (&row.return_type_name, &row.return_type_schema)
            && !is_system_schema(ret_schema)
        {
            builder.add_custom_type(Some(ret_schema.clone()), Some(ret_name.clone()));
        }

        let depends_on = builder.build();

        let security_type = if row.security_definer {
            "DEFINER".to_string()
        } else {
            "INVOKER".to_string()
        };

        functions.push(Function {
            schema: row.schema,
            name: row.name,
            kind,
            parameters,
            return_type,
            language: row.language,
            definition: row.definition, // Use complete definition from pg_get_functiondef()
            volatility: match row.volatility.as_str() {
                "i" => "IMMUTABLE".to_string(),
                "s" => "STABLE".to_string(),
                _ => "VOLATILE".to_string(),
            },
            is_strict: row.is_strict,
            security_type,
            comment: row.comment,
            depends_on,
        });
    }

    // Phase 2: Populate comprehensive dependencies using pg_depend
    // This adds dependencies on tables, views, other functions, sequences, etc.
    if !functions.is_empty() {
        populate_function_dependencies(&mut functions, pool).await?;
    }

    Ok(functions)
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
