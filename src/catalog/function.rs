use anyhow::Result;
use sqlx::postgres::PgConnection;
use sqlx::postgres::types::Oid;
use std::collections::HashMap;
use tracing::info;

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
        DbObjectId::Function {
            schema: self.schema.clone(),
            name: self.name.clone(),
            arguments: self.arguments.clone(),
        }
    }
}

impl DependsOn for Function {
    fn id(&self) -> DbObjectId {
        DbObjectId::Function {
            schema: self.schema.clone(),
            name: self.name.clone(),
            arguments: self.arguments.clone(),
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

/// Row returned from the function dependencies query.
struct FunctionDependencyRow {
    cls_relkind: Option<String>,
    cls_schema: Option<String>,
    cls_name: Option<String>,
    typ_name: Option<String>,
    typ_schema: Option<String>,
    typ_typtype: Option<String>,
    typ_extension_name: Option<String>,
    proc_name: Option<String>,
    proc_schema: Option<String>,
    proc_args: Option<String>,
    seq_name: Option<String>,
    seq_schema: Option<String>,
}

/// Fetch all function dependencies in a single query.
///
/// Returns a HashMap keyed by function OID, containing all dependencies for each function.
async fn fetch_all_function_dependencies(
    conn: &mut PgConnection,
) -> Result<HashMap<Oid, Vec<FunctionDependencyRow>>> {
    let rows = sqlx::query!(
        r#"
        SELECT DISTINCT
            p.oid AS "func_oid!",

            -- Table or view reference
            cls.relkind::text AS "cls_relkind?",
            cls_n.nspname AS "cls_schema?",
            cls.relname AS "cls_name?",

            -- Type reference (resolve array element type)
            CASE
                WHEN typ.typelem != 0 THEN elem_typ.typname
                ELSE typ.typname
            END AS "typ_name?",
            CASE
                WHEN typ.typelem != 0 THEN elem_typ_n.nspname
                ELSE typ_n.nspname
            END AS "typ_schema?",
            CASE
                WHEN typ.typelem != 0 THEN elem_typ.typtype::text
                ELSE typ.typtype::text
            END AS "typ_typtype?",
            ext_dep_types.extname AS "typ_extension_name?",

            -- Function reference
            proc.proname AS "proc_name?",
            proc_n.nspname AS "proc_schema?",
            pg_catalog.pg_get_function_identity_arguments(proc.oid) AS "proc_args?",

            -- Sequence reference
            seq_cls.relname AS "seq_name?",
            seq_n.nspname AS "seq_schema?"

        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        JOIN pg_depend d ON d.objid = p.oid

        -- Table/view reference
        LEFT JOIN pg_class cls
            ON d.refclassid = 'pg_class'::regclass::oid
            AND d.refobjid = cls.oid
            AND cls.relkind IN ('r', 'v', 'm', 'p') -- tables, views, materialized views, partitioned tables
        LEFT JOIN pg_namespace cls_n ON cls.relnamespace = cls_n.oid

        -- Type reference (with array element type resolution)
        LEFT JOIN pg_type typ
            ON d.refclassid = 'pg_type'::regclass::oid
            AND d.refobjid = typ.oid
        LEFT JOIN pg_namespace typ_n ON typ.typnamespace = typ_n.oid
        -- Element type for array types (typelem != 0 means it's an array type)
        LEFT JOIN pg_type elem_typ ON typ.typelem = elem_typ.oid AND typ.typelem != 0
        LEFT JOIN pg_namespace elem_typ_n ON elem_typ.typnamespace = elem_typ_n.oid
        -- Extension type lookup for pg_depend type references
        LEFT JOIN (
            SELECT DISTINCT dep.objid AS type_oid, e.extname
            FROM pg_depend dep
            JOIN pg_extension e ON dep.refobjid = e.oid
            WHERE dep.deptype = 'e'
        ) ext_dep_types ON ext_dep_types.type_oid = COALESCE(NULLIF(typ.typelem, 0::oid), typ.oid)

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

        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            AND d.deptype = 'n'  -- normal dependencies
            AND d.refclassid IN (
                'pg_class'::regclass::oid,    -- tables, views, sequences
                'pg_type'::regclass::oid,     -- types
                'pg_proc'::regclass::oid      -- functions
            )
            -- Exclude functions that belong to extensions
            AND NOT EXISTS (
                SELECT 1 FROM pg_depend dep
                WHERE dep.objid = p.oid
                AND dep.deptype = 'e'
            )
        ORDER BY p.oid
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    // Group by func_oid
    let mut deps_by_func: HashMap<Oid, Vec<FunctionDependencyRow>> = HashMap::new();
    for row in rows {
        deps_by_func
            .entry(row.func_oid)
            .or_default()
            .push(FunctionDependencyRow {
                cls_relkind: row.cls_relkind,
                cls_schema: row.cls_schema,
                cls_name: row.cls_name,
                typ_name: row.typ_name,
                typ_schema: row.typ_schema,
                typ_typtype: row.typ_typtype,
                typ_extension_name: row.typ_extension_name,
                proc_name: row.proc_name,
                proc_schema: row.proc_schema,
                proc_args: row.proc_args,
                seq_name: row.seq_name,
                seq_schema: row.seq_schema,
            });
    }
    Ok(deps_by_func)
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
fn populate_function_dependencies(
    functions: &mut [Function],
    func_oids: &[Oid],
    deps_map: &HashMap<Oid, Vec<FunctionDependencyRow>>,
) {
    for (function, oid) in functions.iter_mut().zip(func_oids.iter()) {
        let Some(deps) = deps_map.get(oid) else {
            continue;
        };

        let function_id = function.id();

        for dep in deps {
            // Table or view dependency
            if let Some(relkind) = dep.cls_relkind.as_deref() {
                if let (Some(schema), Some(name)) = (&dep.cls_schema, &dep.cls_name) {
                    // Skip system schemas
                    if is_system_schema(schema) {
                        continue;
                    }

                    let dep_id = match relkind {
                        "r" | "p" => DbObjectId::Table {
                            schema: schema.clone(),
                            name: name.clone(),
                        },
                        "v" | "m" => DbObjectId::View {
                            schema: schema.clone(),
                            name: name.clone(),
                        },
                        "S" => DbObjectId::Sequence {
                            schema: schema.clone(),
                            name: name.clone(),
                        },
                        _ => continue,
                    };

                    if dep_id != function_id && !function.depends_on.contains(&dep_id) {
                        function.depends_on.push(dep_id);
                    }
                }
                continue;
            }

            // Type dependency (beyond what DependencyBuilder already added)
            // Check for extension types first, then use typtype to distinguish domains from other types
            if let (Some(typ_schema), Some(typ_name)) = (&dep.typ_schema, &dep.typ_name) {
                if !is_system_schema(typ_schema) {
                    let dep_id = if let Some(ext_name) = &dep.typ_extension_name {
                        // Type is from an extension - depend on the extension
                        DbObjectId::Extension {
                            name: ext_name.clone(),
                        }
                    } else if dep.typ_typtype.as_deref() == Some("d") {
                        DbObjectId::Domain {
                            schema: typ_schema.clone(),
                            name: typ_name.clone(),
                        }
                    } else {
                        DbObjectId::Type {
                            schema: typ_schema.clone(),
                            name: typ_name.clone(),
                        }
                    };
                    if !function.depends_on.contains(&dep_id) {
                        function.depends_on.push(dep_id);
                    }
                }
                continue;
            }

            // Function dependency
            if let (Some(proc_schema), Some(proc_name), Some(proc_args)) =
                (&dep.proc_schema, &dep.proc_name, &dep.proc_args)
            {
                if !is_system_schema(proc_schema) {
                    let dep_id = DbObjectId::Function {
                        schema: proc_schema.clone(),
                        name: proc_name.clone(),
                        arguments: proc_args.clone(),
                    };
                    if dep_id != function_id && !function.depends_on.contains(&dep_id) {
                        function.depends_on.push(dep_id);
                    }
                }
                continue;
            }

            // Sequence dependency
            if let (Some(seq_schema), Some(seq_name)) = (&dep.seq_schema, &dep.seq_name)
                && !is_system_schema(seq_schema)
            {
                let dep_id = DbObjectId::Sequence {
                    schema: seq_schema.clone(),
                    name: seq_name.clone(),
                };
                if !function.depends_on.contains(&dep_id) {
                    function.depends_on.push(dep_id);
                }
            }
        }
    }
}

pub async fn fetch(conn: &mut PgConnection) -> Result<Vec<Function>> {
    info!("Fetching functions...");
    let rows = sqlx::query!(
        r#"
        SELECT
            n.nspname AS schema,
            p.proname AS name,
            p.oid AS "func_oid!",
            p.prokind::text AS "kind!",
            pg_catalog.pg_get_function_result(p.oid) AS return_type,
            pg_catalog.pg_get_function_identity_arguments(p.oid) AS "arguments!",
            pg_catalog.pg_get_functiondef(p.oid) AS "definition!",
            l.lanname AS language,
            p.provolatile::text AS "volatility!",
            p.proisstrict AS is_strict,
            p.prosecdef AS security_definer,
            p.proretset AS returns_set,
            p.pronargs AS num_args,
            -- Resolve array element type for return type
            CASE
                WHEN ret_type.typelem != 0 THEN elem_ret.typname
                ELSE ret_type.typname
            END AS "return_type_name?",
            CASE
                WHEN ret_type.typelem != 0 THEN elem_ret_ns.nspname
                ELSE ret_ns.nspname
            END AS "return_type_schema?",
            -- Get typtype for return type to distinguish domains ('d') from other custom types
            CASE
                WHEN ret_type.typelem != 0 THEN elem_ret.typtype::text
                ELSE ret_type.typtype::text
            END AS "return_type_typtype?",
            d.description AS "comment?",
            -- Check if return type (or element type for arrays) is from an extension
            ext_ret_types.extname IS NOT NULL AS "is_return_type_extension!: bool",
            ext_ret_types.extname AS "return_type_extension_name?"
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        JOIN pg_language l ON p.prolang = l.oid
        LEFT JOIN pg_type ret_type ON p.prorettype = ret_type.oid
        LEFT JOIN pg_namespace ret_ns ON ret_type.typnamespace = ret_ns.oid
        -- Element type for array return types
        LEFT JOIN pg_type elem_ret ON ret_type.typelem = elem_ret.oid AND ret_type.typelem != 0
        LEFT JOIN pg_namespace elem_ret_ns ON elem_ret.typnamespace = elem_ret_ns.oid
        LEFT JOIN pg_description d ON d.objoid = p.oid AND d.objsubid = 0
        -- Extension return type lookup: compute once as derived table, then hash join
        LEFT JOIN (
            SELECT DISTINCT dep.objid AS type_oid, e.extname
            FROM pg_depend dep
            JOIN pg_extension e ON dep.refobjid = e.oid
            WHERE dep.deptype = 'e'
        ) ext_ret_types ON ext_ret_types.type_oid = COALESCE(NULLIF(ret_type.typelem, 0::oid), ret_type.oid)
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND p.prokind != 'a'  -- Exclude aggregates, they're handled separately
        -- Exclude functions that belong to extensions
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend dep
            WHERE dep.objid = p.oid
            AND dep.deptype = 'e'
        )
        ORDER BY n.nspname, p.proname
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    // Query function parameter types with proper schema information
    info!("Fetching function parameters...");
    let param_rows = sqlx::query!(
        r#"
        SELECT
            p.oid AS "func_oid!",
            -- Resolve array element type for parameters
            CASE
                WHEN t.typelem != 0 THEN elem_t.typname
                ELSE t.typname
            END AS "type_name!",
            CASE
                WHEN t.typelem != 0 THEN elem_tn.nspname
                ELSE tn.nspname
            END AS "type_schema!",
            -- Get typtype to distinguish domains ('d') from other custom types ('e', 'c', etc.)
            CASE
                WHEN t.typelem != 0 THEN elem_t.typtype::text
                ELSE t.typtype::text
            END AS "typtype!",
            pg_catalog.format_type(t.oid, NULL) AS "formatted_type!",
            COALESCE(p.proargnames[param_num], '') AS "param_name!",
            p.proargmodes[param_num - 1] AS "param_mode",
            -- Check if parameter type (or element type for arrays) is from an extension
            ext_types.extname IS NOT NULL AS "is_extension_type!: bool",
            ext_types.extname AS "extension_name?"
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        CROSS JOIN LATERAL unnest(p.proargtypes) WITH ORDINALITY AS param_types(type_oid, param_num)
        JOIN pg_type t ON param_types.type_oid = t.oid
        JOIN pg_namespace tn ON t.typnamespace = tn.oid
        -- Element type for array parameters
        LEFT JOIN pg_type elem_t ON t.typelem = elem_t.oid AND t.typelem != 0
        LEFT JOIN pg_namespace elem_tn ON elem_t.typnamespace = elem_tn.oid
        -- Extension type lookup: compute once as derived table, then hash join
        LEFT JOIN (
            SELECT DISTINCT dep.objid AS type_oid, e.extname
            FROM pg_depend dep
            JOIN pg_extension e ON dep.refobjid = e.oid
            WHERE dep.deptype = 'e'
        ) ext_types ON ext_types.type_oid = COALESCE(NULLIF(t.typelem, 0::oid), t.oid)
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND p.prokind != 'a'  -- Exclude aggregates, they're handled separately
        -- Exclude functions that belong to extensions
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend dep
            WHERE dep.objid = p.oid
            AND dep.deptype = 'e'
        )
        ORDER BY p.oid, param_types.param_num
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    // Group parameters by function OID (with extension info for dependency building)
    // Tuple: (FunctionParam, type_schema, type_name, typtype, is_extension_type, extension_name)
    type ParamWithExtInfo = (FunctionParam, String, String, String, bool, Option<String>);
    let mut params_by_function: HashMap<Oid, Vec<ParamWithExtInfo>> = HashMap::new();
    for param in param_rows {
        let param_name = if param.param_name.is_empty() {
            None
        } else {
            Some(param.param_name)
        };

        // Build qualified type name for custom types
        let data_type = if is_system_schema(&param.type_schema) {
            param.formatted_type.clone()
        } else {
            format!("{}.{}", param.type_schema, param.type_name)
        };

        params_by_function.entry(param.func_oid).or_default().push((
            FunctionParam {
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
            },
            param.type_schema,
            param.type_name,
            param.typtype,
            param.is_extension_type,
            param.extension_name,
        ));
    }

    // Process results into functions, collecting OIDs for dependency lookup
    let mut functions = Vec::new();
    let mut func_oids = Vec::new();
    for row in rows {
        // Get parameters (with extension info) for this function
        let params_with_ext_info = params_by_function.remove(&row.func_oid).unwrap_or_default();

        // Separate parameters from extension info
        let parameters: Vec<FunctionParam> = params_with_ext_info
            .iter()
            .map(|(param, _, _, _, _, _)| param.clone())
            .collect();

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

        // Add dependencies for parameter types (extension or custom type)
        // Use add_type_dependency to properly distinguish domains from other custom types
        for (_, type_schema, type_name, typtype, is_extension, extension_name) in
            &params_with_ext_info
        {
            builder.add_type_dependency(
                Some(type_schema.clone()),
                Some(type_name.clone()),
                Some(typtype.clone()),
                *is_extension,
                extension_name.clone(),
            );
        }

        // Add dependency for return type (extension or custom type)
        // Use add_type_dependency to properly distinguish domains from other custom types
        builder.add_type_dependency(
            row.return_type_schema.clone(),
            row.return_type_name.clone(),
            row.return_type_typtype.clone(),
            row.is_return_type_extension,
            row.return_type_extension_name.clone(),
        );

        let depends_on = builder.build();

        let security_type = if row.security_definer {
            "DEFINER".to_string()
        } else {
            "INVOKER".to_string()
        };

        // Collect OID for dependency lookup
        func_oids.push(row.func_oid);

        functions.push(Function {
            schema: row.schema,
            name: row.name,
            kind,
            arguments: row.arguments,
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
        info!("Fetching function dependencies...");
        let deps_map = fetch_all_function_dependencies(&mut *conn).await?;
        populate_function_dependencies(&mut functions, &func_oids, &deps_map);
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
