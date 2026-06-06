//! Shared SQL rendering for GRANT and REVOKE statements
//!
//! This module provides consistent grant rendering across both schema generation
//! and migration operations to ensure identical SQL output.

use crate::catalog::grant::{Grant, GranteeType};
use crate::catalog::id::DbObjectId;
use crate::render::quote_ident;

/// Render a complete GRANT statement for the given grant.
///
/// This function handles all PostgreSQL grant object types:
/// - Tables and views (without object type keyword)
/// - Schemas, functions, sequences, types (with object type keyword)
/// - Role and PUBLIC grantees
/// - WITH GRANT OPTION clause
/// - Proper SQL formatting and identifier quoting
pub fn render_grant_statement(grant: &Grant) -> String {
    let grantee = match &grant.grantee {
        GranteeType::Role(name) => quote_ident(name),
        GranteeType::Public => "PUBLIC".to_string(),
    };

    let grant_option = if grant.with_grant_option {
        " WITH GRANT OPTION"
    } else {
        ""
    };

    let (privileges, object_clause) = render_privileges_and_object(grant);

    format!(
        "GRANT {} ON {} TO {}{};",
        privileges, object_clause, grantee, grant_option
    )
}

/// Render a complete REVOKE statement for the given grant.
pub fn render_revoke_statement(grant: &Grant) -> String {
    let grantee = match &grant.grantee {
        GranteeType::Role(name) => quote_ident(name),
        GranteeType::Public => "PUBLIC".to_string(),
    };

    let (privileges, object_clause) = render_privileges_and_object(grant);

    format!(
        "REVOKE {} ON {} FROM {};",
        privileges, object_clause, grantee
    )
}

/// Render the privilege list and object clause for a grant. Column grants attach
/// the column to each privilege (`SELECT (col), UPDATE (col)`) and reference the
/// bare relation; all other objects list privileges plainly.
fn render_privileges_and_object(grant: &Grant) -> (String, String) {
    if let Some(column) = grant.target.column_name() {
        // Column grants attach the column to each privilege and reference the bare relation.
        let col = quote_ident(column);
        let privileges = grant
            .privileges
            .iter()
            .map(|p| format!("{} ({})", p, col))
            .collect::<Vec<_>>()
            .join(", ");
        let (schema, table) = grant.target.schema_and_name();
        let object_clause = format!("{}.{}", quote_ident(&schema), quote_ident(&table));
        return (privileges, object_clause);
    }
    (
        grant.privileges.join(", "),
        render_grant_object_clause(&grant.target.object),
    )
}

/// Render the object clause for GRANT/REVOKE statements.
///
/// PostgreSQL GRANT syntax rules:
/// - Tables and views: No object type keyword (just schema.name)
/// - Other objects: Require object type keyword (e.g., SCHEMA name, FUNCTION schema.name)
pub fn render_grant_object_clause(object: &DbObjectId) -> String {
    match object {
        // Tables and views don't require a keyword.
        DbObjectId::Table { schema, name } | DbObjectId::View { schema, name } => {
            format!("{}.{}", quote_ident(schema), quote_ident(name))
        }
        DbObjectId::Schema { name } => format!("SCHEMA {}", quote_ident(name)),
        DbObjectId::Function {
            schema,
            name,
            arguments,
        } => format!(
            "FUNCTION {}.{}({})",
            quote_ident(schema),
            quote_ident(name),
            arguments
        ),
        DbObjectId::Procedure {
            schema,
            name,
            arguments,
        } => format!(
            "PROCEDURE {}.{}({})",
            quote_ident(schema),
            quote_ident(name),
            arguments
        ),
        // PostgreSQL grants on aggregates use the FUNCTION keyword, not AGGREGATE.
        DbObjectId::Aggregate {
            schema,
            name,
            arguments,
        } => format!(
            "FUNCTION {}.{}({})",
            quote_ident(schema),
            quote_ident(name),
            arguments
        ),
        DbObjectId::Sequence { schema, name } => {
            format!("SEQUENCE {}.{}", quote_ident(schema), quote_ident(name))
        }
        DbObjectId::Type { schema, name } => {
            format!("TYPE {}.{}", quote_ident(schema), quote_ident(name))
        }
        DbObjectId::Domain { schema, name } => {
            format!("DOMAIN {}.{}", quote_ident(schema), quote_ident(name))
        }
        // Columns are handled by render_privileges_and_object; the rest are not
        // grantable object kinds.
        DbObjectId::Index { .. }
        | DbObjectId::Constraint { .. }
        | DbObjectId::Trigger { .. }
        | DbObjectId::Policy { .. }
        | DbObjectId::Extension { .. }
        | DbObjectId::Operator { .. }
        | DbObjectId::Cast { .. }
        | DbObjectId::Grant { .. }
        | DbObjectId::Comment { .. }
        | DbObjectId::Column { .. } => {
            unreachable!("not a grantable object kind: {object}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::target::AttrTarget;

    #[test]
    fn test_render_grant_on_table() {
        let grant = Grant {
            target: AttrTarget::object(DbObjectId::Table {
                schema: "public".to_string(),
                name: "users".to_string(),
            }),
            grantee: GranteeType::Role("app_user".to_string()),
            privileges: vec!["SELECT".to_string(), "INSERT".to_string()],
            with_grant_option: false,
            depends_on: vec![],
            object_owner: "postgres".to_string(),
            is_default_acl: false,
        };

        let sql = render_grant_statement(&grant);
        assert_eq!(
            sql,
            "GRANT SELECT, INSERT ON \"public\".\"users\" TO \"app_user\";"
        );
    }

    #[test]
    fn test_render_grant_on_view_no_view_keyword() {
        let grant = Grant {
            target: AttrTarget::object(DbObjectId::View {
                schema: "public".to_string(),
                name: "current_subscriptions".to_string(),
            }),
            grantee: GranteeType::Role("postgres".to_string()),
            privileges: vec!["SELECT".to_string()],
            with_grant_option: false,
            depends_on: vec![],
            object_owner: "postgres".to_string(),
            is_default_acl: false,
        };

        let sql = render_grant_statement(&grant);
        // Should NOT contain "VIEW" keyword
        assert_eq!(
            sql,
            "GRANT SELECT ON \"public\".\"current_subscriptions\" TO \"postgres\";"
        );
        assert!(!sql.contains("VIEW"));
    }

    #[test]
    fn test_render_grant_on_view_all_privileges() {
        let grant = Grant {
            target: AttrTarget::object(DbObjectId::View {
                schema: "public".to_string(),
                name: "current_subscriptions".to_string(),
            }),
            grantee: GranteeType::Role("postgres".to_string()),
            privileges: vec![
                "DELETE".to_string(),
                "INSERT".to_string(),
                "REFERENCES".to_string(),
                "SELECT".to_string(),
                "TRIGGER".to_string(),
                "TRUNCATE".to_string(),
                "UPDATE".to_string(),
            ],
            with_grant_option: false,
            depends_on: vec![],
            object_owner: "postgres".to_string(),
            is_default_acl: false,
        };

        let sql = render_grant_statement(&grant);
        // Should NOT contain "VIEW" keyword even with all privileges
        assert_eq!(
            sql,
            "GRANT DELETE, INSERT, REFERENCES, SELECT, TRIGGER, TRUNCATE, UPDATE ON \"public\".\"current_subscriptions\" TO \"postgres\";"
        );
        assert!(!sql.contains("VIEW"));
    }

    #[test]
    fn test_render_grant_on_schema() {
        let grant = Grant {
            target: AttrTarget::object(DbObjectId::Schema {
                name: "analytics".to_string(),
            }),
            grantee: GranteeType::Role("data_analyst".to_string()),
            privileges: vec!["USAGE".to_string()],
            with_grant_option: false,
            depends_on: vec![],
            object_owner: "postgres".to_string(),
            is_default_acl: false,
        };

        let sql = render_grant_statement(&grant);
        assert_eq!(
            sql,
            "GRANT USAGE ON SCHEMA \"analytics\" TO \"data_analyst\";"
        );
    }

    #[test]
    fn test_render_grant_on_function() {
        let grant = Grant {
            target: AttrTarget::object(DbObjectId::Function {
                schema: "public".to_string(),
                name: "calculate_total".to_string(),
                arguments: "integer, numeric".to_string(),
            }),
            grantee: GranteeType::Role("app_user".to_string()),
            privileges: vec!["EXECUTE".to_string()],
            with_grant_option: false,
            depends_on: vec![],
            object_owner: "postgres".to_string(),
            is_default_acl: false,
        };

        let sql = render_grant_statement(&grant);
        assert_eq!(
            sql,
            "GRANT EXECUTE ON FUNCTION \"public\".\"calculate_total\"(integer, numeric) TO \"app_user\";"
        );
    }

    #[test]
    fn test_render_grant_on_procedure() {
        let grant = Grant {
            target: AttrTarget::object(DbObjectId::Procedure {
                schema: "public".to_string(),
                name: "analyze_database".to_string(),
                arguments: "".to_string(),
            }),
            grantee: GranteeType::Role("app_user".to_string()),
            privileges: vec!["EXECUTE".to_string()],
            with_grant_option: false,
            depends_on: vec![],
            object_owner: "postgres".to_string(),
            is_default_acl: false,
        };

        let sql = render_grant_statement(&grant);
        assert_eq!(
            sql,
            "GRANT EXECUTE ON PROCEDURE \"public\".\"analyze_database\"() TO \"app_user\";"
        );
    }

    #[test]
    fn test_render_grant_on_aggregate() {
        let grant = Grant {
            target: AttrTarget::object(DbObjectId::Aggregate {
                schema: "public".to_string(),
                name: "array_agg_custom".to_string(),
                arguments: "integer".to_string(),
            }),
            grantee: GranteeType::Role("app_user".to_string()),
            privileges: vec!["EXECUTE".to_string()],
            with_grant_option: false,
            depends_on: vec![],
            object_owner: "postgres".to_string(),
            is_default_acl: false,
        };

        let sql = render_grant_statement(&grant);
        // PostgreSQL grants on aggregates use FUNCTION keyword, not AGGREGATE
        assert_eq!(
            sql,
            "GRANT EXECUTE ON FUNCTION \"public\".\"array_agg_custom\"(integer) TO \"app_user\";"
        );
    }

    #[test]
    fn test_render_grant_with_grant_option() {
        let grant = Grant {
            target: AttrTarget::object(DbObjectId::Table {
                schema: "public".to_string(),
                name: "orders".to_string(),
            }),
            grantee: GranteeType::Role("manager".to_string()),
            privileges: vec!["ALL".to_string()],
            with_grant_option: true,
            depends_on: vec![],
            object_owner: "postgres".to_string(),
            is_default_acl: false,
        };

        let sql = render_grant_statement(&grant);
        assert_eq!(
            sql,
            "GRANT ALL ON \"public\".\"orders\" TO \"manager\" WITH GRANT OPTION;"
        );
    }

    #[test]
    fn test_render_grant_to_public() {
        let grant = Grant {
            target: AttrTarget::object(DbObjectId::View {
                schema: "public".to_string(),
                name: "public_stats".to_string(),
            }),
            grantee: GranteeType::Public,
            privileges: vec!["SELECT".to_string()],
            with_grant_option: false,
            depends_on: vec![],
            object_owner: "postgres".to_string(),
            is_default_acl: false,
        };

        let sql = render_grant_statement(&grant);
        assert_eq!(
            sql,
            "GRANT SELECT ON \"public\".\"public_stats\" TO PUBLIC;"
        );
    }

    #[test]
    fn test_render_revoke_statement() {
        let grant = Grant {
            target: AttrTarget::object(DbObjectId::Table {
                schema: "public".to_string(),
                name: "sensitive_data".to_string(),
            }),
            grantee: GranteeType::Role("temp_user".to_string()),
            privileges: vec!["SELECT".to_string(), "INSERT".to_string()],
            with_grant_option: false,
            depends_on: vec![],
            object_owner: "postgres".to_string(),
            is_default_acl: false,
        };

        let sql = render_revoke_statement(&grant);
        assert_eq!(
            sql,
            "REVOKE SELECT, INSERT ON \"public\".\"sensitive_data\" FROM \"temp_user\";"
        );
    }

    #[test]
    fn test_render_grant_on_sequence() {
        let grant = Grant {
            target: AttrTarget::object(DbObjectId::Sequence {
                schema: "public".to_string(),
                name: "users_id_seq".to_string(),
            }),
            grantee: GranteeType::Role("app_user".to_string()),
            privileges: vec!["USAGE".to_string()],
            with_grant_option: false,
            depends_on: vec![],
            object_owner: "postgres".to_string(),
            is_default_acl: false,
        };

        let sql = render_grant_statement(&grant);
        assert_eq!(
            sql,
            "GRANT USAGE ON SEQUENCE \"public\".\"users_id_seq\" TO \"app_user\";"
        );
    }

    #[test]
    fn test_render_grant_on_column() {
        let grant = Grant {
            target: AttrTarget::column(
                DbObjectId::Table {
                    schema: "public".to_string(),
                    name: "users".to_string(),
                },
                "email",
            ),
            grantee: GranteeType::Role("app_user".to_string()),
            privileges: vec!["SELECT".to_string(), "UPDATE".to_string()],
            with_grant_option: false,
            depends_on: vec![],
            object_owner: "postgres".to_string(),
            is_default_acl: false,
        };

        let sql = render_grant_statement(&grant);
        assert_eq!(
            sql,
            "GRANT SELECT (\"email\"), UPDATE (\"email\") ON \"public\".\"users\" TO \"app_user\";"
        );
    }

    #[test]
    fn test_render_revoke_on_column() {
        let grant = Grant {
            target: AttrTarget::column(
                DbObjectId::Table {
                    schema: "public".to_string(),
                    name: "users".to_string(),
                },
                "ssn",
            ),
            grantee: GranteeType::Role("app_user".to_string()),
            privileges: vec!["SELECT".to_string()],
            with_grant_option: false,
            depends_on: vec![],
            object_owner: "postgres".to_string(),
            is_default_acl: false,
        };

        let sql = render_revoke_statement(&grant);
        assert_eq!(
            sql,
            "REVOKE SELECT (\"ssn\") ON \"public\".\"users\" FROM \"app_user\";"
        );
    }

    #[test]
    fn test_render_grant_on_type() {
        let grant = Grant {
            target: AttrTarget::object(DbObjectId::Type {
                schema: "public".to_string(),
                name: "status_enum".to_string(),
            }),
            grantee: GranteeType::Role("app_user".to_string()),
            privileges: vec!["USAGE".to_string()],
            with_grant_option: false,
            depends_on: vec![],
            object_owner: "postgres".to_string(),
            is_default_acl: false,
        };

        let sql = render_grant_statement(&grant);
        assert_eq!(
            sql,
            "GRANT USAGE ON TYPE \"public\".\"status_enum\" TO \"app_user\";"
        );
    }
}
