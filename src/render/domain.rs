//! SQL rendering for domain operations

use crate::catalog::id::DbObjectId;
use crate::diff::operations::DomainOperation;
use crate::render::{RenderedSql, Safety, SqlRenderer, quote_ident};

impl SqlRenderer for DomainOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            DomainOperation::Create {
                schema,
                name,
                definition,
            } => vec![RenderedSql {
                sql: format!(
                    "CREATE DOMAIN {}.{} {};",
                    quote_ident(schema),
                    quote_ident(name),
                    definition
                ),
                safety: Safety::Safe,
            }],
            DomainOperation::Drop { schema, name } => vec![RenderedSql {
                sql: format!("DROP DOMAIN {}.{};", quote_ident(schema), quote_ident(name)),
                safety: Safety::Safe,
            }],
            DomainOperation::AlterSetNotNull { schema, name } => vec![RenderedSql {
                sql: format!(
                    "ALTER DOMAIN {}.{} SET NOT NULL;",
                    quote_ident(schema),
                    quote_ident(name)
                ),
                safety: Safety::Safe,
            }],
            DomainOperation::AlterDropNotNull { schema, name } => vec![RenderedSql {
                sql: format!(
                    "ALTER DOMAIN {}.{} DROP NOT NULL;",
                    quote_ident(schema),
                    quote_ident(name)
                ),
                safety: Safety::Safe,
            }],
            DomainOperation::AlterSetDefault {
                schema,
                name,
                default,
            } => vec![RenderedSql {
                sql: format!(
                    "ALTER DOMAIN {}.{} SET DEFAULT {};",
                    quote_ident(schema),
                    quote_ident(name),
                    default
                ),
                safety: Safety::Safe,
            }],
            DomainOperation::AlterDropDefault { schema, name } => vec![RenderedSql {
                sql: format!(
                    "ALTER DOMAIN {}.{} DROP DEFAULT;",
                    quote_ident(schema),
                    quote_ident(name)
                ),
                safety: Safety::Safe,
            }],
            DomainOperation::AddConstraint {
                schema,
                name,
                constraint_name,
                expression,
            } => vec![RenderedSql {
                sql: format!(
                    "ALTER DOMAIN {}.{} ADD CONSTRAINT {} {};",
                    quote_ident(schema),
                    quote_ident(name),
                    quote_ident(constraint_name),
                    expression
                ),
                safety: Safety::Safe,
            }],
            DomainOperation::DropConstraint {
                schema,
                name,
                constraint_name,
            } => vec![RenderedSql {
                sql: format!(
                    "ALTER DOMAIN {}.{} DROP CONSTRAINT {};",
                    quote_ident(schema),
                    quote_ident(name),
                    quote_ident(constraint_name)
                ),
                safety: Safety::Safe,
            }],
            DomainOperation::Comment(op) => op.to_sql(),
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            DomainOperation::Create { schema, name, .. }
            | DomainOperation::Drop { schema, name }
            | DomainOperation::AlterSetNotNull { schema, name }
            | DomainOperation::AlterDropNotNull { schema, name }
            | DomainOperation::AlterSetDefault { schema, name, .. }
            | DomainOperation::AlterDropDefault { schema, name }
            | DomainOperation::AddConstraint { schema, name, .. }
            | DomainOperation::DropConstraint { schema, name, .. } => DbObjectId::Domain {
                schema: schema.clone(),
                name: name.clone(),
            },
            DomainOperation::Comment(op) => op.db_object_id(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_create_domain() {
        let op = DomainOperation::Create {
            schema: "public".to_string(),
            name: "email_address".to_string(),
            definition: "AS TEXT CHECK (VALUE ~ '^[^@]+@[^@]+$')".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert_eq!(
            rendered[0].sql,
            "CREATE DOMAIN \"public\".\"email_address\" AS TEXT CHECK (VALUE ~ '^[^@]+@[^@]+$');"
        );
        assert_eq!(rendered[0].safety, Safety::Safe);
    }

    #[test]
    fn test_render_drop_domain() {
        let op = DomainOperation::Drop {
            schema: "app".to_string(),
            name: "old_domain".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert_eq!(rendered[0].sql, "DROP DOMAIN \"app\".\"old_domain\";");
        assert_eq!(rendered[0].safety, Safety::Safe);
    }

    #[test]
    fn test_render_alter_set_not_null() {
        let op = DomainOperation::AlterSetNotNull {
            schema: "public".to_string(),
            name: "positive_int".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(
            rendered[0].sql,
            "ALTER DOMAIN \"public\".\"positive_int\" SET NOT NULL;"
        );
        assert_eq!(rendered[0].safety, Safety::Safe);
    }

    #[test]
    fn test_render_alter_drop_not_null() {
        let op = DomainOperation::AlterDropNotNull {
            schema: "public".to_string(),
            name: "nullable_int".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(
            rendered[0].sql,
            "ALTER DOMAIN \"public\".\"nullable_int\" DROP NOT NULL;"
        );
    }

    #[test]
    fn test_render_alter_set_default() {
        let op = DomainOperation::AlterSetDefault {
            schema: "public".to_string(),
            name: "status".to_string(),
            default: "'pending'".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(
            rendered[0].sql,
            "ALTER DOMAIN \"public\".\"status\" SET DEFAULT 'pending';"
        );
    }

    #[test]
    fn test_render_alter_drop_default() {
        let op = DomainOperation::AlterDropDefault {
            schema: "public".to_string(),
            name: "counter".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(
            rendered[0].sql,
            "ALTER DOMAIN \"public\".\"counter\" DROP DEFAULT;"
        );
    }

    #[test]
    fn test_render_add_constraint() {
        let op = DomainOperation::AddConstraint {
            schema: "public".to_string(),
            name: "positive_int".to_string(),
            constraint_name: "positive_check".to_string(),
            expression: "CHECK (VALUE > 0)".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(
            rendered[0].sql,
            "ALTER DOMAIN \"public\".\"positive_int\" ADD CONSTRAINT \"positive_check\" CHECK (VALUE > 0);"
        );
    }

    #[test]
    fn test_render_drop_constraint() {
        let op = DomainOperation::DropConstraint {
            schema: "public".to_string(),
            name: "email".to_string(),
            constraint_name: "email_check".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(
            rendered[0].sql,
            "ALTER DOMAIN \"public\".\"email\" DROP CONSTRAINT \"email_check\";"
        );
    }

    #[test]
    fn test_has_destructive_sql() {
        let create = DomainOperation::Create {
            schema: "s".to_string(),
            name: "d".to_string(),
            definition: "AS TEXT".to_string(),
        };
        let drop = DomainOperation::Drop {
            schema: "s".to_string(),
            name: "d".to_string(),
        };
        let alter = DomainOperation::AlterSetNotNull {
            schema: "s".to_string(),
            name: "d".to_string(),
        };

        // Domains can be recreated from schema, so DROP DOMAIN is not destructive
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
            !alter
                .to_sql()
                .iter()
                .any(|s| s.safety == Safety::Destructive)
        );
    }

    #[test]
    fn test_db_object_id() {
        let op = DomainOperation::Create {
            schema: "app".to_string(),
            name: "mydomain".to_string(),
            definition: "AS INTEGER".to_string(),
        };
        assert_eq!(
            op.db_object_id(),
            DbObjectId::Domain {
                schema: "app".to_string(),
                name: "mydomain".to_string()
            }
        );
    }
}
