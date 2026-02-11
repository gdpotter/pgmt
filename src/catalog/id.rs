use std::fmt;

/// A globally unique identifier for any database object in pgmt.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DbObjectId {
    Schema {
        name: String,
    },

    Table {
        schema: String,
        name: String,
    },

    View {
        schema: String,
        name: String,
    },

    Type {
        schema: String,
        name: String,
    },
    Domain {
        schema: String,
        name: String,
    },
    Function {
        schema: String,
        name: String,
        arguments: String,
    },
    Sequence {
        schema: String,
        name: String,
    },
    Index {
        schema: String,
        name: String,
    },
    Constraint {
        schema: String,
        table: String,
        name: String,
    },
    Grant {
        id: String, // Unique identifier: "grantee@object_type:object_name"
    },
    Trigger {
        schema: String,
        table: String,
        name: String,
    },
    Policy {
        schema: String,
        table: String,
        name: String,
    },
    Comment {
        object_id: Box<DbObjectId>, // The object being commented on
    },
    Extension {
        name: String,
    },
    Aggregate {
        schema: String,
        name: String,
        arguments: String,
    },
    /// Column-level dependency for BEGIN ATOMIC functions (PostgreSQL 14+)
    /// and other objects that have pg_depend entries with refobjsubid > 0
    Column {
        schema: String,
        table: String,
        column: String,
    },
}

impl DbObjectId {
    /// Get the schema name for this object, if applicable.
    /// Returns None for schema-less objects like Schema (where name IS the schema),
    /// Extension (database-wide), Grant (uses composite ID), and Comment (delegates to inner).
    pub fn schema(&self) -> Option<&str> {
        match self {
            DbObjectId::Schema { name } => Some(name.as_str()),
            DbObjectId::Table { schema, .. }
            | DbObjectId::View { schema, .. }
            | DbObjectId::Type { schema, .. }
            | DbObjectId::Domain { schema, .. }
            | DbObjectId::Function { schema, .. }
            | DbObjectId::Sequence { schema, .. }
            | DbObjectId::Index { schema, .. }
            | DbObjectId::Constraint { schema, .. }
            | DbObjectId::Trigger { schema, .. }
            | DbObjectId::Policy { schema, .. }
            | DbObjectId::Aggregate { schema, .. }
            | DbObjectId::Column { schema, .. } => Some(schema.as_str()),
            DbObjectId::Grant { .. } | DbObjectId::Extension { .. } => None,
            DbObjectId::Comment { object_id } => object_id.schema(),
        }
    }
}

impl fmt::Display for DbObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Schema { name } => write!(f, "schema {name}"),
            Self::Table { schema, name } => write!(f, "table {schema}.{name}"),
            Self::View { schema, name } => write!(f, "view {schema}.{name}"),
            Self::Type { schema, name } => write!(f, "type {schema}.{name}"),
            Self::Domain { schema, name } => write!(f, "domain {schema}.{name}"),
            Self::Function {
                schema,
                name,
                arguments,
            } => write!(f, "function {schema}.{name}({arguments})"),
            Self::Sequence { schema, name } => write!(f, "sequence {schema}.{name}"),
            Self::Index { schema, name } => write!(f, "index {schema}.{name}"),
            Self::Constraint {
                schema,
                table,
                name,
            } => write!(f, "constraint {schema}.{table}.{name}"),
            Self::Grant { id } => write!(f, "grant {id}"),
            Self::Trigger {
                schema,
                table,
                name,
            } => write!(f, "trigger {schema}.{table}.{name}"),
            Self::Policy {
                schema,
                table,
                name,
            } => write!(f, "policy {schema}.{table}.{name}"),
            Self::Comment { object_id } => write!(f, "comment on {object_id}"),
            Self::Extension { name } => write!(f, "extension {name}"),
            Self::Aggregate {
                schema,
                name,
                arguments,
            } => write!(f, "aggregate {schema}.{name}({arguments})"),
            Self::Column {
                schema,
                table,
                column,
            } => write!(f, "column {schema}.{table}.{column}"),
        }
    }
}

pub trait DependsOn {
    fn id(&self) -> DbObjectId;
    fn depends_on(&self) -> &[DbObjectId];
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_method() {
        // Schema variant returns its name as the schema
        assert_eq!(
            DbObjectId::Schema {
                name: "public".to_string()
            }
            .schema(),
            Some("public")
        );

        // Objects with schema field return that schema
        assert_eq!(
            DbObjectId::Table {
                schema: "app".to_string(),
                name: "users".to_string()
            }
            .schema(),
            Some("app")
        );

        assert_eq!(
            DbObjectId::Function {
                schema: "utils".to_string(),
                name: "calculate".to_string(),
                arguments: "integer".to_string()
            }
            .schema(),
            Some("utils")
        );

        // Grant and Extension return None (no schema)
        assert_eq!(
            DbObjectId::Grant {
                id: "user@table:public.users".to_string()
            }
            .schema(),
            None
        );

        assert_eq!(
            DbObjectId::Extension {
                name: "pgcrypto".to_string()
            }
            .schema(),
            None
        );

        // Comment delegates to inner object
        assert_eq!(
            DbObjectId::Comment {
                object_id: Box::new(DbObjectId::Table {
                    schema: "test".to_string(),
                    name: "items".to_string()
                })
            }
            .schema(),
            Some("test")
        );

        // Column returns its schema
        assert_eq!(
            DbObjectId::Column {
                schema: "app".to_string(),
                table: "users".to_string(),
                column: "email".to_string()
            }
            .schema(),
            Some("app")
        );
    }

    #[test]
    fn test_display() {
        assert_eq!(
            DbObjectId::Schema {
                name: "public".into()
            }
            .to_string(),
            "schema public"
        );
        assert_eq!(
            DbObjectId::Table {
                schema: "public".into(),
                name: "users".into()
            }
            .to_string(),
            "table public.users"
        );
        assert_eq!(
            DbObjectId::View {
                schema: "public".into(),
                name: "user_rankings".into()
            }
            .to_string(),
            "view public.user_rankings"
        );
        assert_eq!(
            DbObjectId::Type {
                schema: "public".into(),
                name: "status".into()
            }
            .to_string(),
            "type public.status"
        );
        assert_eq!(
            DbObjectId::Domain {
                schema: "public".into(),
                name: "email".into()
            }
            .to_string(),
            "domain public.email"
        );
        assert_eq!(
            DbObjectId::Function {
                schema: "public".into(),
                name: "calculate_score".into(),
                arguments: "integer".into()
            }
            .to_string(),
            "function public.calculate_score(integer)"
        );
        assert_eq!(
            DbObjectId::Sequence {
                schema: "public".into(),
                name: "users_id_seq".into()
            }
            .to_string(),
            "sequence public.users_id_seq"
        );
        assert_eq!(
            DbObjectId::Index {
                schema: "public".into(),
                name: "users_pkey".into()
            }
            .to_string(),
            "index public.users_pkey"
        );
        assert_eq!(
            DbObjectId::Constraint {
                schema: "public".into(),
                table: "orders".into(),
                name: "orders_user_fk".into()
            }
            .to_string(),
            "constraint public.orders.orders_user_fk"
        );
        assert_eq!(
            DbObjectId::Grant {
                id: "reader@table:public.users".into()
            }
            .to_string(),
            "grant reader@table:public.users"
        );
        assert_eq!(
            DbObjectId::Trigger {
                schema: "public".into(),
                table: "users".into(),
                name: "audit_trigger".into()
            }
            .to_string(),
            "trigger public.users.audit_trigger"
        );
        assert_eq!(
            DbObjectId::Policy {
                schema: "public".into(),
                table: "users".into(),
                name: "user_isolation".into()
            }
            .to_string(),
            "policy public.users.user_isolation"
        );
        assert_eq!(
            DbObjectId::Comment {
                object_id: Box::new(DbObjectId::Table {
                    schema: "public".into(),
                    name: "users".into()
                })
            }
            .to_string(),
            "comment on table public.users"
        );
        assert_eq!(
            DbObjectId::Extension {
                name: "pgcrypto".into()
            }
            .to_string(),
            "extension pgcrypto"
        );
        assert_eq!(
            DbObjectId::Aggregate {
                schema: "public".into(),
                name: "array_agg".into(),
                arguments: "integer".into()
            }
            .to_string(),
            "aggregate public.array_agg(integer)"
        );
        assert_eq!(
            DbObjectId::Column {
                schema: "public".into(),
                table: "users".into(),
                column: "email".into()
            }
            .to_string(),
            "column public.users.email"
        );
    }
}
