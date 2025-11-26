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
            | DbObjectId::Aggregate { schema, .. } => Some(schema.as_str()),
            DbObjectId::Grant { .. } | DbObjectId::Extension { .. } => None,
            DbObjectId::Comment { object_id } => object_id.schema(),
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
    }
}
