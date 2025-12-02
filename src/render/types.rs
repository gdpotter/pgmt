//! SQL rendering for type operations

use crate::catalog::id::DbObjectId;
use crate::diff::operations::TypeOperation;
use crate::render::{RenderedSql, Safety, SqlRenderer, quote_ident};

impl SqlRenderer for TypeOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            TypeOperation::Create {
                schema,
                name,
                kind,
                definition,
            } => vec![RenderedSql {
                sql: match kind.as_str() {
                    "ENUM" => format!(
                        "CREATE TYPE {}.{} AS ENUM {};",
                        quote_ident(schema),
                        quote_ident(name),
                        definition
                    ),
                    "COMPOSITE" => format!(
                        "CREATE TYPE {}.{} AS {};",
                        quote_ident(schema),
                        quote_ident(name),
                        definition
                    ),
                    "RANGE" => format!(
                        "CREATE TYPE {}.{} AS RANGE {}",
                        quote_ident(schema),
                        quote_ident(name),
                        definition
                    ),
                    _ => format!(
                        "CREATE TYPE {}.{} AS {} {}",
                        quote_ident(schema),
                        quote_ident(name),
                        kind,
                        definition
                    ),
                },
                safety: Safety::Safe,
            }],
            TypeOperation::Drop { schema, name } => vec![RenderedSql {
                sql: format!("DROP TYPE {}.{};", quote_ident(schema), quote_ident(name)),
                safety: Safety::Destructive,
            }],
            TypeOperation::Alter {
                schema,
                name,
                action,
                definition,
            } => vec![RenderedSql {
                sql: format!(
                    "ALTER TYPE {}.{} {} {};",
                    quote_ident(schema),
                    quote_ident(name),
                    action,
                    definition
                ),
                safety: Safety::Safe,
            }],
            TypeOperation::Comment(op) => op.to_sql(),
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            TypeOperation::Create { schema, name, .. }
            | TypeOperation::Drop { schema, name }
            | TypeOperation::Alter { schema, name, .. } => DbObjectId::Type {
                schema: schema.clone(),
                name: name.clone(),
            },
            TypeOperation::Comment(op) => op.db_object_id(),
        }
    }

    fn is_destructive(&self) -> bool {
        matches!(self, TypeOperation::Drop { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_create_enum_type() {
        let op = TypeOperation::Create {
            schema: "public".to_string(),
            name: "status".to_string(),
            kind: "ENUM".to_string(),
            definition: "('pending', 'active', 'completed')".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert_eq!(
            rendered[0].sql,
            "CREATE TYPE \"public\".\"status\" AS ENUM ('pending', 'active', 'completed');"
        );
        assert_eq!(rendered[0].safety, Safety::Safe);
    }

    #[test]
    fn test_render_create_composite_type() {
        let op = TypeOperation::Create {
            schema: "public".to_string(),
            name: "address".to_string(),
            kind: "COMPOSITE".to_string(),
            definition: "(street TEXT, city TEXT, zip VARCHAR(10))".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(
            rendered[0].sql,
            "CREATE TYPE \"public\".\"address\" AS (street TEXT, city TEXT, zip VARCHAR(10));"
        );
    }

    #[test]
    fn test_render_create_range_type() {
        let op = TypeOperation::Create {
            schema: "public".to_string(),
            name: "floatrange".to_string(),
            kind: "RANGE".to_string(),
            definition: "(SUBTYPE = float8)".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(
            rendered[0].sql,
            "CREATE TYPE \"public\".\"floatrange\" AS RANGE (SUBTYPE = float8)"
        );
    }

    #[test]
    fn test_render_drop_type() {
        let op = TypeOperation::Drop {
            schema: "public".to_string(),
            name: "old_type".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert_eq!(rendered[0].sql, "DROP TYPE \"public\".\"old_type\";");
        assert_eq!(rendered[0].safety, Safety::Destructive);
    }

    #[test]
    fn test_render_alter_type_add_value() {
        let op = TypeOperation::Alter {
            schema: "public".to_string(),
            name: "status".to_string(),
            action: "ADD VALUE".to_string(),
            definition: "'archived'".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(
            rendered[0].sql,
            "ALTER TYPE \"public\".\"status\" ADD VALUE 'archived';"
        );
        assert_eq!(rendered[0].safety, Safety::Safe);
    }

    #[test]
    fn test_render_alter_type_add_value_before() {
        let op = TypeOperation::Alter {
            schema: "public".to_string(),
            name: "status".to_string(),
            action: "ADD VALUE".to_string(),
            definition: "'draft' BEFORE 'pending'".to_string(),
        };
        let rendered = op.to_sql();
        assert_eq!(
            rendered[0].sql,
            "ALTER TYPE \"public\".\"status\" ADD VALUE 'draft' BEFORE 'pending';"
        );
    }

    #[test]
    fn test_is_destructive() {
        let create = TypeOperation::Create {
            schema: "s".to_string(),
            name: "t".to_string(),
            kind: "ENUM".to_string(),
            definition: "('a')".to_string(),
        };
        let drop = TypeOperation::Drop {
            schema: "s".to_string(),
            name: "t".to_string(),
        };
        let alter = TypeOperation::Alter {
            schema: "s".to_string(),
            name: "t".to_string(),
            action: "ADD VALUE".to_string(),
            definition: "'b'".to_string(),
        };

        assert!(!create.is_destructive());
        assert!(drop.is_destructive());
        assert!(!alter.is_destructive());
    }

    #[test]
    fn test_db_object_id() {
        let op = TypeOperation::Create {
            schema: "app".to_string(),
            name: "mytype".to_string(),
            kind: "ENUM".to_string(),
            definition: "('x')".to_string(),
        };
        assert_eq!(
            op.db_object_id(),
            DbObjectId::Type {
                schema: "app".to_string(),
                name: "mytype".to_string()
            }
        );
    }
}
