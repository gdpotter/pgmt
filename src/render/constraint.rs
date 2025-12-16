//! SQL rendering for constraint operations

use crate::catalog::id::DbObjectId;
use crate::diff::operations::ConstraintOperation;
use crate::render::{RenderedSql, SqlRenderer};

impl SqlRenderer for ConstraintOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            ConstraintOperation::Create(constraint) => {
                vec![RenderedSql::new(
                    crate::render::sql::render_create_constraint(constraint),
                )]
            }
            ConstraintOperation::Drop(identifier) => {
                vec![RenderedSql::new(
                    crate::render::sql::render_drop_constraint(
                        &identifier.schema,
                        &identifier.table,
                        &identifier.name,
                    ),
                )]
            }
            ConstraintOperation::Comment(comment_op) => comment_op.to_sql(),
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            ConstraintOperation::Create(constraint) => constraint.id(),
            ConstraintOperation::Drop(identifier) => identifier.to_db_object_id(),
            ConstraintOperation::Comment(comment_op) => comment_op.db_object_id(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::constraint::{Constraint, ConstraintType};
    use crate::diff::operations::ConstraintIdentifier;
    use crate::render::Safety;

    fn create_unique_constraint() -> Constraint {
        Constraint {
            schema: "public".to_string(),
            table: "users".to_string(),
            name: "users_email_key".to_string(),
            constraint_type: ConstraintType::Unique {
                columns: vec!["email".to_string()],
            },
            comment: None,
            depends_on: vec![],
        }
    }

    fn create_fk_constraint() -> Constraint {
        Constraint {
            schema: "public".to_string(),
            table: "orders".to_string(),
            name: "orders_user_id_fkey".to_string(),
            constraint_type: ConstraintType::ForeignKey {
                columns: vec!["user_id".to_string()],
                referenced_schema: "public".to_string(),
                referenced_table: "users".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: Some("CASCADE".to_string()),
                on_update: None,
                deferrable: false,
                initially_deferred: false,
            },
            comment: None,
            depends_on: vec![],
        }
    }

    #[test]
    fn test_render_create_unique_constraint() {
        let constraint = create_unique_constraint();
        let op = ConstraintOperation::Create(constraint);
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert!(rendered[0].sql.contains("ALTER TABLE"));
        assert!(rendered[0].sql.contains("ADD CONSTRAINT"));
        assert!(rendered[0].sql.contains("users_email_key"));
        assert!(rendered[0].sql.contains("UNIQUE"));
        assert_eq!(rendered[0].safety, Safety::Safe);
    }

    #[test]
    fn test_render_create_fk_constraint() {
        let constraint = create_fk_constraint();
        let op = ConstraintOperation::Create(constraint);
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert!(rendered[0].sql.contains("FOREIGN KEY"));
        assert!(rendered[0].sql.contains("REFERENCES"));
        assert!(rendered[0].sql.contains("ON DELETE CASCADE"));
    }

    #[test]
    fn test_render_drop_constraint() {
        let identifier = ConstraintIdentifier {
            schema: "public".to_string(),
            table: "users".to_string(),
            name: "users_email_key".to_string(),
        };
        let op = ConstraintOperation::Drop(identifier);
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert!(rendered[0].sql.contains("ALTER TABLE"));
        assert!(rendered[0].sql.contains("DROP CONSTRAINT"));
        assert!(rendered[0].sql.contains("users_email_key"));
        assert_eq!(rendered[0].safety, Safety::Safe);
    }

    #[test]
    fn test_has_destructive_sql() {
        let constraint = create_unique_constraint();
        let identifier = ConstraintIdentifier::from_constraint(&constraint);

        let create_op = ConstraintOperation::Create(constraint);
        let drop_op = ConstraintOperation::Drop(identifier);

        // Constraints can be recreated from schema, so DROP CONSTRAINT is not destructive
        assert!(
            !create_op
                .to_sql()
                .iter()
                .any(|s| s.safety == Safety::Destructive)
        );
        assert!(
            !drop_op
                .to_sql()
                .iter()
                .any(|s| s.safety == Safety::Destructive)
        );
    }

    #[test]
    fn test_db_object_id() {
        let constraint = create_unique_constraint();
        let op = ConstraintOperation::Create(constraint.clone());
        assert_eq!(
            op.db_object_id(),
            DbObjectId::Constraint {
                schema: "public".to_string(),
                table: "users".to_string(),
                name: "users_email_key".to_string()
            }
        );
    }
}
