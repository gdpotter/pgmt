//! SQL rendering for grant operations

use crate::catalog::id::DbObjectId;
use crate::diff::operations::GrantOperation;
use crate::render::{RenderedSql, SqlRenderer};

impl SqlRenderer for GrantOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            GrantOperation::Grant { grant } => {
                vec![RenderedSql::new(
                    crate::render::sql::render_grant_statement(grant),
                )]
            }
            GrantOperation::Revoke { grant } => {
                vec![RenderedSql::new(
                    crate::render::sql::render_revoke_statement(grant),
                )]
            }
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            GrantOperation::Grant { grant } => DbObjectId::Grant { id: grant.id() },
            GrantOperation::Revoke { grant } => DbObjectId::Grant { id: grant.id() },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::grant::{Grant, GranteeType, ObjectType};
    use crate::render::Safety;

    fn create_table_grant() -> Grant {
        Grant {
            grantee: GranteeType::Role("app_user".to_string()),
            object: ObjectType::Table {
                schema: "public".to_string(),
                name: "users".to_string(),
            },
            privileges: vec!["SELECT".to_string(), "INSERT".to_string()],
            with_grant_option: false,
            depends_on: vec![],
            object_owner: "admin".to_string(),
            is_default_acl: false,
        }
    }

    #[test]
    fn test_render_grant() {
        let grant = create_table_grant();
        let op = GrantOperation::Grant { grant };
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert!(rendered[0].sql.contains("GRANT"));
        assert!(rendered[0].sql.contains("SELECT"));
        assert!(rendered[0].sql.contains("INSERT"));
        assert!(rendered[0].sql.contains("app_user"));
    }

    #[test]
    fn test_render_revoke() {
        let grant = create_table_grant();
        let op = GrantOperation::Revoke { grant };
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert!(rendered[0].sql.contains("REVOKE"));
    }

    #[test]
    fn test_render_grant_to_public() {
        let grant = Grant {
            grantee: GranteeType::Public,
            object: ObjectType::Function {
                schema: "public".to_string(),
                name: "my_func".to_string(),
                arguments: "integer".to_string(),
            },
            privileges: vec!["EXECUTE".to_string()],
            with_grant_option: false,
            depends_on: vec![],
            object_owner: "admin".to_string(),
            is_default_acl: false,
        };
        let op = GrantOperation::Grant { grant };
        let rendered = op.to_sql();
        assert!(rendered[0].sql.contains("PUBLIC"));
    }

    #[test]
    fn test_has_destructive_sql() {
        let grant = create_table_grant();
        let grant_op = GrantOperation::Grant {
            grant: grant.clone(),
        };
        let revoke_op = GrantOperation::Revoke { grant };

        // Grants/revokes don't destroy data - permissions can be re-granted
        assert!(
            !grant_op
                .to_sql()
                .iter()
                .any(|s| s.safety == Safety::Destructive)
        );
        assert!(
            !revoke_op
                .to_sql()
                .iter()
                .any(|s| s.safety == Safety::Destructive)
        );
    }

    #[test]
    fn test_db_object_id() {
        let grant = create_table_grant();
        let op = GrantOperation::Grant {
            grant: grant.clone(),
        };
        let expected_id = grant.id();
        assert_eq!(op.db_object_id(), DbObjectId::Grant { id: expected_id });
    }
}
