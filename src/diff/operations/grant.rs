//! Grant operations

use super::SqlRenderer;
use crate::catalog::grant::Grant;
use crate::catalog::id::DbObjectId;
use crate::render::RenderedSql;

#[derive(Debug, Clone)]
pub enum GrantOperation {
    Grant { grant: Grant },
    Revoke { grant: Grant },
}

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

    fn is_destructive(&self) -> bool {
        matches!(self, GrantOperation::Revoke { .. })
    }
}
