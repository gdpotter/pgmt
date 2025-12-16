//! Grant operations

use super::OperationKind;
use crate::catalog::grant::Grant;

#[derive(Debug, Clone)]
pub enum GrantOperation {
    Grant { grant: Grant },
    Revoke { grant: Grant },
}

impl GrantOperation {
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            // GRANT creates a new permission
            Self::Grant { .. } => OperationKind::Create,
            // REVOKE removes a permission - needs to run before the object is dropped
            // so it's classified as Drop for proper ordering
            Self::Revoke { .. } => OperationKind::Drop,
        }
    }
}
