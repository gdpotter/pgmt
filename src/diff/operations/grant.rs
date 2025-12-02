//! Grant operations

use crate::catalog::grant::Grant;

#[derive(Debug, Clone)]
pub enum GrantOperation {
    Grant { grant: Grant },
    Revoke { grant: Grant },
}
