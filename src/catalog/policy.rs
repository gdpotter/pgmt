//! The logical row-level-security policy, once names are resolved and OIDs are
//! gone.
//!
//! Loading lives in `catalog::raw::policy`.

use crate::catalog::{DependsOn, id::DbObjectId};

/// Command type for RLS policies
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyCommand {
    All,    // 'a' or '*' - applies to all commands
    Select, // 'r' - SELECT only
    Insert, // 'a' - INSERT only
    Update, // 'w' - UPDATE only
    Delete, // 'd' - DELETE only
}

/// Represents a PostgreSQL Row-Level Security policy
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Policy {
    pub schema: String,
    pub table_name: String,
    pub name: String,

    /// Command type this policy applies to
    pub command: PolicyCommand,

    /// true = PERMISSIVE, false = RESTRICTIVE
    pub permissive: bool,

    /// Roles this policy applies to (empty = PUBLIC)
    pub roles: Vec<String>,

    /// USING expression (for SELECT, UPDATE, DELETE)
    pub using_expr: Option<String>,

    /// WITH CHECK expression (for INSERT, UPDATE)
    pub with_check_expr: Option<String>,

    /// Comment on the policy
    pub comment: Option<String>,

    /// Dependencies (primarily the table)
    pub depends_on: Vec<DbObjectId>,
}

impl DependsOn for Policy {
    fn id(&self) -> DbObjectId {
        DbObjectId::Policy {
            schema: self.schema.clone(),
            table: self.table_name.clone(),
            name: self.name.clone(),
        }
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}
