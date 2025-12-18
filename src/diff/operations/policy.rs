use super::{CommentOperation, CommentTarget, OperationKind};
use crate::catalog::id::DbObjectId;
use crate::catalog::policy::Policy;

/// Identifier for a policy
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyIdentifier {
    pub schema: String,
    pub table: String,
    pub name: String,
}

impl PolicyIdentifier {
    pub fn new(schema: String, table: String, name: String) -> Self {
        Self {
            schema,
            table,
            name,
        }
    }

    pub fn from_policy(policy: &Policy) -> Self {
        Self {
            schema: policy.schema.clone(),
            table: policy.table_name.clone(),
            name: policy.name.clone(),
        }
    }
}

impl CommentTarget for PolicyIdentifier {
    const OBJECT_TYPE: &'static str = "POLICY";

    fn identifier(&self) -> String {
        format!(
            "\"{}\" ON \"{}\".\"{}\"",
            self.name, self.schema, self.table
        )
    }

    fn db_object_id(&self) -> DbObjectId {
        DbObjectId::Policy {
            schema: self.schema.clone(),
            table: self.table.clone(),
            name: self.name.clone(),
        }
    }
}

/// Operations that can be performed on policies
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyOperation {
    Create {
        policy: Box<Policy>,
    },
    Drop {
        identifier: PolicyIdentifier,
    },
    Alter {
        identifier: PolicyIdentifier,
        new_roles: Option<Vec<String>>,
        new_using: Option<Option<String>>,
        new_with_check: Option<Option<String>>,
    },
    /// Policy needs full replacement (command or permissive changed)
    Replace {
        old_policy: Box<Policy>,
        new_policy: Box<Policy>,
    },
    Comment(CommentOperation<PolicyIdentifier>),
}

impl PolicyOperation {
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            Self::Create { .. } => OperationKind::Create,
            Self::Drop { .. } => OperationKind::Drop,
            Self::Alter { .. } | Self::Replace { .. } | Self::Comment(_) => OperationKind::Alter,
        }
    }
}
