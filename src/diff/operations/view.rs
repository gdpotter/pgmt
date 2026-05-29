//! View operations for schema migrations

use super::OperationKind;
use super::comments::CommentOperation;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViewOption {
    SecurityInvoker,
    SecurityBarrier,
}

impl ViewOption {
    pub fn as_str(&self) -> &'static str {
        match self {
            ViewOption::SecurityInvoker => "security_invoker",
            ViewOption::SecurityBarrier => "security_barrier",
        }
    }
}

#[derive(Debug, Clone)]
pub enum ViewOperation {
    Create {
        schema: String,
        name: String,
        definition: String,
        security_invoker: bool,
        security_barrier: bool,
    },
    Drop {
        schema: String,
        name: String,
    },
    Replace {
        schema: String,
        name: String,
        definition: String,
        security_invoker: bool,
        security_barrier: bool,
    },
    SetOption {
        schema: String,
        name: String,
        option: ViewOption,
        enabled: bool,
    },
    Comment(CommentOperation),
    ColumnComment(CommentOperation),
}

impl ViewOperation {
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            Self::Create { .. } => OperationKind::Create,
            Self::Drop { .. } => OperationKind::Drop,
            Self::Replace { .. } => OperationKind::Alter,
            Self::SetOption { .. } => OperationKind::Alter,
            Self::Comment(_) => OperationKind::Alter,
            Self::ColumnComment(_) => OperationKind::Alter,
        }
    }
}
