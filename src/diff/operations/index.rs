use crate::catalog::index::Index;
use crate::diff::operations::{CommentOperation, OperationKind};

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum IndexOperation {
    Create(Index),
    Drop {
        schema: String,
        name: String,
    },
    Comment(CommentOperation),
    /// Set a table to use an index for clustering (CLUSTER table USING index)
    Cluster {
        table_schema: String,
        table_name: String,
        index_schema: String,
        index_name: String,
    },
    /// Remove clustering from a table (ALTER TABLE SET WITHOUT CLUSTER)
    SetWithoutCluster {
        schema: String,
        name: String,
    },
    /// Rebuild an invalid index (REINDEX INDEX)
    Reindex {
        schema: String,
        name: String,
        concurrently: bool,
    },
}

impl IndexOperation {
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            Self::Create(_) => OperationKind::Create,
            Self::Drop { .. } => OperationKind::Drop,
            Self::Comment(_)
            | Self::Cluster { .. }
            | Self::SetWithoutCluster { .. }
            | Self::Reindex { .. } => OperationKind::Alter,
        }
    }
}
