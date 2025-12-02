use crate::catalog::id::DbObjectId;
use crate::catalog::index::Index;
use crate::diff::operations::{CommentOperation, CommentTarget};
use crate::render::quote_ident;

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum IndexOperation {
    Create(Index),
    Drop {
        schema: String,
        name: String,
    },
    Comment(CommentOperation<IndexTarget>),
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

#[derive(Debug, Clone)]
pub struct IndexTarget {
    pub schema: String,
    pub name: String,
}

impl CommentTarget for IndexTarget {
    const OBJECT_TYPE: &'static str = "INDEX";

    fn identifier(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.name))
    }

    fn db_object_id(&self) -> DbObjectId {
        DbObjectId::Index {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }
}
