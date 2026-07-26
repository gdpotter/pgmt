use super::id::{DbObjectId, DependsOn};

/* ---------- Data structures ---------- */

#[derive(Debug, Clone, PartialEq)]
pub enum IndexType {
    Btree,
    Hash,
    Gist,
    Gin,
    Spgist,
    Brin,
    Custom(String), // For extension-provided index types
}

impl IndexType {
    pub fn from_string(s: &str) -> Self {
        match s {
            "btree" => IndexType::Btree,
            "hash" => IndexType::Hash,
            "gist" => IndexType::Gist,
            "gin" => IndexType::Gin,
            "spgist" => IndexType::Spgist,
            "brin" => IndexType::Brin,
            _ => IndexType::Custom(s.to_string()),
        }
    }
}

impl std::fmt::Display for IndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexType::Btree => write!(f, "btree"),
            IndexType::Hash => write!(f, "hash"),
            IndexType::Gist => write!(f, "gist"),
            IndexType::Gin => write!(f, "gin"),
            IndexType::Spgist => write!(f, "spgist"),
            IndexType::Brin => write!(f, "brin"),
            IndexType::Custom(s) => write!(f, "{}", s),
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndexColumn {
    pub expression: String, // Column name or expression
    pub collation: Option<String>,
    pub opclass: Option<String>,        // Operator class
    pub ordering: Option<String>,       // ASC/DESC for btree indexes
    pub nulls_ordering: Option<String>, // NULLS FIRST/LAST
}

#[derive(Debug, Clone)]
pub struct Index {
    pub schema: String,
    pub name: String,
    pub table_schema: String,
    pub table_name: String,
    pub index_type: IndexType,
    pub is_unique: bool,
    pub is_clustered: bool,
    pub is_valid: bool,
    pub columns: Vec<IndexColumn>,
    pub include_columns: Vec<String>, // INCLUDE columns for covering indexes
    pub predicate: Option<String>,    // WHERE clause for partial indexes
    pub tablespace: Option<String>,
    pub storage_parameters: Vec<(String, String)>, // WITH (...) options
    pub comment: Option<String>,
    pub depends_on: Vec<DbObjectId>,
}

impl Index {
    pub fn id(&self) -> DbObjectId {
        DbObjectId::Index {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }
}

impl DependsOn for Index {
    fn id(&self) -> DbObjectId {
        self.id()
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}
