use super::id::{DbObjectId, DependsOn};

/// A user-defined PostgreSQL cast (`CREATE CAST`).
///
/// Casts are not schema-scoped; their identity is the (source type, target type)
/// pair. `source` and `target` are canonical `format_type` names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cast {
    pub source: String,
    pub target: String,
    /// The full reconstructed `CREATE CAST` statement (no trailing `;`).
    pub definition: String,
    pub comment: Option<String>,
    pub depends_on: Vec<DbObjectId>,
}

impl Cast {
    pub fn id(&self) -> DbObjectId {
        DbObjectId::Cast {
            source: self.source.clone(),
            target: self.target.clone(),
        }
    }
}

impl DependsOn for Cast {
    fn id(&self) -> DbObjectId {
        DbObjectId::Cast {
            source: self.source.clone(),
            target: self.target.clone(),
        }
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}
