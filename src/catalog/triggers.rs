use crate::catalog::{DependsOn, id::DbObjectId};

/// Represents a PostgreSQL trigger
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trigger {
    pub schema: String,
    pub table_name: String,
    pub name: String,
    pub function_schema: String,
    pub function_name: String,
    pub function_args: String,
    pub comment: Option<String>,
    pub depends_on: Vec<DbObjectId>,

    /// Complete trigger definition from pg_get_triggerdef()
    /// This is the authoritative source for trigger recreation
    pub definition: String,
}

impl DependsOn for Trigger {
    fn id(&self) -> DbObjectId {
        DbObjectId::Trigger {
            schema: self.schema.clone(),
            table: self.table_name.clone(),
            name: self.name.clone(),
        }
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}
