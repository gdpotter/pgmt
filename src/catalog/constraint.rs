use super::id::{DbObjectId, DependsOn};

/* ---------- Data structures ---------- */

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConstraintType {
    Unique {
        columns: Vec<String>,
    },
    ForeignKey {
        columns: Vec<String>,
        referenced_schema: String,
        referenced_table: String,
        referenced_columns: Vec<String>,
        on_delete: Option<String>,
        on_update: Option<String>,
        deferrable: bool,
        initially_deferred: bool,
    },
    Check {
        expression: String,
    },
    Exclusion {
        elements: Vec<String>,
        operator_classes: Vec<String>,
        operators: Vec<String>,
        index_method: String,
        predicate: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct Constraint {
    pub schema: String,
    pub table_name: String,
    pub name: String,
    pub constraint_type: ConstraintType,
    pub comment: Option<String>,
    pub depends_on: Vec<DbObjectId>,
}

impl Constraint {
    pub fn id(&self) -> DbObjectId {
        DbObjectId::Constraint {
            schema: self.schema.clone(),
            table: self.table_name.clone(),
            name: self.name.clone(),
        }
    }
}

impl DependsOn for Constraint {
    fn id(&self) -> DbObjectId {
        self.id()
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}
