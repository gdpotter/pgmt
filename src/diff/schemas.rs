//! Diff schemas: CREATE any new, DROP any missing. Comments are handled
//! centrally by [`crate::diff::comments`] (including normalizing PostgreSQL's
//! default `public` schema comment — see `Schema`'s `Attached` impl).

use crate::catalog::schema::Schema;
use crate::diff::operations::{MigrationStep, SchemaOperation};

pub fn diff(old: Option<&Schema>, new: Option<&Schema>) -> Vec<MigrationStep> {
    match (old, new) {
        // CREATE a new schema (never the implicit public schema)
        (None, Some(n)) => {
            if n.name == "public" {
                return Vec::new();
            }
            vec![MigrationStep::Schema(SchemaOperation::Create {
                name: n.name.clone(),
            })]
        }
        // DROP an existing schema (never the public schema)
        (Some(o), None) => {
            if o.name == "public" {
                return Vec::new();
            }
            vec![MigrationStep::Schema(SchemaOperation::Drop {
                name: o.name.clone(),
            })]
        }
        // Schema exists in both: nothing structural can change.
        (Some(_), Some(_)) | (None, None) => Vec::new(),
    }
}
