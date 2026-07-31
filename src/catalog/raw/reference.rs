//! The one resolution of a `pg_depend` reference edge into a dependency.
//!
//! Several kinds record what their definition names as plain `pg_depend` rows
//! pointing at `pg_type`, `pg_proc`, `pg_operator` or `pg_opclass` — an index
//! over an expression, a CHECK constraint calling a function, a domain default
//! doing the same. The raw fetches differ (the edge originates in a different
//! catalog table each time), but what a resolved edge *means* does not, so it is
//! spelled once here.

use sqlx::postgres::types::Oid;

use super::exclusion::is_system_schema;
use super::shared::{SharedCatalog, class};
use crate::catalog::id::DbObjectId;

/// One `pg_depend` edge from an object to a type, routine, operator or operator
/// class its definition uses.
///
/// The routine and operator fields are the parts of an identity only `pg_proc` /
/// `pg_operator` can give; each group is `None` for an edge to any other
/// catalog.
#[derive(Debug, Clone)]
pub struct RawReference {
    /// The OID of the object the edge originates from, in that object's own
    /// catalog table.
    pub source_oid: Oid,
    /// Name of the `pg_catalog` table the reference addresses (`pg_type`,
    /// `pg_proc`, `pg_operator`, `pg_opclass`).
    pub ref_class: String,
    pub ref_oid: Oid,

    /// Referenced routine (`pg_proc`).
    pub function_namespace: Option<Oid>,
    pub function_name: Option<String>,
    pub function_args: Option<String>,

    /// Referenced operator (`pg_operator`). The operand types are already
    /// rendered by `format_type`; `NONE` stands for an absent operand.
    pub operator_namespace: Option<Oid>,
    pub operator_name: Option<String>,
    pub operator_left_type: Option<String>,
    pub operator_right_type: Option<String>,
}

impl RawReference {
    /// The dependency this edge creates, or `None` when it creates none.
    ///
    /// An extension-provided referent (pg_trgm's `gin_trgm_ops`,
    /// fuzzystrmatch's `soundex()`, citext) resolves to the extension itself:
    /// the object is filtered from the catalog, so the dependent must depend on
    /// what creates it. A built-in type or a `pg_catalog` routine yields
    /// nothing, and so does an in-database operator class, which is not a
    /// catalog object of its own.
    pub fn dependency(&self, shared: &SharedCatalog) -> Option<DbObjectId> {
        let ref_class = class::intern(&self.ref_class);
        if let Some(extension) =
            ref_class.and_then(|class| shared.extensions.owner(class, self.ref_oid))
        {
            return Some(DbObjectId::Extension {
                name: extension.to_string(),
            });
        }

        match self.ref_class.as_str() {
            // The shared type map is what classifies a type reference: a
            // reference to an array resolves to its element type, and a domain
            // is a domain rather than a type. Rebuilding either from a join on
            // `pg_type` here would give identities no other object's
            // dependencies use.
            class::PG_TYPE => shared
                .resolve_type(self.ref_oid)
                .and_then(|referent| referent.dependency()),
            class::PG_PROC => {
                let schema = shared.namespaces.name(self.function_namespace?)?;
                if is_system_schema(schema) {
                    return None;
                }
                Some(DbObjectId::Function {
                    schema: schema.to_string(),
                    name: self.function_name.clone()?,
                    arguments: self.function_args.clone().unwrap_or_default(),
                })
            }
            class::PG_OPERATOR => {
                let schema = shared.namespaces.name(self.operator_namespace?)?;
                if is_system_schema(schema) {
                    return None;
                }
                Some(DbObjectId::Operator {
                    schema: schema.to_string(),
                    name: self.operator_name.clone()?,
                    arguments: format!(
                        "{}, {}",
                        self.operator_left_type.as_deref().unwrap_or("NONE"),
                        self.operator_right_type.as_deref().unwrap_or("NONE")
                    ),
                })
            }
            _ => None,
        }
    }
}
