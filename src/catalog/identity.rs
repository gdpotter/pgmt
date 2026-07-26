//! Lightweight catalog identity for fast file-to-object tracking
//!
//! This module provides a minimal catalog representation that only contains object identities
//! (DbObjectId), not full object details. It's used during schema processing to track which
//! files create which objects, where we only need to know object existence, not their details.
//!
//! The single UNION ALL query is far cheaper than a full Catalog::load_unfiltered() because it:
//! - Runs one query instead of the load's ~40
//! - Skips columns, comments, dependencies, function bodies, etc.
//! - Returns only the minimal info needed to construct DbObjectId
//!
//! The query itself is not written here: it is composed from the per-kind branch
//! definitions in [`crate::catalog::raw::snapshot`], which build their filters
//! from the same exclusion rules the catalog converters apply. What this module
//! owns is the decoding — turning each row back into a [`DbObjectId`].
//!
//! Because the query is assembled at runtime, `sqlx` cannot check it at compile
//! time, and nothing makes a branch agree with the converter it mirrors. The
//! gate is the consistency test (`tests/catalog/identity_consistency.rs`), which
//! asserts this snapshot and a full catalog load report the identical set of
//! object identities.

use anyhow::Result;
use sqlx::PgPool;
use std::collections::BTreeSet;

use crate::catalog::id::DbObjectId;
use crate::catalog::raw::snapshot;

/// One row of the snapshot query: a kind tag plus the coordinates a
/// [`DbObjectId`] of that kind is built from. Every column is text, and the
/// coordinates a kind does not have are null.
#[derive(sqlx::FromRow)]
struct IdentityRow {
    #[sqlx(rename = "type")]
    kind: String,
    schema: Option<String>,
    name: String,
    #[sqlx(rename = "tbl")]
    table: Option<String>,
    args: Option<String>,
}

/// Lightweight catalog containing only object identities (no details)
///
/// Used for efficient file-to-object tracking where we need to diff
/// "what objects exist" but don't need full object metadata.
#[derive(Debug, Clone)]
pub struct CatalogIdentity {
    pub objects: BTreeSet<DbObjectId>,
}

impl CatalogIdentity {
    /// Load all object identities from the database using a single UNION ALL query
    pub async fn load(pool: &PgPool) -> Result<Self> {
        // The query is composed entirely from the branch definitions in
        // `raw::snapshot` — catalog table names and predicates written in this
        // repository, with no value from the database or the user reaching it.
        let rows: Vec<IdentityRow> = sqlx::query_as(sqlx::AssertSqlSafe(snapshot::query()))
            .fetch_all(pool)
            .await?;

        let mut objects = BTreeSet::new();

        for row in rows {
            let object_id = match row.kind.as_str() {
                "schema" => DbObjectId::Schema {
                    name: row.name.clone(),
                },
                "table" => DbObjectId::Table {
                    schema: row.schema.clone().unwrap_or_default(),
                    name: row.name.clone(),
                },
                "view" => DbObjectId::View {
                    schema: row.schema.clone().unwrap_or_default(),
                    name: row.name.clone(),
                },
                "sequence" => DbObjectId::Sequence {
                    schema: row.schema.clone().unwrap_or_default(),
                    name: row.name.clone(),
                },
                "index" => DbObjectId::Index {
                    schema: row.schema.clone().unwrap_or_default(),
                    name: row.name.clone(),
                },
                "function" => DbObjectId::Function {
                    schema: row.schema.clone().unwrap_or_default(),
                    name: row.name.clone(),
                    arguments: row.args.clone().unwrap_or_default(),
                },
                "procedure" => DbObjectId::Procedure {
                    schema: row.schema.clone().unwrap_or_default(),
                    name: row.name.clone(),
                    arguments: row.args.clone().unwrap_or_default(),
                },
                "aggregate" => DbObjectId::Aggregate {
                    schema: row.schema.clone().unwrap_or_default(),
                    name: row.name.clone(),
                    arguments: row.args.clone().unwrap_or_default(),
                },
                "type" => DbObjectId::Type {
                    schema: row.schema.clone().unwrap_or_default(),
                    name: row.name.clone(),
                },
                "domain" => DbObjectId::Domain {
                    schema: row.schema.clone().unwrap_or_default(),
                    name: row.name.clone(),
                },
                "constraint" => DbObjectId::Constraint {
                    schema: row.schema.clone().unwrap_or_default(),
                    table: row.table.clone().unwrap_or_default(),
                    name: row.name.clone(),
                },
                "trigger" => DbObjectId::Trigger {
                    schema: row.schema.clone().unwrap_or_default(),
                    table: row.table.clone().unwrap_or_default(),
                    name: row.name.clone(),
                },
                "policy" => DbObjectId::Policy {
                    schema: row.schema.clone().unwrap_or_default(),
                    table: row.table.clone().unwrap_or_default(),
                    name: row.name.clone(),
                },
                "operator" => DbObjectId::Operator {
                    schema: row.schema.clone().unwrap_or_default(),
                    name: row.name.clone(),
                    arguments: row.args.clone().unwrap_or_default(),
                },
                "cast" => DbObjectId::Cast {
                    source: row.name.clone(),
                    target: row.table.clone().unwrap_or_default(),
                },
                "extension" => DbObjectId::Extension {
                    name: row.name.clone(),
                },
                other => {
                    // Log unexpected type but don't fail - defensive coding
                    tracing::warn!("Unknown object type in identity query: {}", other);
                    continue;
                }
            };

            objects.insert(object_id);
        }

        Ok(Self { objects })
    }
}

/// Find objects that exist in new but not in old (set difference)
pub fn find_new_objects(old: &CatalogIdentity, new: &CatalogIdentity) -> Vec<DbObjectId> {
    new.objects.difference(&old.objects).cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_new_objects_empty() {
        let old = CatalogIdentity {
            objects: BTreeSet::new(),
        };
        let new = CatalogIdentity {
            objects: BTreeSet::new(),
        };

        let result = find_new_objects(&old, &new);
        assert!(result.is_empty());
    }

    #[test]
    fn test_find_new_objects_detects_additions() {
        let old = CatalogIdentity {
            objects: BTreeSet::from([DbObjectId::Schema {
                name: "existing".to_string(),
            }]),
        };
        let new = CatalogIdentity {
            objects: BTreeSet::from([
                DbObjectId::Schema {
                    name: "existing".to_string(),
                },
                DbObjectId::Table {
                    schema: "existing".to_string(),
                    name: "new_table".to_string(),
                },
            ]),
        };

        let result = find_new_objects(&old, &new);
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            DbObjectId::Table {
                schema: "existing".to_string(),
                name: "new_table".to_string(),
            }
        );
    }
}
