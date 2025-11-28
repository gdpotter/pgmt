//! Lightweight catalog identity for fast file-to-object tracking
//!
//! This module provides a minimal catalog representation that only contains object identities
//! (DbObjectId), not full object details. It's used during schema processing to track which
//! files create which objects, where we only need to know object existence, not their details.
//!
//! The single UNION ALL query is ~10-25x faster than a full Catalog::load() because it:
//! - Runs one query instead of 50+
//! - Skips columns, comments, dependencies, function bodies, etc.
//! - Returns only the minimal info needed to construct DbObjectId

use anyhow::Result;
use sqlx::PgPool;
use std::collections::BTreeSet;

use crate::catalog::id::DbObjectId;

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
        let rows = sqlx::query!(
            r#"
            -- Schemas (excluding system schemas)
            SELECT 'schema' AS "type!", NULL AS "schema?", nspname AS "name!", NULL AS "tbl?", NULL AS "args?"
            FROM pg_namespace
            WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'public')

            UNION ALL

            -- Tables (excluding extension-owned)
            SELECT 'table', n.nspname, c.relname, NULL, NULL
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relkind = 'r'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND NOT EXISTS (
                SELECT 1 FROM pg_depend dep
                WHERE dep.objid = c.oid AND dep.deptype = 'e'
              )

            UNION ALL

            -- Views (excluding extension-owned)
            SELECT 'view', n.nspname, c.relname, NULL, NULL
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relkind = 'v'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND NOT EXISTS (
                SELECT 1 FROM pg_depend dep
                WHERE dep.objid = c.oid AND dep.deptype = 'e'
              )

            UNION ALL

            -- Materialized views (tracked as views, excluding extension-owned)
            SELECT 'view', n.nspname, c.relname, NULL, NULL
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relkind = 'm'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND NOT EXISTS (
                SELECT 1 FROM pg_depend dep
                WHERE dep.objid = c.oid AND dep.deptype = 'e'
              )

            UNION ALL

            -- Sequences (excluding extension-owned)
            SELECT 'sequence', n.nspname, c.relname, NULL, NULL
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relkind = 'S'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND NOT EXISTS (
                SELECT 1 FROM pg_depend dep
                WHERE dep.objid = c.oid AND dep.deptype = 'e'
              )

            UNION ALL

            -- Indexes (excluding constraint-backing indexes and extension-owned)
            SELECT 'index', n.nspname, c.relname, NULL, NULL
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relkind = 'i'
              AND NOT EXISTS (SELECT 1 FROM pg_constraint con WHERE con.conindid = c.oid)
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND NOT EXISTS (
                SELECT 1 FROM pg_depend dep
                WHERE dep.objid = c.oid AND dep.deptype = 'e'
              )

            UNION ALL

            -- Functions and procedures (excluding extension-owned)
            SELECT 'function', n.nspname, p.proname, NULL, NULL
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE p.prokind IN ('f', 'p')
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
              AND NOT EXISTS (
                SELECT 1 FROM pg_depend dep
                WHERE dep.objid = p.oid AND dep.deptype = 'e'
              )

            UNION ALL

            -- Aggregates (need argument signature for identity, excluding extension-owned)
            SELECT 'aggregate', n.nspname, p.proname, NULL, pg_get_function_identity_arguments(p.oid)
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE p.prokind = 'a'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
              AND NOT EXISTS (
                SELECT 1 FROM pg_depend dep
                WHERE dep.objid = p.oid AND dep.deptype = 'e'
              )

            UNION ALL

            -- Custom types (enum and composite) - excludes row types and extension-owned
            SELECT 'type', n.nspname, t.typname, NULL, NULL
            FROM pg_type t
            JOIN pg_namespace n ON t.typnamespace = n.oid
            WHERE t.typtype IN ('e', 'c')
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
              AND NOT EXISTS (
                SELECT 1 FROM pg_class c
                WHERE c.reltype = t.oid
                  AND c.relkind IN ('r', 'v', 'm', 'S')
              )
              AND NOT EXISTS (
                SELECT 1 FROM pg_depend dep
                WHERE dep.objid = t.oid AND dep.deptype = 'e'
              )

            UNION ALL

            -- Domains (excluding extension-owned)
            SELECT 'domain', n.nspname, t.typname, NULL, NULL
            FROM pg_type t
            JOIN pg_namespace n ON t.typnamespace = n.oid
            WHERE t.typtype = 'd'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
              AND NOT EXISTS (
                SELECT 1 FROM pg_depend dep
                WHERE dep.objid = t.oid AND dep.deptype = 'e'
              )

            UNION ALL

            -- Constraints (unique, foreign key, check, exclusion - primary keys handled by table)
            SELECT 'constraint', n.nspname, co.conname, cl.relname, NULL
            FROM pg_constraint co
            JOIN pg_class cl ON co.conrelid = cl.oid
            JOIN pg_namespace n ON cl.relnamespace = n.oid
            WHERE co.contype IN ('u', 'f', 'c', 'x')
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')

            UNION ALL

            -- Triggers
            SELECT 'trigger', n.nspname, t.tgname, c.relname, NULL
            FROM pg_trigger t
            JOIN pg_class c ON t.tgrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE NOT t.tgisinternal
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')

            UNION ALL

            -- Extensions
            SELECT 'extension', NULL, extname, NULL, NULL
            FROM pg_extension
            WHERE extname NOT IN ('plpgsql')
            "#
        )
        .fetch_all(pool)
        .await?;

        let mut objects = BTreeSet::new();

        for row in rows {
            let object_id = match row.r#type.as_str() {
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
                    table: row.tbl.clone().unwrap_or_default(),
                    name: row.name.clone(),
                },
                "trigger" => DbObjectId::Trigger {
                    schema: row.schema.clone().unwrap_or_default(),
                    table: row.tbl.clone().unwrap_or_default(),
                    name: row.name.clone(),
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
