//! Lightweight catalog identity for fast file-to-object tracking
//!
//! This module provides a minimal catalog representation that only contains object identities
//! (DbObjectId), not full object details. It's used during schema processing to track which
//! files create which objects, where we only need to know object existence, not their details.
//!
//! The single UNION ALL query is ~10-25x faster than a full Catalog::load_unfiltered() because it:
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

            -- Views (excluding extension-owned). relkind 'v' only: the snapshot
            -- mirrors exactly what the catalog fetchers capture and nothing
            -- more, and the view catalog does not capture materialized views. An
            -- identity no fetcher can yield would be attributed to a file and
            -- then never found in the catalog it is diffed against.
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

            -- Sequences (excluding extension-owned and identity-owned; the
            -- latter are internal to their GENERATED ... AS IDENTITY column)
            SELECT 'sequence', n.nspname, c.relname, NULL, NULL
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relkind = 'S'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND NOT EXISTS (
                SELECT 1 FROM pg_depend dep
                WHERE dep.objid = c.oid AND dep.deptype = 'e'
              )
              AND NOT EXISTS (
                SELECT 1 FROM pg_depend dep
                WHERE dep.objid = c.oid
                  AND dep.classid = 'pg_class'::regclass
                  AND dep.deptype = 'i'
              )

            UNION ALL

            -- Indexes (excluding constraint-backing indexes and extension-owned).
            -- Only primary key, unique and exclusion constraints own their
            -- backing index (the constraint catalog reports those); a foreign
            -- key's conindid merely points at the *referenced* table's index,
            -- which stays a user index of its own.
            -- Indexes created by extension scripts get no pg_depend 'e' entry of
            -- their own — membership is recorded on the parent table, so check both.
            SELECT 'index', n.nspname, c.relname, NULL, NULL
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relkind = 'i'
              AND NOT EXISTS (
                SELECT 1 FROM pg_constraint con
                WHERE con.conindid = c.oid AND con.contype IN ('p', 'u', 'x')
              )
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND NOT EXISTS (
                SELECT 1 FROM pg_depend dep
                WHERE dep.objid = c.oid AND dep.deptype = 'e'
              )
              AND NOT EXISTS (
                SELECT 1 FROM pg_index idx
                JOIN pg_depend dep ON dep.objid = idx.indrelid AND dep.deptype = 'e'
                WHERE idx.indexrelid = c.oid
              )

            UNION ALL

            -- Functions (excluding extension-owned). Procedures are a distinct
            -- identity variant, as in the function catalog.
            SELECT 'function', n.nspname, p.proname, NULL, pg_get_function_identity_arguments(p.oid)
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE p.prokind = 'f'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
              AND NOT EXISTS (
                SELECT 1 FROM pg_depend dep
                WHERE dep.objid = p.oid AND dep.deptype = 'e'
              )

            UNION ALL

            -- Procedures (excluding extension-owned)
            SELECT 'procedure', n.nspname, p.proname, NULL, pg_get_function_identity_arguments(p.oid)
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE p.prokind = 'p'
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

            -- Custom types (enum, composite, range) - excludes row types and
            -- extension-owned; domains are handled separately
            SELECT 'type', n.nspname, t.typname, NULL, NULL
            FROM pg_type t
            JOIN pg_namespace n ON t.typnamespace = n.oid
            WHERE t.typtype IN ('e', 'c', 'r')
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

            -- Constraints (unique, foreign key, check, exclusion - primary keys
            -- handled by table). Constraints on extension-owned tables are
            -- excluded via the parent table — they have no 'e' entry of their own.
            SELECT 'constraint', n.nspname, co.conname, cl.relname, NULL
            FROM pg_constraint co
            JOIN pg_class cl ON co.conrelid = cl.oid
            JOIN pg_namespace n ON cl.relnamespace = n.oid
            WHERE co.contype IN ('u', 'f', 'c', 'x')
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND NOT EXISTS (
                SELECT 1 FROM pg_depend dep
                WHERE dep.objid = cl.oid AND dep.deptype = 'e'
              )

            UNION ALL

            -- Triggers (excluding those on extension-owned relations, via the parent)
            SELECT 'trigger', n.nspname, t.tgname, c.relname, NULL
            FROM pg_trigger t
            JOIN pg_class c ON t.tgrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE NOT t.tgisinternal
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND NOT EXISTS (
                SELECT 1 FROM pg_depend dep
                WHERE dep.objid = c.oid AND dep.deptype = 'e'
              )

            UNION ALL

            -- RLS policies (excluding those on extension-owned relations, via
            -- the parent — policies get no 'e' entry of their own)
            SELECT 'policy', n.nspname, pol.polname, c.relname, NULL
            FROM pg_policy pol
            JOIN pg_class c ON pol.polrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND NOT EXISTS (
                SELECT 1 FROM pg_depend dep
                WHERE dep.objid = c.oid AND dep.deptype = 'e'
              )

            UNION ALL

            -- Operators (excluding extension-owned). "args" is the canonical
            -- "left, right" operand string DROP/COMMENT ON OPERATOR require,
            -- with NONE for an absent operand.
            SELECT 'operator', n.nspname, o.oprname,
                   NULL,
                   CASE WHEN o.oprleft = 0 THEN 'NONE' ELSE format_type(o.oprleft, NULL) END
                     || ', '
                     || CASE WHEN o.oprright = 0 THEN 'NONE' ELSE format_type(o.oprright, NULL) END
            FROM pg_operator o
            JOIN pg_namespace n ON o.oprnamespace = n.oid
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND NOT EXISTS (
                SELECT 1 FROM pg_depend dep
                WHERE dep.objid = o.oid
                  AND dep.classid = 'pg_operator'::regclass
                  AND dep.deptype = 'e'
              )

            UNION ALL

            -- Casts (excluding extension-owned). Casts are not schema-scoped;
            -- their identity is the (source, target) type pair, carried in the
            -- "name" and "tbl" columns. Only user casts qualify: creating one
            -- requires owning the source or target type, so at least one side
            -- is outside the system schemas.
            SELECT 'cast', NULL,
                   format_type(c.castsource, NULL),
                   format_type(c.casttarget, NULL),
                   NULL
            FROM pg_cast c
            JOIN pg_type st ON c.castsource = st.oid
            JOIN pg_namespace st_n ON st.typnamespace = st_n.oid
            JOIN pg_type tt ON c.casttarget = tt.oid
            JOIN pg_namespace tt_n ON tt.typnamespace = tt_n.oid
            WHERE (
                st_n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                OR tt_n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              )
              AND NOT EXISTS (
                SELECT 1 FROM pg_depend dep
                WHERE dep.objid = c.oid
                  AND dep.classid = 'pg_cast'::regclass
                  AND dep.deptype = 'e'
              )

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
                    table: row.tbl.clone().unwrap_or_default(),
                    name: row.name.clone(),
                },
                "trigger" => DbObjectId::Trigger {
                    schema: row.schema.clone().unwrap_or_default(),
                    table: row.tbl.clone().unwrap_or_default(),
                    name: row.name.clone(),
                },
                "policy" => DbObjectId::Policy {
                    schema: row.schema.clone().unwrap_or_default(),
                    table: row.tbl.clone().unwrap_or_default(),
                    name: row.name.clone(),
                },
                "operator" => DbObjectId::Operator {
                    schema: row.schema.clone().unwrap_or_default(),
                    name: row.name.clone(),
                    arguments: row.args.clone().unwrap_or_default(),
                },
                "cast" => DbObjectId::Cast {
                    source: row.name.clone(),
                    target: row.tbl.clone().unwrap_or_default(),
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
