//! The identity snapshot query, composed from one branch definition per object
//! kind.
//!
//! `CatalogIdentity` needs the identity of every object in the database and
//! nothing else, in one round trip — the converters' N fetches would cost far
//! more than the answer is worth. So the snapshot is a single `UNION ALL`, one
//! branch per object kind, and each branch is built here out of the SQL
//! spellings in [`super::exclusion::sql`]: the branch selects the columns a
//! [`crate::catalog::id::DbObjectId`] is built from, and filters with the same
//! rules the matching converter applies in Rust.
//!
//! The invariant is set equality: for any database, the identities this query
//! reports and the identities a full catalog load yields are the same set
//! (`tests/catalog/identity_consistency.rs`). A branch whose selection drifts
//! from its converter's is a drift in that test.
//!
//! Three asymmetries are deliberate and not drift:
//!
//! - **Grants** have no branch. They are state attached to an object, not
//!   objects with identities of their own.
//! - **`public`** is never reported. Every database has it from initdb onward,
//!   so no schema file creates it, and attributing it to one would make the
//!   first file that touches the database appear to own it.
//! - **Materialized views** are reported by neither the snapshot nor the
//!   catalog. An identity no fetcher can yield would be attributed to a file and
//!   then never found in the catalog it is diffed against.

use super::exclusion::sql;

/// The columns every branch projects, in order. They are what a
/// [`crate::catalog::id::DbObjectId`] is built from: the kind tag, then the
/// object's schema, name, parent table and argument string, each `NULL` for a
/// kind that has no such coordinate.
const COLUMNS: [&str; 5] = ["type", "schema", "name", "tbl", "args"];

/// One object kind's branch of the snapshot query.
pub struct Branch {
    kind: &'static str,
    from: String,
    schema: String,
    name: String,
    table: String,
    args: String,
    predicates: Vec<String>,
}

impl Branch {
    /// A branch for `kind`, reading the catalog tables in `from` (a `FROM` body,
    /// joins included). Every identity column defaults to `NULL`.
    fn new(kind: &'static str, from: &str) -> Self {
        Self {
            kind,
            from: from.to_string(),
            schema: "NULL".to_string(),
            name: "NULL".to_string(),
            table: "NULL".to_string(),
            args: "NULL".to_string(),
            predicates: Vec::new(),
        }
    }

    fn schema(mut self, expr: &str) -> Self {
        self.schema = expr.to_string();
        self
    }

    fn name(mut self, expr: &str) -> Self {
        self.name = expr.to_string();
        self
    }

    fn table(mut self, expr: &str) -> Self {
        self.table = expr.to_string();
        self
    }

    fn args(mut self, expr: &str) -> Self {
        self.args = expr.to_string();
        self
    }

    /// Add a predicate. All predicates of a branch are `AND`ed; each is
    /// parenthesised, so a predicate may itself be a disjunction.
    fn filter(mut self, predicate: impl Into<String>) -> Self {
        self.predicates.push(predicate.into());
        self
    }

    /// The branch as a `SELECT`. Every projected column is cast to `text`: the
    /// catalog columns are a mix of `name`, `text` and untyped `NULL`, and the
    /// union's column types must not depend on which branch happens to come
    /// first.
    fn sql(&self) -> String {
        let projections: Vec<String> = [
            format!("'{}'", self.kind),
            self.schema.clone(),
            self.name.clone(),
            self.table.clone(),
            self.args.clone(),
        ]
        .iter()
        .zip(COLUMNS)
        .map(|(expr, column)| format!("    ({expr})::text AS \"{column}\""))
        .collect();

        let mut sql = format!("SELECT\n{}\nFROM {}", projections.join(",\n"), self.from);
        if !self.predicates.is_empty() {
            let conditions: Vec<String> = self
                .predicates
                .iter()
                .map(|predicate| format!("({predicate})"))
                .collect();
            sql.push_str(&format!("\nWHERE {}", conditions.join("\n  AND ")));
        }
        sql
    }
}

/// The full snapshot query: every branch, `UNION ALL`ed into one round trip.
pub fn query() -> String {
    branches()
        .iter()
        .map(Branch::sql)
        .collect::<Vec<_>>()
        .join("\n\nUNION ALL\n\n")
}

/// One branch per object kind the catalog models, each mirroring the converter
/// of the same name in this module's siblings.
pub fn branches() -> Vec<Branch> {
    vec![
        // raw::schema — the namespace map minus PostgreSQL's own namespaces.
        // `public` is dropped on top of that, for the snapshot only.
        Branch::new("schema", "pg_namespace n")
            .name("n.nspname")
            .filter(sql::not_a_system_namespace("n.nspname"))
            .filter("n.nspname <> 'public'"),
        // raw::table
        Branch::new(
            "table",
            "pg_class c\n     JOIN pg_namespace n ON c.relnamespace = n.oid",
        )
        .schema("n.nspname")
        .name("c.relname")
        .filter("c.relkind = 'r'")
        .filter(sql::not_a_system_namespace("n.nspname"))
        .filter(sql::not_extension_owned("pg_class", "c.oid")),
        // raw::view
        Branch::new(
            "view",
            "pg_class c\n     JOIN pg_namespace n ON c.relnamespace = n.oid",
        )
        .schema("n.nspname")
        .name("c.relname")
        .filter("c.relkind = 'v'")
        .filter(sql::not_a_system_namespace("n.nspname"))
        .filter(sql::not_extension_owned("pg_class", "c.oid")),
        // raw::sequence
        Branch::new(
            "sequence",
            "pg_class c\n     JOIN pg_namespace n ON c.relnamespace = n.oid",
        )
        .schema("n.nspname")
        .name("c.relname")
        .filter("c.relkind = 'S'")
        .filter(sql::not_a_system_namespace("n.nspname"))
        .filter(sql::not_extension_owned("pg_class", "c.oid"))
        .filter(sql::not_an_identity_sequence("c.oid")),
        // raw::index. The enumeration is `pg_index`, as the raw fetch's is: it
        // is the set of indexes whatever the index relation's own relkind says.
        // An index belongs to an extension through its own OID (a standalone
        // extension index) or through the table it is on (an index an extension
        // script created records membership only on the parent), and either
        // schema being a system one puts it out of scope.
        Branch::new(
            "index",
            "pg_index idx\n     \
             JOIN pg_class i ON idx.indexrelid = i.oid\n     \
             JOIN pg_namespace n ON i.relnamespace = n.oid\n     \
             JOIN pg_class t ON idx.indrelid = t.oid\n     \
             JOIN pg_namespace tn ON t.relnamespace = tn.oid",
        )
        .schema("n.nspname")
        .name("i.relname")
        .filter(sql::not_a_system_namespace("n.nspname"))
        .filter(sql::not_a_system_namespace("tn.nspname"))
        .filter(sql::not_extension_owned("pg_class", "i.oid"))
        .filter(sql::parent_relation_not_extension_owned("t.oid"))
        .filter(sql::not_a_constraint_backing_index("i.oid")),
        // raw::function, whose converter calls everything that is not a
        // procedure a function — a window function included.
        Branch::new(
            "function",
            "pg_proc p\n     JOIN pg_namespace n ON p.pronamespace = n.oid",
        )
        .schema("n.nspname")
        .name("p.proname")
        .args("pg_catalog.pg_get_function_identity_arguments(p.oid)")
        .filter("p.prokind NOT IN ('a', 'p')")
        .filter(sql::not_a_system_namespace("n.nspname"))
        .filter(sql::not_extension_owned("pg_proc", "p.oid")),
        // raw::function, procedure half.
        Branch::new(
            "procedure",
            "pg_proc p\n     JOIN pg_namespace n ON p.pronamespace = n.oid",
        )
        .schema("n.nspname")
        .name("p.proname")
        .args("pg_catalog.pg_get_function_identity_arguments(p.oid)")
        .filter("p.prokind = 'p'")
        .filter(sql::not_a_system_namespace("n.nspname"))
        .filter(sql::not_extension_owned("pg_proc", "p.oid")),
        // raw::aggregate, which enumerates `pg_aggregate` joined to its
        // `pg_proc` row — exactly the routines of `prokind = 'a'`.
        Branch::new(
            "aggregate",
            "pg_proc p\n     JOIN pg_namespace n ON p.pronamespace = n.oid",
        )
        .schema("n.nspname")
        .name("p.proname")
        .args("pg_catalog.pg_get_function_identity_arguments(p.oid)")
        .filter("p.prokind = 'a'")
        .filter(sql::not_a_system_namespace("n.nspname"))
        .filter(sql::not_extension_owned("pg_proc", "p.oid")),
        // raw::custom_type — enums, composites and ranges. The row-type test
        // mirrors the one in that fetch's own query: a relation's row type is
        // the relation, not a type a schema file wrote, and only a standalone
        // composite's own backing entry (relkind 'c') is not such a relation.
        Branch::new(
            "type",
            "pg_type t\n     JOIN pg_namespace n ON t.typnamespace = n.oid",
        )
        .schema("n.nspname")
        .name("t.typname")
        .filter("t.typtype IN ('e', 'c', 'r')")
        .filter(
            "NOT EXISTS (\n    SELECT 1 FROM pg_class c\n    \
             WHERE c.reltype = t.oid\n      \
             AND c.relkind != 'c'\n)",
        )
        .filter(sql::not_a_system_namespace("n.nspname"))
        .filter(sql::not_extension_owned("pg_type", "t.oid")),
        // raw::domain
        Branch::new(
            "domain",
            "pg_type t\n     JOIN pg_namespace n ON t.typnamespace = n.oid",
        )
        .schema("n.nspname")
        .name("t.typname")
        .filter("t.typtype = 'd'")
        .filter(sql::not_a_system_namespace("n.nspname"))
        .filter(sql::not_extension_owned("pg_type", "t.oid")),
        // raw::constraint. Primary keys are carried by their table, so they are
        // not constraints of their own here.
        Branch::new(
            "constraint",
            "pg_constraint co\n     \
             JOIN pg_class cl ON co.conrelid = cl.oid\n     \
             JOIN pg_namespace n ON cl.relnamespace = n.oid",
        )
        .schema("n.nspname")
        .name("co.conname")
        .table("cl.relname")
        .filter("cl.relkind = 'r'")
        .filter("co.contype IN ('u', 'f', 'c', 'x')")
        .filter(sql::not_a_system_namespace("n.nspname"))
        .filter(sql::parent_relation_not_extension_owned("cl.oid")),
        // raw::trigger
        Branch::new(
            "trigger",
            "pg_trigger tg\n     \
             JOIN pg_class c ON tg.tgrelid = c.oid\n     \
             JOIN pg_namespace n ON c.relnamespace = n.oid",
        )
        .schema("n.nspname")
        .name("tg.tgname")
        .table("c.relname")
        .filter("c.relkind IN ('r', 'v', 'm')")
        .filter("NOT tg.tgisinternal")
        .filter(sql::not_a_system_namespace("n.nspname"))
        .filter(sql::parent_relation_not_extension_owned("c.oid")),
        // raw::policy
        Branch::new(
            "policy",
            "pg_policy pol\n     \
             JOIN pg_class c ON pol.polrelid = c.oid\n     \
             JOIN pg_namespace n ON c.relnamespace = n.oid",
        )
        .schema("n.nspname")
        .name("pol.polname")
        .table("c.relname")
        .filter(sql::not_a_system_namespace("n.nspname"))
        .filter(sql::parent_relation_not_extension_owned("c.oid")),
        // raw::operator. The identity args are the canonical "left, right"
        // operand string `DROP`/`COMMENT ON OPERATOR` require, with NONE for an
        // absent operand.
        Branch::new(
            "operator",
            "pg_operator o\n     JOIN pg_namespace n ON o.oprnamespace = n.oid",
        )
        .schema("n.nspname")
        .name("o.oprname")
        .args(
            "CASE WHEN o.oprleft = 0 THEN 'NONE' \
             ELSE pg_catalog.format_type(o.oprleft, NULL) END\n       \
             || ', '\n       \
             || CASE WHEN o.oprright = 0 THEN 'NONE' \
             ELSE pg_catalog.format_type(o.oprright, NULL) END",
        )
        .filter(sql::not_a_system_namespace("n.nspname"))
        .filter(sql::not_extension_owned("pg_operator", "o.oid")),
        // raw::cast. A cast is not schema-scoped: its identity is the (source,
        // target) type pair, carried in the "name" and "tbl" columns. Creating
        // one requires owning the source or the target type, so every user cast
        // has at least one side outside the system schemas; each side is
        // resolved through the array indirection first, as the converter's
        // `resolve_type` does.
        Branch::new(
            "cast",
            "pg_cast ca\n     \
             JOIN pg_type st ON ca.castsource = st.oid\n     \
             JOIN pg_type ste ON ste.oid = COALESCE(NULLIF(st.typelem, 0), st.oid)\n     \
             JOIN pg_namespace stn ON ste.typnamespace = stn.oid\n     \
             JOIN pg_type tt ON ca.casttarget = tt.oid\n     \
             JOIN pg_type tte ON tte.oid = COALESCE(NULLIF(tt.typelem, 0), tt.oid)\n     \
             JOIN pg_namespace ttn ON tte.typnamespace = ttn.oid",
        )
        .name("pg_catalog.format_type(ca.castsource, NULL)")
        .table("pg_catalog.format_type(ca.casttarget, NULL)")
        .filter(format!(
            "{} OR {}",
            sql::not_a_system_namespace("stn.nspname"),
            sql::not_a_system_namespace("ttn.nspname")
        ))
        .filter(sql::not_extension_owned("pg_cast", "ca.oid")),
        // raw::extension
        Branch::new("extension", "pg_extension e")
            .name("e.extname")
            .filter(sql::not_a_built_in_extension("e.extname")),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_every_branch_projects_the_identity_columns() {
        for branch in branches() {
            let sql = branch.sql();
            for column in COLUMNS {
                assert!(
                    sql.contains(&format!("AS \"{column}\"")),
                    "branch {} does not project {column}:\n{sql}",
                    branch.kind
                );
            }
        }
    }

    #[test]
    fn test_query_unions_every_branch() {
        let query = query();
        assert_eq!(query.matches("UNION ALL").count(), branches().len() - 1);
        for branch in branches() {
            assert!(
                query.contains(&format!("('{}')::text", branch.kind)),
                "branch {} is missing from the query",
                branch.kind
            );
        }
    }

    #[test]
    fn test_exclusion_predicates_are_class_qualified() {
        // An OID identifies a row within one catalog table, so every
        // extension-ownership test names the catalog its OID belongs to.
        let query = query();
        for fragment in query.split("dep.objid = ").skip(1) {
            assert!(
                fragment.contains("dep.classid = '"),
                "an extension-ownership test is not class-qualified:\n{fragment}"
            );
        }
    }
}
