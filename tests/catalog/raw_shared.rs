//! Tests for the shared (cross-cutting) raw-catalog fetches and the
//! OID → DbObjectId index built on top of them.

use crate::helpers::harness::with_test_db;
use crate::helpers::raw::load_converted;
use anyhow::Result;
use pgmt::catalog::Catalog;
use pgmt::catalog::id::{DbObjectId, DependsOn};
use pgmt::catalog::raw::oid_index::OidIndex;
use pgmt::catalog::raw::shared::{self, class};
use pgmt::catalog::raw::{
    aggregate as raw_aggregate, cast as raw_cast, constraint as raw_constraint,
    custom_type as raw_custom_type, domain as raw_domain, extension as raw_extension,
    function as raw_function, index as raw_index, merge_indexes, operator as raw_operator,
    policy as raw_policy, schema as raw_schema, sequence as raw_sequence, table as raw_table,
    trigger as raw_trigger, view as raw_view,
};
use sqlx::postgres::types::Oid;
use std::collections::BTreeSet;

/// Look up a relation's OID by name, the way a raw fetch would carry it.
async fn relation_oid(db: &crate::helpers::harness::TestDatabase, qualified: &str) -> Oid {
    let row: (Oid,) = sqlx::query_as("SELECT $1::regclass::oid")
        .bind(qualified)
        .fetch_one(db.pool())
        .await
        .unwrap();
    row.0
}

#[tokio::test]
async fn test_namespace_map_resolves_schema_names() -> Result<()> {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA app").await;

        let namespaces = shared::fetch_namespaces(&mut *db.conn().await).await?;

        let app_oid: (Oid,) = sqlx::query_as("SELECT oid FROM pg_namespace WHERE nspname = 'app'")
            .fetch_one(db.pool())
            .await?;

        assert_eq!(namespaces.name(app_oid.0), Some("app"));

        // The map is deliberately unfiltered: it is a resolution table, not a
        // scoping decision.
        let names: Vec<&str> = namespaces.iter().map(|(_, name)| name).collect();
        assert!(names.contains(&"public"));
        assert!(names.contains(&"pg_catalog"));

        assert_eq!(namespaces.name(Oid(1)), None);

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_extension_ownership_covers_first_class_objects() -> Result<()> {
    with_test_db(async |db| {
        db.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")
            .await;

        let ownership = shared::fetch_extension_ownership(&mut *db.conn().await).await?;

        let uuid_fn: (Oid,) =
            sqlx::query_as("SELECT oid FROM pg_proc WHERE proname = 'uuid_generate_v4'")
                .fetch_one(db.pool())
                .await?;
        assert_eq!(
            ownership.owner(class::PG_PROC, uuid_fn.0),
            Some("uuid-ossp")
        );

        // A user function of the same class is untouched.
        db.execute("CREATE FUNCTION mine() RETURNS integer AS $$ SELECT 1 $$ LANGUAGE sql")
            .await;
        let ownership = shared::fetch_extension_ownership(&mut *db.conn().await).await?;
        let mine: (Oid,) = sqlx::query_as("SELECT oid FROM pg_proc WHERE proname = 'mine'")
            .fetch_one(db.pool())
            .await?;
        assert!(!ownership.is_owned(class::PG_PROC, mine.0));

        Ok(())
    })
    .await
}

/// A constraint, index, trigger, or policy created by an extension script gets
/// no `deptype = 'e'` row of its own; membership is recorded only on the parent
/// table. Ownership of a relation sub-object is therefore answered by the
/// parent's OID.
#[tokio::test]
async fn test_extension_ownership_of_relation_subobjects_comes_from_the_parent() -> Result<()> {
    with_test_db(async |db| {
        db.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")
            .await;
        db.execute(
            r#"
            CREATE TABLE ext_owned (
                id integer NOT NULL,
                name text,
                CONSTRAINT ext_owned_id_check CHECK (id > 0)
            )
        "#,
        )
        .await;
        db.execute("CREATE INDEX ext_owned_name_idx ON ext_owned (name)")
            .await;
        db.execute("ALTER EXTENSION \"uuid-ossp\" ADD TABLE ext_owned")
            .await;

        let ownership = shared::fetch_extension_ownership(&mut *db.conn().await).await?;

        let table_oid = relation_oid(db, "ext_owned").await;
        let index_oid = relation_oid(db, "ext_owned_name_idx").await;
        let constraint_oid: (Oid,) =
            sqlx::query_as("SELECT oid FROM pg_constraint WHERE conname = 'ext_owned_id_check'")
                .fetch_one(db.pool())
                .await?;

        assert_eq!(
            ownership.owner(class::PG_CLASS, table_oid),
            Some("uuid-ossp")
        );

        // Asking about the sub-object's own OID answers "not owned" — this is
        // exactly the leak the parent rule exists to prevent.
        assert!(!ownership.is_owned(class::PG_CLASS, index_oid));
        assert!(!ownership.is_owned(class::PG_CONSTRAINT, constraint_oid.0));

        assert!(ownership.is_relation_subobject_owned(table_oid));
        assert_eq!(
            ownership.owner_of_relation_subobject(table_oid),
            Some("uuid-ossp")
        );

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_descriptions_carry_object_and_subobject_comments() -> Result<()> {
    with_test_db(async |db| {
        db.execute("CREATE TABLE users (id integer, email text)")
            .await;
        db.execute("COMMENT ON TABLE users IS 'People'").await;
        db.execute("COMMENT ON COLUMN users.email IS 'Contact address'")
            .await;
        db.execute("CREATE SCHEMA app").await;
        db.execute("COMMENT ON SCHEMA app IS 'Application objects'")
            .await;

        let descriptions = shared::fetch_descriptions(&mut *db.conn().await).await?;

        let users = relation_oid(db, "users").await;
        assert_eq!(descriptions.object(class::PG_CLASS, users), Some("People"));

        // Column comments live under the same objoid with objsubid = attnum.
        let subobjects: Vec<(i32, &str)> =
            descriptions.subobjects(class::PG_CLASS, users).collect();
        assert_eq!(subobjects, vec![(2, "Contact address")]);

        let app: (Oid,) = sqlx::query_as("SELECT oid FROM pg_namespace WHERE nspname = 'app'")
            .fetch_one(db.pool())
            .await?;
        assert_eq!(
            descriptions.object(class::PG_NAMESPACE, app.0),
            Some("Application objects")
        );

        Ok(())
    })
    .await
}

/// Comments attach by index lookup: identities first, then the index, then the
/// OID-addressed state resolved through it.
#[tokio::test]
async fn test_comments_attach_through_the_oid_index() -> Result<()> {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA app").await;
        db.execute("CREATE TABLE app.users (id integer, email text)")
            .await;
        db.execute("CREATE TABLE app.orders (id integer)").await;
        db.execute("COMMENT ON TABLE app.users IS 'People'").await;
        db.execute("COMMENT ON COLUMN app.users.email IS 'Contact address'")
            .await;

        let shared = shared::fetch(&mut *db.conn().await).await?;

        // Stand-in for a raw fetch: OIDs paired with the identity the converter
        // would derive from them.
        let rows: Vec<(Oid, String, String)> = sqlx::query_as(
            "SELECT c.oid, n.nspname, c.relname
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE c.relkind = 'r' AND n.nspname = 'app'",
        )
        .fetch_all(db.pool())
        .await?;

        let index = OidIndex::from_pairs(
            class::PG_CLASS,
            rows.into_iter()
                .map(|(oid, schema, name)| (oid, DbObjectId::Table { schema, name })),
        )?;
        assert_eq!(index.len(), 2);

        let users_oid = relation_oid(db, "app.users").await;
        let resolved = index.get(class::PG_CLASS, users_oid).cloned();
        assert_eq!(
            resolved,
            Some(DbObjectId::Table {
                schema: "app".to_string(),
                name: "users".to_string(),
            })
        );

        // What the index buys: the comment is reached without a per-kind
        // pg_description join, and it is the comment the converted table carries.
        let attached = shared.descriptions.object(class::PG_CLASS, users_oid);
        let tables = load_converted(&mut *db.conn().await, raw_table::load).await?;
        let users = tables
            .iter()
            .find(|t| t.schema == "app" && t.name == "users")
            .expect("app.users should be in the catalog");
        assert_eq!(attached, users.comment.as_deref());
        assert_eq!(attached, Some("People"));

        let email_attnum: (i32,) = sqlx::query_as(
            "SELECT attnum::int FROM pg_attribute
             WHERE attrelid = $1 AND attname = 'email'",
        )
        .bind(users_oid)
        .fetch_one(db.pool())
        .await?;
        assert_eq!(
            shared
                .descriptions
                .get(class::PG_CLASS, users_oid, email_attnum.0),
            Some("Contact address")
        );

        // An uncommented object simply has no entry.
        let orders_oid = relation_oid(db, "app.orders").await;
        assert!(index.contains(class::PG_CLASS, orders_oid));
        assert_eq!(
            shared.descriptions.object(class::PG_CLASS, orders_oid),
            None
        );

        Ok(())
    })
    .await
}

/// A type reference resolves to what a dependency must name: an array reference
/// resolves to its element type, and the classification comes from the shared
/// type map rather than a per-kind join.
#[tokio::test]
async fn test_type_map_resolves_array_references_to_their_element_type() -> Result<()> {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA app").await;
        db.execute("CREATE TYPE app.status AS ENUM ('active', 'retired')")
            .await;
        // A type whose name starts with an underscore is a legitimate type, not
        // an array: only typelem decides.
        db.execute("CREATE TYPE app._internal_status AS ENUM ('draft')")
            .await;

        let shared = shared::fetch(&mut *db.conn().await).await?;

        let type_oid = |name: &'static str| async move {
            let row: (Oid,) = sqlx::query_as("SELECT $1::regtype::oid")
                .bind(name)
                .fetch_one(db.pool())
                .await
                .unwrap();
            row.0
        };

        let scalar = shared.resolve_type(type_oid("app.status").await).unwrap();
        assert_eq!(scalar.name, "status");
        assert_eq!(scalar.schema, Some("app"));
        assert_eq!(scalar.typtype, "e");
        assert!(!scalar.is_array);
        assert_eq!(
            scalar.dependency(),
            Some(DbObjectId::Type {
                schema: "app".to_string(),
                name: "status".to_string(),
            })
        );

        let array = shared.resolve_type(type_oid("app.status[]").await).unwrap();
        assert_eq!(array.name, "status");
        assert!(array.is_array);
        assert_eq!(array.dependency(), scalar.dependency());

        let underscored = shared
            .resolve_type(type_oid("app._internal_status").await)
            .unwrap();
        assert_eq!(underscored.name, "_internal_status");
        assert!(!underscored.is_array);

        // A built-in creates no dependency, and a table's row type resolves to
        // the table rather than to a standalone type.
        let builtin = shared.resolve_type(type_oid("text").await).unwrap();
        assert_eq!(builtin.dependency(), None);

        db.execute("CREATE TABLE app.users (id integer)").await;
        let shared = shared::fetch(&mut *db.conn().await).await?;
        let row_type = shared.resolve_type(type_oid("app.users").await).unwrap();
        assert_eq!(row_type.typtype, "c");
        assert_eq!(row_type.relkind, Some("r"));
        assert_eq!(
            row_type.dependency(),
            Some(DbObjectId::Table {
                schema: "app".to_string(),
                name: "users".to_string(),
            })
        );

        Ok(())
    })
    .await
}

/// Sub-object comments cross the firewall the same way object comments do: the
/// index resolves the parent OID, and the attnum keying stays on the raw side.
#[tokio::test]
async fn test_subobject_comments_resolve_through_the_index() -> Result<()> {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA app").await;
        db.execute("CREATE TABLE app.users (id integer, email text)")
            .await;
        db.execute("CREATE TABLE app.orders (id integer)").await;
        db.execute("COMMENT ON COLUMN app.users.email IS 'Contact address'")
            .await;

        let shared = shared::fetch(&mut *db.conn().await).await?;
        let users_oid = relation_oid(db, "app.users").await;
        let orders_oid = relation_oid(db, "app.orders").await;

        let users_id = DbObjectId::Table {
            schema: "app".to_string(),
            name: "users".to_string(),
        };
        let index = OidIndex::from_pairs(
            class::PG_CLASS,
            [
                (users_oid, users_id.clone()),
                (
                    orders_oid,
                    DbObjectId::Table {
                        schema: "app".to_string(),
                        name: "orders".to_string(),
                    },
                ),
            ],
        )?;

        let by_object = index.subobject_comments(&shared.descriptions, class::PG_CLASS);
        let users_columns = by_object
            .get(&users_id)
            .expect("users has a column comment");
        assert_eq!(users_columns.get(&2), Some(&"Contact address"));
        assert_eq!(users_columns.len(), 1);
        // A table with no commented column has no entry at all.
        assert_eq!(by_object.len(), 1);

        // And the converted table carries it on the named column.
        let tables = load_converted(&mut *db.conn().await, raw_table::load).await?;
        let users = tables
            .iter()
            .find(|t| t.schema == "app" && t.name == "users")
            .expect("app.users should be in the catalog");
        assert_eq!(users.columns[0].comment, None);
        assert_eq!(users.columns[1].comment.as_deref(), Some("Contact address"));

        Ok(())
    })
    .await
}

/// A schema wide enough that every kind contributes to the index.
const SCHEMA: &[&str] = &[
    "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"",
    "CREATE SCHEMA app",
    "CREATE TYPE app.status AS ENUM ('active', 'retired')",
    "CREATE TYPE app.point2d AS (x integer, y integer)",
    "CREATE DOMAIN app.email AS text CHECK (VALUE LIKE '%@%')",
    "CREATE SEQUENCE app.counter",
    r#"CREATE TABLE app.users (
        id integer PRIMARY KEY,
        email app.email NOT NULL,
        state app.status NOT NULL DEFAULT 'active',
        home app.point2d,
        CONSTRAINT users_id_positive CHECK (id > 0)
    )"#,
    "CREATE INDEX users_email_idx ON app.users (email)",
    "CREATE VIEW app.active_users AS SELECT id, email FROM app.users WHERE state = 'active'",
    r#"CREATE FUNCTION app.touch() RETURNS trigger AS $$
       BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql"#,
    "CREATE TRIGGER users_touch BEFORE UPDATE ON app.users FOR EACH ROW EXECUTE FUNCTION app.touch()",
    "ALTER TABLE app.users ENABLE ROW LEVEL SECURITY",
    "CREATE POLICY users_self ON app.users USING (id > 0)",
    "CREATE FUNCTION app.combine(integer, integer) RETURNS integer AS $$ SELECT $1 + $2 $$ LANGUAGE sql IMMUTABLE",
    "CREATE OPERATOR app.=== (LEFTARG = integer, RIGHTARG = integer, FUNCTION = app.combine)",
    "CREATE AGGREGATE app.total (integer) (SFUNC = app.combine, STYPE = integer, INITCOND = '0')",
    "CREATE FUNCTION app.email_to_status(app.email) RETURNS app.status AS $$ SELECT 'active'::app.status $$ LANGUAGE sql IMMUTABLE",
    "CREATE CAST (app.email AS app.status) WITH FUNCTION app.email_to_status(app.email)",
];

/// The identity of every object a full catalog load produces, grants aside:
/// grants are attached state rather than objects the index addresses.
fn catalog_object_ids(catalog: &Catalog) -> BTreeSet<DbObjectId> {
    let mut ids = BTreeSet::new();

    // Schemas have no DependsOn impl; their identity is just the name.
    for schema in &catalog.schemas {
        ids.insert(DbObjectId::Schema {
            name: schema.name.clone(),
        });
    }

    fn collect<T: DependsOn>(items: &[T], ids: &mut BTreeSet<DbObjectId>) {
        ids.extend(items.iter().map(|item| item.id()));
    }

    collect(&catalog.tables, &mut ids);
    collect(&catalog.views, &mut ids);
    collect(&catalog.types, &mut ids);
    collect(&catalog.domains, &mut ids);
    collect(&catalog.functions, &mut ids);
    collect(&catalog.aggregates, &mut ids);
    collect(&catalog.operators, &mut ids);
    collect(&catalog.casts, &mut ids);
    collect(&catalog.sequences, &mut ids);
    collect(&catalog.indexes, &mut ids);
    collect(&catalog.constraints, &mut ids);
    collect(&catalog.triggers, &mut ids);
    collect(&catalog.policies, &mut ids);
    collect(&catalog.extensions, &mut ids);

    ids
}

/// A converter's OID index outlives the converter: the per-kind indexes of one
/// load merge into a single index in which every object that load produced is
/// addressable. That is what lets OID-addressed state beyond comments be
/// resolved once, rather than by re-deriving an index per consumer.
#[tokio::test]
async fn test_merged_oid_index_addresses_every_loaded_object() -> Result<()> {
    with_test_db(async |db| {
        for statement in SCHEMA {
            db.execute(statement).await;
        }

        let mut conn = db.conn().await;
        let shared = shared::fetch(&mut conn).await?;
        let mut indexes = Vec::new();

        raw_schema::load_with_exclusions(&shared)?.collect_into("schema", &mut indexes);
        raw_table::load_with_exclusions(&mut conn, &shared)
            .await?
            .collect_into("table", &mut indexes);
        raw_view::load_with_exclusions(&mut conn, &shared)
            .await?
            .collect_into("view", &mut indexes);
        raw_custom_type::load_with_exclusions(&mut conn, &shared)
            .await?
            .collect_into("type", &mut indexes);
        raw_domain::load_with_exclusions(&mut conn, &shared)
            .await?
            .collect_into("domain", &mut indexes);
        raw_function::load_with_exclusions(&mut conn, &shared)
            .await?
            .collect_into("function", &mut indexes);
        raw_aggregate::load_with_exclusions(&mut conn, &shared)
            .await?
            .collect_into("aggregate", &mut indexes);
        raw_operator::load_with_exclusions(&mut conn, &shared)
            .await?
            .collect_into("operator", &mut indexes);
        raw_cast::load_with_exclusions(&mut conn, &shared)
            .await?
            .collect_into("cast", &mut indexes);
        raw_sequence::load_with_exclusions(&mut conn, &shared)
            .await?
            .collect_into("sequence", &mut indexes);
        raw_index::load_with_exclusions(&mut conn, &shared)
            .await?
            .collect_into("index", &mut indexes);
        raw_constraint::load_with_exclusions(&mut conn, &shared)
            .await?
            .collect_into("constraint", &mut indexes);
        raw_trigger::load_with_exclusions(&mut conn, &shared)
            .await?
            .collect_into("trigger", &mut indexes);
        raw_policy::load_with_exclusions(&mut conn, &shared)
            .await?
            .collect_into("policy", &mut indexes);
        raw_extension::load_with_exclusions(&mut conn, &shared)
            .await?
            .collect_into("extension", &mut indexes);

        let merged = merge_indexes(indexes)?;
        let addressed: BTreeSet<DbObjectId> = merged.iter().map(|(_, _, id)| id.clone()).collect();

        let catalog = Catalog::load_unfiltered(db.pool()).await?;
        let missing: Vec<String> = catalog_object_ids(&catalog)
            .difference(&addressed)
            .map(|id| id.to_string())
            .collect();
        assert!(
            missing.is_empty(),
            "objects the merged index cannot address: {:?}",
            missing
        );

        // The index addresses objects through the catalog table each one's
        // OID-addressed state is keyed by, so a table is reachable under
        // `pg_class` and a composite type under both `pg_type` and the
        // `pg_class` entry its attribute comments hang off.
        let users = relation_oid(db, "app.users").await;
        assert_eq!(
            merged.get(class::PG_CLASS, users),
            Some(&DbObjectId::Table {
                schema: "app".to_string(),
                name: "users".to_string(),
            })
        );
        let point2d: (Oid,) = sqlx::query_as("SELECT 'app.point2d'::regtype::oid")
            .fetch_one(db.pool())
            .await?;
        let point2d_relation = relation_oid(db, "app.point2d").await;
        let point2d_id = DbObjectId::Type {
            schema: "app".to_string(),
            name: "point2d".to_string(),
        };
        assert_eq!(merged.get(class::PG_TYPE, point2d.0), Some(&point2d_id));
        assert_eq!(
            merged.get(class::PG_CLASS, point2d_relation),
            Some(&point2d_id)
        );

        Ok(())
    })
    .await
}
