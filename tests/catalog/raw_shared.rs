//! Tests for the shared (cross-cutting) raw-catalog fetches and the
//! OID → DbObjectId index built on top of them.

use crate::helpers::harness::with_test_db;
use anyhow::Result;
use pgmt::catalog::id::DbObjectId;
use pgmt::catalog::raw::index::OidIndex;
use pgmt::catalog::raw::shared::{self, class};
use pgmt::catalog::table;
use sqlx::postgres::types::Oid;

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
            rows.into_iter()
                .map(|(oid, schema, name)| (oid, DbObjectId::Table { schema, name })),
        )?;
        assert_eq!(index.len(), 2);

        let users_oid = relation_oid(db, "app.users").await;
        let resolved = index.get(users_oid).cloned();
        assert_eq!(
            resolved,
            Some(DbObjectId::Table {
                schema: "app".to_string(),
                name: "users".to_string(),
            })
        );

        // What the index buys: the comment is reached without a per-kind
        // pg_description join, and it matches what the fat fetcher produces.
        let attached = shared.descriptions.object(class::PG_CLASS, users_oid);
        let tables = table::fetch(&mut *db.conn().await).await?;
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
        assert!(index.contains(orders_oid));
        assert_eq!(
            shared.descriptions.object(class::PG_CLASS, orders_oid),
            None
        );

        Ok(())
    })
    .await
}
