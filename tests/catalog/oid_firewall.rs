//! The logical catalog must carry no physical coordinates.
//!
//! Logical structs are compared by value across two different databases, so an
//! OID (or anything derived from one) on a logical struct would make the same
//! schema in two databases compare unequal and generate a spurious
//! drop/recreate.

use crate::helpers::harness::{PgTestInstance, TestDatabase};
use anyhow::Result;
use pgmt::catalog::Catalog;
use pgmt::diff::plan;

/// A schema wide enough that every logical struct kind participates in the
/// comparison.
const SCHEMA: &[&str] = &[
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
        tags text[],
        CONSTRAINT users_id_positive CHECK (id > 0)
    )"#,
    r#"CREATE TABLE app.orders (
        id integer PRIMARY KEY,
        user_id integer NOT NULL REFERENCES app.users (id),
        total numeric(10, 2) NOT NULL
    )"#,
    "CREATE INDEX orders_total_idx ON app.orders (total) WHERE total > 0",
    "CREATE UNIQUE INDEX users_email_idx ON app.users (email)",
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
    "COMMENT ON TABLE app.users IS 'People'",
    "COMMENT ON COLUMN app.users.email IS 'Contact address'",
    "COMMENT ON SCHEMA app IS 'Application objects'",
];

async fn apply(db: &TestDatabase, statements: &[&str]) {
    for statement in statements {
        db.execute(statement).await;
    }
}

/// Advance the OID counter so the second database allocates a disjoint range for
/// the same schema.
async fn churn_oids(db: &TestDatabase) {
    db.execute(
        r#"
        DO $$
        DECLARE i integer;
        BEGIN
            FOR i IN 1..200 LOOP
                EXECUTE format('CREATE TABLE churn_%s (a integer, b text)', i);
                EXECUTE format('CREATE TYPE churn_type_%s AS (a integer)', i);
                EXECUTE format('DROP TABLE churn_%s', i);
                EXECUTE format('DROP TYPE churn_type_%s', i);
            END LOOP;
        END $$
    "#,
    )
    .await;
}

#[tokio::test]
async fn test_identical_schemas_with_skewed_oids_produce_no_diff() -> Result<()> {
    let pg = PgTestInstance::new().await;
    let first = pg.create_test_database().await;
    let second = pg.create_test_database().await;

    let result = async {
        churn_oids(&second).await;
        apply(&first, SCHEMA).await;
        apply(&second, SCHEMA).await;

        let first_catalog = Catalog::load_unfiltered(first.pool()).await?;
        let second_catalog = Catalog::load_unfiltered(second.pool()).await?;

        // The churn must have actually moved the counter, or the test proves
        // nothing.
        let (first_min,): (i64,) = sqlx::query_as(
            "SELECT min(c.oid)::bigint FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'app'",
        )
        .fetch_one(first.pool())
        .await?;
        let (second_min,): (i64,) = sqlx::query_as(
            "SELECT min(c.oid)::bigint FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'app'",
        )
        .fetch_one(second.pool())
        .await?;
        assert!(
            second_min > first_min,
            "expected skewed OID ranges, got {first_min} and {second_min}"
        );

        let forward = plan(&first_catalog, &second_catalog)?;
        assert!(
            forward.is_empty(),
            "identical schemas diffed non-empty: {:#?}",
            forward
        );

        let backward = plan(&second_catalog, &first_catalog)?;
        assert!(
            backward.is_empty(),
            "identical schemas diffed non-empty in reverse: {:#?}",
            backward
        );

        Ok(())
    }
    .await;

    first.cleanup().await;
    second.cleanup().await;
    result
}
