//! `CatalogIdentity::load` and a full `Catalog` load must see the same objects.
//!
//! The identity snapshot is a hand-maintained UNION ALL that re-implements every
//! per-kind filter. When a branch is missing or a filter drifts, objects get
//! misattributed during incremental apply and module partitioning, silently. The
//! symmetric difference below turns that class of drift into a test failure.

use crate::helpers::harness::with_test_db;
use anyhow::Result;
use pgmt::catalog::Catalog;
use pgmt::catalog::id::{DbObjectId, DependsOn};
use pgmt::catalog::identity::CatalogIdentity;
use std::collections::BTreeSet;

/// A schema touching every object kind either side can report.
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
        nickname text,
        CONSTRAINT users_id_positive CHECK (id > 0),
        CONSTRAINT users_nickname_unique UNIQUE (nickname)
    )"#,
    // A standalone unique index (not a constraint) that a foreign key then
    // references: pg_constraint.conindid on the FK points at this index.
    "CREATE UNIQUE INDEX users_email_idx ON app.users (email)",
    r#"CREATE TABLE app.orders (
        id integer PRIMARY KEY,
        user_id integer NOT NULL REFERENCES app.users (id),
        user_email app.email REFERENCES app.users (email),
        total numeric(10, 2) NOT NULL
    )"#,
    "CREATE INDEX orders_total_idx ON app.orders (total) WHERE total > 0",
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
    "CREATE PROCEDURE app.noop() AS $$ BEGIN END $$ LANGUAGE plpgsql",
    "CREATE TYPE app.span AS RANGE (SUBTYPE = integer)",
    // Materialized views are modeled by neither side; both must ignore it.
    "CREATE MATERIALIZED VIEW app.order_totals AS SELECT user_id, sum(total) AS total FROM app.orders GROUP BY user_id",
    "GRANT SELECT ON app.users TO test_read_only",
];

fn catalog_object_ids(catalog: &Catalog) -> BTreeSet<DbObjectId> {
    let mut ids = BTreeSet::new();

    // Schemas have no DependsOn impl; their identity is just the name.
    // `public` is omitted: the identity snapshot never reports it, since it
    // exists in every database from initdb onward.
    for schema in &catalog.schemas {
        if schema.name != "public" {
            ids.insert(DbObjectId::Schema {
                name: schema.name.clone(),
            });
        }
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
    // Grants are deliberately absent: they are attached state, not objects with
    // an identity of their own, and the snapshot does not track them.

    ids
}

#[tokio::test]
async fn test_identity_snapshot_matches_full_catalog() -> Result<()> {
    with_test_db(async |db| {
        for statement in SCHEMA {
            db.execute(statement).await;
        }

        let catalog = Catalog::load_unfiltered(db.pool()).await?;
        let snapshot = CatalogIdentity::load(db.pool()).await?;

        let from_catalog = catalog_object_ids(&catalog);
        let from_snapshot = snapshot.objects.clone();

        let only_in_catalog: Vec<String> = from_catalog
            .difference(&from_snapshot)
            .map(|id| id.to_string())
            .collect();
        let only_in_snapshot: Vec<String> = from_snapshot
            .difference(&from_catalog)
            .map(|id| id.to_string())
            .collect();

        assert!(
            only_in_catalog.is_empty() && only_in_snapshot.is_empty(),
            "identity snapshot and full catalog disagree\n\
             only in Catalog::load_unfiltered ({}):\n  {:?}\n\
             only in CatalogIdentity::load ({}):\n  {:?}",
            only_in_catalog.len(),
            only_in_catalog,
            only_in_snapshot.len(),
            only_in_snapshot,
        );

        Ok(())
    })
    .await
}
