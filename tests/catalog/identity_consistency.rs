//! `CatalogIdentity::load` and a full `Catalog` load must see the same objects.
//!
//! The identity snapshot is one UNION ALL composed from the shared exclusion
//! fragments the converters apply, with a branch per kind. A missing branch, or
//! a branch whose filters do not add up to what its converter does, misattributes
//! objects during incremental apply and module partitioning, silently — and none
//! of it is compile-time checked. The symmetric difference below turns that class
//! of drift into a test failure.

use crate::helpers::harness::with_test_db;
use anyhow::Result;
use pgmt::catalog::Catalog;
use pgmt::catalog::id::{DbObjectId, DependsOn};
use pgmt::catalog::identity::CatalogIdentity;
use sqlx::{Executor, PgPool};
use std::collections::BTreeSet;

/// A schema touching every object kind either side can report.
const SCHEMA: &[&str] = &[
    "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"",
    "CREATE SCHEMA app",
    "CREATE TYPE app.status AS ENUM ('active', 'retired')",
    "CREATE TYPE app.point2d AS (x integer, y integer)",
    "CREATE DOMAIN app.email AS text CHECK (VALUE LIKE '%@%')",
    "CREATE COLLATION app.case_insensitive (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
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
    // A partitioned table and its partition: their row types are the relations'
    // own, so neither side may report them as composite types.
    r#"CREATE TABLE app.events (
        id integer,
        occurred_on date NOT NULL
    ) PARTITION BY RANGE (occurred_on)"#,
    "CREATE TABLE app.events_2024 PARTITION OF app.events \
     FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
    // Both sequence shapes: an identity column's sequence is internal to the
    // column and belongs to neither side, while a SERIAL column's is a sequence
    // of its own that both sides must report. The exclusion constraint owns its
    // backing index, which neither side reports as an index.
    r#"CREATE TABLE app.tickets (
        id integer GENERATED ALWAYS AS IDENTITY,
        seq serial,
        slot text,
        EXCLUDE USING btree (slot WITH =)
    )"#,
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
    collect(&catalog.collations, &mut ids);
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

/// Load both sides and assert they report the same identities.
async fn assert_sides_agree(pool: &PgPool) -> Result<()> {
    let catalog = Catalog::load_unfiltered(pool).await?;
    let snapshot = CatalogIdentity::load(pool).await?;

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
}

#[tokio::test]
async fn test_identity_snapshot_matches_full_catalog() -> Result<()> {
    with_test_db(async |db| {
        for statement in SCHEMA {
            db.execute(statement).await;
        }

        assert_sides_agree(db.pool()).await
    })
    .await
}

/// A temporary table's trigger and policy belong to neither side.
///
/// A session that creates a temporary object gets a `pg_temp_N` namespace, and
/// the `pg_class`, `pg_trigger` and `pg_policy` rows hanging off it are visible
/// to every session in the cluster. Both sides must read "system schema" the
/// same way, or a temporary table's sub-objects are excluded by the converters
/// and still reported by the snapshot.
#[tokio::test]
async fn test_temporary_objects_belong_to_neither_side() -> Result<()> {
    with_test_db(async |db| {
        db.execute(
            r#"CREATE FUNCTION public.touch() RETURNS trigger AS $$
               BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql"#,
        )
        .await;

        // The temporary objects live for as long as the session that created
        // them, so the connection is held for the duration of the assertions.
        let mut session = db.pool().acquire().await?;
        for statement in [
            "CREATE TEMP TABLE scratch (id integer PRIMARY KEY, note text)",
            "CREATE TRIGGER scratch_touch BEFORE UPDATE ON scratch \
             FOR EACH ROW EXECUTE FUNCTION public.touch()",
            "ALTER TABLE scratch ENABLE ROW LEVEL SECURITY",
            "CREATE POLICY scratch_all ON scratch USING (id > 0)",
        ] {
            session
                .execute(sqlx::AssertSqlSafe(statement.to_string()))
                .await?;
        }

        assert_sides_agree(db.pool()).await
    })
    .await
}

/// `Catalog::object_ids` + `Catalog::id_present_in` must answer exactly what
/// `Catalog::contains_id` answers.
///
/// `contains_id` scans a vector per question, so callers with many questions
/// (filtering the dependency maps, cascade expansion, plan ordering) resolve
/// against a prebuilt id set instead. That set is assembled kind by kind and
/// the sub-object resolution rule is spelled out in both places, so a newly
/// tracked kind left out of `object_ids` — or a sub-object rule that drifts —
/// would silently prune live dependencies. Neither is compile-time checked.
#[tokio::test]
async fn test_id_present_in_agrees_with_contains_id() -> Result<()> {
    with_test_db(async |db| {
        for statement in SCHEMA {
            db.execute(statement).await;
        }

        let catalog = Catalog::load_unfiltered(db.pool()).await?;
        let present = catalog.object_ids();

        // Every stored object is reachable both ways. An empty catalog would
        // make the loops below vacuous, so require real content first.
        assert!(
            present.len() > 20,
            "fixture should populate many kinds, got {}",
            present.len()
        );
        for id in &present {
            assert!(
                catalog.contains_id(id),
                "object_ids yielded {id:?}, which contains_id denies"
            );
            assert!(
                Catalog::id_present_in(&present, id),
                "id_present_in denies {id:?}, which it produced"
            );
        }

        // The dependency keys are the real caller: every one of them must get
        // the same verdict from both, including comment and column ids, which
        // resolve through a parent rather than being members of the set.
        for id in catalog.forward_deps.keys() {
            assert_eq!(
                Catalog::id_present_in(&present, id),
                catalog.contains_id(id),
                "verdicts differ for dependency key {id:?}"
            );
        }

        // Absent ids agree too, including through the sub-object rules.
        let missing = DbObjectId::Table {
            schema: "app".to_string(),
            name: "no_such_table".to_string(),
        };
        for id in [
            missing.clone(),
            DbObjectId::Comment {
                object_id: Box::new(missing.clone()),
            },
            DbObjectId::Column {
                schema: "app".to_string(),
                table: "no_such_table".to_string(),
                column: "whatever".to_string(),
            },
        ] {
            assert!(!catalog.contains_id(&id), "{id:?} should be absent");
            assert!(
                !Catalog::id_present_in(&present, &id),
                "{id:?} should be absent"
            );
        }

        Ok(())
    })
    .await
}
