//! Tests for the named reasons a raw row does not become a logical object.
//!
//! The property that makes the reasons worth having: a converter's raw input is
//! fully accounted for by its output. Every raw row is either converted or
//! excluded with a reason, and the residue — rows that are neither — is empty.

use crate::helpers::harness::with_test_db;
use anyhow::Result;
use pgmt::catalog::raw::exclusion::ExclusionReason;
use pgmt::catalog::raw::shared;
use pgmt::catalog::raw::{
    aggregate as raw_aggregate, cast as raw_cast, custom_type as raw_custom_type,
    domain as raw_domain, function as raw_function, operator as raw_operator, table as raw_table,
    view as raw_view,
};
use sqlx::postgres::types::Oid;
use std::collections::BTreeSet;

/// A database holding a user table, an extension that installs operators
/// (citext), and a user table adopted into an extension — enough for every
/// reason a converter can give.
async fn setup(db: &crate::helpers::harness::TestDatabase) {
    db.execute("CREATE EXTENSION IF NOT EXISTS citext").await;
    db.execute("CREATE TABLE users (id integer PRIMARY KEY, email text)")
        .await;
    db.execute("CREATE TABLE adopted (id integer)").await;
    db.execute("ALTER EXTENSION citext ADD TABLE adopted").await;
}

#[tokio::test]
async fn test_every_raw_table_row_is_converted_or_excluded() -> Result<()> {
    with_test_db(async |db| {
        setup(db).await;

        let mut conn = db.conn().await;
        let shared = shared::fetch(&mut conn).await?;
        let raw = raw_table::fetch(&mut conn).await?;
        let converted = raw_table::convert(&raw, &shared)?;

        let accounted: BTreeSet<u32> = converted
            .objects
            .iter()
            .map(|entry| entry.oid.0)
            .chain(converted.excluded.iter().map(|row| row.oid.0))
            .collect();
        let all: BTreeSet<u32> = raw.tables.iter().map(|row| row.oid.0).collect();

        let residue: Vec<&str> = raw
            .tables
            .iter()
            .filter(|row| !accounted.contains(&row.oid.0))
            .map(|row| row.name.as_str())
            .collect();
        assert!(
            residue.is_empty(),
            "raw table rows neither converted nor excluded: {:?}",
            residue
        );
        assert_eq!(accounted, all);
        assert_eq!(
            converted.objects.len() + converted.excluded.len(),
            raw.tables.len(),
            "a row was counted twice"
        );

        // The user table converts; the one the extension adopted does not, and
        // says why.
        assert!(
            converted
                .objects
                .iter()
                .any(|entry| entry.table.name == "users")
        );

        let adopted = converted
            .excluded
            .iter()
            .find(|row| row.name == "adopted")
            .expect("the adopted table should be excluded");
        assert_eq!(
            adopted.reason,
            ExclusionReason::ExtensionOwned {
                extension: "citext".to_string()
            }
        );
        assert_eq!(adopted.kind, "table");
        assert_eq!(adopted.qualified_name(), "public.adopted");

        // The catalog's own tables are excluded as system-schema rows.
        let system: Vec<&str> = converted
            .excluded_for("SystemSchema")
            .map(|row| row.schema.as_str())
            .collect();
        assert!(
            system.contains(&"pg_catalog"),
            "expected pg_catalog tables to be excluded as SystemSchema, got {:?}",
            system
        );
        assert!(
            converted
                .excluded_for("SystemSchema")
                .any(|row| row.schema == "pg_catalog" && row.name == "pg_class")
        );

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_every_raw_operator_row_is_converted_or_excluded() -> Result<()> {
    with_test_db(async |db| {
        setup(db).await;
        db.execute(
            r#"
            CREATE FUNCTION same_len(text, text) RETURNS boolean
            AS $$ SELECT length($1) = length($2) $$ LANGUAGE sql IMMUTABLE
        "#,
        )
        .await;
        db.execute("CREATE OPERATOR === (LEFTARG = text, RIGHTARG = text, FUNCTION = same_len)")
            .await;

        let mut conn = db.conn().await;
        let shared = shared::fetch(&mut conn).await?;
        let raw = raw_operator::fetch(&mut conn).await?;
        let converted = raw_operator::convert(&raw, &shared)?;

        let accounted: BTreeSet<u32> = converted
            .objects
            .iter()
            .map(|(oid, _)| oid.0)
            .chain(converted.excluded.iter().map(|row| row.oid.0))
            .collect();
        let all: BTreeSet<u32> = raw.iter().map(|row| row.oid.0).collect();

        let residue: Vec<&str> = raw
            .iter()
            .filter(|row| !accounted.contains(&row.oid.0))
            .map(|row| row.name.as_str())
            .collect();
        assert!(
            residue.is_empty(),
            "raw operator rows neither converted nor excluded: {:?}",
            residue
        );
        assert_eq!(accounted, all);
        assert_eq!(
            converted.objects.len() + converted.excluded.len(),
            raw.len(),
            "a row was counted twice"
        );

        // The user operator converts.
        assert!(
            converted
                .objects
                .iter()
                .any(|(_, operator)| operator.name == "===")
        );

        // citext's operators are excluded by name, not silently.
        let citext_owned: Vec<&str> = converted
            .excluded
            .iter()
            .filter(|row| {
                row.reason
                    == ExclusionReason::ExtensionOwned {
                        extension: "citext".to_string(),
                    }
            })
            .map(|row| row.name.as_str())
            .collect();
        assert!(
            !citext_owned.is_empty(),
            "citext installs operators; none were excluded as ExtensionOwned"
        );

        // Built-in operators are excluded as system-schema rows.
        assert!(
            converted
                .excluded_for("SystemSchema")
                .any(|row| row.schema == "pg_catalog"),
            "expected pg_catalog operators to be excluded as SystemSchema"
        );

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_every_raw_view_row_is_converted_or_excluded() -> Result<()> {
    with_test_db(async |db| {
        setup(db).await;
        db.execute("CREATE VIEW user_emails AS SELECT id, email FROM users")
            .await;
        db.execute("CREATE VIEW adopted_ids AS SELECT id FROM adopted")
            .await;
        db.execute("ALTER EXTENSION citext ADD VIEW adopted_ids")
            .await;

        let mut conn = db.conn().await;
        let shared = shared::fetch(&mut conn).await?;
        let raw = raw_view::fetch(&mut conn).await?;
        let converted = raw_view::convert(&raw, &shared)?;

        let accounted: BTreeSet<u32> = converted
            .objects
            .iter()
            .map(|entry| entry.oid.0)
            .chain(converted.excluded.iter().map(|row| row.oid.0))
            .collect();
        let all: BTreeSet<u32> = raw.views.iter().map(|row| row.oid.0).collect();

        let residue: Vec<&str> = raw
            .views
            .iter()
            .filter(|row| !accounted.contains(&row.oid.0))
            .map(|row| row.name.as_str())
            .collect();
        assert!(
            residue.is_empty(),
            "raw view rows neither converted nor excluded: {:?}",
            residue
        );
        assert_eq!(accounted, all);
        assert_eq!(
            converted.objects.len() + converted.excluded.len(),
            raw.views.len(),
            "a row was counted twice"
        );

        assert!(
            converted
                .objects
                .iter()
                .any(|entry| entry.view.name == "user_emails")
        );

        let adopted = converted
            .excluded
            .iter()
            .find(|row| row.name == "adopted_ids")
            .expect("the adopted view should be excluded");
        assert_eq!(
            adopted.reason,
            ExclusionReason::ExtensionOwned {
                extension: "citext".to_string()
            }
        );
        assert_eq!(adopted.kind, "view");

        // The catalog's own views are excluded as system-schema rows.
        assert!(
            converted
                .excluded_for("SystemSchema")
                .any(|row| row.schema == "pg_catalog"),
            "expected pg_catalog views to be excluded as SystemSchema"
        );

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_every_raw_type_row_is_converted_or_excluded() -> Result<()> {
    with_test_db(async |db| {
        setup(db).await;
        db.execute("CREATE TYPE status AS ENUM ('active', 'inactive')")
            .await;
        db.execute("CREATE TYPE adopted_pair AS (a integer, b integer)")
            .await;
        db.execute("ALTER EXTENSION citext ADD TYPE adopted_pair")
            .await;

        let mut conn = db.conn().await;
        let shared = shared::fetch(&mut conn).await?;
        let raw = raw_custom_type::fetch(&mut conn).await?;
        let converted = raw_custom_type::convert(&raw, &shared)?;

        let accounted: BTreeSet<u32> = converted
            .objects
            .iter()
            .map(|entry| entry.oid.0)
            .chain(converted.excluded.iter().map(|row| row.oid.0))
            .collect();
        let all: BTreeSet<u32> = raw.types.iter().map(|row| row.oid.0).collect();

        let residue: Vec<&str> = raw
            .types
            .iter()
            .filter(|row| !accounted.contains(&row.oid.0))
            .map(|row| row.name.as_str())
            .collect();
        assert!(
            residue.is_empty(),
            "raw type rows neither converted nor excluded: {:?}",
            residue
        );
        assert_eq!(accounted, all);
        assert_eq!(
            converted.objects.len() + converted.excluded.len(),
            raw.types.len(),
            "a row was counted twice"
        );

        assert!(
            converted
                .objects
                .iter()
                .any(|entry| entry.custom_type.name == "status")
        );

        let adopted = converted
            .excluded
            .iter()
            .find(|row| row.name == "adopted_pair")
            .expect("the adopted type should be excluded");
        assert_eq!(
            adopted.reason,
            ExclusionReason::ExtensionOwned {
                extension: "citext".to_string()
            }
        );
        assert_eq!(adopted.kind, "type");

        // The built-in range types live in pg_catalog and are excluded as
        // system-schema rows.
        assert!(
            converted
                .excluded_for("SystemSchema")
                .any(|row| row.schema == "pg_catalog" && row.name == "int4range"),
            "expected pg_catalog range types to be excluded as SystemSchema"
        );

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_every_raw_domain_row_is_converted_or_excluded() -> Result<()> {
    with_test_db(async |db| {
        setup(db).await;
        db.execute("CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0)")
            .await;
        db.execute("CREATE DOMAIN adopted_code AS text").await;
        db.execute("ALTER EXTENSION citext ADD DOMAIN adopted_code")
            .await;

        let mut conn = db.conn().await;
        let shared = shared::fetch(&mut conn).await?;
        let raw = raw_domain::fetch(&mut conn).await?;
        let converted = raw_domain::convert(&raw, &shared)?;

        let accounted: BTreeSet<u32> = converted
            .objects
            .iter()
            .map(|(oid, _)| oid.0)
            .chain(converted.excluded.iter().map(|row| row.oid.0))
            .collect();
        let all: BTreeSet<u32> = raw.domains.iter().map(|row| row.oid.0).collect();

        let residue: Vec<&str> = raw
            .domains
            .iter()
            .filter(|row| !accounted.contains(&row.oid.0))
            .map(|row| row.name.as_str())
            .collect();
        assert!(
            residue.is_empty(),
            "raw domain rows neither converted nor excluded: {:?}",
            residue
        );
        assert_eq!(accounted, all);
        assert_eq!(
            converted.objects.len() + converted.excluded.len(),
            raw.domains.len(),
            "a row was counted twice"
        );

        assert!(
            converted
                .objects
                .iter()
                .any(|(_, domain)| domain.name == "positive_int")
        );

        let adopted = converted
            .excluded
            .iter()
            .find(|row| row.name == "adopted_code")
            .expect("the adopted domain should be excluded");
        assert_eq!(
            adopted.reason,
            ExclusionReason::ExtensionOwned {
                extension: "citext".to_string()
            }
        );
        assert_eq!(adopted.kind, "domain");

        // information_schema is built out of domains, all of them system rows.
        assert!(
            converted
                .excluded_for("SystemSchema")
                .any(|row| row.schema == "information_schema"),
            "expected information_schema domains to be excluded as SystemSchema"
        );

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_every_raw_function_row_is_converted_or_excluded() -> Result<()> {
    with_test_db(async |db| {
        setup(db).await;
        db.execute("CREATE FUNCTION greet(name text) RETURNS text AS $$ SELECT 'hi ' || name $$ LANGUAGE sql")
            .await;
        db.execute("CREATE FUNCTION adopted_fn() RETURNS integer AS $$ SELECT 1 $$ LANGUAGE sql")
            .await;
        db.execute("ALTER EXTENSION citext ADD FUNCTION adopted_fn()")
            .await;

        let mut conn = db.conn().await;
        let shared = shared::fetch(&mut conn).await?;
        let raw = raw_function::fetch(&mut conn).await?;
        let converted = raw_function::convert(&raw, &shared)?;

        let accounted: BTreeSet<u32> = converted
            .objects
            .iter()
            .map(|(oid, _)| oid.0)
            .chain(converted.excluded.iter().map(|row| row.oid.0))
            .collect();
        let all: BTreeSet<u32> = raw.functions.iter().map(|row| row.oid.0).collect();

        let residue: Vec<&str> = raw
            .functions
            .iter()
            .filter(|row| !accounted.contains(&row.oid.0))
            .map(|row| row.name.as_str())
            .collect();
        assert!(
            residue.is_empty(),
            "raw function rows neither converted nor excluded: {:?}",
            residue
        );
        assert_eq!(accounted, all);
        assert_eq!(
            converted.objects.len() + converted.excluded.len(),
            raw.functions.len(),
            "a row was counted twice"
        );

        assert!(
            converted
                .objects
                .iter()
                .any(|(_, function)| function.name == "greet")
        );

        let adopted = converted
            .excluded
            .iter()
            .find(|row| row.name == "adopted_fn")
            .expect("the adopted function should be excluded");
        assert_eq!(
            adopted.reason,
            ExclusionReason::ExtensionOwned {
                extension: "citext".to_string()
            }
        );
        assert_eq!(adopted.kind, "function");

        // The built-in routines are excluded as system-schema rows.
        assert!(
            converted
                .excluded_for("SystemSchema")
                .any(|row| row.schema == "pg_catalog"),
            "expected pg_catalog functions to be excluded as SystemSchema"
        );

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_every_raw_aggregate_row_is_converted_or_excluded() -> Result<()> {
    with_test_db(async |db| {
        setup(db).await;
        db.execute(
            "CREATE FUNCTION concat_sfunc(state text, val text) RETURNS text \
             AS $$ SELECT COALESCE(state || ',', '') || val $$ LANGUAGE sql",
        )
        .await;
        db.execute("CREATE AGGREGATE concat_all(text) (SFUNC = concat_sfunc, STYPE = text)")
            .await;
        db.execute("CREATE AGGREGATE adopted_agg(text) (SFUNC = concat_sfunc, STYPE = text)")
            .await;
        db.execute("ALTER EXTENSION citext ADD AGGREGATE adopted_agg(text)")
            .await;

        let mut conn = db.conn().await;
        let shared = shared::fetch(&mut conn).await?;
        let raw = raw_aggregate::fetch(&mut conn).await?;
        let converted = raw_aggregate::convert(&raw, &shared)?;

        let accounted: BTreeSet<u32> = converted
            .objects
            .iter()
            .map(|(oid, _)| oid.0)
            .chain(converted.excluded.iter().map(|row| row.oid.0))
            .collect();
        let all: BTreeSet<u32> = raw.iter().map(|row| row.oid.0).collect();

        let residue: Vec<&str> = raw
            .iter()
            .filter(|row| !accounted.contains(&row.oid.0))
            .map(|row| row.name.as_str())
            .collect();
        assert!(
            residue.is_empty(),
            "raw aggregate rows neither converted nor excluded: {:?}",
            residue
        );
        assert_eq!(accounted, all);
        assert_eq!(
            converted.objects.len() + converted.excluded.len(),
            raw.len(),
            "a row was counted twice"
        );

        assert!(
            converted
                .objects
                .iter()
                .any(|(_, aggregate)| aggregate.name == "concat_all")
        );

        let adopted = converted
            .excluded
            .iter()
            .find(|row| row.name == "adopted_agg")
            .expect("the adopted aggregate should be excluded");
        assert_eq!(
            adopted.reason,
            ExclusionReason::ExtensionOwned {
                extension: "citext".to_string()
            }
        );
        assert_eq!(adopted.kind, "aggregate");

        // The built-in aggregates (sum, count, …) are system-schema rows.
        assert!(
            converted
                .excluded_for("SystemSchema")
                .any(|row| row.schema == "pg_catalog" && row.name == "count"),
            "expected pg_catalog aggregates to be excluded as SystemSchema"
        );

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_every_raw_cast_row_is_converted_or_excluded() -> Result<()> {
    with_test_db(async |db| {
        setup(db).await;
        db.execute("CREATE TYPE celsius AS (deg double precision)")
            .await;
        db.execute("CREATE TYPE fahrenheit AS (deg double precision)")
            .await;
        db.execute(
            "CREATE FUNCTION c_to_f(celsius) RETURNS fahrenheit \
             AS $$ SELECT ROW(($1).deg * 9.0 / 5.0 + 32.0)::fahrenheit $$ LANGUAGE sql IMMUTABLE",
        )
        .await;
        db.execute("CREATE CAST (celsius AS fahrenheit) WITH FUNCTION c_to_f(celsius)")
            .await;
        db.execute("CREATE CAST (fahrenheit AS celsius) WITH INOUT")
            .await;
        db.execute("ALTER EXTENSION citext ADD CAST (fahrenheit AS celsius)")
            .await;

        let mut conn = db.conn().await;
        let shared = shared::fetch(&mut conn).await?;
        let raw = raw_cast::fetch(&mut conn).await?;
        let converted = raw_cast::convert(&raw, &shared)?;

        let accounted: BTreeSet<u32> = converted
            .objects
            .iter()
            .map(|(oid, _)| oid.0)
            .chain(converted.excluded.iter().map(|row| row.oid.0))
            .collect();
        let all: BTreeSet<u32> = raw.iter().map(|row| row.oid.0).collect();

        let residue: Vec<String> = raw
            .iter()
            .filter(|row| !accounted.contains(&row.oid.0))
            .map(|row| format!("{} AS {}", row.source, row.target))
            .collect();
        assert!(
            residue.is_empty(),
            "raw cast rows neither converted nor excluded: {:?}",
            residue
        );
        assert_eq!(accounted, all);
        assert_eq!(
            converted.objects.len() + converted.excluded.len(),
            raw.len(),
            "a row was counted twice"
        );

        assert!(
            converted
                .objects
                .iter()
                .any(|(_, cast)| cast.source == "celsius" && cast.target == "fahrenheit")
        );

        let adopted = converted
            .excluded
            .iter()
            .find(|row| row.name == "fahrenheit AS celsius")
            .expect("the adopted cast should be excluded");
        assert_eq!(
            adopted.reason,
            ExclusionReason::ExtensionOwned {
                extension: "citext".to_string()
            }
        );
        assert_eq!(adopted.kind, "cast");

        // Every built-in cast converts between built-in types, and is the
        // server's rather than a user's.
        assert!(
            converted
                .excluded_for("SystemSchema")
                .any(|row| row.name == "integer AS bigint"),
            "expected built-in casts to be excluded as SystemSchema"
        );

        Ok(())
    })
    .await
}

/// Named exclusions are the physical layer only: the converter drops what is not
/// user schema at all, and leaves config-driven scoping (`ObjectFilter`) to the
/// logical side. A user table in a schema pgmt happens not to manage is still
/// converted here.
#[tokio::test]
async fn test_conversion_yields_the_physical_catalog() -> Result<()> {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA unmanaged").await;
        db.execute("CREATE TABLE unmanaged.notes (id integer)")
            .await;

        let mut conn = db.conn().await;
        let shared = shared::fetch(&mut conn).await?;
        let raw = raw_table::fetch(&mut conn).await?;
        let converted = raw_table::convert(&raw, &shared)?;

        assert!(
            converted
                .objects
                .iter()
                .any(|entry| entry.table.schema == "unmanaged" && entry.table.name == "notes"),
            "a user table outside the managed universe is still part of the physical catalog"
        );
        assert!(
            !converted
                .excluded
                .iter()
                .any(|row| row.schema == "unmanaged")
        );

        Ok(())
    })
    .await
}

/// The excluded rows keep their OIDs so a future census can reconcile them
/// against a catalog enumeration without inventing an identity for an object
/// that deliberately has none.
#[tokio::test]
async fn test_excluded_rows_keep_their_catalog_oid() -> Result<()> {
    with_test_db(async |db| {
        setup(db).await;

        let mut conn = db.conn().await;
        let shared = shared::fetch(&mut conn).await?;
        let raw = raw_table::fetch(&mut conn).await?;
        let converted = raw_table::convert(&raw, &shared)?;

        let adopted_oid: (Oid,) = sqlx::query_as("SELECT 'adopted'::regclass::oid")
            .fetch_one(db.pool())
            .await?;
        let adopted = converted
            .excluded
            .iter()
            .find(|row| row.name == "adopted")
            .expect("the adopted table should be excluded");
        assert_eq!(adopted.oid, adopted_oid.0);

        Ok(())
    })
    .await
}
