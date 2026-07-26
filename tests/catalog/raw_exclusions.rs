//! Tests for the named reasons a raw row does not become a logical object.
//!
//! The property that makes the reasons worth having: a converter's raw input is
//! fully accounted for by its output. Every raw row is either converted or
//! excluded with a reason, and the residue — rows that are neither — is empty.

use crate::helpers::harness::with_test_db;
use anyhow::Result;
use pgmt::catalog::raw::exclusion::ExclusionReason;
use pgmt::catalog::raw::shared;
use pgmt::catalog::raw::{operator as raw_operator, table as raw_table};
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
