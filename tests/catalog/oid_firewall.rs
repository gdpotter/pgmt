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
    // The identities the OID index has to reconstruct from more than a
    // (schema, name) pair: an operator is addressed by its operand types and a
    // cast by its type pair, and a composite attribute by an attnum that differs
    // between databases.
    "COMMENT ON OPERATOR app.=== (integer, integer) IS 'Sums, oddly'",
    "COMMENT ON CAST (app.email AS app.status) IS 'Everyone is active'",
    "COMMENT ON COLUMN app.point2d.x IS 'Horizontal'",
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

/// Whether a source line declares a field (or binding) whose type or name is a
/// physical catalog coordinate.
fn declares_physical_coordinate(line: &str) -> bool {
    let line = line.trim();
    if line.starts_with("//") {
        return false;
    }
    let Some((name, type_)) = line.split_once(':') else {
        return false;
    };
    let name = name.trim().trim_start_matches("pub ").trim();
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_lowercase() || c == '_') {
        return false;
    }
    let type_ = type_.trim().trim_end_matches([',', ';']).trim();

    type_
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .any(|word| word == "Oid")
        || (name.contains("oid") && matches!(type_, "u32" | "i32" | "i64" | "Option<u32>"))
        || (name.contains("attnum")
            && matches!(type_, "i16" | "i32" | "Option<i16>" | "Option<i32>"))
}

/// No logical struct may carry an OID or an attnum.
///
/// The diff test above only catches a leak that changes a comparison: a field
/// that happens to hold the same value in both databases — an attnum equal by
/// construction because both databases built the table the same way — passes it
/// while still being a physical coordinate one `ALTER TABLE DROP COLUMN` away
/// from breaking every cross-database diff. Reading the source catches the field
/// itself, before a fixture has to be unlucky enough to expose it.
///
/// `src/catalog/raw/` is exempt: that module is where OIDs are supposed to live.
#[test]
fn test_logical_catalog_source_carries_no_physical_coordinates() {
    let catalog_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/catalog");
    let mut offenders = Vec::new();
    let mut files_scanned = 0;

    for entry in std::fs::read_dir(&catalog_dir).expect("src/catalog is readable") {
        let path = entry.expect("readable directory entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        files_scanned += 1;
        let source = std::fs::read_to_string(&path).expect("readable source file");
        for (number, line) in source.lines().enumerate() {
            if declares_physical_coordinate(line) {
                offenders.push(format!(
                    "{}:{}: {}",
                    path.file_name().unwrap().to_string_lossy(),
                    number + 1,
                    line.trim()
                ));
            }
        }
    }

    assert!(files_scanned > 10, "expected to scan the catalog modules");
    assert!(
        offenders.is_empty(),
        "the logical catalog must carry no OIDs or attnums; \
         physical coordinates belong in src/catalog/raw/:\n  {}",
        offenders.join("\n  ")
    );
}

mod detector_tests {
    use super::declares_physical_coordinate;

    #[test]
    fn test_detector_flags_physical_coordinates() {
        assert!(declares_physical_coordinate("    pub oid: Oid,"));
        assert!(declares_physical_coordinate("    attnum: i16,"));
        assert!(declares_physical_coordinate(
            "    pub relation_oid: Option<Oid>,"
        ));
        assert!(declares_physical_coordinate("    pub table_oid: u32,"));
    }

    #[test]
    fn test_detector_ignores_logical_fields() {
        assert!(!declares_physical_coordinate("    pub name: String,"));
        assert!(!declares_physical_coordinate(
            "    pub columns: Vec<Column>,"
        ));
        assert!(!declares_physical_coordinate(
            "    // attnum: i16 would leak"
        ));
        assert!(!declares_physical_coordinate("          AND a.attnum > 0"));
        assert!(!declares_physical_coordinate("    pub sort_order: i32,"));
    }
}
