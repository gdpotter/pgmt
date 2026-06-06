use crate::helpers::harness::with_test_db;
use pgmt::catalog::cast::fetch;
use pgmt::catalog::id::{DbObjectId, DependsOn};

/// Two composite "temperature" types plus a conversion function — a clean basis
/// for a user WITH FUNCTION cast that doesn't collide with any built-in cast.
const TEMP_TYPES: &str = "CREATE TYPE celsius AS (deg double precision); \
                          CREATE TYPE fahrenheit AS (deg double precision)";
const C_TO_F_FN: &str = "CREATE FUNCTION c_to_f(celsius) RETURNS fahrenheit \
                         AS $$ SELECT ROW(($1).deg * 9.0 / 5.0 + 32.0)::fahrenheit $$ \
                         LANGUAGE sql IMMUTABLE";
const C_TO_F_CAST: &str = "CREATE CAST (celsius AS fahrenheit) WITH FUNCTION c_to_f(celsius)";

#[tokio::test]
async fn test_fetch_basic_cast() {
    with_test_db(async |db| {
        db.execute(TEMP_TYPES).await;
        db.execute(C_TO_F_FN).await;
        db.execute(C_TO_F_CAST).await;

        let casts = fetch(&mut *db.conn().await).await.unwrap();
        assert_eq!(casts.len(), 1);
        let cast = &casts[0];
        assert_eq!(cast.source, "celsius");
        assert_eq!(cast.target, "fahrenheit");
        assert!(cast.comment.is_none());
        assert!(
            cast.definition.contains(
                "CREATE CAST (celsius AS fahrenheit) WITH FUNCTION public.c_to_f(celsius)"
            ),
            "definition: {}",
            cast.definition
        );
    })
    .await;
}

#[tokio::test]
async fn test_fetch_cast_dependencies() {
    with_test_db(async |db| {
        db.execute(TEMP_TYPES).await;
        db.execute(C_TO_F_FN).await;
        db.execute(C_TO_F_CAST).await;

        let casts = fetch(&mut *db.conn().await).await.unwrap();
        assert_eq!(casts.len(), 1);
        let deps = casts[0].depends_on();

        // Both the source and target types and the implementing function.
        assert!(
            deps.contains(&DbObjectId::Type {
                schema: "public".to_string(),
                name: "celsius".to_string()
            }),
            "deps: {deps:?}"
        );
        assert!(
            deps.contains(&DbObjectId::Type {
                schema: "public".to_string(),
                name: "fahrenheit".to_string()
            }),
            "deps: {deps:?}"
        );
        assert!(
            deps.iter()
                .any(|d| matches!(d, DbObjectId::Function { name, .. } if name == "c_to_f")),
            "deps: {deps:?}"
        );
    })
    .await;
}

#[tokio::test]
async fn test_fetch_cast_with_comment() {
    with_test_db(async |db| {
        db.execute(TEMP_TYPES).await;
        db.execute(C_TO_F_FN).await;
        db.execute(C_TO_F_CAST).await;
        db.execute("COMMENT ON CAST (celsius AS fahrenheit) IS 'temperature conversion'")
            .await;

        let casts = fetch(&mut *db.conn().await).await.unwrap();
        assert_eq!(casts.len(), 1);
        assert_eq!(casts[0].comment, Some("temperature conversion".to_string()));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_io_cast() {
    with_test_db(async |db| {
        db.execute("CREATE TYPE celsius AS (deg double precision)")
            .await;
        // A WITH INOUT (I/O conversion) cast — no implementing function.
        db.execute("CREATE CAST (celsius AS text) WITH INOUT AS ASSIGNMENT")
            .await;

        let casts = fetch(&mut *db.conn().await).await.unwrap();
        assert_eq!(casts.len(), 1);
        let cast = &casts[0];
        assert_eq!(cast.source, "celsius");
        assert_eq!(cast.target, "text");
        assert!(
            cast.definition.contains("WITH INOUT"),
            "definition: {}",
            cast.definition
        );
        assert!(
            cast.definition.contains("AS ASSIGNMENT"),
            "definition: {}",
            cast.definition
        );
        // No function dependency for an I/O cast; only the source type.
        let deps = cast.depends_on();
        assert!(deps.contains(&DbObjectId::Type {
            schema: "public".to_string(),
            name: "celsius".to_string()
        }));
        assert!(
            !deps
                .iter()
                .any(|d| matches!(d, DbObjectId::Function { .. })),
            "I/O cast must have no function dependency: {deps:?}"
        );
    })
    .await;
}

#[tokio::test]
async fn test_skip_builtin_casts() {
    with_test_db(async |db| {
        // A fresh database has only built-in (pinned) casts; none are user-defined.
        let casts = fetch(&mut *db.conn().await).await.unwrap();
        assert!(
            casts.is_empty(),
            "built-in casts must be excluded, got {} casts",
            casts.len()
        );
    })
    .await;
}

const MONEY_TYPE: &str = "CREATE TYPE money_amount AS (cents bigint)";
const MONEY_FN: &str = "CREATE FUNCTION money_to_bigint(money_amount) RETURNS bigint \
                        AS $$ SELECT ($1).cents $$ LANGUAGE sql IMMUTABLE";
const MONEY_CAST: &str =
    "CREATE CAST (money_amount AS bigint) WITH FUNCTION money_to_bigint(money_amount)";

/// Cast-function routing closes the view->cast ordering gap: a view that applies
/// the cast records only the function in pg_depend, but `Catalog::load` inserts
/// the cast as an intermediate (`view -> cast -> function`) in the dependency
/// graph, so the cast is ordered before the view.
#[tokio::test]
async fn test_cast_function_routing_orders_cast_before_view() {
    with_test_db(async |db| {
        db.execute(MONEY_TYPE).await;
        db.execute(MONEY_FN).await;
        db.execute(MONEY_CAST).await;
        db.execute("CREATE TABLE accounts (balance money_amount)")
            .await;
        db.execute("CREATE VIEW big_balances AS SELECT balance::bigint AS cents FROM accounts")
            .await;

        let catalog = pgmt::catalog::Catalog::load(db.pool()).await.unwrap();
        let view_id = DbObjectId::View {
            schema: "public".to_string(),
            name: "big_balances".to_string(),
        };
        let cast_id = DbObjectId::Cast {
            source: "money_amount".to_string(),
            target: "bigint".to_string(),
        };
        let view_deps = catalog
            .forward_deps
            .get(&view_id)
            .expect("view should be in the dependency graph");
        assert!(
            view_deps.contains(&cast_id),
            "routing should make the view depend on the cast: {view_deps:?}"
        );
    })
    .await;
}

/// Documents the over-connection cost of routing: pg_depend cannot distinguish a
/// view that *uses* the cast from one that calls the cast's function *directly*,
/// so both gain the synthetic dependency on the cast.
#[tokio::test]
async fn test_cast_function_routing_over_connects_direct_caller() {
    with_test_db(async |db| {
        db.execute(MONEY_TYPE).await;
        db.execute(MONEY_FN).await;
        db.execute(MONEY_CAST).await;
        // This view calls the function directly; it never uses the cast.
        db.execute("CREATE VIEW direct_caller AS SELECT money_to_bigint(NULL::money_amount) AS c")
            .await;

        let catalog = pgmt::catalog::Catalog::load(db.pool()).await.unwrap();
        let view_id = DbObjectId::View {
            schema: "public".to_string(),
            name: "direct_caller".to_string(),
        };
        let cast_id = DbObjectId::Cast {
            source: "money_amount".to_string(),
            target: "bigint".to_string(),
        };
        let view_deps = catalog
            .forward_deps
            .get(&view_id)
            .expect("view should be in the dependency graph");
        assert!(
            view_deps.contains(&cast_id),
            "routing over-connects: a direct function caller also depends on the cast: {view_deps:?}"
        );
    })
    .await;
}

/// Characterization test for the underlying pg_depend behavior: a view that
/// applies a function-based cast records a dependency on the cast's *function*,
/// never on the pg_cast entry. (The graph-level `view -> cast` edge is added
/// separately by cast-function routing; see the routing tests above.)
#[tokio::test]
async fn test_view_using_cast_records_function_not_cast() {
    with_test_db(async |db| {
        db.execute("CREATE TYPE money_amount AS (cents bigint)")
            .await;
        db.execute(
            "CREATE FUNCTION money_to_bigint(money_amount) RETURNS bigint \
             AS $$ SELECT ($1).cents $$ LANGUAGE sql IMMUTABLE",
        )
        .await;
        db.execute(
            "CREATE CAST (money_amount AS bigint) WITH FUNCTION money_to_bigint(money_amount)",
        )
        .await;
        db.execute("CREATE TABLE t (m money_amount)").await;
        db.execute("CREATE VIEW v AS SELECT m::bigint AS b FROM t")
            .await;

        let views = pgmt::catalog::view::fetch(&mut *db.conn().await)
            .await
            .unwrap();
        let view = views.iter().find(|v| v.name == "v").expect("view v");

        // The cast is invisible to pg_depend: the view depends on the cast's
        // function instead. (This is why casts-in-views are not auto-ordered.)
        assert!(
            !view
                .depends_on
                .iter()
                .any(|d| matches!(d, DbObjectId::Cast { .. })),
            "PostgreSQL records no view->cast edge: {:?}",
            view.depends_on
        );
        assert!(
            view.depends_on.iter().any(
                |d| matches!(d, DbObjectId::Function { name, .. } if name == "money_to_bigint")
            ),
            "the view should depend on the cast function instead: {:?}",
            view.depends_on
        );
    })
    .await;
}
