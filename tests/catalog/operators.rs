use crate::helpers::harness::with_test_db;
use pgmt::catalog::id::{DbObjectId, DependsOn};
use pgmt::catalog::operator::fetch;

/// An IMMUTABLE integer equality function usable as an operator implementation.
const INT_EQ_FN: &str = "CREATE FUNCTION my_int_eq(integer, integer) RETURNS boolean \
                         AS $$ SELECT $1 = $2 $$ LANGUAGE sql IMMUTABLE";

#[tokio::test]
async fn test_fetch_basic_operator() {
    with_test_db(async |db| {
        db.execute(INT_EQ_FN).await;
        db.execute(
            "CREATE OPERATOR === (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_int_eq)",
        )
        .await;

        let operators = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(operators.len(), 1);
        let op = &operators[0];
        assert_eq!(op.schema, "public");
        assert_eq!(op.name, "===");
        assert_eq!(op.arguments, "integer, integer");
        assert!(op.comment.is_none());

        // The reconstructed definition drives CREATE OPERATOR rendering.
        assert!(
            op.definition.contains("CREATE OPERATOR public.==="),
            "definition: {}",
            op.definition
        );
        assert!(
            op.definition.contains("FUNCTION = public.my_int_eq"),
            "definition: {}",
            op.definition
        );
        assert!(op.definition.contains("LEFTARG = integer"));
        assert!(op.definition.contains("RIGHTARG = integer"));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_operator_dependencies() {
    with_test_db(async |db| {
        db.execute("CREATE TYPE money_amount AS (cents bigint)")
            .await;
        db.execute(
            "CREATE FUNCTION money_eq(money_amount, money_amount) RETURNS boolean \
             AS $$ SELECT ($1).cents = ($2).cents $$ LANGUAGE sql IMMUTABLE",
        )
        .await;
        db.execute(
            "CREATE OPERATOR === (LEFTARG = money_amount, RIGHTARG = money_amount, \
             FUNCTION = money_eq, COMMUTATOR = ===)",
        )
        .await;

        let operators = fetch(&mut *db.conn().await).await.unwrap();
        assert_eq!(operators.len(), 1);
        let deps = operators[0].depends_on();

        // Schema, implementing function, and the operand type are all dependencies.
        assert!(deps.contains(&DbObjectId::Schema {
            name: "public".to_string()
        }));
        assert!(
            deps.iter()
                .any(|d| matches!(d, DbObjectId::Function { name, .. } if name == "money_eq")),
            "deps: {deps:?}"
        );
        assert!(
            deps.contains(&DbObjectId::Type {
                schema: "public".to_string(),
                name: "money_amount".to_string()
            }),
            "deps: {deps:?}"
        );

        // Commutator/negator are NOT dependencies: they reference each other and
        // PostgreSQL resolves them via shell operators, so a hard edge would cycle.
        assert!(
            !deps
                .iter()
                .any(|d| matches!(d, DbObjectId::Operator { .. })),
            "operator must not depend on its commutator/negator: {deps:?}"
        );
    })
    .await;
}

#[tokio::test]
async fn test_fetch_operator_with_comment() {
    with_test_db(async |db| {
        db.execute(INT_EQ_FN).await;
        db.execute(
            "CREATE OPERATOR === (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_int_eq)",
        )
        .await;
        db.execute("COMMENT ON OPERATOR === (integer, integer) IS 'custom equality'")
            .await;

        let operators = fetch(&mut *db.conn().await).await.unwrap();
        assert_eq!(operators.len(), 1);
        assert_eq!(operators[0].comment, Some("custom equality".to_string()));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_prefix_operator() {
    with_test_db(async |db| {
        db.execute(
            "CREATE FUNCTION my_int_neg(integer) RETURNS integer \
             AS $$ SELECT -$1 $$ LANGUAGE sql IMMUTABLE",
        )
        .await;
        // A prefix operator has no left operand.
        db.execute("CREATE OPERATOR @@ (RIGHTARG = integer, FUNCTION = my_int_neg)")
            .await;

        let operators = fetch(&mut *db.conn().await).await.unwrap();
        assert_eq!(operators.len(), 1);
        let op = &operators[0];
        assert_eq!(op.name, "@@");
        // The absent left operand renders as NONE, matching DROP/COMMENT syntax.
        assert_eq!(op.arguments, "NONE, integer");
        assert!(op.definition.contains("RIGHTARG = integer"));
        assert!(
            !op.definition.contains("LEFTARG"),
            "prefix operator must not emit LEFTARG: {}",
            op.definition
        );
    })
    .await;
}

#[tokio::test]
async fn test_fetch_operator_with_array_operand() {
    with_test_db(async |db| {
        db.execute("CREATE TYPE color AS ENUM ('r', 'g', 'b')")
            .await;
        db.execute(
            "CREATE FUNCTION color_arr_eq(color[], color[]) RETURNS boolean \
             AS $$ SELECT $1[1] = $2[1] $$ LANGUAGE sql IMMUTABLE",
        )
        .await;
        db.execute(
            "CREATE OPERATOR ~~~ (LEFTARG = color[], RIGHTARG = color[], FUNCTION = color_arr_eq)",
        )
        .await;

        let operators = fetch(&mut *db.conn().await).await.unwrap();
        assert_eq!(operators.len(), 1);
        let op = &operators[0];
        assert_eq!(op.name, "~~~");
        // format_type preserves the array brackets in the identity + LEFTARG/RIGHTARG.
        assert_eq!(op.arguments, "color[], color[]");
        assert!(
            op.definition.contains("LEFTARG = color[]"),
            "definition: {}",
            op.definition
        );

        // The dependency must resolve to the ELEMENT type `color`, never the array
        // type `_color` (per the array-handling rule in CLAUDE.md).
        let deps = op.depends_on();
        assert!(
            deps.contains(&DbObjectId::Type {
                schema: "public".to_string(),
                name: "color".to_string()
            }),
            "expected element-type dependency on color, got {deps:?}"
        );
        assert!(
            !deps.iter().any(|d| matches!(
                d,
                DbObjectId::Type { name, .. } if name == "_color"
            )),
            "must not depend on the array type _color: {deps:?}"
        );
    })
    .await;
}

#[tokio::test]
async fn test_skip_builtin_operators() {
    with_test_db(async |db| {
        // A fresh database has only built-in operators (in pg_catalog); none are
        // user-defined, so fetch returns nothing.
        let operators = fetch(&mut *db.conn().await).await.unwrap();
        assert!(
            operators.is_empty(),
            "built-in operators must be excluded, got {} operators",
            operators.len()
        );
    })
    .await;
}
