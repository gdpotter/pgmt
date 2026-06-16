use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::diff::operations::{MigrationStep, OperationKind, OperatorOperation};

/// An IMMUTABLE integer equality function usable as an operator implementation.
const INT_EQ_FN: &str = "CREATE FUNCTION my_int_eq(integer, integer) RETURNS boolean \
                         AS $$ SELECT $1 = $2 $$ LANGUAGE sql IMMUTABLE";
const INT_EQ_OP: &str =
    "CREATE OPERATOR === (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_int_eq)";

/// Index of the first step matching `pred` whose operation is a CREATE.
fn create_pos<F: Fn(&MigrationStep) -> bool>(steps: &[MigrationStep], pred: F) -> usize {
    steps
        .iter()
        .position(|s| pred(s) && s.operation_kind() == OperationKind::Create)
        .expect("expected a matching create step")
}

#[tokio::test]
async fn test_create_operator_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;
    helper
        .run_migration_test(&[], &[], &[INT_EQ_FN, INT_EQ_OP], |steps, final_catalog| {
            let create = steps
                .iter()
                .find(|s| {
                    matches!(
                        s,
                        MigrationStep::Operator(OperatorOperation::Create { operator })
                            if operator.name == "==="
                    )
                })
                .expect("should have an operator Create step");

            match create {
                MigrationStep::Operator(OperatorOperation::Create { operator }) => {
                    assert_eq!(operator.schema, "public");
                    assert_eq!(operator.arguments, "integer, integer");
                }
                _ => unreachable!(),
            }

            // Round-trip: the operator exists after applying the migration.
            assert_eq!(final_catalog.operators.len(), 1);
            assert_eq!(final_catalog.operators[0].name, "===");
            Ok(())
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_drop_operator_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;
    helper
        .run_migration_test(&[], &[INT_EQ_FN, INT_EQ_OP], &[], |steps, final_catalog| {
            let drop = steps
                .iter()
                .find(|s| matches!(s, MigrationStep::Operator(OperatorOperation::Drop { .. })))
                .expect("should have an operator Drop step");

            match drop {
                MigrationStep::Operator(OperatorOperation::Drop { identifier }) => {
                    assert_eq!(identifier.name, "===");
                    assert_eq!(identifier.arguments, "integer, integer");
                }
                _ => unreachable!(),
            }

            assert!(final_catalog.operators.is_empty());
            Ok(())
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_operator_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;
    helper
        .run_migration_test(
            &[INT_EQ_FN, INT_EQ_OP],
            &[],
            &["COMMENT ON OPERATOR === (integer, integer) IS 'custom equality'"],
            |steps, final_catalog| {
                assert!(
                    steps.iter().any(|s| matches!(s, MigrationStep::Comment(_))),
                    "should have an operator Comment step"
                );
                assert_eq!(
                    final_catalog.operators[0].comment,
                    Some("custom equality".to_string())
                );
                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_drop_operator_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;
    helper
        .run_migration_test(
            &[INT_EQ_FN, INT_EQ_OP],
            &["COMMENT ON OPERATOR === (integer, integer) IS 'custom equality'"],
            &[],
            |steps, final_catalog| {
                assert!(
                    steps.iter().any(|s| matches!(s, MigrationStep::Comment(_))),
                    "should have an operator Comment (drop) step"
                );
                assert_eq!(final_catalog.operators[0].comment, None);
                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_operator_ordered_after_function_and_type() -> Result<()> {
    let helper = MigrationTestHelper::new().await;
    helper
        .run_migration_test(
            &[],
            &[],
            &[
                "CREATE TYPE money_amount AS (cents bigint)",
                "CREATE FUNCTION money_eq(money_amount, money_amount) RETURNS boolean \
                 AS $$ SELECT ($1).cents = ($2).cents $$ LANGUAGE sql IMMUTABLE",
                "CREATE OPERATOR === (LEFTARG = money_amount, RIGHTARG = money_amount, \
                 FUNCTION = money_eq)",
            ],
            |steps, _final_catalog| {
                let op_pos = create_pos(steps, |s| matches!(s, MigrationStep::Operator(_)));
                let fn_pos = create_pos(steps, |s| matches!(s, MigrationStep::Function(_)));
                let ty_pos = create_pos(steps, |s| matches!(s, MigrationStep::Type(_)));

                assert!(
                    fn_pos < op_pos,
                    "function must be created before the operator"
                );
                assert!(ty_pos < op_pos, "type must be created before the operator");
                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_view_using_custom_operator_ordered_after_it() -> Result<()> {
    let helper = MigrationTestHelper::new().await;
    helper
        .run_migration_test(
            &[],
            &[],
            &[
                INT_EQ_FN,
                INT_EQ_OP,
                "CREATE TABLE t (a integer, b integer)",
                "CREATE VIEW v AS SELECT a, b FROM t WHERE a === b",
            ],
            |steps, final_catalog| {
                // The pg_depend win: the view records a dependency on the custom
                // operator, so the operator is created first. (If it weren't, the
                // round-trip apply of CREATE VIEW would fail.)
                let op_pos = create_pos(steps, |s| matches!(s, MigrationStep::Operator(_)));
                let view_pos = create_pos(steps, |s| matches!(s, MigrationStep::View(_)));
                assert!(
                    op_pos < view_pos,
                    "operator must be created before the view that uses it"
                );

                assert_eq!(final_catalog.views.len(), 1);
                assert_eq!(final_catalog.operators.len(), 1);
                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_replace_operator_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;
    // Two equality functions; the operator switches from one to the other. An
    // operator cannot change its implementing function in place, so this is a
    // structural change → DROP + CREATE.
    helper
        .run_migration_test(
            &[
                "CREATE FUNCTION my_eq(integer, integer) RETURNS boolean \
                 AS $$ SELECT $1 = $2 $$ LANGUAGE sql IMMUTABLE",
                "CREATE FUNCTION my_eq2(integer, integer) RETURNS boolean \
                 AS $$ SELECT $1 = $2 $$ LANGUAGE sql IMMUTABLE",
            ],
            &["CREATE OPERATOR === (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_eq)"],
            &["CREATE OPERATOR === (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_eq2)"],
            |steps, final_catalog| {
                assert!(
                    steps.iter().any(|s| matches!(
                        s,
                        MigrationStep::Operator(OperatorOperation::Replace { .. })
                    )),
                    "a function change should produce an operator Replace step"
                );

                // Round-trip: the operator now points at the new function.
                assert_eq!(final_catalog.operators.len(), 1);
                assert!(
                    final_catalog.operators[0]
                        .definition
                        .contains("FUNCTION = public.my_eq2"),
                    "definition: {}",
                    final_catalog.operators[0].definition
                );
                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_operator_optional_clauses_round_trip() -> Result<()> {
    let helper = MigrationTestHelper::new().await;
    // A pair of equality/inequality operators exercising every optional clause:
    // COMMUTATOR, NEGATOR, RESTRICT, JOIN, HASHES, MERGES. integer operands are
    // used so HASHES/MERGES are valid (integer has hash + btree opclasses).
    helper
        .run_migration_test(
            &[],
            &[],
            &[
                "CREATE FUNCTION my_eq(integer, integer) RETURNS boolean \
                 AS $$ SELECT $1 = $2 $$ LANGUAGE sql IMMUTABLE",
                "CREATE FUNCTION my_ne(integer, integer) RETURNS boolean \
                 AS $$ SELECT $1 <> $2 $$ LANGUAGE sql IMMUTABLE",
                // Custom symbols (not the built-in =/<>) so the self-commutator
                // references the operator being created rather than a built-in.
                "CREATE OPERATOR === ( \
                    LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_eq, \
                    COMMUTATOR = ===, NEGATOR = !==, HASHES, MERGES, \
                    RESTRICT = eqsel, JOIN = eqjoinsel)",
                "CREATE OPERATOR !== ( \
                    LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_ne, \
                    COMMUTATOR = !==, NEGATOR = ===, \
                    RESTRICT = neqsel, JOIN = neqjoinsel)",
            ],
            |steps, final_catalog| {
                // The CREATE step for `===` renders every optional clause.
                let eq_create = steps
                    .iter()
                    .find_map(|s| match s {
                        MigrationStep::Operator(OperatorOperation::Create { operator })
                            if operator.name == "===" =>
                        {
                            Some(operator)
                        }
                        _ => None,
                    })
                    .expect("should have a Create step for the === operator");

                let def = &eq_create.definition;
                for clause in [
                    "FUNCTION = public.my_eq",
                    "COMMUTATOR = OPERATOR(public.===)",
                    "NEGATOR = OPERATOR(public.!==)",
                    "RESTRICT = eqsel",
                    "JOIN = eqjoinsel",
                    "HASHES",
                    "MERGES",
                ] {
                    assert!(def.contains(clause), "missing `{clause}` in: {def}");
                }

                // Round-trip applied cleanly and both operators came back.
                assert_eq!(final_catalog.operators.len(), 2);
                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_operator_in_custom_schema_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;
    helper
        .run_migration_test(
            &[],
            &[],
            &[
                "CREATE SCHEMA app",
                "CREATE FUNCTION app.my_eq(integer, integer) RETURNS boolean \
                 AS $$ SELECT $1 = $2 $$ LANGUAGE sql IMMUTABLE",
                "CREATE OPERATOR app.=== (LEFTARG = integer, RIGHTARG = integer, \
                 FUNCTION = app.my_eq)",
            ],
            |steps, final_catalog| {
                // The schema must be created before the operator that lives in it.
                let schema_pos = create_pos(steps, |s| matches!(s, MigrationStep::Schema(_)));
                let op_pos = create_pos(steps, |s| matches!(s, MigrationStep::Operator(_)));
                assert!(schema_pos < op_pos, "schema must precede its operator");

                // Round-trip: the operator is in `app` and renders schema-qualified.
                assert_eq!(final_catalog.operators.len(), 1);
                let op = &final_catalog.operators[0];
                assert_eq!(op.schema, "app");
                assert!(
                    op.definition.contains("CREATE OPERATOR app.==="),
                    "definition: {}",
                    op.definition
                );
                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_operator_cascades_on_type_change() -> Result<()> {
    let helper = MigrationTestHelper::new().await;
    let money_eq = "CREATE FUNCTION money_eq(money_amount, money_amount) RETURNS boolean \
                    AS $$ SELECT ($1).cents = ($2).cents $$ LANGUAGE sql IMMUTABLE";
    let money_op = "CREATE OPERATOR === (LEFTARG = money_amount, RIGHTARG = money_amount, \
                    FUNCTION = money_eq)";
    helper
        .run_migration_test(
            &[],
            &[
                "CREATE TYPE money_amount AS (cents bigint)",
                money_eq,
                money_op,
            ],
            &[
                "CREATE TYPE money_amount AS (cents numeric)",
                money_eq,
                money_op,
            ],
            |steps, final_catalog| {
                // The composite type is dropped+recreated; the operator depends on
                // it (directly and via its function) so it must cascade.
                assert!(
                    steps.iter().any(|s| matches!(
                        s,
                        MigrationStep::Operator(OperatorOperation::Drop { .. })
                    )),
                    "operator should be dropped when its operand type is recreated"
                );
                assert!(
                    steps.iter().any(|s| matches!(
                        s,
                        MigrationStep::Operator(OperatorOperation::Create { .. })
                    )),
                    "operator should be recreated after its operand type"
                );

                // Round-trip leaves exactly one operator backed by the new type.
                assert_eq!(final_catalog.operators.len(), 1);
                assert_eq!(final_catalog.types.len(), 1);
                Ok(())
            },
        )
        .await?;
    Ok(())
}
