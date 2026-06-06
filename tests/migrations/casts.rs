use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;
use pgmt::diff::operations::{CastOperation, MigrationStep, OperationKind};

const TEMP_TYPES: &str = "CREATE TYPE celsius AS (deg double precision); \
                          CREATE TYPE fahrenheit AS (deg double precision)";
const C_TO_F_FN: &str = "CREATE FUNCTION c_to_f(celsius) RETURNS fahrenheit \
                         AS $$ SELECT ROW(($1).deg * 9.0 / 5.0 + 32.0)::fahrenheit $$ \
                         LANGUAGE sql IMMUTABLE";
const C_TO_F_CAST: &str = "CREATE CAST (celsius AS fahrenheit) WITH FUNCTION c_to_f(celsius)";

/// Index of the first step matching `pred` whose operation is a CREATE.
fn create_pos<F: Fn(&MigrationStep) -> bool>(steps: &[MigrationStep], pred: F) -> usize {
    steps
        .iter()
        .position(|s| pred(s) && s.operation_kind() == OperationKind::Create)
        .expect("expected a matching create step")
}

#[tokio::test]
async fn test_create_cast_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;
    helper
        .run_migration_test(
            &[TEMP_TYPES, C_TO_F_FN],
            &[],
            &[C_TO_F_CAST],
            |steps, final_catalog| {
                let create = steps
                    .iter()
                    .find(|s| matches!(s, MigrationStep::Cast(CastOperation::Create { .. })))
                    .expect("should have a cast Create step");
                match create {
                    MigrationStep::Cast(CastOperation::Create { cast }) => {
                        assert_eq!(cast.source, "celsius");
                        assert_eq!(cast.target, "fahrenheit");
                    }
                    _ => unreachable!(),
                }

                assert_eq!(final_catalog.casts.len(), 1);
                assert_eq!(final_catalog.casts[0].source, "celsius");
                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_drop_cast_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;
    helper
        .run_migration_test(
            &[TEMP_TYPES, C_TO_F_FN],
            &[C_TO_F_CAST],
            &[],
            |steps, final_catalog| {
                let drop = steps
                    .iter()
                    .find(|s| matches!(s, MigrationStep::Cast(CastOperation::Drop { .. })))
                    .expect("should have a cast Drop step");
                match drop {
                    MigrationStep::Cast(CastOperation::Drop { identifier }) => {
                        assert_eq!(identifier.source, "celsius");
                        assert_eq!(identifier.target, "fahrenheit");
                    }
                    _ => unreachable!(),
                }
                assert!(final_catalog.casts.is_empty());
                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_cast_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;
    helper
        .run_migration_test(
            &[TEMP_TYPES, C_TO_F_FN, C_TO_F_CAST],
            &[],
            &["COMMENT ON CAST (celsius AS fahrenheit) IS 'temperature conversion'"],
            |steps, final_catalog| {
                assert!(
                    steps
                        .iter()
                        .any(|s| matches!(s, MigrationStep::Cast(CastOperation::Comment(_)))),
                    "should have a cast Comment step"
                );
                assert_eq!(
                    final_catalog.casts[0].comment,
                    Some("temperature conversion".to_string())
                );
                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_drop_cast_comment_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;
    helper
        .run_migration_test(
            &[TEMP_TYPES, C_TO_F_FN, C_TO_F_CAST],
            &["COMMENT ON CAST (celsius AS fahrenheit) IS 'temperature conversion'"],
            &[],
            |steps, final_catalog| {
                assert!(
                    steps
                        .iter()
                        .any(|s| matches!(s, MigrationStep::Cast(CastOperation::Comment(_)))),
                    "should have a cast Comment (drop) step"
                );
                assert_eq!(final_catalog.casts[0].comment, None);
                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_cast_ordered_after_types_and_function() -> Result<()> {
    let helper = MigrationTestHelper::new().await;
    helper
        .run_migration_test(
            &[],
            &[],
            &[TEMP_TYPES, C_TO_F_FN, C_TO_F_CAST],
            |steps, _final_catalog| {
                let cast_pos = create_pos(steps, |s| matches!(s, MigrationStep::Cast(_)));
                let fn_pos = create_pos(steps, |s| matches!(s, MigrationStep::Function(_)));

                // Cast comes after both temperature type creates...
                let last_type_pos = steps
                    .iter()
                    .enumerate()
                    .filter(|(_, s)| {
                        matches!(s, MigrationStep::Type(_))
                            && s.operation_kind() == OperationKind::Create
                    })
                    .map(|(i, _)| i)
                    .max()
                    .expect("expected type create steps");

                assert!(cast_pos > last_type_pos, "cast must follow its types");
                assert!(cast_pos > fn_pos, "cast must follow its function");
                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_cast_assignment_context_round_trip() -> Result<()> {
    let helper = MigrationTestHelper::new().await;
    helper
        .run_migration_test(
            &["CREATE TYPE celsius AS (deg double precision)"],
            &[],
            &["CREATE CAST (celsius AS text) WITH INOUT AS ASSIGNMENT"],
            |steps, final_catalog| {
                let create = steps
                    .iter()
                    .find_map(|s| match s {
                        MigrationStep::Cast(CastOperation::Create { cast }) => Some(cast),
                        _ => None,
                    })
                    .expect("should have a cast Create step");
                assert!(create.definition.contains("WITH INOUT"));
                assert!(create.definition.contains("AS ASSIGNMENT"));

                // Round-trip preserved the I/O method and assignment context.
                assert_eq!(final_catalog.casts.len(), 1);
                let cast = &final_catalog.casts[0];
                assert!(cast.definition.contains("WITH INOUT"));
                assert!(cast.definition.contains("AS ASSIGNMENT"));
                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_replace_cast_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;
    // The cast gains AS ASSIGNMENT. There is no ALTER CAST, so this is DROP+CREATE.
    helper
        .run_migration_test(
            &[TEMP_TYPES, C_TO_F_FN],
            &[C_TO_F_CAST],
            &["CREATE CAST (celsius AS fahrenheit) WITH FUNCTION c_to_f(celsius) AS ASSIGNMENT"],
            |steps, final_catalog| {
                assert!(
                    steps
                        .iter()
                        .any(|s| matches!(s, MigrationStep::Cast(CastOperation::Replace { .. }))),
                    "a context change should produce a cast Replace step"
                );
                assert_eq!(final_catalog.casts.len(), 1);
                assert!(
                    final_catalog.casts[0].definition.contains("AS ASSIGNMENT"),
                    "definition: {}",
                    final_catalog.casts[0].definition
                );
                Ok(())
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_cast_cascades_on_type_change() -> Result<()> {
    let helper = MigrationTestHelper::new().await;
    helper
        .run_migration_test(
            &[],
            &[
                "CREATE TYPE celsius AS (deg double precision)",
                "CREATE TYPE fahrenheit AS (deg double precision)",
                C_TO_F_FN,
                C_TO_F_CAST,
            ],
            &[
                // celsius changes its attribute type → drop + recreate, cascading
                // the conversion function and the cast that depend on it.
                "CREATE TYPE celsius AS (deg real)",
                "CREATE TYPE fahrenheit AS (deg double precision)",
                C_TO_F_FN,
                C_TO_F_CAST,
            ],
            |steps, final_catalog| {
                assert!(
                    steps
                        .iter()
                        .any(|s| matches!(s, MigrationStep::Cast(CastOperation::Drop { .. }))),
                    "cast should be dropped when its source type is recreated"
                );
                assert!(
                    steps
                        .iter()
                        .any(|s| matches!(s, MigrationStep::Cast(CastOperation::Create { .. }))),
                    "cast should be recreated after its source type"
                );
                assert_eq!(final_catalog.casts.len(), 1);
                assert_eq!(final_catalog.types.len(), 2);
                Ok(())
            },
        )
        .await?;
    Ok(())
}
