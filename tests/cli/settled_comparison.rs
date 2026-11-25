//! Postgres does not always store the same thing for SQL it rendered itself
//! as for the SQL that first created an object: an untyped literal in a later
//! UNION arm gains a column alias on re-parse, and a varchar `IN (...)` list
//! gains a cast on each element. The desired state is in the re-parsed form,
//! so a target built from hand-written SQL holds the other one.
//!
//! The two forms behave identically, so no command may report the difference:
//! not `migrate validate` or `migrate new` for hand-written migrations, and
//! not `diff`, `apply` or `migrate diff` for databases those migrations built.

use crate::helpers::cli::with_cli_helper;
use anyhow::Result;
use predicates::prelude::*;

const VIEW_SQL: &str = "CREATE VIEW public.events AS
SELECT * FROM (
    SELECT 'started'::text AS kind, NULL::uuid AS ref
    UNION ALL
    SELECT 'fell_back', NULL
) s;
";

const CHECK_SQL: &str = "CREATE TABLE public.jobs (status varchar(20) NOT NULL CHECK (status IN ('queued', 'done')));\n";

/// Hand-written SQL: the schema files, verbatim.
fn hand_written_sql() -> String {
    format!("{CHECK_SQL}{VIEW_SQL}")
}

fn write_schema(helper: &crate::helpers::cli::CliTestHelper) -> Result<()> {
    helper.write_schema_files(&[("jobs.sql", CHECK_SQL), ("events.sql", VIEW_SQL)])
}

async fn run_in_dev(helper: &crate::helpers::cli::CliTestHelper, sql: &str) -> Result<()> {
    let pool = helper.connect_to_dev_db().await?;
    sqlx::raw_sql(sqlx::AssertSqlSafe(sql.to_string()))
        .execute(&pool)
        .await?;
    pool.close().await;
    Ok(())
}

async fn run_hand_written_sql_in_dev(helper: &crate::helpers::cli::CliTestHelper) -> Result<()> {
    run_in_dev(helper, &hand_written_sql()).await
}

const JOBS_WITH_NOTE_SQL: &str = "CREATE TABLE public.jobs (status varchar(20) NOT NULL CHECK (status IN ('queued', 'done')), note text);\n";

#[tokio::test]
async fn test_migrate_validate_accepts_hand_written_migration() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        write_schema(helper)?;
        helper.write_migration_file("1000_initial.sql", &hand_written_sql())?;

        helper
            .command()
            .args(["migrate", "validate"])
            .assert()
            .success();

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_migrate_new_after_hand_written_migration_has_no_changes() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        write_schema(helper)?;
        helper.write_migration_file("1000_initial.sql", &hand_written_sql())?;

        helper
            .command()
            .args(["migrate", "new", "followup"])
            .assert()
            .success()
            .stdout(predicate::str::contains("No changes detected"));
        assert_eq!(helper.list_migration_files()?.len(), 1);

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_diff_accepts_database_built_from_hand_written_sql() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        write_schema(helper)?;
        run_hand_written_sql_in_dev(helper).await?;

        helper
            .command()
            .arg("diff")
            .assert()
            .success()
            .stdout(predicate::str::contains("No differences found"));

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_apply_to_database_built_from_hand_written_sql_has_no_changes() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        write_schema(helper)?;
        run_hand_written_sql_in_dev(helper).await?;

        helper
            .command()
            .args(["apply", "--force"])
            .assert()
            .success()
            .stdout(predicate::str::contains("No schema changes detected"));

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_migrate_diff_accepts_target_migrated_by_hand_written_migration() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        write_schema(helper)?;
        helper.write_migration_file("1000_initial.sql", &hand_written_sql())?;
        let target = helper.create_extra_database().await?;

        helper
            .command()
            .args(["migrate", "apply", "--target-url", &target])
            .assert()
            .success();
        helper
            .command()
            .args(["migrate", "diff", "--target-url", &target])
            .assert()
            .success();

        Ok(())
    })
    .await
}

/// Only the rendering is forgiven: a real change next to the hand-written
/// objects is still found, and the plan for it leaves them alone.
#[tokio::test]
async fn test_diff_finds_a_real_change_next_to_hand_written_sql() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        write_schema(helper)?;
        run_hand_written_sql_in_dev(helper).await?;
        helper.write_schema_file("jobs.sql", JOBS_WITH_NOTE_SQL)?;

        helper
            .command()
            .arg("diff")
            .assert()
            .code(1)
            .stdout(predicate::str::contains("note"))
            .stdout(predicate::str::contains("VIEW").not())
            .stdout(predicate::str::contains("jobs_status_check").not());

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_validate_accepts_database_built_from_hand_written_sql() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        write_schema(helper)?;
        run_hand_written_sql_in_dev(helper).await?;

        helper.command().arg("validate").assert().success();

        Ok(())
    })
    .await
}

/// The plan made from the settled target runs against the real one, and
/// leaves it matching the schema files.
#[tokio::test]
async fn test_apply_of_a_real_change_next_to_hand_written_sql_converges() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        write_schema(helper)?;
        run_hand_written_sql_in_dev(helper).await?;
        helper.write_schema_file("jobs.sql", JOBS_WITH_NOTE_SQL)?;

        helper
            .command()
            .args(["apply", "--force"])
            .assert()
            .success();
        helper
            .command()
            .arg("diff")
            .assert()
            .success()
            .stdout(predicate::str::contains("No differences found"));

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_migrate_new_writes_only_the_real_change_after_hand_written_migration() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        write_schema(helper)?;
        helper.write_migration_file("1000_initial.sql", &hand_written_sql())?;
        helper.write_schema_file("jobs.sql", JOBS_WITH_NOTE_SQL)?;

        helper
            .command()
            .args(["migrate", "new", "add_note"])
            .assert()
            .success();

        let generated = helper
            .list_migration_files()?
            .into_iter()
            .find(|f| f.contains("add_note"))
            .expect("migrate new should write a migration");
        let sql = helper.read_migration_file(&generated)?;
        assert!(sql.contains("note"), "{sql}");
        assert!(!sql.contains("CONSTRAINT"), "{sql}");
        assert!(!sql.contains("VIEW"), "{sql}");

        Ok(())
    })
    .await
}

/// The replay runs as the shadow's user, so every object comes back owned by
/// it. A target whose objects another role owns still settles.
#[tokio::test]
async fn test_diff_accepts_hand_written_objects_owned_by_another_role() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        write_schema(helper)?;
        run_hand_written_sql_in_dev(helper).await?;
        run_in_dev(
            helper,
            "DO $$ BEGIN CREATE ROLE test_settle_owner; EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$;
             ALTER TABLE public.jobs OWNER TO test_settle_owner;
             ALTER VIEW public.events OWNER TO test_settle_owner;",
        )
        .await?;

        helper
            .command()
            .arg("diff")
            .assert()
            .success()
            .stdout(predicate::str::contains("No differences found"))
            .stderr(predicate::str::contains("Could not rebuild").not());

        Ok(())
    })
    .await
}

/// A SQL function's body is checked when it is created, and its call to
/// another function is no catalog dependency, so only the file's `-- require:`
/// puts the callee first. The target's replay must honor it too.
#[tokio::test]
async fn test_target_replay_follows_require_order() -> Result<()> {
    const CALLEE_SQL: &str =
        "CREATE FUNCTION public.b_callee() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;\n";
    const CALLER_SQL: &str = "-- require: b_callee.sql\nCREATE FUNCTION public.a_caller() RETURNS int LANGUAGE sql AS $$ SELECT public.b_callee() $$;\n";

    with_cli_helper(async |helper| {
        helper.init_project()?;
        write_schema(helper)?;
        helper.write_schema_files(&[("b_callee.sql", CALLEE_SQL), ("a_caller.sql", CALLER_SQL)])?;
        run_hand_written_sql_in_dev(helper).await?;
        run_in_dev(helper, &format!("{CALLEE_SQL}{CALLER_SQL}")).await?;

        helper
            .command()
            .arg("diff")
            .assert()
            .success()
            .stdout(predicate::str::contains("No differences found"))
            .stderr(predicate::str::contains("Could not rebuild").not());

        Ok(())
    })
    .await
}
