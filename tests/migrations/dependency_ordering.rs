//! Ordering across kinds: an object whose definition *calls* something must be
//! created after it.
//!
//! A CHECK expression, a DEFAULT expression, an index expression or a view body
//! can name a function or an operator, and `pg_depend` is the only record of
//! that reference — nothing in the object's own row mentions it. When a
//! converter drops such an edge the plan still orders, it just orders wrong:
//! the topological sort falls back to the diff's emission order (schemas,
//! extensions, types, domains, sequences, tables, indexes, constraints,
//! triggers, policies, views, functions, operators), which puts every one of
//! these kinds ahead of the routines and operators they call. The migration
//! then fails on the target with `function ... does not exist`.
//!
//! Each case here applies its plan to a fresh database, so a missing edge is a
//! failure rather than a differently-shaped step list.

use crate::helpers::migration::MigrationTestHelper;
use anyhow::Result;

/// Plan the creation of `sql` from an empty database and apply the plan.
async fn plan_and_apply(sql: &[&str]) -> Result<()> {
    MigrationTestHelper::new()
        .await
        .run_migration_test(&[], &[], sql, |_steps, _catalog| Ok(()))
        .await
        .map(|_| ())
}

const PREDICATE: &str = "CREATE FUNCTION is_positive(n integer) RETURNS boolean IMMUTABLE LANGUAGE sql \
     AS $$ SELECT n > 0 $$";
const OPERATOR: &str = "CREATE OPERATOR === (leftarg = integer, rightarg = integer, \
     procedure = int4eq, commutator = ===)";

#[tokio::test]
async fn test_domain_check_calling_a_function_follows_it() -> Result<()> {
    plan_and_apply(&[
        PREDICATE,
        "CREATE DOMAIN positive AS integer CONSTRAINT is_positive CHECK (is_positive(VALUE))",
    ])
    .await
}

#[tokio::test]
async fn test_domain_default_calling_a_function_follows_it() -> Result<()> {
    plan_and_apply(&[
        "CREATE FUNCTION one() RETURNS integer IMMUTABLE LANGUAGE sql AS $$ SELECT 1 $$",
        "CREATE DOMAIN counter AS integer DEFAULT one()",
    ])
    .await
}

#[tokio::test]
async fn test_domain_check_using_an_operator_follows_it() -> Result<()> {
    plan_and_apply(&[
        OPERATOR,
        "CREATE DOMAIN self_equal AS integer CONSTRAINT reflexive CHECK (VALUE === VALUE)",
    ])
    .await
}

#[tokio::test]
async fn test_table_check_calling_a_function_follows_it() -> Result<()> {
    plan_and_apply(&[
        PREDICATE,
        "CREATE TABLE readings (value integer, CONSTRAINT value_positive CHECK (is_positive(value)))",
    ])
    .await
}

#[tokio::test]
async fn test_table_check_using_an_operator_follows_it() -> Result<()> {
    plan_and_apply(&[
        OPERATOR,
        "CREATE TABLE readings (value integer, CONSTRAINT reflexive CHECK (value === value))",
    ])
    .await
}

/// The reporter's shape: a composite type whose name begins with an underscore,
/// a function taking it, and a domain over it whose CHECK calls the function.
/// The leading underscore is incidental — it is a legal type name, not an array
/// marker — but it is how the case arrived.
#[tokio::test]
async fn test_domain_over_composite_with_check_calling_a_function() -> Result<()> {
    plan_and_apply(&[
        "CREATE TYPE _pair AS (first smallint, second smallint)",
        "CREATE FUNCTION is_valid_pair(raw _pair) RETURNS boolean IMMUTABLE LANGUAGE plpgsql \
         AS $$ BEGIN RETURN (raw).first > (raw).second; END; $$",
        "CREATE DOMAIN pair AS _pair CONSTRAINT valid_pair CHECK (is_valid_pair(VALUE))",
    ])
    .await
}

/// A table column typed by a domain whose CHECK calls a function: the chain has
/// to order function → domain → table.
#[tokio::test]
async fn test_column_of_a_domain_whose_check_calls_a_function() -> Result<()> {
    plan_and_apply(&[
        PREDICATE,
        "CREATE DOMAIN positive AS integer CONSTRAINT is_positive CHECK (is_positive(VALUE))",
        "CREATE TABLE readings (value positive)",
    ])
    .await
}

#[tokio::test]
async fn test_index_expression_calling_a_function_follows_it() -> Result<()> {
    plan_and_apply(&[
        "CREATE FUNCTION doubled(n integer) RETURNS integer IMMUTABLE LANGUAGE sql \
         AS $$ SELECT n * 2 $$",
        "CREATE TABLE readings (value integer)",
        "CREATE INDEX readings_doubled ON readings (doubled(value))",
    ])
    .await
}

#[tokio::test]
async fn test_index_predicate_calling_a_function_follows_it() -> Result<()> {
    plan_and_apply(&[
        PREDICATE,
        "CREATE TABLE readings (value integer)",
        "CREATE INDEX readings_positive ON readings (value) WHERE is_positive(value)",
    ])
    .await
}

#[tokio::test]
async fn test_column_default_calling_a_function_follows_it() -> Result<()> {
    plan_and_apply(&[
        "CREATE FUNCTION one() RETURNS integer IMMUTABLE LANGUAGE sql AS $$ SELECT 1 $$",
        "CREATE TABLE readings (value integer DEFAULT one())",
    ])
    .await
}

#[tokio::test]
async fn test_generated_column_calling_a_function_follows_it() -> Result<()> {
    plan_and_apply(&[
        "CREATE FUNCTION doubled(n integer) RETURNS integer IMMUTABLE LANGUAGE sql \
         AS $$ SELECT n * 2 $$",
        "CREATE TABLE readings (value integer, twice integer GENERATED ALWAYS AS (doubled(value)) STORED)",
    ])
    .await
}

#[tokio::test]
async fn test_view_calling_a_function_follows_it() -> Result<()> {
    plan_and_apply(&[
        PREDICATE,
        "CREATE VIEW checks AS SELECT is_positive(1) AS ok",
    ])
    .await
}

#[tokio::test]
async fn test_trigger_follows_its_function() -> Result<()> {
    plan_and_apply(&[
        "CREATE TABLE readings (value integer)",
        "CREATE FUNCTION touch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$",
        "CREATE TRIGGER readings_touch BEFORE INSERT ON readings FOR EACH ROW \
         EXECUTE FUNCTION touch()",
    ])
    .await
}

#[tokio::test]
async fn test_composite_attribute_of_a_domain_follows_it() -> Result<()> {
    plan_and_apply(&[
        "CREATE DOMAIN positive AS integer CHECK (VALUE > 0)",
        "CREATE TYPE measurement AS (amount positive)",
    ])
    .await
}

/// A trigger's WHEN condition can call a function that is not the trigger
/// function; `pg_trigger.tgfoid` names only the latter.
#[tokio::test]
async fn test_trigger_when_clause_follows_the_function_it_calls() -> Result<()> {
    plan_and_apply(&[
        PREDICATE,
        "CREATE TABLE readings (value integer)",
        "CREATE FUNCTION touch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$",
        "CREATE TRIGGER readings_touch BEFORE UPDATE ON readings FOR EACH ROW \
         WHEN (is_positive(OLD.value)) EXECUTE FUNCTION touch()",
    ])
    .await
}
