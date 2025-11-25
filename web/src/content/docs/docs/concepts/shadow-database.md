---
title: Shadow Database
description: How pgmt uses temporary databases to safely determine what schema changes are needed.
---

A **shadow database** is a temporary, isolated PostgreSQL environment that pgmt creates automatically to understand what your schema files represent as actual database objects.

The core challenge: you have schema files (text), and pgmt needs to know what database structure they define. The shadow database is how pgmt solves this.

## Why Shadow Databases Exist

You might wonder: why spin up a database at all? Why not just parse the SQL files?

**PostgreSQL SQL is complex.** Views can have CTEs, window functions, subqueries. Functions can be written in PL/pgSQL, SQL, or other languages. Indexes can use expressions. Types can be nested. Extensions add new syntax.

Rather than reimplementing PostgreSQL's SQL parser and dependency resolution (and inevitably getting edge cases wrong), pgmt lets PostgreSQL do what it does best: understand PostgreSQL SQL.

The shadow database is where this happens. pgmt applies your schema files to a fresh PostgreSQL instance, then reads the resulting catalog to see what objects were created.

## What This Enables

**Safe validation:** Your schema files might have syntax errors, circular dependencies, or invalid references. The shadow database catches these before they touch any real database.

**Accurate diffing:** pgmt doesn't try to parse your SQL files and guess what they mean. It actually runs them in PostgreSQL and reads the resulting catalog. This means it understands your schema exactly the way PostgreSQL does.

**Dependency enforcement:** PostgreSQL's own dependency rules are enforced when building the shadow database. If you try to create a view before its table, the shadow database build fails immediately with a clear error.

## The Lifecycle

Every time pgmt needs to understand your schema files:

1. **Spin up** - Create a fresh PostgreSQL environment (Docker container by default)
2. **Build** - Apply your schema files in dependency order
3. **Read** - Query PostgreSQL's system catalogs (`pg_class`, `pg_views`, `pg_proc`, etc.) to see what objects exist
4. **Use** - Compare this catalog against another database, or generate SQL
5. **Destroy** - Tear down the environment

The shadow database is ephemeral - it exists only for the duration of the operation. Every run starts with a completely clean slate.

**This means:** pgmt supports every PostgreSQL feature automatically. Custom aggregates, procedural languages, extensions, expression indexes - if PostgreSQL can create it, pgmt can understand it. There's no "supported features" list to maintain because pgmt delegates parsing to PostgreSQL itself.

## Configuration

By default, pgmt manages shadow databases automatically - it spins up a docker container, creates a temporary database, uses it, and cleans up. No configuration needed.

For advanced cases (specific PostgreSQL versions, existing databases, etc.), see the [Configuration Reference](../reference/configuration).

## Performance Impact

Creating a shadow database adds ~100-500ms overhead. For most schemas, total operation time is under a second. The safety benefits of validating changes before applying them far outweigh this small cost.

Shadow databases are schema-only - avoid `INSERT` statements in schema files as they slow down operations and aren't needed for schema management.

---

**Related Concepts:**

- [How pgmt Works](how-it-works) - The complete workflow
- [Dependency Tracking](dependency-tracking) - How pgmt orders schema files
- [Configuration](../reference/configuration) - Shadow database settings
