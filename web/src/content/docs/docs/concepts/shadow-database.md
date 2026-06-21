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

1. **Spin up** - Get a fresh PostgreSQL database (by default, an ephemeral branch of a Docker container)
2. **Build** - Apply your schema files in dependency order
3. **Read** - Query PostgreSQL's system catalogs (`pg_class`, `pg_views`, `pg_proc`, etc.) to see what objects exist
4. **Use** - Compare this catalog against another database, or generate SQL
5. **Destroy** - Drop the ephemeral branch

The shadow database is ephemeral - it exists only for the duration of the operation. Every run starts with a completely clean slate.

**How the clean slate works:** for Docker-managed shadows, the container's
freshly-initialized database is treated as a read-only pristine source, and
every operation works on an ephemeral branch of it (`CREATE DATABASE ...
TEMPLATE` - a fast file-level copy). The branch can't miss any state, always
starts from the current baseline, and inherits whatever the image provides
(PostGIS's `topology` schema, Supabase's `auth`/`storage`, custom init
scripts). For external `shadow.url` databases, pgmt instead drops the schemas
it manages - it won't create or drop databases on a server it doesn't own,
since that database's lifecycle may belong to CI or other orchestration. If
the database does exist solely for pgmt (a CI service container, say), set
`reset: branch` to opt into the same branching semantics - see the
[Configuration Reference](../reference/configuration).

**This means:** pgmt supports every PostgreSQL feature automatically. Custom aggregates, procedural languages, extensions, expression indexes - if PostgreSQL can create it, pgmt can understand it. There's no "supported features" list to maintain because pgmt delegates parsing to PostgreSQL itself.

## Configuration

By default, pgmt manages shadow databases automatically - it starts a Docker container and works on a fresh, ephemeral branch of it for each operation, dropping the branch when done. No configuration needed.

For advanced cases (specific PostgreSQL versions, existing databases, etc.), see the [Configuration Reference](../reference/configuration).

## Performance Impact

Creating a shadow database adds ~100-500ms overhead. For most schemas, total operation time is under a second. The safety benefits of validating changes before applying them far outweigh this small cost.

Shadow databases are schema-only - avoid `INSERT` statements in schema files as they slow down operations and aren't needed for schema management.

---

**Related Concepts:**

- [How pgmt Works](how-it-works) - The complete workflow
- [Dependency Tracking](dependency-tracking) - How pgmt orders schema files
- [Configuration](../reference/configuration) - Shadow database settings
