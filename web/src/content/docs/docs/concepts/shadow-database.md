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
[Configuration Reference](/docs/reference/configuration).

## Reading vs. Generating

The shadow database splits pgmt's job in two, and they have different limits.

**Reading your schema is PostgreSQL's job.** pgmt never parses your SQL, so it
can't misread a CTE, a PL/pgSQL body, an expression index, or syntax an
extension adds. If PostgreSQL accepts the file, pgmt sees exactly what
PostgreSQL built.

**Generating changes is pgmt's job.** To write a migration, pgmt has to know
how to create, alter, and drop each kind of object, in the right order. That
part is implemented object type by object type, which is why there's a
[Supported Features](/docs/reference/supported-features) list. Most PostgreSQL
objects are covered; the list says which aren't yet.

## Comparing Like with Like

PostgreSQL doesn't store your SQL as you wrote it. `CHECK (status IN ('a', 'b'))`
on a `varchar` column comes back as `= ANY (ARRAY[...])` with casts added, and
running that printed form through PostgreSQL again can store it slightly
differently once more. The rule is the same; the stored form isn't.

So pgmt doesn't compare against your files' first pass through PostgreSQL.
It runs its own generated SQL on a fresh shadow too, and compares results that
have both been through the same round trip. When a database or your migration
history differs from your schema files, the other side gets the same treatment.
Only real differences are reported, at the cost of an extra shadow build.

## Container Runtime Requirements

By default, pgmt provisions the shadow database as a throwaway container. This needs a container runtime with a Docker-compatible API — which is nearly all of them:

- **Docker** (Desktop or Engine) — works out of the box.
- **Podman** — expose its Docker-compatible API socket: `systemctl --user enable --now podman.socket`. pgmt checks the standard Podman socket locations automatically.
- **Colima, OrbStack, Rancher Desktop** and similar — all speak the Docker API; pgmt checks their common socket locations, and any of them can also be selected via `DOCKER_HOST`.
- **Remote or nonstandard setups** — set `DOCKER_HOST` to any Docker-compatible endpoint.

Note this is separate from wherever your _dev database_ runs. pgmt connects to your dev database directly over postgres — it doesn't need container access to it. The runtime requirement exists only so pgmt can create and destroy shadow instances on its own.

**Why a whole container, and not a database on a server you already have?** Some of what pgmt manages — roles in particular — is cluster-wide, not per-database. The shadow needs to be a pristine, disposable _cluster_ so that building your schema (roles and all) can't leak state into a server you care about. A container is the cheapest way to get one.

**No container runtime at all?** Set `shadow.url` to a database on a **dedicated, throwaway server** (a CI service container, say) and pgmt will use it as the shadow instead. Because of the cluster-wide state above, never point it at a server that hosts anything you care about — pgmt resets the shadow on every run. See the [Configuration Reference](/docs/reference/configuration).

## Configuration

By default, pgmt manages shadow databases automatically - it starts a container and works on a fresh, ephemeral branch of it for each operation, dropping the branch when done. No configuration needed.

For advanced cases (specific PostgreSQL versions, existing databases, etc.), see the [Configuration Reference](/docs/reference/configuration).

## Performance Impact

Most of the time goes to building your schema on fresh shadows, so it grows
with the size of your schema. A small schema is quick. On a 1,000-table schema,
a no-op `pgmt apply` takes around 7 seconds. Starting the container the first
time (and pulling the image) adds to the first run.

Shadow databases are schema-only - avoid `INSERT` statements in schema files as they slow down operations and aren't needed for schema management.

---

**Related Concepts:**

- [How pgmt Works](/docs/concepts/how-it-works) - The complete workflow
- [Dependency Tracking](/docs/concepts/dependency-tracking) - How pgmt orders schema files
- [Configuration](/docs/reference/configuration) - Shadow database settings
