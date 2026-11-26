---
title: Architecture
description: pgmt's internal module organization.
---

The main modules under `src/`:

| Module                | Purpose                                                         |
| --------------------- | --------------------------------------------------------------- |
| `catalog/`            | Introspects PostgreSQL system tables                            |
| `diff/`               | Compares catalogs and orders the resulting steps                |
| `render/`             | Turns diff steps into SQL                                       |
| `schema_loader.rs`    | Loads schema files with dependency resolution                   |
| `schema_ops.rs`       | Builds the desired state from schema files on a shadow database |
| `migrate/`            | Generates migration files                                       |
| `migration/`          | Parses migration files, sections, and baselines                 |
| `migration_tracking/` | Records applied migrations and sections, and the deploy lock    |
| `modules/`            | Module partitioning, sectioning, and apply-time module handling |
| `db/`                 | Connections, shadow branching, and executing SQL                |
| `docker/`             | Shadow database containers                                      |
| `config/`             | `pgmt.yaml` loading and connection resolution                   |
| `commands/`           | CLI implementation                                              |

## Catalog System (`src/catalog/`)

Reads database structure from PostgreSQL's system catalogs (`pg_class`, `pg_attribute`, `pg_depend`, etc.) in two layers: `catalog/raw/` fetches OID-keyed rows and converts them into the logical structs in `catalog/`, whose identity is name-based so two databases holding the same schema compare equal.

- Fetches all database objects (tables, views, functions, indexes, etc.)
- Tracks dependencies between objects
- Attaches comments from `pg_description` through an OID → identity index
- Excludes system and extension-owned objects, each with a named reason

**Key files:** `raw/` for fetching and conversion (one module per object kind, plus `shared.rs` for the state every kind needs); `id.rs` for dependency tracking.

## Diff System (`src/diff/`)

Compares two catalogs (old vs new) to determine what changed.

- Detects creates, drops, and modifications
- Generates `MigrationStep` operations in dependency order
- Classifies operations as safe vs destructive

**Key files:** `tables.rs`, `views.rs` for object diffing; `cascade.rs` for dependency-aware drops; `diff_order.rs` for topological sorting.

## Schema Loader (`src/schema_loader.rs`)

Loads `.sql` files from the schema directory.

- Parses `-- require:` dependencies
- Builds dependency graph and detects cycles
- Loads files in correct order

## Migration System (`src/migrate/`, `src/migration/`, `src/migration_tracking/`)

Manages the migration file lifecycle.

- Generates timestamped migration files from diff operations (`migrate/generation.rs`)
- Parses sections and baselines (`migration/section_parser.rs`, `migration/baseline.rs`)
- Tracks applied migrations in the `pgmt_migrations` table, with per-section status and checksums (`migration_tracking/section_tracking.rs`)
- Serializes concurrent deploys behind a Postgres advisory lock (`migration_tracking/advisory_lock.rs`)
- On module projects, tags sections with their module and records which modules each target holds (`src/modules/`)

## Core Patterns

**Dependency tracking:** All objects implement `DependsOn` trait for topological sorting. Migrations create/drop objects in correct order.

**Shadow database:** Schema operations use a temporary shadow database to safely determine changes before modifying the dev database.

**Configuration:** Dual type system - `ConfigInput` (partial configs with `Option<T>`) and `Config` (resolved values) enables merging CLI args, YAML, and defaults.

## One Engine, Many Uses

Every command is a schema diff: introspect two states, compare them, render
the steps. To keep commands from drifting apart, each stage of that sentence
has exactly one implementation, and commands compose them rather than
re-implementing any part:

| Stage                                         | Single entry point                                                         |
| --------------------------------------------- | -------------------------------------------------------------------------- |
| What counts as "managed"                      | `ObjectFilter::from_config` (`src/config/filter.rs`)                       |
| What the schema files describe                | `schema_ops::build_desired_state`                                          |
| What history (baseline + migrations) produces | `migration::baseline::get_migration_starting_state` (section-aware replay) |
| The diff itself                               | `diff::plan` (diff → cascade expansion → topological order)                |

So `apply` is `plan(dev, desired)`, `migrate diff` is `plan(target, desired)`,
`migrate new` is `plan(history, desired)`, `migrate baseline` and schema-file
generation are `plan(empty, desired)`, and `migrate validate` checks that
`plan(history, desired)` is empty. A behavior change in any stage reaches
every command at once; a command bypassing these entry points is a bug.

Database connections follow the same philosophy (`src/config/connections.rs`):
each database (dev, shadow, target) has a typed value obtainable only through
its CLI args struct's `resolve()`, which encodes the flag > `PGMT_*` env >
pgmt.yaml precedence. The resolved `Config` carries no connection strings, so
a command that connects to a database must declare the matching flag in its
clap surface — `--help` is a compile-time-accurate list of which databases
each command touches.

## Operation Classification

Migration operations have two separate classifications that are sometimes conflated:

**OperationKind** (`Create`, `Drop`, `Alter`) — Used for ordering migrations. Drops must happen before creates for the same object type. Defined in `src/diff/operations/mod.rs`.

**Safety** (`Safe`, `Destructive`) — Used for warnings and execution modes. Indicates risk of data loss. Defined in `src/render/mod.rs` as part of `RenderedSql`.

These are separate concerns. A `Drop` operation can be `Safe`:

- `DROP FUNCTION` is `OperationKind::Drop` but `Safety::Safe` (can be recreated)
- `DROP TABLE` is `OperationKind::Drop` AND `Safety::Destructive` (loses data)

## Cascade Mechanism

When a column type changes, PostgreSQL blocks `ALTER COLUMN TYPE` if dependent objects (views, functions) reference that column. pgmt handles this by synthesizing DROP and CREATE operations for affected objects.

The `cascade::expand()` function in `src/diff/cascade.rs`:

1. Detects column type changes that would fail
2. Finds all dependent views/functions via the dependency graph
3. Synthesizes DROP operations (before the ALTER)
4. Synthesizes CREATE operations (after the ALTER)
5. Re-applies grants since DROP implicitly revokes them

This is implemented via `Catalog::synthesize_drop_create()` which uses existing diff functions to generate the operations.
