---
title: Architecture
description: pgmt's internal module organization.
---

pgmt is organized into focused modules:

| Module           | Purpose                                       |
| ---------------- | --------------------------------------------- |
| `catalog/`       | Introspects PostgreSQL system tables          |
| `diff/`          | Compares catalogs to detect changes           |
| `schema_loader/` | Loads schema files with dependency resolution |
| `migration/`     | Generates, tracks, and applies migrations     |
| `commands/`      | CLI implementation                            |

## Catalog System (`src/catalog/`)

Reads database structure from PostgreSQL's system catalogs (`pg_class`, `pg_attribute`, `pg_depend`, etc.).

- Fetches all database objects (tables, views, functions, indexes, etc.)
- Tracks dependencies between objects
- Supports comments via `pg_description`
- Filters out system and extension-owned objects

**Key files:** `table.rs`, `view.rs`, `function.rs` for object-specific fetching; `id.rs` for dependency tracking.

## Diff System (`src/diff/`)

Compares two catalogs (old vs new) to determine what changed.

- Detects creates, drops, and modifications
- Generates `MigrationStep` operations in dependency order
- Classifies operations as safe vs destructive

**Key files:** `tables.rs`, `views.rs` for object diffing; `cascade.rs` for dependency-aware drops; `diff_order.rs` for topological sorting.

## Schema Loader (`src/schema_loader/`)

Loads `.sql` files from the schema directory.

- Parses `-- require:` dependencies
- Builds dependency graph and detects cycles
- Loads files in correct order

## Migration System (`src/migration/`)

Manages migration file lifecycle.

- Generates numbered migration files from diff operations
- Tracks applied migrations in `pgmt_migrations` table
- Validates checksums to detect modifications
- Supports multi-section migrations

**Key files:** `mod.rs` for file I/O; `baseline.rs` for baselines; `section_parser.rs` for multi-section parsing.

## Core Patterns

**Dependency tracking:** All objects implement `DependsOn` trait for topological sorting. Migrations create/drop objects in correct order.

**Shadow database:** Schema operations use a temporary shadow database to safely determine changes before modifying the dev database.

**Configuration:** Dual type system - `ConfigInput` (partial configs with `Option<T>`) and `Config` (resolved values) enables merging CLI args, YAML, and defaults.

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
