# Changelog

All notable, user-facing changes to pgmt are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Features

- Add `pgmt migrate provision` to stand up a new database from a baseline plus its post-baseline migrations — the on-ramp for fresh environments (demo, staging, preview, disaster recovery), where `migrate apply` only maintains a database that's already established.

## 0.5.1 - 2026-06-24

### Bug Fixes

- Order same-object ALTER steps by emission order
- Reclaim shadow container volumes on teardown

## 0.5.0 - 2026-06-21

### Breaking Changes

- Connection settings now resolve with a single precedence — CLI flag > `PGMT_*` environment variable > `pgmt.yaml` — and the environment variables were renamed: `DEV_DATABASE_URL` → `PGMT_DEV_URL` and `TARGET_DATABASE_URL` → `PGMT_TARGET_URL`. The old names are no longer read.
- A dev database URL is now required. The implicit `postgres://localhost/pgmt_dev` default was removed; pgmt now errors and lists the available sources if none is set.
- Connection flags are now per-command — each command exposes only the URLs it actually uses (for example, `--target-url` only on commands that touch the target). The no-op `pgmt apply --target-url` was removed.

### Features

- Shadow databases are now ephemeral branches of an untouched source. pgmt copies the source database with `CREATE DATABASE ... TEMPLATE` for each run and drops the copy on exit, so the database you point it at is read-only and never modified. External shadows (`shadow.url`) gain `reset: clean | branch`.
- Detect image-provided substrate schemas (PostGIS's `tiger`/`topology`, Supabase's `auth`/`storage`, custom init scripts) during `init` import and offer to exclude them from management.
- Support identity columns (`GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY`).
- Custom shadow image and platform support in `init` via `--shadow-image`, `--shadow-platform`, and `--shadow-url`, including extension-availability detection for nonstandard images.
- Track index dependencies on extensions — operator classes (e.g. `gin_trgm_ops`) and extension-owned functions used in index expressions now pull in the owning extension.
- `init` pre-fills prompts from an existing `pgmt.yaml` on re-init and confirms before resetting an external shadow database.

### Bug Fixes

- Track all operators in exclusion constraints ([#9](https://github.com/gdpotter/pgmt/pull/9))
- Re-state the full ACL for objects recreated via DROP+CREATE, so grants are no longer silently lost on recreate
- Drop the old primary key before column changes in table alters ([#6](https://github.com/gdpotter/pgmt/issues/6))
- Accept the documented `exclude.schemas` / `exclude.tables` config keys, which were previously ignored in favor of `exclude_schemas` / `exclude_tables` (both spellings now work)
- Exclude sub-objects (constraints, indexes, triggers, policies) of extension-owned tables from the catalog, fixing spurious steps and validation failures on PostGIS databases
- Mirror that extension sub-object filtering in the apply-time identity snapshot, fixing a flood of dependency warnings on PostGIS databases

### Performance

- Poll shadow database readiness every 250ms instead of every 2s, cutting shadow cold-start latency

## 0.4.9 - 2026-06-07

### Features

- Fold per-column grants on a relation into single statements
- Emit only privilege deltas when diffing existing grants
- Order function-based casts before the views that use them
- Support custom casts
- Support custom operators
- Support column-level grants
- Sequence drops before creates for same-name objects across types

## 0.4.8 - 2026-05-17

### Features

- Handle comments on view columns and composite type attributes

## 0.4.7 - 2026-03-29

### Features

- Move baseline under migrate, clean migrations by default

## 0.4.6 - 2026-03-19

### Features

- Add cascade support for domain, index, sequence, and aggregate

### Bug Fixes

- Preserve column order in ADD COLUMN statements
- Include security options in CREATE OR REPLACE VIEW
- Cascade drop/recreate for dependent custom types

## 0.4.5 - 2026-02-17

### Bug Fixes

- Track object-level dependencies for RLS policies
- Migrate commands now respect configured directory paths

## 0.4.4 - 2026-02-16

### Features

- Drop V prefix from migration filenames, make prefix configurable

### Bug Fixes

- Improve database connection errors and standardize apply logging
- Improve shadow database error handling and container lifecycle

## 0.4.3 - 2026-02-12

### Features

- Rework website and README for better first impressions
- Concise default output for apply command
- Add blog with first post

### Bug Fixes

- Order FK constraint comments after constraint creation
- Cascade dependents when function signature changes

## 0.4.2 - 2026-02-09

### Features

- Add Supabase and managed platform support

## 0.4.1 - 2026-02-03

### Features

- Add column order validation to migrate validate
- Add column-level dependency tracking for RLS policies
- Add column order validation for migrate new
- Add column-level dependency tracking for BEGIN ATOMIC functions

### Bug Fixes

- Consolidate type dependency resolution and fix composite type handling
- Show PostgreSQL DETAIL field in migration error messages

## 0.4.0 - 2026-01-20

### Features

- Improve apply command for CI/CD and non-interactive use

### Bug Fixes

- Cascade dependent objects before ALTER COLUMN TYPE
- Improve array type handling consistency across catalog types
- Preserve array brackets in DROP FUNCTION parameter types
- Track table/view composite types correctly in function dependencies
- Ensure constraint comments depend on parent table creation

## 0.3.0 - 2025-12-21

### Features

- Add view security options support
- Add Row-Level Security support
- Add comment support for primary key constraints

### Bug Fixes

- Include RLS settings when creating new tables
