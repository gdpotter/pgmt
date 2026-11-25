# Changelog

All notable, user-facing changes to pgmt are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## 0.6.7 - 2026-11-25

### Bug Fixes

- `pgmt migrate validate` accepts migrations whose SQL was written by hand again, and `pgmt diff`, `pgmt apply`, `pgmt validate`, and `pgmt migrate diff` no longer report a change on a database built from hand-written SQL. Since 0.6.6 compares against what Postgres stores for pgmt's own generated SQL ([#21](https://github.com/gdpotter/pgmt/issues/21)), an object created from other SQL could differ only in how Postgres prints it, such as a `CHECK (status IN (...))` on a `varchar` column without a cast on each element, or a later `UNION` arm without a column alias. Those objects were reported as changed, and `pgmt migrate new` generated a migration to replace them, dropping and re-adding a CHECK constraint (which revalidates the table). When a database or the result of your migrations differs from your schema files, pgmt now also runs its generated SQL for that side on a fresh shadow database and compares the two results, so only real differences are reported. This costs another shadow database run when there are differences, and nothing when there are none.

## 0.6.6 - 2026-11-25

### Bug Fixes

- `pgmt apply --watch` keeps watching when the initial apply fails, so a broken schema file no longer means restarting it by hand until the schema applies ([#24](https://github.com/gdpotter/pgmt/pull/24)). A failed apply now shows the Postgres error behind it ([#22](https://github.com/gdpotter/pgmt/pull/22)): watch mode logged only the outermost message, so a schema file that failed to apply showed a generic list of causes rather than the database error, and nothing at all when `RUST_LOG` filtered pgmt's logs. A save made while the initial apply runs is also no longer missed.
- `pgmt apply --watch` no longer loses a save made while an apply is running. Changes that arrived during an apply were ignored, leaving the dev database behind your schema files with nothing on screen to say so. Watch mode now waits until files stop changing for half a second, applies once, and applies again for anything saved in the meantime. A failed apply also no longer turns one save into several full applies, one per file event the editor emitted.
- `pgmt apply --watch` no longer stops watching when an apply fails with an error that happens to contain "cancelled" or "interrupted", such as a statement comparing against a `'cancelled'` status value. Watch mode now stops only on Ctrl+C.
- A schema file that fails to apply is no longer reported under a generic "Common causes" list and "Troubleshooting tips" ([#25](https://github.com/gdpotter/pgmt/issues/25)). The list was attached to every failure, so an error that named the exact file and line came with suggestions like "Schema directory doesn't exist" and "Circular dependencies", and the tips mostly repeated what Postgres had just said. The error now leads with the file, line, and Postgres message, and a missing `-- require:` file or a dependency cycle is reported in one line. An undefined object still points you at `-- require:` headers, and an object that already exists at a likely duplicate definition, since those say something Postgres can't.
- `pgmt apply`, `pgmt diff`, and `pgmt migrate new` no longer report the same change again after it has been applied ([#21](https://github.com/gdpotter/pgmt/issues/21)). Postgres does not always store a statement the same way when it parses its own output back in: a `CHECK (status IN (...))` on a `varchar` column comes back with its casts moved, and an untyped `NULL` in a later `UNION` arm gains a column alias. pgmt compared targets against your schema files as Postgres first stored them, not against what pgmt's generated SQL produces, so some objects never matched. Every `pgmt apply` and every new migration changed them again: a view replaced, a CHECK or domain constraint dropped and re-added (which revalidates the table), a partial index rebuilt, a policy altered, a trigger recreated. pgmt now runs its own generated SQL on a second fresh shadow database and compares against the result, which makes these commands slower, and generated SQL now writes these expressions the way Postgres stores them. If the generated SQL cannot build your schema from scratch, the command fails and names the statement, instead of the problem going unnoticed until a baseline or a fresh database hits it.
- A foreign key to columns covered by a `UNIQUE` constraint or a unique index, rather than the primary key, no longer breaks building the schema from scratch. The foreign key could be ordered before the key it references and fail with `there is no unique constraint matching given keys`. This broke `pgmt apply` to an empty database, migrations that create both tables, and `pgmt migrate provision` from a baseline.
- An extension created with its comment removed (`COMMENT ON EXTENSION ... IS NULL`) no longer keeps the comment from its control file. `CREATE EXTENSION` attaches that comment and pgmt emitted nothing to clear it. An extension that keeps its default comment no longer gets a redundant `COMMENT ON EXTENSION`.
- An exclusion constraint with a `WHERE` clause (`EXCLUDE USING gist (...) WHERE (...)`) is now generated with the parentheses Postgres requires around its predicate. Without them, the `ADD CONSTRAINT` failed with `syntax error at or near ...`, so `pgmt apply`, migrations, and baselines that created a partial exclusion constraint could not run ([#20](https://github.com/gdpotter/pgmt/pull/20)).
- A migration that adds an enum value and uses it now applies. Postgres refuses to use a new enum value before the transaction that added it commits (`unsafe use of new value`), and `pgmt migrate new` put the `ALTER TYPE ... ADD VALUE` and everything using the value — a column default, a CHECK constraint, a partial index, a view — in the same transaction. It now commits in between, writing the migration as `-- pgmt:section` blocks when it has to.
- Changing a column's default from one value to another now generates `ALTER COLUMN ... SET DEFAULT`. pgmt only noticed a default being added or removed, so `pgmt apply` and new migrations left a changed default at its old value.
- Enum labels containing a single quote are now escaped in generated `CREATE TYPE` and `ALTER TYPE ... ADD VALUE` statements, which failed with a syntax error before.
- The `schema.verbose_file_processing` setting is removed. It was documented as printing per-file progress while loading schema files, but it never did anything; `--verbose` already reports that progress. A `pgmt.yaml` that still sets it loads as before.

### Features

- `pgmt apply --watch` ends each failed apply with a one-line status naming the file, line, and Postgres error, such as `blog/index.sql:5: UNION types jsonb and text cannot be matched`, so the cause stays visible when the full error is taller than your terminal ([#25](https://github.com/gdpotter/pgmt/issues/25)). Changed files are now shown relative to the project instead of as absolute paths.

### Performance

- `pgmt apply` sends its changes to the database 500 statements per round trip instead of one at a time, and runs each section of the plan as one transaction. Applying a 1,000-table schema to an empty database spent about 9.5 seconds sending statements and now spends under 3, and the saving is larger against a remote database, where each round trip costs more. A failed `pgmt apply` now rolls back the section it was running instead of leaving every statement before the failure applied, and the error still names the statement that failed.
- Postgres no longer JIT-compiles the queries pgmt uses to read a database's schema. On a large schema they cost enough to trigger JIT, and compiling them took about 40% of each read; `pgmt apply` reads three per run. A no-op `pgmt apply` of a 1,000-table schema went from about 8.4 to 7.2 seconds.
- `pgmt apply`, `pgmt diff`, and `pgmt migrate diff` read the dev or target database while the schema files are applied to the shadow database, instead of waiting for them to finish.

## 0.6.5 - 2026-09-22

### Bug Fixes

- Baselines written by `pgmt migrate new --create-baseline` and `pgmt migrate update` no longer re-create the objects a shadow database image provides. On an image that ships extensions or platform schemas (PostGIS, Supabase), those were written into every baseline — while the migration generated by the same command correctly left them out — so provisioning a fresh target built from that image collided with objects already installed, and since a baseline applies atomically, the whole file rolled back. Substrate in a schema your `objects` config excludes was already filtered out, so this changes the generated baseline only where an image installs into a managed schema, PostGIS in `public` being the usual case.
- `pgmt migrate update --dry-run` no longer writes the files it claims to be previewing. Without `--migration-version` the flag did nothing beyond printing a banner, so a preview rewrote the migration and regenerated its paired baseline; with a version it spared the migration file but still regenerated the baseline, and both finished by reporting "No changes were made." A dry run now leaves the migration, the baseline, and `--backup`'s `.sql.bak` untouched, and reports the paths it would have written.
- `pgmt migrate baseline --dry-run` now previews instead of running when it is combined with `--keep-migrations`. The preview only ran when `--keep-migrations` was absent, so passing the two flags together skipped it and wrote the baseline for real.
- A failed `pgmt migrate new`, `pgmt migrate update`, or `pgmt migrate baseline` no longer leaves its ephemeral shadow branch database behind. The branch was reclaimed only when the phase succeeded, so a run that ended in an error — a schema file that fails to apply, a baseline that fails validation — leaked one database per attempt.
- `pgmt init --defaults` is now fully non-interactive. Re-running it in an initialized project updates the existing `pgmt.yaml` instead of reaching a prompt, which failed with `IO error: not a terminal` from a script or CI. Without `--defaults`, an existing `pgmt.yaml` with no terminal attached now fails with a message naming `--defaults` and `--fresh`.
- The `pgmt init` success banner no longer claims the schema directory contains "modular files" when nothing was imported. A `--defaults` or `--no-import` run leaves the directory empty, and the banner now says so and how to import.

### Features

- `pgmt migrate new --empty` writes a stub migration to fill in by hand, for SQL a schema diff cannot produce (data backfills, `CREATE INDEX CONCURRENTLY`). It skips the diff entirely, so it needs no shadow database and leaves pending schema changes for the next ordinary `migrate new`.
- A section with no SQL now applies as a no-op instead of failing validation. Empty sections were rejected at apply time, so an unfilled migration broke a deploy rather than a local run — and any comment in the file defeated the check anyway.
- `pgmt migrate apply --dry-run` previews the pending migrations, the sections each would run, and the module sections it would skip, without touching the target — matching `migrate provision --dry-run`. The first-contact and partial-baseline guards still run, so a preview surfaces the same refusals a real apply would.

## 0.6.4 - 2026-08-27

### Bug Fixes

- Log records now go to stderr instead of stdout. `pgmt migrate diff --format json > drift.json` and `--format sql | psql` could be corrupted by a log line interleaved into the payload. The line that usually did it was a slow-statement warning, which fires only when a statement is slow — so the failure appeared under CI load and never in development.
- `pgmt migrate diff --format json` names the changed object in three stable fields — `kind`, `schema`, `name` — plus a readable `object`, instead of a single `type` field carrying Rust's internal `Debug` rendering (`Table { schema: "public", name: "partners" }`). `pgmt diff --format detailed` shows the readable form too.
- `pgmt migrate diff` no longer reports pgmt's own `{tracking_table}_modules` table as drift. `migrate apply` creates it on every target, module project or not, and only the `_sections` companion was being filtered out.
- `--output-sql` writes the file whatever `--format` selects. It previously did nothing unless `--format sql` was also given, so `--format json --output-sql fix.sql` silently produced no file. Its confirmation message moved to stderr so it cannot corrupt the chosen format's output.
- Generated migrations no longer emit SQL statements without a trailing semicolon. `DROP TRIGGER`, `DROP CAST`, `DROP AGGREGATE`, `DROP OPERATOR`, and `CREATE TYPE ... AS RANGE` ran into the statement that followed them, so any migration containing one failed to parse. Modifying a trigger, cast, or operator was affected as well as dropping it, since those are rendered as a drop followed by a create. `pgmt diff --format sql` also stops doubling the semicolon on statements that already had one.

### Performance

- Loading the catalog no longer renders definitions for routines an extension owns. `pg_get_functiondef` and its siblings were called for every function in the database and the result discarded for anything pgmt does not manage; on a database carrying a large extension (PostGIS alone declares over a thousand routines) that dominated the load. Measured at roughly 4x faster on a database with 2,000 extension-owned functions.

## 0.6.3 - 2026-08-10

### Features

- Support custom collations (`CREATE COLLATION` with libc, ICU, and PG17+ builtin providers, including `deterministic` and ICU `rules`): create, drop, and comment, with dependency ordering and cascades across the domains, table columns, composite type attributes, indexes, and views that use them. Previously `pgmt apply` silently ignored collations — a schema with `CREATE COLLATION` and a dependent object failed on the target — and column `COLLATE` clauses were dropped from generated SQL ([#15](https://github.com/gdpotter/pgmt/issues/15))

### Bug Fixes

- `--verbose` now reports progress while the schema is applied to the shadow database. That phase is the longest part of a run on a large schema and previously logged nothing above debug, so `--verbose` went silent for as long as it took and offered no way to tell a slow run from a hung one. Starting or pulling the shadow container, and verifying the final state, are likewise reported now.

### Performance

- `pgmt migrate apply` no longer queries the target once per migration file. It read the section-tracking table twice and derived the re-anchor cursor twice for every migration it walked, so a no-op deploy against a remote database cost four round trips per migration — roughly 45 seconds for a project with a hundred migrations. Those reads are now a single up-front query, and the cursor is derived only when a committed re-anchor actually needs it.
- Database connections no longer ping the server before every statement (sqlx's `test_before_acquire` default), which was doubling the round-trip cost of every query pgmt issues against a remote target.
- Applying schema files to the shadow database no longer re-reads the whole catalog after every file. Working out which file created which object — what `-- require:` dependencies are expanded from — cost a full catalog scan per file, over a catalog that grew as the run progressed, so it dominated every `pgmt apply` on a large schema and grew quadratically with it. Each file is now bracketed by a boundary that costs a handful of index lookups, and the catalog is read once at the end.
- Reading indexes from the catalog no longer scans `pg_constraint` once per index. Naming the constraint an index backs was a correlated subquery on a column `pg_constraint` has no index for, so the scan repeated for every index in the database — quadratic on a schema with many of them, and paid twice per `pgmt apply`.
- Scoping a catalog to the managed universe no longer scans the object lists once per dependency entry. On a large schema this pruning ran for tens of seconds of pure CPU — around a quarter of the total time of `pgmt apply`, with no query to attribute it to.

## 0.6.2 - 2026-07-31

### Bug Fixes

- A CHECK constraint, a domain CHECK constraint, a domain `DEFAULT`, and a trigger's `WHEN` condition now depend on the functions and operators their expressions call, so migrations create those first. A domain whose CHECK calls a function was created before the function existed and the migration failed with `function ... does not exist`.

## 0.6.1 - 2026-07-29

### Features

- Podman is now supported as a container runtime for shadow databases — its rootless and rootful Docker-compatible sockets are probed automatically, after the standard Docker socket so Docker wins when both are present.

### Bug Fixes

- When no container runtime can be reached, pgmt now explains that the runtime is needed to provision the shadow database (separate from your dev database), gives per-runtime guidance, and points at the `shadow.url` fallback.
- RLS policies defined in a module's schema file are now attributed to that module. They were previously attributed to no file at all, landing in the unmoduled base, where the policy's mandatory reference to its module's table was reported as a "base cannot depend on a module" error.
- Privileges on a user-defined type whose name starts with an underscore, and on a range type, are now tracked. A filter meant to skip PostgreSQL's array types matched any leading underscore, and range types were left out of grant tracking altogether, so `GRANT USAGE` on either was silently dropped from generated schema files and migrations.
- A partitioned table's row type is no longer mistaken for a composite type. It was rendered as a spurious `CREATE TYPE`, and privileges granted on the table itself were attributed to that phantom type.
- Fix which objects incremental apply attributes to which schema file: user-defined operators, casts, range types and procedures were missing from attribution entirely, materialized views were given identities they never had, and a unique index vanished from attribution as soon as a foreign key referenced it. Misattribution showed up as objects silently assigned to the wrong file — and, on module projects, to the wrong module.
- Privileges are now tracked only on objects pgmt actually models. Grants on a materialized view, a partitioned table, or the sequence behind an identity column were read into the catalog even though the object itself never was, so they surfaced as grants on something pgmt could not create — and could be emitted as a `REVOKE` against a target. Those privileges are now left alone.
- `VARIADIC` (and any parameter mode) is now attributed to the correct parameter in function signatures; it was previously shifted onto the preceding parameter when comparing and displaying functions.
- An index over a domain (e.g. an expression cast to one) now depends on the domain rather than a plain type, so migration ordering places it after the domain exists.

## 0.6.0 - 2026-07-23

### Features

- **Modules**: partition a schema into named modules (`modules:` in pgmt.yaml, path globs over schema files) and deploy subsets per target with `--modules` on `migrate apply`/`provision`. Migrations and baselines are generated with module-tagged sections; targets track which modules they hold, skip the rest without a trace, and warn when an established module is left out of a deploy. Modules can be adopted late onto an established database, and re-partitioning (splitting a module, modularizing an existing project) is handled by re-anchoring baselines — existing targets pick up the new partition on their next apply without running any DDL. Fully opt-in: projects without a `modules:` block behave exactly as before.
- Baselines support multi-section headers with real execution modes — a baseline can carry `CREATE INDEX CONCURRENTLY`, and a failed provision resumes per-section instead of restarting.
- Checksums are now per-section: editing a section that hasn't completed is allowed (fix the SQL in place and re-run — pgmt resumes from the failed section), while completed sections stay immutable and are rejected by name.
- Add `pgmt migrate resolve` for explicit tracking-state repair (`--mark-completed`, `--restamp`) when the database and the tracking table have genuinely diverged — the break-glass complement to fix-in-repo recovery.
- `pgmt migrate status` now runs against a deployment target (`--target-url`, falling back to dev), is strictly read-only, flags incomplete migrations with the command to resume them, and summarizes per-module state on module projects.
- Add `pgmt migrate provision` to stand up a new database from a baseline plus its post-baseline migrations — the on-ramp for fresh environments (demo, staging, preview, disaster recovery), where `migrate apply` only maintains a database that's already established.
- `migrate apply` and `migrate provision` now serialize behind a Postgres advisory lock, so concurrent deployers (overlapping CI runs, a manual apply racing a pipeline) can't execute the same migration twice. The second runner prints a notice and waits.
- When a non-transactional section fails, pgmt now checks for invalid indexes left behind by a failed `CREATE INDEX CONCURRENTLY` and names them in the error, along with the `DROP INDEX CONCURRENTLY IF EXISTS` fix.

### Bug Fixes

- Quote user-defined parameter types in generated `DROP FUNCTION` statements, so functions taking a mixed-case custom type (e.g. `"SubscriptionFrequency"`) drop with a valid identifier.
- Transactional sections now record their completion inside the section's own transaction, so a crash mid-apply can never leave a section's DDL committed but its tracking row stuck at `running` (which previously made the resume fail on `already exists`).
- `migrate apply` warns loudly when a migration sits below the baseline watermark with no tracking row — a late-merged migration that would otherwise be silently skipped on every target forever — and points at `pgmt migrate update` to regenerate it above the baseline.

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
