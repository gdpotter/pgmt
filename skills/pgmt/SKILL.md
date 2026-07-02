---
name: pgmt
description: Use when working in a project that uses pgmt (PostgreSQL schema migration tool). Triggers for tasks like creating or altering tables, columns, indexes, views, functions, triggers, grants, RLS policies, extensions, custom types, domains, or any SQL schema object; generating or applying migrations; or any work in a repo containing a `pgmt.yaml` with `schema/` and `migrations/` directories. pgmt is declarative during development (edit schema files, run `pgmt apply`) and generates reviewable migration files for production. An agent without this skill will typically hand-write migration SQL, which is wrong.
---

# Working with pgmt

pgmt is a **declarative** PostgreSQL schema tool. You edit SQL files under `schema/` as the source of truth; pgmt diffs them against the database and generates the DDL. This is not Flyway/Alembic/Rails — **do not hand-write migration files**.

Schema files should always reflect the **final desired state** of the database, not incremental changes. If asked to "add a column", the edit is to the `CREATE TABLE` statement itself, not a new `ALTER TABLE` somewhere.

## How to recognize a pgmt project

- A `pgmt.yaml` file somewhere in the project (often at the repo root, but can live in a subdirectory for a specific package/service)
- Alongside it: a `schema/` directory of `.sql` files (source of truth) and a `migrations/` directory of generated files named `{unix_timestamp}_{description}.sql`
- Possibly `schema_baselines/` and `roles.sql` in the same directory

Run `pgmt` commands from the directory containing `pgmt.yaml` (or point at it with `--config-file`). If no `pgmt.yaml` exists anywhere, this skill doesn't apply.

## The workflow for any schema change

When asked to add/change/drop a table, column, view, function, index, policy, grant, etc.:

1. **Edit the relevant file under `schema/`.** Find it by grepping for the object name. If a new file is needed, place it in a location that matches the project's existing organization.
2. **Declare dependencies with `-- require:` comments** at the top of the file if it references objects in other files. Example: `-- require: users.sql`. This is pgmt-specific syntax, not standard SQL.
3. **Run `pgmt apply`** to sync the dev database. Review the output — it shows exactly what SQL ran. If destructive changes are involved in a non-interactive shell, it will exit with code 2; re-run with `--require-approval` (then approve interactively), `--safe-only`, or `--force` as appropriate.
4. **Run `pgmt migrate new "short description"`** to generate a reviewable migration file under `migrations/`.
5. **Read the generated migration.** Check that the SQL matches intent — e.g., a column rename should be `RENAME COLUMN`, not `DROP` + `ADD`. Edit the generated file if needed *before* it has been applied anywhere.
6. **Commit both** the `schema/` changes and the new `migrations/` file together.

To preview without touching anything: `pgmt apply --dry-run` or `pgmt diff`.

## Module projects (`modules:` in pgmt.yaml)

If `pgmt.yaml` has a `modules:` map, the project partitions its schema into named **modules** (deployable slices). Files map to modules by path globs; files matching no module are "the base" and deploy everywhere. Everything below changes:

- **Placement matters.** Put new schema files in the directory of the module that owns the object. A file matching two modules' globs is an error.
- **Cross-module references** require the referencing module to declare `depends_on: [other]` in `pgmt.yaml` — otherwise `migrate new` warns. A base (unmoduled) file referencing a module's object is a hard error: move the file into a module or the object into the base.
- **Generated migrations carry section headers** like `-- pgmt:section name="billing" module="billing"`. Never edit `module=`/`remaps=` attributes or move SQL between sections — ownership is derived from them.
- **`pgmt migrate apply` deploys ONLY the base by default.** To deploy modules, name them: `pgmt migrate apply --modules billing` (dependencies are pulled in automatically) or `--modules all`. `PGMT_MODULES` env var works too. This is the #1 mistake to avoid: a bare apply on a module project silently skips every module section (it prints skip notices — read them).
- **Re-anchor errors are normal, not failures to work around.** If `migrate new` fails with "partition re-anchor required" (after moving files between modules, first-time modularization, or dropping an object another module references), re-run the same command with `--create-baseline`. Do NOT try to avoid the error by restructuring the change.
- **Adopting a module onto an existing database:** `pgmt migrate apply --modules X` works when pgmt allows it; if it errors with "requires baseline content", run `pgmt migrate provision --modules X` instead (the module's dependencies must already be on that target).
- **Coupled migrations:** if apply refuses with "deploy them together", pass both modules in `--modules` — don't retry one at a time.

## Never do these

- **Do not hand-create files under `migrations/`.** They are generated by `pgmt migrate new`.
- **Do not edit an already-committed/applied migration** to "fix" it. Migrations are checksummed and immutable once applied. Create a *new* migration that corrects the issue.
- **Do not run `ALTER TABLE` or other DDL directly against the dev database** to make a schema change. Edit the `schema/` file and run `pgmt apply`.
- **Do not insert new columns in the middle of a `CREATE TABLE` definition.** PostgreSQL's `ALTER TABLE ADD COLUMN` only appends; adding a column in the middle will cause `pgmt migrate new` to fail with a column order validation error. Add new columns at the end.
- **Do not put `INSERT`/seed data in `schema/` files.** Schema files are schema only.
- **Do not delete a migration file** to redo it. Create a new migration that reverses/amends it.
- **Do not run bare `pgmt migrate apply` on a module project** expecting modules to deploy — name them with `--modules`.
- **Do not hand-edit `-- pgmt:section` headers** (`module=`, `remaps=`) in generated migrations or baselines.

## Rebase / merge

If `git pull` brings in new migrations from teammates while your own migration is uncommitted or in-progress, your migration is now stale. Regenerate it:

```bash
pgmt migrate update             # updates the latest migration
pgmt migrate update {version}   # updates a specific one (e.g. 1734567890)
```

## Existing database onboarding

For a project adopting pgmt against an existing DB:

```bash
pgmt init --dev-url postgres://... --create-baseline
```

This reverse-engineers `schema/` files and writes a baseline so the first real migration only contains your *changes*, not the entire existing schema.

## Command quick reference

| Command | Purpose |
| --- | --- |
| `pgmt apply` | Sync `schema/` → dev DB. Use `--watch` for continuous mode, `--dry-run` to preview. |
| `pgmt diff` | Preview what `apply` would do. |
| `pgmt migrate new "desc"` | Generate a migration file from current diff. |
| `pgmt migrate update [ver]` | Regenerate an unapplied migration (after rebase). |
| `pgmt migrate apply` | Deploy pending migrations to a target DB. Module projects: add `--modules <list|all>` (bare apply = base only). Failed migrations resume per-section on re-run. |
| `pgmt migrate provision` | Stand up a fresh DB from baseline + migrations; module projects: `--modules` for subset deploys and adopting modules onto existing targets. |
| `pgmt migrate baseline` | Collapse the migration log into a baseline checkpoint (replays history; errors if there are no migrations). |
| `pgmt migrate status` | Show which migrations are applied; half-applied ones show INCOMPLETE. |
| `pgmt migrate validate` | CI check: migrations reproduce the declared schema. |
| `pgmt validate` | Check `schema/` consistency. |

For subcommand flags, run `pgmt <command> --help` — don't guess.

## Things outside this skill's scope

- Production deployment strategies, lock-timeout tuning, multi-section migrations, `pgmt:section` annotations — let the human decide. Don't add these speculatively to generated migrations.
- Editing `pgmt.yaml` — if the project already has one, trust it.
- Writing destructive migrations without the user's explicit confirmation of intent.