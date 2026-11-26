---
title: Troubleshooting
description: Common pgmt errors, how to debug them, and how to get help.
---

## Debugging

When something isn't working as expected:

```bash
# See what pgmt is doing
pgmt apply --verbose

# Full debug output
pgmt apply --debug

# Preview without making changes
pgmt apply --dry-run
```

For migration issues:

```bash
# Check if migrations are consistent with schema
pgmt migrate validate

# See what's different between schema files and database
pgmt diff
```

For dependency ordering issues:

```bash
# Analyze the dependency graph
pgmt debug dependencies

# Focus on a specific object
pgmt debug dependencies --object public.users

# Human-readable format
pgmt debug dependencies --format text
```

If `pgmt migrate baseline` or `pgmt migrate new` fails with ordering errors, use the debug command to inspect which objects depend on which, and add `-- require:` headers to your schema files as needed.

## Common Setup Issues

**Can't connect to database:**

```bash
# Is PostgreSQL running?
pg_isready -h localhost -p 5432

# Does the database exist?
createdb myapp_dev
```

**Could not connect to a container runtime:**

By default pgmt builds your schema in a throwaway container, so it needs Docker
or another Docker-compatible runtime. This is separate from your dev database.

- **Docker:** make sure the daemon is running.
- **Podman:** enable its Docker-compatible socket with
  `systemctl --user enable --now podman.socket`.
- **Other runtimes:** point `DOCKER_HOST` at a Docker-compatible socket.
- **No container runtime at all:** set `databases.shadow.url` to a database on
  a dedicated, throwaway server. pgmt resets it on every run, so never use one
  that holds anything you care about. See
  [Shadow Database](/docs/concepts/shadow-database#container-runtime-requirements).

**Shadow database creation fails with a `shadow.url`:**

With `reset: branch`, pgmt creates a temporary copy of the named database, so
the user in `shadow.url` needs the `CREATEDB` privilege. The default
`reset: clean` only drops the schemas pgmt manages and doesn't need
`CREATEDB`.

## Deployment Issues

**A migration failed partway through:** fix the SQL and run
`pgmt migrate apply` again. Completed sections are skipped and the failed one
is retried. `pgmt migrate status --target-url <url>` shows which sections
completed. See [When a Migration Fails](/docs/guides/production-operations#when-a-migration-fails).

**"was modified after it was applied":** a section that already ran on the
target has changed in the migration file. Restore it from git and put further
changes in a new migration. If the edit was deliberate and the database already
matches it, see [Checksum Mismatches](/docs/guides/production-operations#checksum-mismatches).

**"is below the baseline watermark and was never applied":** a migration was
merged after a newer baseline was created, so it would never run on any target.
Regenerate it above the baseline with `pgmt migrate update <version>`.

**Tracking state doesn't match the database:** if a section really did run (or
really didn't) but pgmt's records say otherwise, use `pgmt migrate resolve`.
See [Repairing Tracking State](/docs/guides/production-operations#repairing-tracking-state).

## Column Order Validation Errors

**Error: new column must come after existing column**

When running `pgmt migrate new`, you see:

```
Error: Column order validation failed.
Table public.users: new column 'email' must come after existing column 'name'
```

**Why this happens:** PostgreSQL's `ALTER TABLE ADD COLUMN` always appends columns to the end. If you define new columns in the middle of your schema file, the physical order in production won't match.

**Fix:** Move the new column to the end of your table definition:

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,           -- existing
    created_at TIMESTAMP, -- existing
    email TEXT           -- new column goes at the end
);
```

**To disable this check:** Set `migration.column_order: relaxed` in `pgmt.yaml`. But be aware that your schema files will drift from physical column order, which can cause issues with `SELECT *`, `COPY`, and functions returning row types.

## Reporting Issues

When opening an issue on [GitHub](https://github.com/gdpotter/pgmt/issues), include:

- pgmt version (`pgmt --version`)
- PostgreSQL version (`psql --version`)
- Full error output
- Steps to reproduce

## Links

- [GitHub Issues](https://github.com/gdpotter/pgmt/issues) - Bug reports
- [GitHub Discussions](https://github.com/gdpotter/pgmt/discussions) - Questions
- `pgmt --help` or `pgmt <command> --help` - CLI reference
