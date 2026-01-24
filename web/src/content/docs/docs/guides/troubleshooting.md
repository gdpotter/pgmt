---
title: Getting Help
description: How to debug issues and get help with pgmt.
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

If `pgmt baseline create` or `pgmt migrate new` fails with ordering errors, use the debug command to inspect which objects depend on which, and add `-- require:` headers to your schema files as needed.

## Common Setup Issues

**Can't connect to database:**

```bash
# Is PostgreSQL running?
pg_isready -h localhost -p 5432

# Does the database exist?
createdb myapp_dev
```

**Shadow database creation fails:**

Your user needs `CREATEDB` privilege. Or configure a manual shadow database:

```yaml
# pgmt.yaml
databases:
  shadow:
    auto: false
    url: postgres://localhost/myapp_shadow
```

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
