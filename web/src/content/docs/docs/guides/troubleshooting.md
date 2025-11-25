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
