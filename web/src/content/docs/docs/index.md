---
title: pgmt Documentation
description: PostgreSQL migration tool for schema-as-code development with explicit migrations.
---

Edit SQL files. Let pgmt figure out the dependencies, generate the migrations, and keep your environments in sync.

Most people start with the **[Quick Start](/docs/getting-started/quick-start)** — you'll have automatic dependency tracking working in under 10 minutes. Already have a database? See [Adopt Existing Database](/docs/guides/existing-database) to import your schema.

## What's in these docs

**[Using pgmt](/docs/guides/schema-organization)**: Organize schema files like code, iterate instantly with `pgmt apply`, and manage roles and permissions.

**[Going to Production](/docs/guides/multi-section-migrations)**: Handle the hard stuff. Concurrent indexes, data backfills, per-section timeouts, CI/CD integration, and baseline management.

**[How It Works](/docs/concepts/philosophy)**: Why pgmt can handle complex dependency chains that break other tools, and why explicit migrations matter.

**[Reference](/docs/cli/)**: CLI commands, configuration options, and the full list of supported PostgreSQL features.

## Get help

- **[Troubleshooting](/docs/guides/troubleshooting)** — Common issues and solutions
- **[GitHub Discussions](https://github.com/gdpotter/pgmt/discussions)** — Ask questions
- **[GitHub Issues](https://github.com/gdpotter/pgmt/issues)** — Report bugs

:::note
**Status: Alpha** — Core features are functional, but the API may evolve.
:::
