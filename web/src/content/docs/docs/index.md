---
title: pgmt Documentation
description: PostgreSQL migration tool for schema-as-code development with explicit migrations.
---

pgmt lets you manage your PostgreSQL schema like code — edit database objects directly, see changes instantly, and generate explicit migrations when you're ready to deploy.

> **Status: Alpha** — Core features are functional, but the API may evolve.

## Start here

**[Quick Start Guide](/docs/getting-started/quick-start)** — Install pgmt, set up a project, and apply your first schema change in under 10 minutes.

Already have a database? See [Adopt Existing Database](/docs/guides/existing-database) to import your schema.

## What's in these docs

**[Using pgmt](/docs/guides/schema-organization)** — How to organize schema files, the day-to-day migration workflow, and managing roles and permissions.

**[Going to Production](/docs/guides/multi-section-migrations)** — Multi-section migrations with per-section timeouts and retries, CI/CD integration, and baseline management.

**[Under the Hood](/docs/concepts/philosophy)** — How the schema diffing engine works, shadow database validation, and automatic dependency tracking.

**[Reference](/docs/cli/)** — CLI commands, configuration options, and the full list of supported PostgreSQL features.

## Get help

- **[Troubleshooting](/docs/guides/troubleshooting)** — Common issues and solutions
- **[GitHub Discussions](https://github.com/gdpotter/pgmt/discussions)** — Ask questions
- **[GitHub Issues](https://github.com/gdpotter/pgmt/issues)** — Report bugs
