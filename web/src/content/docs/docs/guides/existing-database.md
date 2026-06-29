---
title: Adopt Existing Database
description: Import an existing PostgreSQL database into pgmt and start managing it with migrations.
---

When you import an existing database, pgmt creates a **baseline** - a snapshot of your current schema. Without it, the first migration would try to recreate everything, breaking any database that already has those objects.

This is one of two on-ramps into pgmt: `pgmt init` **adopts an existing database** (the schema is already there — pgmt records a baseline and starts managing changes from that point), while `pgmt migrate provision` **stands up a fresh database** from that baseline. This guide covers the first; see [Provisioning a New Environment](/docs/guides/baseline-management#provisioning-a-new-environment) for the second.

## Import Workflow

Point pgmt at your existing database:

```bash
mkdir my-app && cd my-app
pgmt init --dev-url postgres://localhost/your_existing_db
```

pgmt will:

1. Analyze your database and show what it found (tables, views, functions, etc.)
2. Ask which object types to manage
3. Generate schema files from your database
4. Validate the generated schema
5. Prompt to create a baseline - choose **Yes**

If validation fails (common with complex functions that have hidden dependencies), pgmt shows the errors and lets you fix them before creating the baseline.

## Shadow Database for Extension-Heavy Schemas

pgmt validates your generated schema (and later generates migrations) against a throwaway **shadow database**. By default that's the stock `postgres` Docker image — which does **not** include extensions like PostGIS or TimescaleDB. If your schema uses one, an auto/version-only shadow fails with errors like `type "geography" does not exist`.

`pgmt init` inspects the source database and warns when it finds such an extension. Point the shadow at an image that includes it:

```bash
# PostGIS schema. The official postgis/postgis images are amd64-only, so on
# Apple Silicon (arm64) request linux/amd64 to run under emulation.
pgmt init --dev-url postgres://localhost/gis_db \
    --shadow-image postgis/postgis:16-3.5 \
    --shadow-platform linux/amd64
```

This is persisted to `pgmt.yaml` so every later command uses the same image:

```yaml
databases:
  shadow:
    docker:
      image: postgis/postgis:16-3.5
      platform: linux/amd64 # omit on amd64 hosts
```

The same image must be available wherever pgmt runs (your machine, CI, teammates), since `migrate apply` and validation all spin up a shadow. See [Configuration](/docs/reference/configuration) for all shadow options.

During import, pgmt also checks what the fresh shadow already provides: schemas
like PostGIS's `tiger` and `topology` exist before your schema is applied, so
they belong to the image, not your project. pgmt offers to exclude them from
management (all pre-selected) and records the choice in `pgmt.yaml` under
`objects.exclude.schemas` — without this, every diff would fight the image over
objects it recreates.

## Fix Dependencies

Validation might fail with errors like:

```
❌ Schema validation failed

ERROR: relation "users" does not exist
CONTEXT: function create_user_profile()
```

This means a function references a table that pgmt couldn't detect as a dependency. Add it explicitly:

```sql
-- schema/functions.sql
-- require: tables.sql

CREATE FUNCTION create_user_profile() ...
```

Test your fix:

```bash
pgmt apply --dry-run
```

Once it passes, create the baseline:

```bash
pgmt migrate baseline
```

## Make Your First Change

Test the workflow with a small change:

```bash
# Add a new table
cat >> schema/tables.sql << 'EOF'

CREATE TABLE user_profiles (
    user_id INTEGER PRIMARY KEY REFERENCES users(id),
    bio TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
EOF

# Apply to dev
pgmt apply

# Generate migration
pgmt migrate new "add user profiles"
```

The migration should contain only the new table, not your existing schema. That's the baseline working.

## Team Onboarding

When teammates clone the repo, they provision a fresh database from the baseline:

```bash
git clone your-repo && cd your-repo
createdb myapp_dev
pgmt migrate provision --target-url postgres://localhost/myapp_dev
```

`migrate provision` applies the baseline and then every migration after it, leaving them with a complete database. From there they iterate locally with `pgmt apply`. (Use `provision`, not `migrate apply`, for a brand-new database — `apply` only runs pending migrations and never applies the baseline, so it can't build the schema from scratch.)

## CLI Options

```bash
# Interactive (recommended)
pgmt init --dev-url postgres://localhost/your_db

# Non-interactive with baseline
pgmt init --dev-url postgres://localhost/your_db --create-baseline

# Skip baseline (rare - only for empty databases)
pgmt init --dev-url postgres://localhost/your_db --no-baseline
```

## Migrating from Other Tools

If you're coming from Flyway, Liquibase, Django, or Rails migrations:

1. Initialize pgmt from your current database state with `--create-baseline`
2. Archive (don't delete) your old migration tracking table
3. Use pgmt for all new changes going forward

Your ORM still handles data access - pgmt just manages the schema structure.

## Reorganizing Schema Files

pgmt generates schema files grouped by object type (`tables.sql`, `views.sql`). You may want to reorganize by domain after import. See [Schema Organization](/docs/guides/schema-organization) for patterns.

## Troubleshooting

**Hidden dependencies in functions:**
Functions that query tables have dependencies pgmt can't detect automatically. Add `-- require:` statements.

**Extension-owned objects appearing:**
pgmt filters these automatically. If you see PostGIS types or similar, check your extension configuration.

**Circular dependencies:**
Reorganize into separate files or merge the circular objects into one file.
