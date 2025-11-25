---
title: Adopt Existing Database
description: Import an existing PostgreSQL database into pgmt and start managing it with migrations.
---

When you import an existing database, pgmt creates a **baseline** - a snapshot of your current schema. Without it, the first migration would try to recreate everything, breaking any database that already has those objects.

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
pgmt baseline create
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

When teammates clone the repo:

```bash
git clone your-repo && cd your-repo
createdb myapp_dev
pgmt migrate apply --dev-url postgres://localhost/myapp_dev
```

They get a complete database with all migrations applied, including the baseline.

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
