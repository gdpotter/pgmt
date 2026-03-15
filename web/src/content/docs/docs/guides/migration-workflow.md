---
title: Migration Workflow
description: The day-to-day workflow for creating, updating, and applying migrations with pgmt.
---

pgmt separates local development from migration creation. You edit schema files and apply them directly during development. When you're ready to commit, you generate a migration.

## The Basic Workflow

**1. Edit your schema files:**

```sql
-- schema/users.sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL
);
```

**2. Apply to your dev database:**

```bash
pgmt apply
```

This diffs your schema files against your local database and applies the changes directly. No migration file yet - you're just iterating.

**3. When you're ready to commit, generate a migration:**

```bash
pgmt migrate new "add users table"
```

This creates `migrations/1734567890_add_users_table.sql` containing the SQL to transform any database at the previous state into the new state.

**4. Commit both schema files and migration:**

```bash
git add schema/ migrations/
git commit -m "Add users table"
```

## When Your Branch Is Behind

You're working on a feature. Meanwhile, a teammate merges their changes to main. Your migration might now be based on an outdated state.

```bash
# Pull the latest changes
git pull origin main

# Check if your migration is still valid
pgmt migrate validate
```

If validation passes, you're fine - your changes don't conflict. If it fails, you need to regenerate your migration:

```bash
# Use your migration's version number to be explicit
pgmt migrate update 1734567890
```

**Why the explicit version?** If you and a teammate both created migrations, `migrate update` without a version targets the one with the highest timestamp - which might be theirs, not yours.

This handles same-object changes correctly. If you both modified a view, `migrate update` regenerates your migration from the new baseline (which includes your teammate's changes), so your migration will only contain your delta.

The same applies after resolving git conflicts in schema files. Migration files themselves won't conflict - they have unique timestamps - but if you and a teammate edited the same view or table definition, resolve the schema file conflict and then run `migrate validate` to see if your migration needs regenerating.

**Warning:** `migrate update` regenerates the migration from scratch. If you manually edited the migration (e.g., changed a DROP+ADD to RENAME), those edits are lost. You'll need to re-apply them. Only update migrations that haven't been applied to any environment yet - applied migrations are immutable.

## Applying Migrations

During development, you use `pgmt apply` to sync your dev database directly with schema files.

For staging and production, you apply migration files:

```bash
# Apply to staging
pgmt migrate apply --target-url postgres://staging/myapp

# Apply to production
pgmt migrate apply --target-url postgres://prod/myapp
```

Migrations run in order. pgmt tracks which migrations have been applied in a `pgmt_migrations` table.

## Checking Status

See what's applied and what's pending:

```bash
pgmt migrate status --target-url postgres://prod/myapp
```

```
Applied:
  1734500000 - create_users_table (applied: 2024-12-18 10:00)
  1734510000 - add_posts_table (applied: 2024-12-18 11:00)

Pending:
  1734520000_add_comments_table.sql
```

## Validating Before Deploy

In CI, validate that your migrations produce the expected schema:

```bash
pgmt migrate validate
```

This reconstructs the schema from migrations and compares it to your schema files. If they don't match, something's wrong - maybe you edited schema files without generating a migration.

## Editing Generated Migrations

pgmt generates migrations, but you can edit them. Common reasons:

- **Column renames**: pgmt sees DROP + ADD, you want RENAME
- **Data migrations**: Add UPDATE statements to transform data
- **Performance**: Add CONCURRENTLY to index creation

```sql
-- Generated (loses data):
ALTER TABLE users DROP COLUMN email;
ALTER TABLE users ADD COLUMN email_address TEXT;

-- Edited (preserves data):
ALTER TABLE users RENAME COLUMN email TO email_address;
```

After editing, the migration is yours. If you later run `migrate update`, your edits will be lost - so keep track of what you changed.

## Reverting a Change

You shipped a migration and now you need to undo it. Maybe a view change broke something, or a feature got pulled. The instinct is to delete the migration file - don't. That migration is already recorded in your target databases with a checksum. Deleting it causes checksum errors on the next deploy.

pgmt doesn't have down migrations. Instead, you undo the schema file changes and generate a new forward migration:

```bash
# Revert the schema file changes, but keep migration files intact
git revert <commit> --no-commit
git checkout HEAD -- migrations/

# Generate a migration that undoes the original
pgmt migrate new "revert user status feature"

# Commit the schema file revert and the new migration together
git add schema/ migrations/
git commit -m "revert: remove user status feature"
```

Your migration history now looks like:

```
migrations/1734500000_add_user_status.sql          # original change
migrations/1734600000_revert_user_status_feature.sql # undo
```

Both migrations stay in the chain. The second one reverses the first. This works especially well for views and functions, where the "undo" migration is just a `DROP VIEW` / `CREATE OR REPLACE` back to the previous definition.

**For table changes**, review the generated migration carefully. Reverting a `DROP COLUMN` won't recover the data - it just re-adds an empty column. If you need data recovery, that's a restore-from-backup situation, not a schema revert.

**If you haven't deployed yet**, just revert your schema files and run `pgmt migrate update <version>` to regenerate the migration in place.

## Column Ordering

PostgreSQL's `ALTER TABLE ADD COLUMN` always appends columns to the end of a table. This means if you add a column in the middle of your schema file, the physical column order in production won't match your schema definition.

By default, pgmt validates that new columns are placed at the end of table definitions:

```sql
-- schema/users.sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT now(),
    email TEXT  -- New columns must go at the end
);
```

If you place a new column in the middle, `pgmt migrate new` will error:

```
Error: Column order validation failed.

Table public.users: new column 'email' must come after existing column 'created_at'

To fix: Move new columns to the end of your table definition.
```

To change this behavior:

```yaml
# pgmt.yaml
migration:
  column_order: warn # Warn but allow
  # column_order: relaxed  # Disable validation entirely
```

## Advanced: Multi-Section Migrations

For complex production deployments - concurrent index creation, batched updates, retry logic - see [Multi-Section Migrations](/docs/guides/multi-section-migrations).

```sql
-- pgmt:section name="add_column"
ALTER TABLE users ADD COLUMN status TEXT;

-- pgmt:section name="create_index" mode="non-transactional" retry_attempts="10"
CREATE INDEX CONCURRENTLY idx_users_status ON users(status);
```

## Quick Reference

| Task                   | Command                               |
| ---------------------- | ------------------------------------- |
| Apply schema to dev    | `pgmt apply`                          |
| Generate migration     | `pgmt migrate new "description"`      |
| Update stale migration | `pgmt migrate update <version>`       |
| Apply to target        | `pgmt migrate apply --target-url URL` |
| Check status           | `pgmt migrate status`                 |
| Validate in CI         | `pgmt migrate validate`               |
