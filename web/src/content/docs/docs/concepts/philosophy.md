---
title: Philosophy
description: Understanding pgmt's approach to database migrations and why it's designed this way.
---

Most migration tools focus on tracking changes or managing files. pgmt takes a different approach: it starts with the ability to accurately compare two database states. Once you have that capability, everything else becomes simpler - local development, migration generation, validation, drift detection all build on the same foundation.

## The Core: Schema Diffing

At its heart, pgmt knows how to compare two PostgreSQL database states and tell you what changed. Tables, columns, indexes, views, functions, triggers, constraints, extensions, custom types, grants. All of it.

This is harder than it sounds. A column rename looks identical to a drop + create. A view dependency might be indirect. An index definition needs parsing. PostgreSQL has dozens of object types, each with their own quirks.

Once you have reliable diffing, you can build everything else on top of it:

**Local development** - Edit schema files, run `pgmt apply`, see changes instantly. The diffing engine figures out what changed and applies it to your dev database.

**Migration generation** - When you're ready to commit, `pgmt migrate new` diffs your schema against the migration chain and generates a migration file.

**Validation** - Apply your schema to a temporary database, then diff it against your target. Catches errors before production.

**Drift detection** - Diff your expected schema against production. See what changed outside of migrations.

**Debugging** - `pgmt diff --from dev --to prod` shows exactly how environments diverged.

One capability, applied to different problems.

## Why This Matters

### You define state, not steps

Your schema files show what your database should look like:

```sql
-- schema/tables/users.sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    name TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- schema/views/active_users.sql
-- require: tables/users.sql
CREATE VIEW active_users AS
SELECT * FROM users WHERE is_active = true;
```

Organize your schema across files however you want - by table, by feature, by team ownership. Use `-- require:` comments to declare dependencies between files. It's formatted and structured like real code, not a database dump.

To understand your schema, you read these files. Not 50 migration files scattered across months of development.

When you want to add a column, you edit the file. When you want to see what columns exist, you read the file. The schema files are always current.

### Fast local iteration

While you're developing, you don't create migrations for every change. You just edit your schema files and run:

```bash
pgmt apply
# Or for continuous iteration:
pgmt apply --watch
```

pgmt diffs your schema files against your local database and applies the changes directly. Add a column, drop an index, modify a view - see the results immediately.

You only create a migration when you're ready to commit, open a PR, or deploy to other environments. The local development loop is fast and friction-free.

### But migrations are explicit

Here's where pgmt differs from tools like Prisma Migrate or Django migrations.

When you run `pgmt migrate new`, it generates a migration file that you review:

```sql
-- migrations/V1734567890__add_user_phone.sql
ALTER TABLE users ADD COLUMN phone TEXT;
```

You see exactly what will run in production. If it's wrong, you fix it. If you need to add a default value, update existing rows, or handle the change differently - you edit the migration.

Then `pgmt migrate apply` runs it.

**Why explicit matters:**

Let's say you rename a column from `email` to `email_address`. The diff engine sees:

- Column `email` disappeared
- Column `email_address` appeared

So it generates:

```sql
ALTER TABLE users DROP COLUMN email;
ALTER TABLE users ADD COLUMN email_address TEXT;
```

This would lose your data. But you see this during `pgmt migrate new`, before it touches any database. You edit the migration:

```sql
ALTER TABLE users RENAME COLUMN email TO email_address;
```

Or during local development, you just run the rename manually, then edit your schema file to match.

**This is intentional.** We know the diff engine won't be perfect for every edge case. That's why you review migrations and can edit them. Or during prototyping, apply changes by hand and update your schema files to reflect reality.

### Shadow database validation

Before generating a migration, pgmt:

1. Creates a temporary database
2. Applies your schema files to it
3. Compares the result to your target database
4. Generates the migration from that diff
5. Drops the temporary database

This catches errors before they touch production:

- Circular dependencies (view A depends on view B depends on view A)
- Invalid syntax
- Missing permissions
- Constraint violations

If your schema files are broken, you find out during `migrate new`, not during `migrate apply` in production.

## What This Looks Like

**Adding a table:**

Edit `schema/orders.sql`:

```sql
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    total DECIMAL(10,2)
);
```

Run `pgmt migrate new`. It generates:

```sql
-- migrations/V1734567890__create_orders.sql
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    total DECIMAL(10,2)
);
```

Review it, then `pgmt migrate apply`.

**Modifying a column:**

Change `schema/users.sql`:

```sql
-- Before: email TEXT
-- After:  email TEXT NOT NULL
```

Run `pgmt migrate new`. It generates:

```sql
ALTER TABLE users ALTER COLUMN email SET NOT NULL;
```

Looks good. Apply it.

**Complex refactoring:**

You're splitting `full_name` into `first_name` and `last_name`. Edit `schema/users.sql`:

```sql
-- Remove: full_name TEXT
-- Add:    first_name TEXT, last_name TEXT
```

Run `pgmt migrate new`. It generates:

```sql
ALTER TABLE users DROP COLUMN full_name;
ALTER TABLE users ADD COLUMN first_name TEXT;
ALTER TABLE users ADD COLUMN last_name TEXT;
```

This will lose data. Edit the migration:

```sql
ALTER TABLE users ADD COLUMN first_name TEXT;
ALTER TABLE users ADD COLUMN last_name TEXT;

UPDATE users
SET first_name = split_part(full_name, ' ', 1),
    last_name = split_part(full_name, ' ', 2);

ALTER TABLE users DROP COLUMN full_name;
```

Now apply it. The diff engine generated the starting point, but you refined it.

## Editing Views and Functions Like Code

One place where the declarative approach really shines: views and functions.

**Traditional approach:**

```sql
-- migration_001.sql
CREATE VIEW active_users AS
SELECT id, email FROM users WHERE is_active = true;

-- migration_002.sql
DROP VIEW active_users;
CREATE VIEW active_users AS
SELECT id, email, created_at FROM users WHERE is_active = true;
```

In code review, you see `DROP VIEW` + entire `CREATE VIEW`. Git diff shows everything as additions. Hard to see what actually changed.

**With pgmt:**

```sql
-- schema/views/active_users.sql
CREATE VIEW active_users AS
SELECT
    id,
    email,
    created_at  -- This addition is clearly visible in git diff
FROM users
WHERE is_active = true;
```

Git diff shows exactly what changed - you added one column. pgmt handles the DROP/CREATE mechanics.

This is especially valuable for complex views with joins, CTEs, or window functions. Edit the view definition like any other code file. Review the logical changes in git. Let pgmt generate the migration.

## When to Use pgmt

**Good fit:**

- PostgreSQL projects
- Teams comfortable with SQL
- Projects where you want control over migrations
- Existing databases you want to manage declaratively
- Complex schemas (views, functions, triggers)

**Not a good fit:**

- Multi-database projects (MySQL, SQLite)
- Teams that want pure auto-apply with no review
- Projects where developers don't write SQL

## The Pieces

The diffing engine is the foundation. Built on top:

**[Shadow Database](shadow-database)** - Validates schema in isolation before generating migrations

**[Dependency Tracking](dependency-tracking)** - Orders migrations correctly (tables before views, schemas before tables, etc.)

**[How pgmt Works](how-it-works)** - The full workflow from schema edit to applied migration
