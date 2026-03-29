---
title: Baseline Management
description: Understand when and how to use baselines to consolidate your migration history.
---

Baselines are complete SQL snapshots of your schema at a point in time. They're optional - pgmt can reconstruct state from your migration chain - but useful for performance when migration chains get long.

## When You Need Baselines

**Importing an existing database:** When you run `pgmt init` against a database that already has objects, you create a baseline to establish the starting point. See [Adopt Existing Database](/docs/guides/existing-database).

**Long migration chains:** After 50+ migrations, new environment setup gets slow. A baseline lets pgmt skip replaying the entire chain.

**Keeping the migrations directory manageable:** Since schema files are the source of truth, old migrations are a derived artifact. Periodically consolidating them into a baseline keeps your repo clean.

## How pgmt Works Without Baselines

When you clone a repo and run `pgmt migrate new`, pgmt needs to know the current schema state to generate the right diff. Without a baseline, it reconstructs by applying all migrations in order:

```
Clone repo → Apply M1 → M2 → ... → M50 → Compare to schema files → Generate M51
```

With a baseline:

```
Clone repo → Load baseline → Compare to schema files → Generate M51
```

Both produce the same result. Baselines just make it faster.

**This is why migrations stay incremental:** A new team member cloning the repo, editing schema files, and running `migrate new` gets an ALTER statement - not a full schema recreation. pgmt reconstructs the chain to understand what already exists.

## Commands

**Create a baseline and clean up migrations (default):**

```bash
pgmt migrate baseline
```

This creates a baseline from your current schema files and deletes all existing migrations and old baselines. The baseline version matches the latest migration's version.

**Create a baseline but keep migration files:**

```bash
pgmt migrate baseline --keep-migrations
```

**Preview what would happen:**

```bash
pgmt migrate baseline --dry-run
```

**Create a baseline alongside a migration:**

```bash
pgmt migrate new "v2.0 release" --create-baseline
```

**List baselines:**

```bash
pgmt migrate baseline list
```

## Working with Branches

When you create a baseline and delete old migrations, teammates on other branches may have migrations with versions that predate the baseline. pgmt handles this gracefully:

1. **pgmt detects it:** When a pre-baseline migration is found, pgmt warns:
   ```
   Warning: Migration 1764034955 predates baseline 1773798334 and will be skipped.
   Run 'pgmt migrate update 1764034955' to renumber it.
   ```

2. **Fix with `migrate update`:** The teammate runs `pgmt migrate update <version>`, which regenerates the migration with a new timestamp (after the baseline) based on the current schema diff.

This works because schema files are the source of truth. The migration is regenerated from the diff between the baseline state and the current schema, producing the correct result regardless of the original migration's timestamp.

## Configuration

```yaml
# pgmt.yaml
migration:
  create_baselines_by_default: false  # for migrate new
```

By default, `migrate new` doesn't create baselines. Use `--create-baseline` when you want one.
