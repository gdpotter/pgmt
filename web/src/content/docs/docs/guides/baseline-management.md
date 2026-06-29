---
title: Baseline Management
description: Understand when and how to use baselines to consolidate your migration history.
---

Baselines are complete SQL snapshots of your schema at a point in time. They serve two purposes:

- **Faster reconstruction:** when generating or validating migrations, pgmt can rebuild "current state" from a baseline instead of replaying the entire migration chain.
- **Provisioning new databases:** `pgmt migrate provision` applies a baseline (plus the migrations after it) to stand up a fresh database from scratch.

They're optional for the first purpose (pgmt can always reconstruct from the full chain), but once you consolidate history into a baseline, they become how you provision a brand-new environment.

## When You Need Baselines

**Importing an existing database:** When you run `pgmt init` against a database that already has objects, you create a baseline to establish the starting point. See [Adopt Existing Database](/docs/guides/existing-database).

**Long migration chains:** After 50+ migrations, new environment setup can get slow. A baseline lets pgmt skip replaying the entire chain.

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

## Provisioning a New Environment

`migrate apply` maintains a database that's already established — it runs pending migration files and does **not** apply baselines. So to stand up a *new* database (a demo, a fresh staging environment, a new region, disaster recovery), use `pgmt migrate provision`:

```bash
pgmt migrate provision --target-url postgres://demo/myapp
```

The `migrate provision` command applies the latest baseline (the full schema snapshot), then applies every migration after it, recording each in the tracking table, so the database is left ready for `migrate apply` going forward.

If there's no baseline yet, `migrate provision` simply replays all migrations and behaves identically to `migrate apply`. Preview first with `--dry-run`.

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
  create_baselines_by_default: false # for migrate new
```

By default, `migrate new` doesn't create baselines. Use `--create-baseline` when you want one.
