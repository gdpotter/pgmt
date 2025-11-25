---
title: Baseline Management
description: Understand when and how to use baselines in your pgmt workflow.
---

Baselines are complete SQL snapshots of your schema at a point in time. They're optional - pgmt can reconstruct state from your migration chain - but useful for performance when migration chains get long.

## When You Need Baselines

**Importing an existing database:** When you run `pgmt init` against a database that already has objects, you create a baseline to establish the starting point. See [Adopt Existing Database](/docs/guides/existing-database).

**Long migration chains:** After 50+ migrations, new environment setup gets slow. A baseline lets pgmt skip replaying the entire chain.

**Major releases:** Mark stable points in your schema history for faster rollback or environment provisioning.

## How pgmt Works Without Baselines

When you clone a repo and run `pgmt migrate new`, pgmt needs to know the current schema state to generate the right diff. Without a baseline, it reconstructs by applying all migrations in order:

```
Clone repo → Apply V1 → Apply V2 → ... → Apply V50 → Compare to schema files → Generate V51
```

With a baseline:

```
Clone repo → Load baseline → Compare to schema files → Generate V51
```

Both produce the same result. Baselines just make it faster.

**This is why migrations stay incremental:** A new team member cloning the repo, editing schema files, and running `migrate new` gets an ALTER statement - not a full schema recreation. pgmt reconstructs the chain to understand what already exists.

## Commands

**Create a baseline:**

```bash
# Standalone
pgmt baseline create

# With a migration
pgmt migrate new "v2.0 release" --create-baseline
```

**List baselines:**

```bash
pgmt baseline list
```

**Clean up old baselines:**

```bash
# Keep the 5 most recent
pgmt baseline clean --keep=5

# Preview what would be deleted
pgmt baseline clean --keep=3 --dry-run
```

## Configuration

```yaml
# pgmt.yaml
migration:
  create_baselines_by_default: false # Recommended - create on-demand
```

By default, `migrate new` doesn't create baselines. Use `--create-baseline` when you want one.

## Troubleshooting

**Migration recreates existing objects:**

This happens when pgmt can't determine the current state. Create a baseline to establish it:

```bash
pgmt baseline create
```

**Baseline inconsistent with schema:**

Verify your schema files match the database:

```bash
pgmt apply --dry-run
```
