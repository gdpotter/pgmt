---
title: Production Operations
description: Handling migration failures, recovery, and operational concerns in production.
---

Things go wrong in production. Migrations fail, locks contend, schemas drift. This guide covers what to do when they do.

## When a Migration Fails

If `pgmt migrate apply` fails partway through, what happens next depends on whether you're using [sections](/docs/guides/multi-section-migrations).

### Single-Section Migrations

Single-section migrations run in a transaction by default. If the migration fails, the transaction rolls back and nothing changes. Fix the issue and run `pgmt migrate apply` again.

### Multi-Section Migrations

pgmt tracks section progress. If section 2 of 3 fails, sections that already completed stay completed:

```
✓ Section 1/3: add_column (completed)
✗ Section 2/3: create_index (failed)
⊘ Section 3/3: add_constraint (not started)
```

Run `pgmt migrate apply` again. pgmt skips completed sections and retries from the failure:

```
⊙ Section 1/3: add_column (skipping - already completed)
↻ Section 2/3: create_index (retrying)
```

This is safe because transactional sections roll back on failure - they either fully completed or didn't run. Non-transactional sections (like `CREATE INDEX CONCURRENTLY`) are different - see below.

### Non-Transactional Section Failures

`CREATE INDEX CONCURRENTLY` and similar operations can't run in a transaction. If they fail partway, partial state may be left behind. PostgreSQL marks a failed concurrent index as `INVALID`.

When a non-transactional section fails, pgmt checks for invalid indexes and names them in the error, along with the fix — so the failure output tells you exactly what's stranded. To check by hand:

```sql
-- Check for invalid indexes
SELECT indexname FROM pg_indexes
WHERE indexname = 'idx_users_status'
  AND NOT EXISTS (
    SELECT 1 FROM pg_index WHERE indexrelid = 'idx_users_status'::regclass AND indisvalid
  );
```

Drop the invalid index before retrying:

```sql
DROP INDEX CONCURRENTLY IF EXISTS idx_users_status;
```

Then run `pgmt migrate apply` again. The section will retry from scratch.

**Tip:** Use `retry_attempts` and `on_lock_timeout="retry"` in your section config to handle transient lock contention automatically. See [Multi-Section Migrations](/docs/guides/multi-section-migrations#retry-logic).

## Concurrent Deployments

Two deployers pointed at the same database — overlapping CI runs, a manual apply racing a pipeline — can't step on each other. `migrate apply` and `migrate provision` take a PostgreSQL advisory lock (scoped to the tracking table) for the duration of the run. The second runner prints a notice and waits:

```
Another pgmt operation is running against this database (advisory lock 4779231646775632092 held); waiting...
```

The lock is held on its own connection and released when the run finishes — including when it fails — so a failed deploy never leaves the lock stuck. No configuration is needed, and projects using different tracking tables in one database don't serialize against each other.

## Checksum Mismatches

pgmt records a checksum for every applied migration. If someone edits a migration file after it's been applied, the next `pgmt migrate apply` fails:

```
Migration 1734567890 has been modified after being applied!
Expected checksum: a1b2c3d4...
Actual checksum:   e5f6a7b8...
```

This is intentional. Applied migrations are immutable - they represent what actually ran against your database. If the file changes, pgmt can't be sure the database matches the migration chain.

**To fix:** Restore the original migration file from git. If you need to make changes, create a new migration instead.

## Migrations Are Append-Only

Once a migration is applied to any environment, treat it as permanent:

- **Don't delete it.** Other environments still need to apply it, and the tracking table expects it to exist.
- **Don't edit it.** Checksum validation will catch the modification and block future deploys.
- **Don't reorder it.** Version numbers determine execution order.

If you need to undo a change, create a new migration that reverses it. See [Reverting a Change](/docs/guides/migration-workflow#reverting-a-change).

## Checking What's Applied

Before deploying, check the current state:

```bash
# See applied and pending migrations, with per-section progress for
# anything incomplete (runs against the dev database)
pgmt migrate status
```

Migrations with pending or failed sections are flagged `INCOMPLETE` with the command to resume them. (Running `status` against a production target directly is on the roadmap; today it reads the dev database.)

## Drift Detection

Production databases can drift from expected schema - manual `ALTER TABLE` commands, direct grants, emergency hotfixes. Detect this before it causes problems:

```bash
pgmt migrate diff --target-url postgres://prod/myapp
```

This compares your migration chain's expected state against what's actually in the database. Any differences show up as drift.

For automated drift detection in CI, see [CI/CD Integration](/docs/guides/ci-cd#drift-detection).

**If drift is detected:** Decide whether the drift should be kept or reverted. If kept, update your schema files to match and generate a new migration. If reverted, apply the remediation SQL that `pgmt migrate diff --format sql` generates.

## Spinning Up a New Environment

To stand up a new database from the repo — a fresh staging environment, a new region, or recovering after a disaster — use `pgmt migrate provision` rather than `migrate apply`. `apply` only maintains a database that's already established; `provision` applies the latest baseline and then the migrations after it, leaving the database ready for `migrate apply`:

```bash
pgmt migrate provision --target-url postgres://new-env/myapp
pgmt migrate provision --target-url postgres://new-env/myapp --dry-run   # preview first
```

provision expects a fresh database. If the baseline collides with objects already present, the apply fails atomically (Postgres reports `relation "x" already exists`) and nothing is left behind. To bring an *existing* database under management instead, adopt it with `pgmt init`.
