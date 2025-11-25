---
title: Multi-Section Migrations
description: Handle complex production migrations with sections for transaction control, retry logic, and progress tracking.
---

Some migrations can't run in a single transaction. `CREATE INDEX CONCURRENTLY` can't be inside a transaction. Large data backfills shouldn't hold locks for minutes. Enum value additions require non-transactional execution.

Sections let you handle these in a single migration file instead of splitting across multiple files.

## Why Sections?

**Without sections** - three files for one logical change:

```sql
-- V001__add_status_column.sql
ALTER TABLE users ADD COLUMN status TEXT;

-- V002__create_status_index.sql (separate file for CONCURRENTLY)
CREATE INDEX CONCURRENTLY idx_users_status ON users(status);

-- V003__add_status_constraint.sql
ALTER TABLE users ALTER COLUMN status SET NOT NULL;
```

**With sections** - one file:

```sql
-- V001__add_user_status.sql

-- pgmt:section name="add_column"
ALTER TABLE users ADD COLUMN status TEXT DEFAULT 'active';

-- pgmt:section name="create_index" mode="non-transactional" retry_attempts="10" retry_delay="5s"
CREATE INDEX CONCURRENTLY idx_users_status ON users(status);

-- pgmt:section name="add_constraint"
ALTER TABLE users ALTER COLUMN status SET NOT NULL;
```

Each section can have its own transaction mode, timeout, and retry logic. Sections execute in order. If one fails, pgmt tracks progress and resumes from where it left off.

## Section Syntax

```sql
-- Single line (common):
-- pgmt:section name="section_name" mode="transactional" timeout="30s"

-- Multi-line (for complex config):
-- pgmt:section name="section_name"
-- pgmt:  mode="transactional"
-- pgmt:  timeout="30s"
-- pgmt:  retry_attempts="10"
-- pgmt:  retry_delay="5s"
```

### Options

| Option            | Default         | Description                                           |
| ----------------- | --------------- | ----------------------------------------------------- |
| `name`            | required        | Section identifier                                    |
| `mode`            | `transactional` | `transactional`, `non-transactional`, or `autocommit` |
| `timeout`         | `600s`          | Max execution time per attempt                        |
| `retry_attempts`  | `1`             | Number of attempts                                    |
| `retry_delay`     | `0s`            | Wait between retries                                  |
| `retry_backoff`   | `none`          | `none` or `exponential`                               |
| `on_lock_timeout` | `fail`          | `fail` or `retry`                                     |

Durations: `30s`, `5m`, `2h`, `500ms`, `1m30s`

## Transaction Modes

### `transactional` (default)

Wraps in a transaction. Rolls back on failure.

```sql
-- pgmt:section name="schema_changes" mode="transactional"
ALTER TABLE users ADD COLUMN verified BOOLEAN DEFAULT false;
ALTER TABLE users ADD COLUMN verified_at TIMESTAMP;
```

Use for: schema changes, data modifications that must be atomic.

Cannot use: `CREATE INDEX CONCURRENTLY`, `ALTER TYPE ... ADD VALUE`

### `non-transactional`

No transaction wrapper. Required for concurrent operations.

```sql
-- pgmt:section name="concurrent_index" mode="non-transactional" retry_attempts="10" retry_delay="5s"
CREATE INDEX CONCURRENTLY idx_users_email ON users(email);
```

Use for: `CREATE INDEX CONCURRENTLY`, `ALTER TYPE ... ADD VALUE`, long-running operations.

Note: No automatic rollback. If it fails partway, you may need manual cleanup.

### `autocommit`

Each statement commits independently. Good for batched operations.

```sql
-- pgmt:section name="backfill" mode="autocommit" timeout="30m"
UPDATE users SET status = 'active' WHERE status IS NULL;
```

Use for: large updates where you want progress saved incrementally.

## Retry Logic

Concurrent operations often fail due to lock contention. Add retry logic:

```sql
-- pgmt:section name="create_index"
-- pgmt:  mode="non-transactional"
-- pgmt:  timeout="2s"
-- pgmt:  retry_attempts="10"
-- pgmt:  retry_delay="5s"
-- pgmt:  on_lock_timeout="retry"
CREATE INDEX CONCURRENTLY idx_users_status ON users(status);
```

Execution: Try with 2s timeout → if lock timeout, wait 5s → retry up to 10 times.

### Exponential Backoff

Increase delay between retries:

```sql
-- pgmt:  retry_delay="2s"
-- pgmt:  retry_backoff="exponential"
```

Delays: 2s → 4s → 8s → 16s → ...

## Progress and Resume

pgmt tracks section completion. If a migration fails partway:

```
✓ Section 1/3: add_column (completed)
✓ Section 2/3: backfill (completed)
✗ Section 3/3: create_index (failed after 10 attempts)
```

Run `pgmt migrate apply` again - it skips completed sections and retries from the failure:

```
⊙ Section 1/3: add_column (skipping - already completed)
⊙ Section 2/3: backfill (skipping - already completed)
↻ Section 3/3: create_index (retrying)
```

Check status:

```bash
pgmt migrate status --sections
```

## Common Patterns

### Zero-Downtime Column Addition

```sql
-- pgmt:section name="add_nullable_column" timeout="5s"
ALTER TABLE orders ADD COLUMN priority TEXT;

-- pgmt:section name="backfill" mode="autocommit" timeout="30m" batch_size="10000"
UPDATE orders SET priority = CASE
    WHEN total > 1000 THEN 'high'
    WHEN total > 100 THEN 'medium'
    ELSE 'low'
END WHERE priority IS NULL;

-- pgmt:section name="create_index" mode="non-transactional" retry_attempts="15" retry_delay="10s"
CREATE INDEX CONCURRENTLY idx_orders_priority ON orders(priority);

-- pgmt:section name="add_constraint" timeout="10s"
ALTER TABLE orders ALTER COLUMN priority SET NOT NULL;
```

Why: Add nullable first (fast), backfill data, create index without blocking writes, then add NOT NULL.

### Enum Value Addition

`ALTER TYPE ... ADD VALUE` cannot run in a transaction:

```sql
-- pgmt:section name="add_enum_value" mode="non-transactional" retry_attempts="5" retry_delay="3s"
ALTER TYPE user_role ADD VALUE IF NOT EXISTS 'moderator';

-- pgmt:section name="update_users" timeout="5m"
UPDATE users SET role = 'moderator' WHERE is_moderator = true;
```

## Validation

Preview what will run:

```bash
pgmt migrate apply --dry-run --sections
```

Validate section syntax:

```bash
pgmt migrate validate --sections
```

## Troubleshooting

**Lock timeout after all retries:**

- Increase `retry_attempts` or `timeout`
- Add `retry_backoff="exponential"`
- Run during low-traffic window

**Section partially completed:**

- pgmt tracks progress - just run `migrate apply` again
- Use `--force-restart` only if you need to re-run from scratch

**"Cannot use CONCURRENTLY in transaction":**

- Change `mode="transactional"` to `mode="non-transactional"`

## Best Practices

- Use descriptive section names (`add_priority_column` not `section1`)
- Always add retry logic for `CONCURRENTLY` operations
- Set short timeouts with many retries for lock-sensitive operations
- Use `autocommit` mode for large data updates
- Test multi-section migrations in staging with production-like data
