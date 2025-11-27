---
title: CLI Reference
description: Complete command reference for pgmt.
---

## Quick Reference

| Command                 | Purpose                             |
| ----------------------- | ----------------------------------- |
| `pgmt init`             | Initialize new project              |
| `pgmt apply`            | Apply schema to dev database        |
| `pgmt diff`             | Preview what apply would do         |
| `pgmt migrate new`      | Generate migration                  |
| `pgmt migrate update`   | Regenerate migration after changes  |
| `pgmt migrate apply`    | Apply migrations to target database |
| `pgmt migrate status`   | Show migration status               |
| `pgmt migrate validate` | Validate migrations match schema    |
| `pgmt migrate diff`     | Detect drift in target database     |
| `pgmt baseline create`  | Create baseline snapshot            |
| `pgmt baseline list`    | List baselines                      |
| `pgmt baseline clean`   | Remove old baselines                |

## Global Options

Available for all commands:

```bash
-h, --help                    # Show help
-V, --version                 # Show version
--config-file <FILE>          # Config file (default: pgmt.yaml)
-v, --verbose                 # Verbose output
-q, --quiet                   # Suppress non-essential output
--debug                       # Debug output
```

Database overrides:

```bash
--dev-url <URL>               # Development database
--shadow-url <URL>            # Shadow database
--target-url <URL>            # Target/production database
```

Directory overrides:

```bash
--schema-dir <PATH>           # Schema directory
--migrations-dir <PATH>       # Migrations directory
--baselines-dir <PATH>        # Baselines directory
```

---

## pgmt init

Initialize a new pgmt project. Can be re-run to update an existing configuration.

```bash
pgmt init [OPTIONS]
```

**Options:**

```bash
--dev-url <URL>               # Database URL (required)
--create-baseline             # Create baseline from existing database
--no-baseline                 # Skip baseline creation
--no-import                   # Skip import (empty project)
--defaults                    # Use defaults for all prompts
--schema-dir <DIR>            # Schema directory name (default: "schema")
--migrations-dir <DIR>        # Migrations directory name (default: "migrations")
--baselines-dir <DIR>         # Baselines directory name (default: "schema_baselines")
--auto-shadow                 # Use auto shadow database
--shadow-pg-version <VER>     # PostgreSQL version for auto shadow (e.g., "14", "15", "16")
--roles-file <PATH>           # Path to roles file (default: auto-detect roles.sql)
--fresh                       # Force fresh init (overwrite existing config)
```

**Re-initialization:**

When run in a directory with an existing `pgmt.yaml`, you'll be prompted to:

- **Update** - Modify existing config (shows current values as defaults)
- **Fresh** - Start over with new configuration
- **Cancel** - Keep current configuration

Use `--fresh` to skip this prompt and always overwrite.

**Examples:**

```bash
# Interactive (recommended)
pgmt init --dev-url postgres://localhost/myapp_dev

# Import existing database with baseline
pgmt init --dev-url postgres://localhost/existing_db --create-baseline

# Empty project
pgmt init --defaults --no-import

# Custom directory structure
pgmt init --dev-url postgres://localhost/myapp_dev \
    --schema-dir db/schema \
    --migrations-dir db/migrations \
    --baselines-dir db/baselines

# Specify PostgreSQL version for shadow database
pgmt init --dev-url postgres://localhost/myapp_dev --auto-shadow --shadow-pg-version 14

# Re-initialize with fresh config
pgmt init --dev-url postgres://localhost/newdb --fresh
```

---

## pgmt apply

Apply schema files to the development database.

```bash
pgmt apply [OPTIONS]
```

**Options:**

```bash
--watch                       # Watch for file changes
--dry-run                     # Preview changes without applying
--auto-safe                   # Auto-apply safe changes, prompt for destructive
--safe-only                   # Apply only safe changes
--confirm-all                 # Prompt for each change
--force-all                   # Apply all without prompts
```

**Examples:**

```bash
pgmt apply                    # Interactive
pgmt apply --watch            # Watch mode (auto-safe by default)
pgmt apply --dry-run          # Preview only
pgmt apply --safe-only        # Skip destructive changes
```

---

## pgmt diff

Compare schema files against the development database. Shows what `pgmt apply` would do.

```bash
pgmt diff [OPTIONS]
```

**Options:**

```bash
--format <FORMAT>             # detailed | summary | sql | json
--output-sql <FILE>           # Save SQL to file
```

**Examples:**

```bash
pgmt diff                     # Detailed comparison
pgmt diff --format summary    # Quick overview
pgmt diff --format sql        # SQL to sync
```

**Exit codes:** `0` = no differences, `1` = differences found

---

## pgmt migrate new

Generate a new migration based on schema changes.

```bash
pgmt migrate new [DESCRIPTION] [OPTIONS]
```

**Options:**

```bash
--create-baseline             # Create baseline alongside migration
```

**Examples:**

```bash
pgmt migrate new "add users table"
pgmt migrate new "v2.0 release" --create-baseline
pgmt migrate new                  # Interactive (prompts for description)
```

---

## pgmt migrate update

Regenerate a migration after schema changes or when your branch is behind.

```bash
pgmt migrate update [VERSION] [OPTIONS]
```

**Arguments:**

```bash
VERSION                       # Migration version (e.g., V1234567890 or partial 123456)
                              # If omitted, updates the latest migration
```

**Options:**

```bash
--dry-run                     # Preview without updating
--backup                      # Create .bak file before updating
```

**Examples:**

```bash
pgmt migrate update V1734567890   # Update specific migration
pgmt migrate update --dry-run     # Preview
pgmt migrate update --backup      # Update with backup
```

**Note:** This regenerates the migration from scratch. Manual edits will be lost.

---

## pgmt migrate apply

Apply migrations to a target database.

```bash
pgmt migrate apply [OPTIONS]
```

**Options:**

```bash
--target-url <URL>            # Target database (required unless configured)
```

**Examples:**

```bash
pgmt migrate apply --target-url postgres://prod/myapp
pgmt migrate apply            # Uses target_url from pgmt.yaml
```

---

## pgmt migrate status

Show migration status for a target database.

```bash
pgmt migrate status [OPTIONS]
```

**Options:**

```bash
--target-url <URL>            # Target database
```

**Example output:**

```
Applied:
  V1734500000__create_users.sql     (2024-12-18 10:00)
  V1734510000__add_posts.sql        (2024-12-18 11:00)

Pending:
  V1734520000__add_comments.sql
```

---

## pgmt migrate validate

Validate that migrations produce the expected schema. Use in CI to catch missing migrations.

```bash
pgmt migrate validate [OPTIONS]
```

**Options:**

```bash
--format <FORMAT>             # human | json
--verbose                     # Detailed output
```

**Examples:**

```bash
pgmt migrate validate
pgmt migrate validate --format json
```

**Exit codes:** `0` = valid, `1` = mismatch

---

## pgmt migrate diff

Detect drift between schema files and target database.

```bash
pgmt migrate diff [OPTIONS]
```

**Options:**

```bash
--format <FORMAT>             # detailed | summary | sql | json
--output-sql <FILE>           # Save remediation SQL to file
--target-url <URL>            # Target database
```

**Examples:**

```bash
pgmt migrate diff --target-url postgres://prod/myapp
pgmt migrate diff --format sql --output-sql fix-drift.sql
```

**Exit codes:** `0` = no drift, `1` = drift detected

---

## pgmt baseline create

Create a baseline snapshot from current schema files.

```bash
pgmt baseline create [OPTIONS]
```

**Options:**

```bash
--description <DESC>          # Custom description
```

**Examples:**

```bash
pgmt baseline create
pgmt baseline create --description "Production v2.0"
```

---

## pgmt baseline list

List existing baselines.

```bash
pgmt baseline list [OPTIONS]
```

**Options:**

```bash
--format <FORMAT>             # table | json
```

---

## pgmt baseline clean

Remove old baselines.

```bash
pgmt baseline clean [OPTIONS]
```

**Options:**

```bash
--keep <N>                    # Keep N most recent (default: 5)
--older-than <DAYS>           # Remove baselines older than N days
--dry-run                     # Preview what would be deleted
```

**Examples:**

```bash
pgmt baseline clean --keep 5
pgmt baseline clean --older-than 30
pgmt baseline clean --keep 3 --dry-run
```

---

## pgmt config

Manage configuration.

```bash
pgmt config get <KEY>              # Get value
pgmt config set <KEY> <VALUE>      # Set value
pgmt config list                   # List all values
pgmt config validate               # Validate config file
```

**Examples:**

```bash
pgmt config get databases.dev
pgmt config set databases.dev postgres://localhost/myapp
pgmt config list --format json
```

---

## Environment Variables

```bash
PGMT_CONFIG_FILE              # Override config file location
PGMT_DEV_URL                  # Override dev database URL
PGMT_SHADOW_URL               # Override shadow database URL
PGMT_TARGET_URL               # Override target database URL
```

## Exit Codes

```
0  - Success
1  - General error
2  - Configuration error
3  - Database connection error
4  - Migration error
5  - Validation error
64 - Usage error (invalid arguments)
```
