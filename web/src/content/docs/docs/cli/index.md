---
title: CLI Reference
description: Complete command reference for pgmt.
---

## Quick Reference

| Command                      | Purpose                                  |
| ---------------------------- | ---------------------------------------- |
| `pgmt init`                  | Initialize new project                   |
| `pgmt apply`                 | Apply schema to dev database             |
| `pgmt diff`                  | Preview what apply would do              |
| `pgmt migrate new`           | Generate migration                       |
| `pgmt migrate update`        | Regenerate migration after changes       |
| `pgmt migrate apply`         | Apply migrations to target database      |
| `pgmt migrate provision`     | Set up a new database from a baseline    |
| `pgmt migrate status`        | Show migration status                    |
| `pgmt migrate validate`      | Validate migrations match schema         |
| `pgmt migrate diff`          | Detect drift in target database          |
| `pgmt migrate baseline`      | Create baseline / consolidate migrations |
| `pgmt migrate baseline list` | List baselines                           |
| `pgmt debug dependencies`    | Analyze object dependencies              |

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

Database connection flags (`--dev-url`, `--shadow-url`, `--target-url`)
appear only on commands that actually connect to that database — a command's
`--help` doubles as documentation of which databases it touches. Each flag
overrides the matching `PGMT_*` environment variable, which overrides
pgmt.yaml. Everything else (directories, object scoping, migration settings)
is project configuration and lives in pgmt.yaml only.

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
--shadow-image <IMAGE>        # Shadow Docker image (e.g. "postgis/postgis:16-3.5"); conflicts with --shadow-pg-version/--auto-shadow
--baseline-description <TEXT> # Custom description for the created baseline
--shadow-platform <PLATFORM>  # Platform for the shadow image (e.g. "linux/amd64") for single-arch images
--shadow-url <URL>            # Use an external shadow database at this URL (skips Docker)
--roles-file <PATH>           # Path to roles file (default: auto-detect roles.sql)
--fresh                       # Force fresh init (overwrite existing config)
```

**Re-initialization:**

When run in a directory with an existing `pgmt.yaml`, you'll be prompted to:

- **Update** - Modify existing config (shows current values as defaults)
- **Fresh** - Start over with new configuration
- **Cancel** - Keep current configuration

In Update mode, anything you've configured by hand and init doesn't ask about —
shadow `environment` and `container_name`, `objects` scoping, `migration`
settings — is preserved in the regenerated file, and the prompts are pre-filled
from your current values.

Use `--fresh` to skip this prompt and always overwrite.

**Config-first setup (advanced):**

`pgmt init` only asks the essentials; `pgmt.yaml` is the full interface (see
the [configuration reference](/docs/reference/configuration)). If your setup
needs options the wizard doesn't cover — a custom shadow image with environment
variables, schema scoping for a managed platform — write `pgmt.yaml` first,
then run `pgmt init` and choose **Update**: the import and baseline workflow
runs under your configuration. The shadow clean during import respects
`objects.include.schemas`, so platform-managed schemas are preserved.

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

# Extension-heavy schema (PostGIS) on an arm64 host: use an image that includes
# the extension, and run it under emulation since postgis/postgis is amd64-only
pgmt init --dev-url postgres://localhost/gis_db \
    --shadow-image postgis/postgis:16-3.5 \
    --shadow-platform linux/amd64

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
--dry-run                     # Preview changes without applying
--force                       # Apply all changes without confirmation
--safe-only                   # Apply only safe changes, skip destructive
--require-approval            # Fail if destructive changes exist
--watch                       # Watch for file changes
--dev-url <URL>               # Development database [env: PGMT_DEV_URL]
--shadow-url <URL>            # Shadow database [env: PGMT_SHADOW_URL]
```

**Default behavior:**

- **In terminal (interactive):** Auto-apply safe changes, prompt for destructive
- **In CI/pipes (non-interactive):** Fail with exit code 2 if destructive changes exist

**Exit codes:**

- `0`: Success (or no changes needed)
- `1`: Error
- `2`: Destructive changes exist (in `--require-approval` mode or non-interactive)

**Examples:**

```bash
pgmt apply                    # Interactive - prompts when needed
pgmt apply --watch            # Watch mode - shows which file changed
pgmt apply --dry-run          # Preview only
pgmt apply --force            # Apply all without prompts
pgmt apply --safe-only        # Skip destructive changes

# CI/CD usage:
pgmt apply                    # Fails (exit 2) if destructive ops exist
pgmt apply --force            # Forces all changes in CI
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
--dev-url <URL>               # Development database [env: PGMT_DEV_URL]
--shadow-url <URL>            # Shadow database [env: PGMT_SHADOW_URL]
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
--shadow-url <URL>            # Shadow database [env: PGMT_SHADOW_URL]
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
VERSION                       # Migration version (e.g., 1234567890, V1234567890, or partial 123456)
                              # If omitted, updates the latest migration
```

**Options:**

```bash
--dry-run                     # Preview without updating
--backup                      # Create .bak file before updating
--shadow-url <URL>            # Shadow database [env: PGMT_SHADOW_URL]
```

**Examples:**

```bash
pgmt migrate update 1734567890    # Update specific migration (V prefix also accepted)
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
--target-url <URL>            # Target database [env: PGMT_TARGET_URL] (required)
```

**Examples:**

```bash
pgmt migrate apply --target-url postgres://prod/myapp
pgmt migrate apply            # Uses target_url from pgmt.yaml
```

---

## pgmt migrate provision

Set up a new database from a baseline plus its post-baseline migrations. Use this for a fresh environment (demo, staging, preview, disaster recovery); `migrate apply` only maintains a database that's already established.

```bash
pgmt migrate provision [OPTIONS]
```

**Options:**

```bash
--target-url <URL>            # Target database [env: PGMT_TARGET_URL] (required)
--dry-run                     # Preview what would be applied, without changing the database
```

**What it does, based on the target's state:**

- **Empty + a baseline exists:** applies the baseline, records it, then applies the migrations after it.
- **No baseline in the repo:** replays all migrations from scratch.
- **Already provisioned:** applies any pending migrations (like `migrate apply`).
- **Already populated but unmanaged:** refuses, and points you at `pgmt init` to adopt it.

**Examples:**

```bash
pgmt migrate provision --target-url postgres://localhost/demo
pgmt migrate provision --target-url postgres://localhost/demo --dry-run
```

---

## pgmt migrate status

Show which migrations the development database has applied.

```bash
pgmt migrate status [OPTIONS]
```

**Options:**

```bash
--dev-url <URL>               # Development database [env: PGMT_DEV_URL]
```

**Example output:**

```
Applied:
  1734500000 - create_users (applied: 2024-12-18 10:00)
  1734510000 - add_posts (applied: 2024-12-18 11:00)

Pending:
  1734520000_add_comments.sql
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
--ignore-migrations <NAMES>   # Comma-separated migrations to skip during validation
--shadow-url <URL>            # Shadow database [env: PGMT_SHADOW_URL]
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
--target-url <URL>            # Target database [env: PGMT_TARGET_URL] (required)
--shadow-url <URL>            # Shadow database [env: PGMT_SHADOW_URL]
```

**Examples:**

```bash
pgmt migrate diff --target-url postgres://prod/myapp
pgmt migrate diff --format sql --output-sql fix-drift.sql
```

**Exit codes:** `0` = no drift, `1` = drift detected

---

## pgmt migrate baseline

Create a baseline from current schema files. By default, deletes all existing migrations and old baselines since the baseline supersedes them.

```bash
pgmt migrate baseline [OPTIONS]
```

**Options:**

```bash
--force                       # Skip baseline validation
--keep-migrations             # Don't delete old migrations
--dry-run                     # Preview what would happen
--shadow-url <URL>            # Shadow database [env: PGMT_SHADOW_URL]
```

**Examples:**

```bash
pgmt migrate baseline                      # Baseline + clean up migrations
pgmt migrate baseline --keep-migrations    # Baseline only, keep migrations
pgmt migrate baseline --dry-run            # Preview what would be deleted
```

---

## pgmt migrate baseline list

List existing baselines.

```bash
pgmt migrate baseline list
```

---

## pgmt debug dependencies

Analyze object dependencies from both PostgreSQL introspection and `-- require:` headers. Useful for troubleshooting dependency ordering issues.

```bash
pgmt debug dependencies [OPTIONS]
```

**Options:**

```bash
--format <FORMAT>             # json | text (default: json)
--object <NAME>               # Filter to specific object
--shadow-url <URL>            # Shadow database [env: PGMT_SHADOW_URL]
```

**Examples:**

```bash
pgmt debug dependencies                         # JSON output
pgmt debug dependencies --format text           # Human-readable
pgmt debug dependencies --object public.users   # Filter to one object
```

**Output includes:**

- All tracked objects and their types
- File-to-object mappings (which file defines which objects)
- File dependencies from `-- require:` headers
- Dependency graph showing what each object depends on

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

Connection variables override pgmt.yaml and are themselves overridden by the
matching CLI flag (flag > env > file):

```bash
PGMT_DEV_URL                  # Development database URL
PGMT_SHADOW_URL               # Shadow database URL (instead of auto Docker)
PGMT_TARGET_URL               # Target (production/staging) database URL
PGMT_KEEP_SHADOW_ON_FAILURE   # Keep the shadow container alive after a startup
                              # failure for debugging (any non-empty value)
RUST_LOG                      # Log filter (e.g. RUST_LOG=debug)
```

## Exit Codes

**General:**

| Code | Meaning         |
| ---- | --------------- |
| 0    | Success         |
| 1    | Error (general) |

**Command-specific:**

| Command                 | Code | Meaning                                                                     |
| ----------------------- | ---- | --------------------------------------------------------------------------- |
| `pgmt apply`            | 2    | Destructive operations exist (in non-interactive/`--require-approval` mode) |
| `pgmt diff`             | 1    | Differences detected                                                        |
| `pgmt migrate diff`     | 1    | Drift detected                                                              |
| `pgmt migrate validate` | 1    | Validation failed                                                           |
