---
title: Configuration
description: Complete pgmt.yaml configuration reference.
---

pgmt uses `pgmt.yaml` for project configuration. Database connections resolve
with the conventional precedence — **CLI flag > `PGMT_*` environment variable >
pgmt.yaml** — so the committed file is the shared team default and the env var
is the per-machine or per-environment override. Everything else (directories,
object scoping, migration settings) is project configuration and comes from
pgmt.yaml alone.

## Minimal Config

```yaml
databases:
  dev_url: postgres://localhost/myapp_dev
  shadow:
    auto: true
```

## Full Reference

### databases

```yaml
databases:
  dev_url: postgres://localhost/myapp_dev # Development database (required here or via PGMT_DEV_URL/--dev-url)
  target_url: postgres://prod/myapp # Target for migrate apply/diff (or PGMT_TARGET_URL)

  shadow:
    auto: true # Auto-create shadow database (recommended)
    # OR manual:
    # url: postgres://localhost/myapp_shadow
    # reset: clean # How the url shadow returns to baseline between runs:
    #   clean (default) — drop the schemas pgmt manages in the named database;
    #     never creates or drops databases
    #   branch — treat the named database as a read-only baseline: pgmt works
    #     on an ephemeral copy (CREATE DATABASE ... TEMPLATE) and drops it on
    #     exit, leaving the server as it found it. Requires CREATEDB and that
    #     nothing else connects to the named database. Ideal for CI service
    #     containers.
    # ⚠️ With reset: clean the named database is disposable: pgmt resets it
    # before validating or importing. Never point url at a database holding
    # data you care about.

    # OR Docker with specific version:
    # docker:
    #   version: '16'                           # PostgreSQL version
    #   auto_cleanup: true                      # Clean up container after use

    # OR Docker with a custom image (e.g. Supabase):
    # docker:
    #   image: public.ecr.aws/supabase/postgres:17.6.1.081
    #   environment:
    #     POSTGRES_PASSWORD: your-password       # Custom env vars

    # OR Docker with an extension image pinned to a platform (e.g. PostGIS).
    # The official postgis/postgis images are published for amd64 only, so on
    # arm64 hosts (Apple Silicon) request linux/amd64 to run under emulation:
    # docker:
    #   image: postgis/postgis:16-3.5
    #   platform: linux/amd64                    # Force a platform for single-arch images
```

> **Extension-heavy schemas (PostGIS, TimescaleDB, …):** the stock `postgres`
> image does not contain these extensions, so `auto`/`version` mode will fail at
> the first migration with errors like `type "geography" does not exist`. Point
> the shadow at an image that includes the extension. `pgmt init` detects this
> from the source database and warns you.

### directories

```yaml
directories:
  schema_dir: schema # Schema files
  migrations_dir: migrations # Migration files
  baselines_dir: schema_baselines # Baseline snapshots
  roles_file: roles.sql # Roles for shadow database
```

### objects

```yaml
objects:
  include:
    schemas: ['public', 'app'] # Only manage these schemas
    tables: ['users', 'orders'] # Only manage these tables

  exclude:
    schemas: ['pg_*', 'information_schema'] # Glob patterns supported
    tables: ['cache_*', 'temp_*']
```

There are no per-object-type toggles: your schema files are the source of
truth, so whatever they contain (grants, triggers, comments, …) is what pgmt
manages.

### migration

```yaml
migration:
  default_mode: safe_only # safe_only | confirm_all | force_all
  validate_baseline_consistency: true
  create_baselines_by_default: false # On-demand (recommended)
  column_order: strict # strict | warn | relaxed
  filename_prefix: '' # Default: no prefix. Set to "V" for Flyway compatibility

  tracking_table:
    schema: public
    name: pgmt_migrations
```

### schema

```yaml
schema:
  augment_dependencies_from_files: true # Use -- require: directives for ordering
  validate_file_dependencies: true
  verbose_file_processing: false # Per-file progress while loading schema files
```

These settings apply uniformly to every command that loads your schema files
(`apply`, `diff`, `migrate new/update/validate/diff`, `migrate baseline`) —
they all build the desired state through the same path.

### docker

```yaml
docker:
  auto_cleanup: true
  check_system_identifier: true
```

## Environment Variables

Each connection variable sits between the CLI flag and pgmt.yaml in
precedence (flag > env > file):

```bash
PGMT_DEV_URL                  # Development database URL
PGMT_SHADOW_URL               # Shadow database URL (instead of auto Docker)
PGMT_TARGET_URL               # Target (production/staging) database URL
PGMT_KEEP_SHADOW_ON_FAILURE   # Keep shadow container alive on startup failure (for debugging)
```

The target URL is the common one to supply via environment — CI typically sets
`PGMT_TARGET_URL` from a secret rather than committing it:

```yaml
# GitHub Actions
env:
  PGMT_TARGET_URL: ${{ secrets.PROD_DATABASE_URL }}
```

## CLI Overrides

Connection flags exist on exactly the commands that connect to that database
(see each command's `--help`):

```bash
pgmt apply --dev-url postgres://localhost/other_db
pgmt migrate apply --target-url postgres://staging-host/db
```

## Defaults

| Option                                  | Default                          |
| --------------------------------------- | -------------------------------- |
| `databases.shadow.auto`                 | `true`                           |
| `directories.schema_dir`                | `schema`                         |
| `directories.migrations_dir`            | `migrations`                     |
| `directories.baselines_dir`             | `schema_baselines`               |
| `objects.exclude.schemas`               | `["pg_*", "information_schema"]` |
| `migration.default_mode`                | `safe_only`                      |
| `migration.create_baselines_by_default` | `false`                          |
| `migration.column_order`                | `strict`                         |
| `migration.filename_prefix`             | `""` (empty)                     |
| `migration.tracking_table.schema`       | `public`                         |
| `migration.tracking_table.name`         | `pgmt_migrations`                |
