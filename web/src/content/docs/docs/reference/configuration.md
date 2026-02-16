---
title: Configuration
description: Complete pgmt.yaml configuration reference.
---

pgmt uses `pgmt.yaml` for project configuration. CLI arguments override config file values.

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
  dev_url: postgres://localhost/myapp_dev # Development database (required)
  target_url: postgres://prod/myapp # Target for migrate apply (optional)

  shadow:
    auto: true # Auto-create shadow database (recommended)
    # OR manual:
    # url: postgres://localhost/myapp_shadow

    # OR Docker with specific version:
    # docker:
    #   version: '16'                           # PostgreSQL version
    #   auto_cleanup: true                      # Clean up container after use

    # OR Docker with a custom image (e.g. Supabase):
    # docker:
    #   image: public.ecr.aws/supabase/postgres:17.6.1.081
    #   environment:
    #     POSTGRES_PASSWORD: your-password       # Custom env vars
```

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

  comments: true # Manage object comments
  grants: true # Manage permissions
  triggers: true # Manage triggers
  extensions: true # Manage extensions
```

### migration

```yaml
migration:
  default_mode: safe_only # safe_only | confirm_all | force_all
  validate_baseline_consistency: true
  create_baselines_by_default: false # On-demand (recommended)
  column_order: strict # strict | warn | relaxed
  filename_prefix: "" # Default: no prefix. Set to "V" for Flyway compatibility

  tracking_table:
    schema: public
    name: pgmt_migrations
```

### schema

```yaml
schema:
  augment_dependencies_from_files: true # Use -- require: directives
  validate_file_dependencies: true
```

### docker

```yaml
docker:
  auto_cleanup: true
  check_system_identifier: true
```

## Environment Variables

```bash
PGMT_CONFIG_FILE              # Override config file location
PGMT_DEV_URL                  # Override dev database URL
PGMT_SHADOW_URL               # Override shadow database URL
PGMT_TARGET_URL               # Override target database URL
PGMT_KEEP_SHADOW_ON_FAILURE   # Keep shadow container alive on startup failure (for debugging)
```

Use `${VAR}` syntax in config files to reference environment variables:

```yaml
databases:
  dev_url: ${DEV_DATABASE_URL}
  target_url: ${PROD_DATABASE_URL}
```

## CLI Overrides

```bash
pgmt apply --dev-url postgres://localhost/other_db
pgmt apply --schema-dir custom_schema/
pgmt apply --exclude-schemas "temp_*,cache_*"
pgmt apply --no-comments --no-grants
```

## Defaults

| Option                                  | Default                          |
| --------------------------------------- | -------------------------------- |
| `databases.shadow.auto`                 | `true`                           |
| `directories.schema_dir`                | `schema`                         |
| `directories.migrations_dir`            | `migrations`                     |
| `directories.baselines_dir`             | `schema_baselines`               |
| `objects.exclude.schemas`               | `["pg_*", "information_schema"]` |
| `objects.comments`                      | `true`                           |
| `objects.grants`                        | `true`                           |
| `objects.triggers`                      | `true`                           |
| `objects.extensions`                    | `true`                           |
| `migration.default_mode`                | `safe_only`                      |
| `migration.create_baselines_by_default` | `false`                          |
| `migration.column_order`                | `strict`                         |
| `migration.filename_prefix`             | `""` (empty)                     |
| `migration.tracking_table.schema`       | `public`                         |
| `migration.tracking_table.name`         | `pgmt_migrations`                |
