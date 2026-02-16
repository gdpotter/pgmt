---
title: Using pgmt with Supabase
description: Manage your Supabase database schema with pgmt while keeping platform-managed schemas intact.
---

Supabase injects 25+ schemas into your database (`auth`, `storage`, `realtime`, `vault`, `extensions`, and more). pgmt works alongside Supabase — you manage your application schema, Supabase manages its platform infrastructure.

## How It Works

pgmt needs to know which schemas are yours and which belong to Supabase. You tell it with `objects.include.schemas`:

- **Your schemas** (`public`, plus any you create) get managed by pgmt — cleaned, diffed, and migrated
- **Supabase schemas** (`auth`, `storage`, `realtime`, etc.) are preserved in the shadow database and ignored during diffing
- **Foreign keys** to Supabase tables (like `auth.users`) work because the shadow database runs the same Supabase PostgreSQL image

## Prerequisites

- [pgmt installed](/docs/getting-started/quick-start)
- [Supabase CLI](https://supabase.com/docs/guides/local-development/cli/getting-started) installed
- Docker running (for both Supabase and pgmt's shadow database)

## Setup

### 1. Start Supabase Locally

```bash
supabase init    # If you haven't already
supabase start
```

This starts a local Supabase instance with PostgreSQL on port 54322.

### 2. Initialize pgmt

```bash
pgmt init --dev-url postgres://postgres:postgres@127.0.0.1:54322/postgres --defaults
```

### 3. Configure pgmt.yaml

Update the generated config to use the Supabase PostgreSQL image and scope pgmt to your schemas:

```yaml
databases:
  dev_url: postgres://postgres:postgres@127.0.0.1:54322/postgres

  shadow:
    docker:
      image: public.ecr.aws/supabase/postgres:17.6.1.081
      environment:
        POSTGRES_USER: supabase_admin
        POSTGRES_PASSWORD: your-super-secret-and-long-postgres-password

objects:
  include:
    schemas:
      - public
```

**Why `POSTGRES_USER: supabase_admin`?** The Supabase image's init scripts expect a superuser named `supabase_admin`. If you omit this, pgmt defaults to `POSTGRES_USER=postgres`, and the Supabase init scripts fail — the container exits immediately with an unhelpful error. Always set `POSTGRES_USER: supabase_admin` when using the Supabase image.

**Why the Supabase image?** The shadow database needs the same extensions your schema might use (`pgcrypto`, `pg_graphql`, `pgsodium`, etc.). These are C extensions that only exist in the Supabase PostgreSQL build. If your schema only uses standard PostgreSQL features, you can skip the `shadow.docker` section entirely and use the default `postgres:alpine` image.

**Why `include.schemas`?** This tells pgmt to only manage the `public` schema. Without it, pgmt would try to diff and clean all 25+ Supabase schemas, generating incorrect migrations.

Add any additional application schemas you create:

```yaml
objects:
  include:
    schemas:
      - public
      - app
      - api
```

## Write Your Schema

Create schema files as normal. You can reference Supabase objects like `auth.users`:

```sql
-- schema/profiles.sql
CREATE TABLE public.profiles (
    id UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
    display_name TEXT,
    avatar_url TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);
```

The foreign key to `auth.users` validates correctly because the shadow database runs the Supabase image, which includes the `auth` schema.

## Workflow

The standard pgmt workflow works unchanged:

```bash
# Apply schema to your local Supabase database
pgmt apply

# Preview what would change
pgmt diff

# Generate a migration for deployment
pgmt migrate new "add profiles table"
```

Migrations will only contain changes to your managed schemas — no Supabase platform objects.

## Deploying to Production

Generated migrations can be applied to your hosted Supabase project:

```bash
pgmt migrate apply --target-url "$SUPABASE_DB_URL"
```

Or use the Supabase dashboard to run the migration SQL manually.

### Using `supabase db push`

pgmt generates migration filenames without a prefix by default (e.g., `1734567890_add_profiles_table.sql`), which is compatible with Supabase's migration format. To use pgmt-generated migrations with `supabase db push`:

1. Configure pgmt to write migrations into Supabase's migrations directory:

```yaml
directories:
  migrations_dir: supabase/migrations
```

2. Generate migrations normally:

```bash
pgmt migrate new "add profiles table"
# Creates: supabase/migrations/1734567890_add_profiles_table.sql
```

3. Deploy with `supabase db push`:

```bash
supabase db push
```

**Trade-offs:** Multi-section migrations (concurrent index creation, retry logic) won't work through `supabase db push` — use `pgmt migrate apply` for those. For simple DDL migrations, either deployment method works.

**Local development:** `supabase db reset` works as a "clean slate" option, while `pgmt apply --watch` is recommended for iterative development since it applies changes incrementally without resetting your data.
