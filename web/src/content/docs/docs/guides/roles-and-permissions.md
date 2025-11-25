---
title: Roles and Permissions
description: How pgmt manages database grants and permissions while keeping role management external.
---

pgmt manages GRANT and REVOKE statements on database objects. It does **not** manage role creation, role attributes, or role membership - those are handled externally through SQL scripts, Terraform, or your preferred tools.

## Grants in Schema Files

Define grants alongside the objects they protect:

```sql
-- schema/tables/users.sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL
);

GRANT SELECT ON users TO app_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON users TO app_readwrite;
```

When you run `pgmt apply` or `pgmt migrate new`, these grants are detected, included in migrations, and applied to target databases.

## Creating Roles Externally

Roles must exist before pgmt applies grants. Create them however you prefer:

```sql
-- Run once per environment
CREATE ROLE app_readonly WITH LOGIN PASSWORD 'readonly_password';
CREATE ROLE app_readwrite WITH LOGIN PASSWORD 'readwrite_password';
```

Or use Terraform, Ansible, deployment scripts, etc.

## Shadow Database Roles

The shadow database needs roles to exist for grant validation. Create a `roles.sql` file:

```sql
-- roles.sql
-- Roles for shadow database validation only
-- Production roles are managed externally

-- PostgreSQL doesn't support CREATE ROLE IF NOT EXISTS, so use DO blocks
DO $$ BEGIN CREATE ROLE app_readonly; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE ROLE app_readwrite; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE ROLE app_admin; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
```

Configure it in `pgmt.yaml`:

```yaml
directories:
  roles_file: roles.sql
```

When pgmt creates a shadow database, it applies `roles.sql` first, then loads your schema files. This lets grants validate without errors.

## Schema-Level Grants

For broader permissions:

```sql
-- schema/analytics/schema.sql
CREATE SCHEMA IF NOT EXISTS analytics;

GRANT USAGE ON SCHEMA analytics TO analytics_team;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO analytics_team;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics
    GRANT SELECT ON TABLES TO analytics_team;
```

## Troubleshooting

**"role does not exist" in shadow database:**

Add the missing role to `roles.sql`:

```sql
DO $$ BEGIN CREATE ROLE missing_role; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
```

**Grants not applied to production:**

Ensure roles exist before running migrations:

```bash
# Create roles first (use DO block for idempotency)
psql $PROD_URL -c "DO \$\$ BEGIN CREATE ROLE app_user WITH LOGIN PASSWORD '$PASSWORD'; EXCEPTION WHEN duplicate_object THEN NULL; END \$\$;"

# Then apply migrations
pgmt migrate apply --target-url $PROD_URL
```

**Different role names per environment:**

This is an anti-pattern. Use the same role names everywhere - vary credentials, not names.
