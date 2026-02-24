---
title: Quick Start
description: See why pgmt is different in 10 minutes. Install, create schema, and experience automatic dependency handling.
---

Get started with pgmt in 10 minutes and see what makes it different from other migration tools.

## Install pgmt

**Shell (macOS/Linux):**

```bash
curl -fsSL https://pgmt.dev/install.sh | sh
```

**npm:**

```bash
npm install -g @pgmt/pgmt
```

**Cargo (from source):**

```bash
cargo install pgmt
```

Verify installation:

```bash
pgmt --version
```

## Setup Database

pgmt requires PostgreSQL 13 or later. Choose your preferred option:

**Docker (Recommended):**

```bash
docker run -d \
  --name pgmt-dev \
  -e POSTGRES_PASSWORD=dev \
  -e POSTGRES_DB=myapp_dev \
  -p 5432:5432 \
  postgres:15
```

**Already have PostgreSQL?** Just create a database:

```bash
createdb myapp_dev
```

## Initialize Project

```bash
# Create project directory
mkdir my-app && cd my-app

# Initialize pgmt (Docker users)
pgmt init --dev-url postgres://postgres:dev@localhost/myapp_dev --defaults

# For local PostgreSQL without password
# pgmt init --dev-url postgres://localhost/myapp_dev --defaults
```

This creates:

```
my-app/
├── schema/               # Your SQL schema files
├── migrations/           # Generated migration files
├── schema_baselines/     # Baseline snapshots (created on-demand)
└── pgmt.yaml            # Configuration
```

**Want more control?** Run `pgmt init` without `--defaults` for an interactive setup that:

- Shows you exactly what's in your database (if importing existing schema)
- Lets you configure which object types to manage (comments, grants, triggers, extensions)
- Automatically validates generated schema files
- Provides clear guidance if dependency issues are found

## Create Your First Schema

Create a simple table:

```bash
cat > schema/users.sql << 'EOF'
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    full_name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
EOF
```

Now add a view that depends on it:

```bash
cat > schema/active_users.sql << 'EOF'
-- require: users.sql
CREATE VIEW active_users AS
SELECT * FROM users;
EOF
```

**Notice the `-- require: users.sql`?** This is like `import` in Python or `require` in Node.js - it declares dependencies and lets you organize your schema however makes sense for your project.

Apply the schema to your dev database:

```bash
pgmt apply
```

pgmt will:

- Create a shadow database
- Load your schema files in dependency order
- Compare with your dev database
- Apply the necessary changes

## Automatic Dependency Tracking

Here's what makes pgmt special. Let's rename a column in the base table:

```bash
cat > schema/users.sql << 'EOF'
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    name TEXT NOT NULL,       -- Renamed from full_name
    created_at TIMESTAMP DEFAULT NOW()
);
EOF
```

Now apply the change:

```bash
pgmt apply
```

**What just happened?**

pgmt automatically:

1. Dropped the `active_users` view (because the column structure changed)
2. Renamed the column: `ALTER TABLE users RENAME COLUMN full_name TO name`
3. Recreated the `active_users` view with the same definition

The view file didn't change - it still says `SELECT * FROM users` - but pgmt knows it needs to be recreated because the underlying table structure changed.

### Make It More Complex

Let's add another view that depends on the first one:

```bash
cat > schema/recent_users.sql << 'EOF'
-- require: active_users.sql
CREATE VIEW recent_users AS
SELECT * FROM active_users
WHERE created_at > NOW() - INTERVAL '7 days';
EOF
```

Apply it:

```bash
pgmt apply
```

Now update the `active_users` view definition to add a filter:

```bash
cat > schema/active_users.sql << 'EOF'
-- require: users.sql
CREATE VIEW active_users AS
SELECT * FROM users WHERE email IS NOT NULL;  -- Added filter
EOF
```

Apply again:

```bash
pgmt apply
```

pgmt automatically:

1. Dropped `recent_users` (depends on active_users)
2. Dropped `active_users` (the view we're changing)
3. Created `active_users` (new definition with filter)
4. Created `recent_users` (unchanged, but depends on active_users)

All in the correct order, based on the dependency graph.

### Why This Matters

With traditional migration tools, you'd manually write:

```sql
-- Hope you remember all the dependencies!
DROP VIEW recent_users;
DROP VIEW active_users;
CREATE VIEW active_users AS ...;  -- Better get the order right
CREATE VIEW recent_users AS ...;
```

With pgmt, you edit views and functions like source code. pgmt handles the drop/recreate mechanics, figures out the dependency cascade, and applies changes in the correct order. No manual migration scripts needed.

## Schema Organization

The `-- require:` statements aren't just for dependency resolution - they're for organizing your schema like a real programming language:

```bash
schema/
├── 01_foundation/
│   └── extensions.sql       # PostgreSQL extensions
├── 02_core/
│   ├── users.sql           # Core entities
│   └── posts.sql           # require: users.sql
└── 03_features/
    └── analytics.sql       # require: users.sql, posts.sql
```

Organize by domain, by feature, by team ownership - whatever makes sense for YOUR project. pgmt handles the dependency graph automatically.

## Generate Production Migration

When you're ready to deploy to production, generate an explicit migration:

```bash
pgmt migrate new "initial schema with user views"
```

This creates a migration file like:

```
migrations/1734567890_initial_schema_with_user_views.sql
```

Review the generated SQL:

```bash
cat migrations/*_initial_schema_with_user_views.sql
```

You'll see explicit SQL statements that you can review, test, and version control before deploying to production.

## Pro Tips

### Watch Mode for Active Development

```bash
pgmt apply --watch
```

Automatically applies safe changes as you edit schema files. Prompts for confirmation on destructive operations (like dropping columns).

### Preview Changes Before Applying

```bash
pgmt apply --dry-run
```

See what would change without actually applying it.

### Check Migration Status

```bash
pgmt migrate status
```

See which migrations have been applied to your database.

## Next Steps

### Learn the Workflow

**Organize your schema:**

- [Schema Organization Guide](/docs/guides/schema-organization) - Multi-file patterns, `-- require:` best practices

**Work with a team:**

- [CI/CD Integration](/docs/guides/ci-cd) - Automated migrations, CI pipelines, deployment strategies

**Handle complex changes:**

- [Migration Workflow](/docs/guides/migration-workflow) - Creating, editing, and deploying migrations

### Have an Existing Database?

- [Adopt Existing Database](/docs/guides/existing-database) - Import existing schema, create baselines, team onboarding

### Reference & Support

- [CLI Reference](/docs/cli/) - All commands and options
- [Configuration Guide](/docs/reference/configuration) - Customize pgmt for your environment
- [PostgreSQL Features](/docs/reference/supported-features) - Complete list of supported database objects
- [Troubleshooting](/docs/guides/troubleshooting) - Common issues and solutions
