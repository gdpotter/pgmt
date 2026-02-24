# pgmt — PostgreSQL Schema-as-Code

[![CI](https://github.com/gdpotter/pgmt/actions/workflows/ci.yml/badge.svg)](https://github.com/gdpotter/pgmt/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/pgmt)](https://crates.io/crates/pgmt)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Edit database objects like code. pgmt tracks dependencies, applies changes instantly, and generates production-ready migrations.

> **Status: Alpha** — API may change. Production use requires careful evaluation.

![pgmt demo](demos/out/demo.gif)

One function changed. pgmt detected 3 dependent views, dropped them in the right order, applied the update, and recreated everything — automatically.

## Install

```bash
# Shell (macOS/Linux)
curl -fsSL https://pgmt.dev/install.sh | sh

# npm
npm install -g @pgmt/pgmt

# Cargo (from source)
cargo install pgmt
```

## Quick Start

```bash
# Initialize (new project or from existing database)
pgmt init
pgmt init --dev-url postgres://localhost/my_existing_db

# Edit schema files, then apply to dev
pgmt apply

# Generate migration when ready to ship
pgmt migrate new "add user analytics"

# Deploy to production
pgmt migrate apply --target-url $PROD_DATABASE_URL
```

## Watch Mode

Edit a schema file, save — your dev database updates instantly. No migration files during development, no manual dependency management. The database finally works like the rest of your development environment.

```bash
$ pgmt apply --watch
👀 Watching schema/ for changes...

  schema/functions/calculate_score.sql changed

📋 8 changes
  ✓ Drop view public.executive_dashboard
  ...
  ✓ Create view public.executive_dashboard

✅ Applied 8 changes
```

## Production Migrations

When you're done iterating, `pgmt migrate new` generates an explicit SQL migration file. You review it, edit it if needed, and deploy it. Nothing touches production without your approval.

Multi-section migrations handle the hard stuff — concurrent indexes, data backfills, different timeouts — all in one file with per-section retries:

```sql
-- pgmt:section name="add_column" timeout="5s"
ALTER TABLE users ADD COLUMN verified BOOLEAN;

-- pgmt:section name="backfill" mode="autocommit" timeout="30m"
UPDATE users SET verified = false WHERE verified IS NULL;

-- pgmt:section name="add_index" mode="non-transactional" retry_attempts="10"
CREATE INDEX CONCURRENTLY idx_users_verified ON users(verified);
```

## Documentation

- [Quick Start](https://gdpotter.github.io/pgmt/docs/getting-started/quick-start) — Get up and running
- [Adopt Existing Database](https://gdpotter.github.io/pgmt/docs/guides/existing-database) — Import existing schema
- [CLI Reference](https://gdpotter.github.io/pgmt/docs/cli/) — All commands and options
- [Blog](https://gdpotter.github.io/pgmt/blog) — Why schema-as-code?

## Requirements

- **PostgreSQL 13+** (tested on 13–18)
- **Rust 1.74+** for building from source

## Contributing

```bash
./scripts/test-setup.sh   # One-time setup
cargo test                 # Run tests
```

See the [contributing guide](https://gdpotter.github.io/pgmt/docs/development/contributing) for details.

## License

MIT — see [LICENSE](LICENSE).
