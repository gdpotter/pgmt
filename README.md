# pgmt — PostgreSQL Schema-as-Code

> **Database-first development for PostgreSQL**
> Manage schema like code, deploy through explicit migrations

pgmt is a PostgreSQL migration tool that lets you develop your schema like software code — edit database objects directly, see changes immediately, then generate explicit migrations for production deployment.

## Key Benefits

- **Code-like Development**: Edit views, functions, triggers, and grants like source code — pgmt handles drop/recreate automatically
- **Explicit Control**: Review generated migrations before deployment, not after
- **Full PostgreSQL Support**: Triggers, functions, enums, arrays, JSON, extensions, grants — not just tables
- **Team Friendly**: Schema files show *intent*, migrations show *deployment steps*
- **Production Safe**: Shadow databases, dependency ordering, sectioned migrations

## Quick Start

Install pgmt:
```bash
cargo install --git https://github.com/gdpotter/pgmt.git
```

Initialize your project:
```bash
# New project
pgmt init

# From existing database
pgmt init --dev-url postgres://localhost/my_existing_db
```

Development workflow:
```bash
# 1. Edit schema files like code
vim schema/views/user_analytics.sql
vim schema/functions/calculate_score.sql

# 2. Apply immediately to dev database
pgmt apply

# 3. Generate migration when ready
pgmt migrate new "add user analytics"

# 4. Deploy to production
pgmt migrate apply --target-url $PROD_DATABASE_URL
```

## Documentation

- [Quick Start](https://gdpotter.github.io/pgmt/docs/getting-started/quick-start) — Get up and running
- [Adopt Existing Database](https://gdpotter.github.io/pgmt/docs/guides/existing-database) — Import existing schema with baselines
- [Why Schema-as-Code?](https://gdpotter.github.io/pgmt/docs/concepts/philosophy) — The declarative + explicit approach
- [Schema Organization](https://gdpotter.github.io/pgmt/docs/guides/schema-organization) — Multi-file organization and `-- require:` syntax
- [CLI Reference](https://gdpotter.github.io/pgmt/docs/cli/) — All commands and options

## PostgreSQL Support

**Core Objects:** Tables, Views, Functions, Triggers, Indexes, Constraints, Custom Types, Sequences, Schemas, Extensions
**Advanced Features:** Grants & Privileges, Comments, Complex Constraints, All Index Types, Function Overloading
**PostgreSQL-Specific:** ENUMs, Arrays, JSON/JSONB, Exclusion Constraints, Partial Indexes, Expression Indexes

See the [complete feature matrix](https://gdpotter.github.io/pgmt/docs/reference/supported-features) for details.

## Role & Permissions Management

pgmt manages database object privileges (GRANT/REVOKE) but not roles themselves — create roles using your preferred tools (SQL, Terraform, etc.), then define grants in schema files.

See the [Roles & Permissions Guide](https://gdpotter.github.io/pgmt/docs/guides/roles-and-permissions).

## Roadmap

**Near-term:** Row-level security, advanced function features, smart rename detection
**Long-term:** Schema visualization, migration templates

See the [complete roadmap](https://gdpotter.github.io/pgmt/docs/project/roadmap).

## Development & Contributing

```bash
# Quick setup (one-time)
./scripts/test-setup.sh

# Run tests
cargo test

# Test against specific PostgreSQL version
DATABASE_URL=$(./scripts/test-db-url.sh 18) cargo test

# Build from source
SQLX_OFFLINE=true cargo build
```

See the [contributing guide](https://gdpotter.github.io/pgmt/docs/development/contributing) for more details.

## Community & Support

- [Documentation](https://gdpotter.github.io/pgmt/) — Guides and reference
- [GitHub Discussions](https://github.com/gdpotter/pgmt/discussions) — Questions and patterns
- [GitHub Issues](https://github.com/gdpotter/pgmt/issues) — Bug reports and feature requests

## License

MIT License — see [LICENSE](LICENSE) for details.
