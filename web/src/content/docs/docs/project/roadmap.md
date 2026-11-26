---
title: Roadmap
description: Current status and future development plans for pgmt.
---

**Project Status:** Alpha - Approaching first release. Not yet recommended for production use.

_Last updated: November 2026_

## Current State

pgmt supports most PostgreSQL schema objects: tables, views, functions, aggregates, operators, casts, triggers, indexes, constraints, custom types, domains, collations, sequences, extensions, RLS policies, comments, and grants. See [Supported Features](/docs/reference/supported-features) for the complete list.

### Recently Shipped

- **Modules** (0.6.0): partition a schema into named modules and deploy different subsets to different databases. See [Modules](/docs/guides/modules).
- **Provisioning new environments** (0.6.0): `pgmt migrate provision` builds a fresh database from a baseline plus the migrations after it.
- **Safer deploys** (0.6.0): concurrent `migrate apply` runs serialize behind an advisory lock, failed sections resume where they stopped, and `pgmt migrate resolve` repairs tracking state.
- **Custom collations** (0.6.3) and **Podman** as a shadow database runtime (0.6.1).
- **Large-schema performance** (0.6.3–0.6.6): fewer round trips to remote databases, faster catalog reads, and batched statements in `pgmt apply`.
- **Clearer errors in `pgmt apply --watch`** (0.6.6): failures lead with the file, line, and Postgres error.

See the [changelog](https://github.com/gdpotter/pgmt/blob/main/CHANGELOG.md) for everything else.

## Future Directions

These are areas being explored based on community feedback:

- **Additional PostgreSQL objects** - Materialized views, partitioned tables
- **Object ownership** - Tracking `ALTER ... OWNER TO` alongside grants
- **Developer experience** - Continued work on error messages and large-schema performance
- **Distribution** - Native platform packages (Homebrew, APT, etc.)
- **Migration tooling** - Enhanced conflict resolution, rename detection

## Not In Scope

These features are intentionally outside pgmt's mission:

- **Non-PostgreSQL databases** - pgmt is PostgreSQL-specific by design
- **Data migration & ETL** - Focus is schema structure, not data transformation
- **Database administration** - Connection pooling, backup management, performance monitoring
- **ORM replacement** - pgmt complements ORMs, doesn't replace application data access
- **Real-time schema changes** - Online schema modification without migrations

## Influencing the Roadmap

- Open [GitHub Issues](https://github.com/gdpotter/pgmt/issues) with detailed use cases
- Join [GitHub Discussions](https://github.com/gdpotter/pgmt/discussions) for roadmap planning
- Contribute code via pull requests
