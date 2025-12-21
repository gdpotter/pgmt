---
title: Roadmap
description: Current status and future development plans for pgmt.
---

**Project Status:** Alpha - Approaching first release. Not yet recommended for production use.

_Last updated: December 2024_

## Current State

pgmt supports most PostgreSQL schema objects: tables, views, functions, triggers, indexes, constraints, custom types, sequences, extensions, and grants. See [Supported Features](/docs/reference/supported-features) for the complete list.

## Future Directions

These are areas being explored based on community feedback:

- **Additional PostgreSQL objects** - Materialized views, partitioned tables
- **Developer experience** - Improved error messages, performance optimizations for large schemas
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
