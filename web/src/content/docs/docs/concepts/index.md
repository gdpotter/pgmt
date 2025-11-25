---
title: Core Concepts
description: Understanding the fundamental concepts behind pgmt's design and approach to PostgreSQL schema management.
---

pgmt takes a unique approach to PostgreSQL schema management: **declarative development with explicit production deployments**. These concepts explain why and how.

## The Core Concepts

1. **[Philosophy](philosophy)** - Why pgmt is built around a schema diffing engine
2. **[How pgmt Works](how-it-works)** - The development workflow from local iteration to production deploy
3. **[Shadow Database](shadow-database)** - How pgmt validates changes without touching your databases
4. **[Dependency Tracking](dependency-tracking)** - How pgmt ensures operations happen in the right order

## Key Principles

### Schema Files Are Source of Truth

Schema files show the current state of your database, not a chain of historical migrations. You edit them directly during development, then generate explicit migrations for production deployment.

### Rebuildable from Scratch

You can recreate your database at any time using schema files and migration files. No manual steps required.

### Environment Isolation

Development, staging, and production are completely separate. Shadow databases let you test changes safely before applying them.

---

**Ready to get started?** See the [Quick Start Guide](../getting-started/quick-start) to try pgmt in practice.
