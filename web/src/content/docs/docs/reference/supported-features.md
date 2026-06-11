---
title: Supported Features
description: What pgmt manages, what's partial, and what's not supported.
---

pgmt diffs live PostgreSQL catalogs, so it manages what it can read back from
`pg_catalog`. This page lists object coverage and — more importantly — what's
partial, missing, or has known sharp edges.

- ✅ Supported
- 🚧 Partial — works with documented gaps
- ❌ Not supported

Requires PostgreSQL 13 or later.

## Object Coverage

| Object                  | Status | Notes                                                                                                      |
| ----------------------- | ------ | ---------------------------------------------------------------------------------------------------------- |
| Schemas                 | ✅     |                                                                                                            |
| Tables                  | ✅     | Columns, defaults, type changes, RLS settings                                                              |
| Views                   | ✅     | Including `security_barrier` / `security_invoker`                                                          |
| Materialized views      | ❌     | Planned                                                                                                    |
| Partitioned tables      | ❌     | Planned                                                                                                    |
| Enum types              | ✅     | Adding values only — PostgreSQL can't reorder or remove enum values                                        |
| Composite types         | 🚧     | Create/drop and attribute comments; `ALTER TYPE ADD/DROP/ALTER ATTRIBUTE` not supported                    |
| Domains                 | 🚧     | Create/drop; some `ALTER DOMAIN` constraint operations missing                                             |
| Range types             | ✅     |                                                                                                            |
| Functions & procedures  | ✅     | Overloading; volatility, `STRICT`, `SECURITY`, `PARALLEL` attributes. `OUT`/`INOUT`/`VARIADIC` parameters and parameter defaults not supported |
| Aggregates              | ✅     |                                                                                                            |
| Operators               | ✅     | All clauses (`COMMUTATOR`, `NEGATOR`, `RESTRICT`, `JOIN`, `HASHES`, `MERGES`), prefix operators            |
| Casts                   | ✅     | `WITH FUNCTION`, `WITH INOUT`, `WITHOUT FUNCTION`; see known issues for views using I/O casts              |
| Sequences               | 🚧     | Create/drop, `OWNED BY` (SERIAL integration); some `ALTER SEQUENCE` options missing                        |
| Indexes                 | ✅     | All access methods; partial, expression, and covering indexes; operator classes, collations, storage parameters, tablespaces |
| Constraints             | ✅     | Primary key, unique, foreign key (actions, deferrable), check, exclusion                                   |
| Triggers                | ✅     | All timings and events, `WHEN` conditions, transition tables, column-specific `UPDATE OF`                  |
| Extensions              | ✅     | Extension-owned objects are excluded from management automatically                                         |
| Comments                | ✅     | On all supported objects, including columns and composite attributes                                       |
| Grants                  | ✅     | Tables, views, schemas, functions, sequences, types; column-level grants; `WITH GRANT OPTION`; only privilege deltas are emitted |
| RLS policies            | ✅     | All command types, permissive/restrictive, role targeting, `USING`/`WITH CHECK`                            |
| Roles                   | ❌     | By design — see [Roles and Permissions](/docs/guides/roles-and-permissions)                                |

## Dependency Tracking

Dependencies come from the PostgreSQL catalogs, not from parsing your SQL:
objects are created, altered, and dropped in dependency order, including
function signatures, view references, foreign keys, sequence ownership, and
extension-provided types, functions, and operator classes (an index using
`gin_trgm_ops` is ordered after `CREATE EXTENSION pg_trgm`).

The one gap PostgreSQL itself has: it records no dependencies for the *bodies*
of SQL functions. When a function body references another object, add an
explicit [`-- require:` header](/docs/concepts/dependency-tracking) to the
file.

## Known Issues and Sharp Edges

- **Function body dependencies** — PostgreSQL doesn't track them; use
  `-- require:` (see above).
- **Enum values** — append-only; reordering or removing a value requires a
  manual table rewrite, as in PostgreSQL itself.
- **I/O and binary casts in views** — casts created `WITH INOUT` or
  `WITHOUT FUNCTION` that are used inside a view or function body aren't
  auto-ordered (PostgreSQL records no dependency on them); add `-- require:`
  on the cast's file.
- **Extension schema dependencies** — extensions created in custom schemas may
  not order correctly (schema before extension).
- **Extension CASCADE drops** — dropping an extension with dependent objects
  requires manual CASCADE handling.
- **Complex circular dependencies** — some cycles need manual resolution via
  `-- require:`.

## Out of Scope by Design

- **Data migrations** — pgmt manages schema structure; data transformations
  belong in [multi-section migrations](/docs/guides/multi-section-migrations)
  or external tooling.
- **Role management** — roles must exist before grants reference them; manage
  them with a [roles file](/docs/guides/roles-and-permissions) or external
  tooling.
- **Zero-downtime guarantees** — depends on the specific change; see
  [Production Operations](/docs/guides/production-operations).

Migration generation, application, validation, baselines, and drift detection
are covered in the [Migration Workflow](/docs/guides/migration-workflow) and
[CI/CD](/docs/guides/ci-cd) guides.
