---
title: Modules
description: Partition your schema into named, independently deployable slices while keeping one database and one migration log.
---

Modules let you split one managed schema into named slices — `core`, `billing`, `analytics` — that different teams own and different targets receive. The database stays singular and the migration log stays global and ordered; what changes is that every migration's work is attributed per-module, and `apply`/`provision` can deploy a subset.

Use modules when:

- **Monorepo teams** each own part of one Postgres schema and want to evolve and deploy their slice semi-independently.
- **Per-customer or per-region deployments** where some targets get only some features — a base install everywhere, `billing` only where it's sold.

Modules are **100% opt-in**. No `modules:` block in pgmt.yaml means nothing changes — migrations are generated and applied exactly as before.

## Declaring Modules

```yaml
# pgmt.yaml
directories:
  schema_dir: schema/

modules:
  core:
    paths: ["schema/core/**"]

  billing:
    paths: ["schema/billing/**"]
    depends_on: [core]

  analytics:
    paths: ["schema/analytics/**"]
    depends_on: [core]
```

- **`paths`** (required): globs mapping schema files to the module. Globs are **project-root-relative**, so a module's files can live anywhere in a monorepo.
- **`depends_on`** (optional): modules whose objects this one may reference.
- **Names** must match `[a-z][a-z0-9_]*`. Only `default` is reserved — it names unmoduled sections.
- A file matching **two** modules is an error. Every file has exactly one owner.

Config problems (bad names, overlapping paths, dependency cycles) fail fast at config load, on every command.

### Unmoduled Files: the Base

Files matching no module glob form the **base**. The base is not a module and has no name:

- Every target has it: **the base always deploys**, on every `apply` and `provision`. It can't appear in `--modules`.
- Every module implicitly depends on it, so modules may reference base objects freely.
- Shared infrastructure belongs here: extensions used by several modules, common lookup tables. `roles.sql` stays global regardless (roles are never partitioned).

A project without `modules:` is, in module terms, all base — which is why existing projects keep working unchanged.

## Generated Migrations

On a module project, `pgmt migrate new` partitions the diff into module-tagged [sections](/docs/guides/multi-section-migrations):

```sql
-- pgmt:section name="default"
CREATE EXTENSION pgcrypto;

-- pgmt:section name="core" module="core"
CREATE TABLE public.users (id uuid PRIMARY KEY, email text NOT NULL);

-- pgmt:section name="billing" module="billing"
CREATE TABLE public.invoices (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES public.users(id)
);
```

Unmoduled work lands in a `default` section with no `module` attribute — the same shape a non-module project's history has, which is what makes pre-module history "already the base."

A module can own **several sections in one migration**. Cross-module dependencies can force interleaving (billing's FK drop must run before core's table drop, then billing's new column after core's new table), and operations like `CREATE INDEX CONCURRENTLY` need their own non-transactional section. The first section is named after the module; later ones get numeric suffixes: `billing`, `billing_2`, `billing_3`. All of them carry `module="billing"` — "deploy billing" means every section tagged with the module, whatever the section names.

:::caution
Don't hand-edit `module=` tags or move statements between sections in generated files. Migration files are checksummed, and pgmt derives object ownership from these headers when it replays history. To change what owns an object, move its schema file and let `migrate new` re-anchor (see below).
:::

## Cross-Module References

`depends_on` declares which modules a module may reference. pgmt validates the references it already tracks (foreign keys, function calls, view dependencies) against your declarations when generating migrations:

- A reference into a module you haven't declared **warns** — the migration still generates, but you should add the dependency:

  ```
  Warning: module 'billing' object public.invoices references public.users
           owned by module 'core', but 'billing' does not declare `depends_on: [core]`
  ```

- An **unmoduled (base) file referencing a module's object is an error**: the base deploys everywhere, so it can't depend on an optional module. Fix it by moving the referencing file into the module, or moving the referenced objects into the base.

Declared dependencies also matter at deploy time: requesting a module automatically pulls in everything it depends on.

## Deploying

**Bare `pgmt migrate apply` deploys only the base.** Modules never deploy implicitly — a target gets a module only when a deploy names it:

```bash
pgmt migrate apply --modules billing          # billing + its dependencies + the base
pgmt migrate apply --modules all              # every declared module
PGMT_MODULES=core,billing pgmt migrate apply  # env var fallback
```

Resolution order matches pgmt's connection settings: `--modules` flag > `PGMT_MODULES` env var > default. The default — for `apply` and `provision` alike — is **base-only**: modules deploy only when named. Use `--modules all` when a target should receive everything.

Dependency closure is automatic and announced:

```console
$ pgmt migrate provision --modules billing
Including module 'core' (required by 'billing')
```

Sections of modules you didn't request are skipped with two-tier signalling:

- A **never-established** module prints an info notice — expected on subset targets:

  ```
  Skipping module 'analytics' sections in migration 1734567890 (not established here)
  ```

- A module that **is established** on the target but missing from the requested set warns — its objects live here, so skipping them is schema drift until your deploy command names the module. If you modularize an existing project, this warning is your prompt to update each target's deploy command (e.g. to `--modules all`) once.

**Subset targets record no trace of unrequested modules.** Skipped sections leave no rows in the tracking tables — nothing in a customer's database names a module it never asked for. What was skipped is derived from the checksummed migration files whenever pgmt needs it.

Two guardrails on the flag itself: `--modules` on a project without a `modules:` block is an error, and an unknown name errors listing what's declared (`unknown module 'nonexistent' in --modules (declared: analytics, billing, core)`).

## Adopting a Module Later

A target provisioned with `core,billing` can pick up `analytics` afterwards. If the module's entire history lives in the migration files (it appeared after your latest baseline, or you have no baseline), plain `apply` adopts it by replaying its skipped sections:

```bash
pgmt migrate apply --modules analytics
```

If part of the module's state was consolidated into a baseline, `apply` can't reconstruct it from migrations alone and refuses with guidance:

```console
$ pgmt migrate apply --modules analytics
error: adopting module(s) analytics here requires baseline content — their
       pre-baseline state lives in the committed baseline, not the migrations.
       Adopt via: pgmt migrate provision --modules analytics
```

`migrate provision --modules analytics` applies the module's sections from the latest committed baseline, then its sections from every migration after it:

```console
$ pgmt migrate provision --modules analytics
Adopting module(s) analytics from baseline 1734567890...
```

Two conditions must hold, and adoption refuses with the exact fix if they don't: the module's dependencies must already be on the target (adopt `core` first if not), and the target's **already-established modules must be caught up to that baseline's version**. The latter is the behind-environment case — if `core` is behind, pgmt tells you to run `pgmt migrate apply --modules core` first. Rolling an established module forward is a deliberate `apply` (where its migrations, possibly destructive, are surfaced), never a silent side effect of adopting a different module. Re-running an adoption is a no-op.

## Re-Anchoring: When Ownership Moves

Adoption works because pgmt can always replay a module's history from committed files. Two kinds of change would break that replay, so `migrate new` detects them and requires a **re-anchoring baseline**:

1. **Moving files between modules** — including the first-time case: declaring `modules:` over an existing project. There's no DDL change, but replaying the old history would reproduce the old ownership.
2. **Dropping an object another module's history references** — core drops a table that billing's old FK pointed at, so replaying billing alone would fail on the missing table.

Modularizing an existing project looks like this:

```console
$ pgmt migrate new "modularize"
error: partition re-anchor required:
  - public.users moved from '(unmoduled)' to 'core'

Replaying module history would reproduce the old ownership;
re-run with --create-baseline to emit a re-anchoring baseline.

$ pgmt migrate new "modularize" --create-baseline
No schema changes - emitting re-anchoring baseline only.
Created baseline: schema_baselines/baseline_1734567890.sql
```

The emitted baseline records where each section's objects came from via `remaps`:

```sql
-- pgmt:section name="core" module="core" remaps="(unmoduled)"
CREATE TABLE public.users (...);
```

`(unmoduled)` means the base. The remap records that objects previously owned by the base now belong to `core` — module ownership is always derived from the checksummed files, never stored in the database. After the re-anchor, `migrate new` is quiet again.

The cross-module-drop case works the same way. `migrate new` refuses ("dropping public.accounts (owned by 'core') breaks replay of module 'billing', whose history references it"); with `--create-baseline` it emits the migration — sections ordered drops-first, so billing's FK drop precedes core's table drop — together with the re-anchoring baseline at the same version.

## Coupled Migrations

A migration whose sections interleave across modules may not be partially deployable. If a selected section is preceded by a pending section of an unselected module that's established on the target, `apply` refuses rather than run steps out of order:

```console
$ pgmt migrate apply --modules core
error: migration 1734567890 couples module 'billing' (section 'billing') ahead of
       selected section 'core'; deploy them together (--modules ...,billing)
```

Deploy the coupled modules in one command: `--modules core,billing`.

## Baselines and Checkpoints

`pgmt migrate baseline` (checkpointing the migration log) preserves module tags: the collapsed baseline's sections carry each object's module, so subset deploys and adoption keep working from the checkpoint. A checkpoint never changes ownership, so it never carries `remaps` — re-anchoring is exclusively `migrate new --create-baseline`'s job. See [Baseline Management](/docs/guides/baseline-management).

## Not Yet Implemented

- **Re-tag awareness on pre-existing targets** — a database deployed *before* you modularized doesn't yet read the re-anchor's `remaps` to learn that it already owns the re-tagged modules. Until that lands, treat modularization as safe for **new** targets, and keep deploying pre-existing targets with `--modules all`.
- **`conflicts_with`** — mutually exclusive modules (regional variants like `billing_us` / `billing_eu` that define the same objects, with only one side ever on a target) are planned but not yet available.
- **Per-module status view** — `migrate status` doesn't yet break down establishment and pending work by module.
