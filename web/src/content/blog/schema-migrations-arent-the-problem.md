---
title: "Schema Migrations Aren't the Problem. Iteration Is."
description: 'Why traditional migration tools break down during development, and what we can do about it.'
pubDate: 2026-02-09
---

There's a problem with how we manage PostgreSQL schemas, and it shows up the moment you start using PostgreSQL for more than storing rows.

You're building a feature. It involves a function, a couple of views that depend on it, maybe an RLS policy. You're iterating — trying things, adding a column, realizing you don't need it, tweaking a function signature. You don't know what you're going to end up with yet. That's just how development works.

Then you change the function and get this:

```
ERROR: cannot drop function calculate_score(integer) because other objects depend on it
DETAIL: view user_rankings depends on function calculate_score(integer)
DETAIL: view daily_stats depends on view user_rankings
DETAIL: view executive_dashboard depends on view daily_stats
HINT: Use DROP ... CASCADE to drop the dependent objects too.
```

So you manually figure out the dependency chain, drop everything in the right order, make your change, and recreate everything. Then you do it _again_ for the actual migration file. Six migrations later, you have a feature that works — but maybe only two statements of the migration represent actual design decisions. The rest are navigating through the dependency tree.

## Tables are easy. Everything else has a dependency tree.

If you're coming from Rails, Django, or any modern web framework, you learned migrations as step one of database work. Generate a file, write some SQL, run it. Each migration is an imperative instruction: _add this column_, _create this index_, _drop this table_.

For tables and columns, this works great. Tools like Flyway, Liquibase, and the built-in framework ones have been battle-tested for years and I don't want to diminish that.

But tables and columns are the easy part. PostgreSQL has views, functions, RLS policies, triggers. As soon as your project starts using them at any sort of scale, the imperative model breaks down. That's just not how you develop software. When you're building a feature in Python or TypeScript, you edit your source files. You don't write instructions telling the compiler what changed since the last build. But that's exactly what a migration is — you're hand-writing the transformation steps instead of just declaring what you want.

Every modern web framework has a watch mode for this reason. You change a file, the framework figures out what to rebuild. Database development just... doesn't have that. At that point, it became clear that the problem wasn’t migrations themselves, but the lack of a schema-first workflow.

## Here's what I actually wanted.

Here's what development looks like with pgmt. You maintain your schema as SQL files, organized however makes sense for your project:

```
schema/
├── types/
│   └── priority.sql
├── tables/
│   ├── users.sql
│   └── tasks.sql
├── views/
│   ├── active_users.sql
│   └── user_tasks.sql
└── functions/
    └── calculate_score.sql
```

These files are the desired state of your database. Want to change the scoring function? Edit the file:

```sql
-- schema/functions/calculate_score.sql

CREATE OR REPLACE FUNCTION calculate_score(
  user_id INTEGER,
  include_bonus BOOLEAN DEFAULT false  -- ← just add the parameter
) RETURNS INTEGER AS $$
  -- updated logic here
$$ LANGUAGE plpgsql;
```

Then:

```bash
$ pgmt apply

📋 8 changes

  ✓ Drop view public.executive_dashboard
  ✓ Drop view public.daily_stats
  ✓ Drop view public.user_rankings
  ✓ Drop function public.calculate_score(integer)
  ✓ Create function public.calculate_score(integer, boolean)
  ✓ Create view public.user_rankings
  ✓ Create view public.daily_stats
  ✓ Create view public.executive_dashboard

✅ Applied 8 changes
```

pgmt figured out the dependency chain, handled all the drops and recreates, and applied it to your dev database. You didn't think about it. You just edited the file and ran `apply`.

Want continuous iteration? This is also why pgmt has a watch mode. Run `pgmt apply --watch`, edit a schema file, save — and your dev database updates immediately. No migration files, no manual teardown, no thinking about dependency order. The database finally behaves like the rest of your development environment.

## There's no magic in production.

When you're done iterating and ready to ship, you run:

```bash
$ pgmt migrate new "improve scoring algorithm"
✓ Generated: V1770601799_improve_scoring_algorithm.sql
```

That's a real SQL migration file. You open it, read it, and can edit it. Nothing touches production without you explicitly reviewing and approving it. I didn't want a tool that does things to your database that you don't understand — the whole point is that the migration is right there and you can see exactly what it's going to do.

I wanted both fast iteration during development but also explicit control for deployment. If the generated migration needs a data backfill, a guard clause, or a different approach to a rename — you just edit the file before running it. And pgmt will always validate that your migration properly gets you to the same state as your declared schema.

pgmt also supports multi-section migrations, which let you control how different parts of a migration execute: transactional vs non-transactional steps, different lock or retry behavior, and so on. I’ll go deeper on this in a future post.

## Then there's the review problem.

You can look at a migration that adds a column and immediately understand it. You can look at a migration that drops and recreates five database objects to change one line in a function body and... good luck. What actually changed? What's just the dependency tree forcing your hand? Reviewing that meaningfully is really hard.

Sure, you can (and probably should?) commit a `pg_dump` of your schema alongside migrations so reviewers can diff the actual state. But now you're asking people to thoughtfully review generated output — a big SQL dump where the ordering and formatting are whatever `pg_dump` decided. Not a great developer experience.

With pgmt, the pull request tells a different story. The schema files are what you're actually reviewing:

```diff
  -- schema/functions/calculate_score.sql

  CREATE OR REPLACE FUNCTION calculate_score(
    user_id INTEGER,
+   include_bonus BOOLEAN DEFAULT false
  ) RETURNS INTEGER AS $$
    -- updated logic
  $$ LANGUAGE plpgsql;
```

That diff shows what changed. One parameter added. Your teammates review intent, not mechanics. The migration file is still there to inspect, but you're not asking people to reverse-engineer what changed from a wall of drops and recreates.

And because the schema files are just files in your repo, you can be thoughtful about how they're organized — add comments explaining _why_ an RLS policy exists, group things by feature, order things intentionally. **It's not a dump. It's code you maintain.**

## Here's how it actually works.

pgmt doesn't reimplement PostgreSQL's parser or try to understand your schema on its own. It asks PostgreSQL.

There are two slightly different flows:

### `pgmt apply` (development)

1. Creates a temporary "shadow" PostgreSQL database
2. Applies your schema files to it
3. Reads system catalogs from the shadow database to capture the desired schema (including dependency information)
4. Reads system catalogs from your dev database to capture the current schema
5. Diffs the two catalog snapshots and generates the required SQL
6. Applies that SQL to your dev database
7. Drops the shadow database

### `pgmt migrate new` (migration generation)

1. Creates a temporary "shadow" PostgreSQL database and applies your schema files to it
2. Reads system catalogs from that shadow database to capture the desired schema
3. Resets the temporary database, applies your existing migrations to it, and reads its catalogs to capture the current schema implied by your migration history
4. Diffs those two catalog snapshots to generate a new migration file
5. Drops the temporary databases

PostgreSQL tells pgmt what depends on what. No incomplete reimplementation, no custom parser that misses edge cases. This is also why pgmt is PostgreSQL-only — the whole approach depends on this integration, and trying to be database-agnostic would mean throwing away the best part.

Because the core of pgmt is a robust PostgreSQL diffing engine, there are also commands for schema validation, production drift detection, and other workflows — but those are topics for another post.

## There are some tradeoffs.

It's PostgreSQL-only. If you need MySQL or SQLite support, pgmt isn't the tool. You need a running PostgreSQL instance to generate migrations — the shadow database needs a real Postgres to work. If you're developing against PostgreSQL you almost certainly have one running, but it's still a dependency.

The diff engine also can't read your mind. If you rename a column from email to email_address, the diff sees a column disappear and a new one appear. It'll generate a `DROP COLUMN` and `ADD COLUMN`, which would lose your data. This is why you review the migration before running it — you'd catch that and replace it with `ALTER TABLE ... RENAME COLUMN`. But it does mean you need to actually look at what the tool generates, not just blindly run it. That's kind of the whole philosophy — pgmt gives you a starting point, not the final answer.

## And now you try it.

```bash
# Install
cargo install pgmt

# Initialize from an existing database
pgmt init --dev-url postgres://localhost/mydb

# Or start fresh
pgmt init
```

Edit a schema file. Run `pgmt apply`. See what happens.
