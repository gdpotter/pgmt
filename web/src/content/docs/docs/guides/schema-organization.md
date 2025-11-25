---
title: Schema Organization
description: Organize your PostgreSQL schema across multiple files with dependency management.
---

You can put your entire schema in one file, or split it across many. pgmt doesn't care - it reads all `.sql` files in your schema directory and figures out the dependencies.

## Single File vs Multi-File

For small projects, one file works fine:

```
schema/
└── schema.sql
```

For larger projects, split by object type:

```
schema/
├── 01_schemas/
│   └── app.sql
├── 02_types/
│   ├── priority.sql
│   └── status.sql
├── 03_tables/
│   ├── users.sql
│   └── orders.sql
├── 04_views/
│   └── reports.sql
└── 05_functions/
    └── helpers.sql
```

The numbered prefixes give you alphabetical ordering as a baseline. You can also organize by business domain (`auth/`, `catalog/`, `orders/`) or mix both approaches.

## Dependencies with `-- require:`

When one file depends on another, declare it explicitly:

```sql
-- require: 01_schemas/app.sql, 02_types/user_role.sql
CREATE TABLE app.users (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    role app.user_role DEFAULT 'customer'
);
```

pgmt loads files in this order:

1. Alphabetically by default
2. Adjusted by `-- require:` declarations
3. Topologically sorted so dependencies come first

File paths are relative to your schema directory. The `.sql` extension is optional.

## Complete Example

Here's an e-commerce schema split across files:

**01_schemas/app.sql:**

```sql
CREATE SCHEMA app;
```

**02_types/order_status.sql:**

```sql
-- require: 01_schemas/app.sql
CREATE TYPE app.order_status AS ENUM ('pending', 'confirmed', 'shipped', 'delivered');
```

**03_tables/users.sql:**

```sql
-- require: 01_schemas/app.sql
CREATE TABLE app.users (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

**03_tables/orders.sql:**

```sql
-- require: 01_schemas/app.sql, 02_types/order_status.sql, 03_tables/users.sql
CREATE TABLE app.orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES app.users(id),
    status app.order_status DEFAULT 'pending',
    total DECIMAL(10,2) NOT NULL
);
```

**03_tables/order_items.sql:**

```sql
-- require: 03_tables/orders.sql
CREATE TABLE app.order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES app.orders(id) ON DELETE CASCADE,
    product_name TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10,2) NOT NULL
);
```

**04_views/order_summary.sql:**

```sql
-- require: 03_tables/orders.sql, 03_tables/order_items.sql, 03_tables/users.sql
CREATE VIEW app.order_summary AS
SELECT
    o.id as order_id,
    u.email as customer_email,
    o.status,
    o.total,
    COUNT(oi.id) as item_count
FROM app.orders o
JOIN app.users u ON o.user_id = u.id
LEFT JOIN app.order_items oi ON o.id = oi.order_id
GROUP BY o.id, u.email, o.status, o.total;
```

pgmt resolves this to: `app.sql` → `order_status.sql` → `users.sql` → `orders.sql` → `order_items.sql` → `order_summary.sql`

## Troubleshooting

**Circular dependency:**

```
Error: Circular dependency detected: A requires B, B requires A
```

Restructure your dependencies or merge the files.

**Missing file:**

```
Error: Missing dependency: orders.sql requires missing_file.sql
```

Check the path - it's relative to your schema directory.

**Wrong load order:**

```
Error: relation "users" does not exist
```

Add a `-- require:` statement to the file that needs `users`.

**Debug with dry-run:**

```bash
pgmt apply --dry-run
```

Shows what pgmt will do without touching your database. Use this to verify load order.
