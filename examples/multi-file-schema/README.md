# Multi-File Schema Example

This example demonstrates how to organize your PostgreSQL schema across multiple files with dependencies.

## Structure

```
schema/
├── 01_schemas/
│   └── app.sql              # CREATE SCHEMA app;
├── 02_types/
│   ├── priority.sql         # Custom ENUM type (requires schemas)
│   └── status.sql           # Custom ENUM type (requires schemas)
├── 03_tables/
│   ├── users.sql            # Users table (requires schemas, types)
│   └── tasks.sql            # Tasks table (requires schemas, types, users)
├── 04_views/
│   ├── active_users.sql     # View of active users (requires tables)
│   └── user_tasks.sql       # Join view (requires multiple tables)
└── 05_functions/
    └── task_helpers.sql     # Helper functions (requires all previous)
```

## Dependency Syntax

Use `-- require:` comments to specify dependencies:

```sql
-- require: 01_schemas/app.sql
-- require: 02_types/priority.sql, 02_types/status.sql
CREATE TABLE app.tasks (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    priority app.priority DEFAULT 'medium',
    status app.status DEFAULT 'pending'
);
```

## Features

- **Alphabetical ordering**: Files are processed alphabetically by default
- **Explicit dependencies**: Override alphabetical ordering with `-- require:` 
- **Dependency resolution**: Automatic topological sorting ensures correct order
- **Error detection**: Circular dependencies and missing files are caught

## Loading Order

With the above structure, files will be loaded in this order:

1. `01_schemas/app.sql` (no dependencies)
2. `02_types/priority.sql` (requires schemas)
3. `02_types/status.sql` (requires schemas)  
4. `03_tables/users.sql` (requires schemas + types)
5. `03_tables/tasks.sql` (requires schemas + types + users)
6. `04_views/active_users.sql` (requires users table)
7. `04_views/user_tasks.sql` (requires multiple tables)
8. `05_functions/task_helpers.sql` (requires everything)

This ensures that dependencies are always created before the objects that depend on them.