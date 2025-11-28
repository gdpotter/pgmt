# pgmt Test Suite

This document provides an overview of the test organization, patterns, and guidelines for contributing tests.

## Test Categories

```
tests/
├── catalog/        - Database object fetching from pg_catalog
├── migrations/     - Diff generation and SQL rendering
├── cli/            - Command-line interface behavior
├── unit/           - Pure functions and configuration
├── integration/    - End-to-end workflows
├── commands/       - Command handler tests
├── component/      - Component-level tests with external resources
├── security/       - SQL injection protection tests
├── helpers/        - Shared test utilities
└── fixtures/       - Test data and schemas
```

## Running Tests

```bash
# One-time setup: Start PostgreSQL containers (versions 13-18)
./scripts/test-setup.sh

# Run all tests
cargo test

# Run specific test categories
cargo test catalog           # Catalog tests only
cargo test migrations        # Migration tests only
cargo test cli               # CLI tests only

# Run a specific test
cargo test test_fetch_basic_table
```

## Test Helpers

### Database Tests (`with_test_db`)

For testing database object fetching from pg_catalog:

```rust
use crate::helpers::harness::with_test_db;

#[tokio::test]
async fn test_fetch_basic_table() {
    with_test_db(async |db| {
        db.execute("CREATE TABLE users (id SERIAL PRIMARY KEY)").await;
        let tables = fetch(&mut *db.conn().await).await.unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "users");
    }).await;
    // Database automatically cleaned up!
}
```

### Migration Tests (`MigrationTestHelper`)

For testing diff generation and migration SQL rendering.

**The 3-Vector Pattern:**

Migration tests use a helper that manages two databases and applies SQL in three phases:

```rust
use crate::helpers::migration::MigrationTestHelper;

#[tokio::test]
async fn test_add_column_migration() -> Result<()> {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        // Vector 1: Applied to BOTH databases (shared setup)
        &["CREATE SCHEMA app", "CREATE TABLE app.users (id INT)"],

        // Vector 2: Applied to INITIAL database only (before state)
        &[],

        // Vector 3: Applied to TARGET database only (after state)
        &["ALTER TABLE app.users ADD COLUMN name TEXT"],

        // Verification: Check the generated migration steps
        |steps, final_catalog| {
            assert!(!steps.is_empty());
            // Verify the step is an ALTER TABLE ADD COLUMN
            Ok(())
        }
    ).await?;

    Ok(())
}
```

This pattern tests that pgmt correctly generates migrations from state #2 → state #3.

### CLI Tests (`with_cli_helper`)

For testing command-line interface behavior:

```rust
use crate::helpers::cli::with_cli_helper;
use predicates::prelude::*;

#[tokio::test]
async fn test_migrate_new_command() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        helper.write_schema_file("users.sql", "CREATE TABLE users (id INT);")?;

        helper.command()
            .args(["migrate", "new", "add_users"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Migration generation complete!"));

        let migrations = helper.list_migration_files()?;
        assert_eq!(migrations.len(), 1);

        Ok(())
    }).await
}
```

**Interactive CLI Tests (Unix only):**

```rust
#[cfg(not(windows))]
#[tokio::test]
async fn test_interactive_prompt() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        let mut session = helper.interactive_command(&["migrate", "new"]).await?;

        session.expect("Enter migration description")?;
        session.send_line("my_migration")?;
        session.expect("Migration generation complete!")?;
        session.expect(expectrl::Eof)?;

        Ok(())
    }).await
}
```

## Test Naming Conventions

| Category | Pattern | Example |
|----------|---------|---------|
| Catalog | `test_fetch_<object>` | `test_fetch_table_with_generated_column` |
| Migration | `test_<action>_<object>_migration` | `test_create_enum_migration` |
| CLI | `test_<command>_<scenario>` | `test_migrate_new_with_description` |
| Unit | `test_<function>_<scenario>` | `test_config_merge_precedence` |

## Writing Good Tests

### Do:
- Use descriptive test names that explain what's being tested
- Test edge cases (empty tables, arrays, underscore-prefixed types)
- Include both positive AND negative assertions where relevant
- Use explicit ordering verification for migration tests
- Clean up resources (helpers do this automatically)

### Don't:
- Use generic names like `test_1` or `test_basic`
- Only assert `result.is_ok()` without checking values
- Skip edge cases for database objects
- Forget to verify deterministic ordering

### Example of Good Assertions:

```rust
// Good: Deep verification of structure
assert_eq!(table.columns.len(), 3);
assert_eq!(table.columns[0].name, "id");
assert_eq!(table.columns[0].data_type, "integer");
assert!(table.columns[0].not_null);

// Good: Both positive and negative assertions
assert!(func.depends_on().contains(&expected_type));
assert!(!func.depends_on().contains(&unexpected_type));

// Good: Ordering verification for migrations
assert!(extension_idx < table_idx,
    "Extension must appear before table that uses its types");
```

## Adding Tests for New Database Objects

When adding support for a new database object type, add tests in:

1. **`tests/catalog/<object>.rs`**
   - `test_fetch_basic_<object>()` - Basic object fetching
   - `test_fetch_<object>_with_comment()` - Comment support
   - `test_fetch_<object>_dependencies()` - Dependency tracking

2. **`tests/migrations/<object>.rs`**
   - `test_create_<object>_migration()` - CREATE operations
   - `test_drop_<object>_migration()` - DROP operations
   - `test_<object>_comment_migration()` - COMMENT operations

3. **Update `tests/<category>/mod.rs`** to include your new module

## Troubleshooting

### "DATABASE_URL environment variable is required"
Run `./scripts/test-setup.sh` to start PostgreSQL containers.

### Tests hanging or timing out
The test helpers have a 5-second cleanup timeout. If tests are consistently timing out, check for connection leaks or long-running queries.

### Interactive tests failing
Interactive tests using `expectrl` only work on Unix-like systems. They're automatically skipped on Windows via `#[cfg(not(windows))]`.
