---
title: Testing Guide
description: How to run tests and write new tests for pgmt.
---

How to run tests and write new tests for pgmt.

## Running Tests

```bash
# Run all tests
cargo test

# Run with output
cargo test -- --nocapture

# Run specific test
cargo test test_table_creation

# Run by category
cargo test --test catalog      # Catalog tests
cargo test --test migrations   # Migration tests
cargo test --test cli          # CLI tests
```

## Test Types

### Catalog Tests (`tests/catalog/`)

Test PostgreSQL introspection - verify we correctly read database structure.

**Example:**

```rust
#[tokio::test]
async fn test_fetch_tables() {
    with_test_db(async |db| {
        db.execute("CREATE TABLE users (id INT)").await;
        let tables = fetch(db.pool()).await.unwrap();
        assert_eq!(tables.len(), 1);
    }).await;
}
```

### Migration Tests (`tests/migrations/`)

Test end-to-end migration generation - verify schema changes produce correct SQL.

**Example using 3-vector pattern:**

```rust
#[tokio::test]
async fn test_table_migration() {
    let helper = MigrationTestHelper::new().await;

    helper.run_migration_test(
        &[],  // Both databases
        &[],  // Initial database only
        &["CREATE TABLE users (id INT)"],  // Target database only
        |steps, _catalog| {
            assert!(!steps.is_empty());
            Ok(())
        }
    ).await
}
```

### CLI Tests (`tests/cli/`)

Test command-line interface behavior.

**Example:**

```rust
#[tokio::test]
async fn test_apply_command() {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        helper.write_schema_file("users.sql", "CREATE TABLE users (id INT);")?;

        helper.command()
            .args(["apply"])
            .assert()
            .success();

        Ok(())
    }).await
}
```

## Test Helpers

### `with_test_db`

Provides isolated test database with automatic cleanup:

```rust
use crate::helpers::harness::with_test_db;

#[tokio::test]
async fn test_something() {
    with_test_db(async |db| {
        // Use db.execute() and db.pool()
        // Database automatically dropped when closure completes
    }).await;
}
```

**What it handles:**

- Creates unique temporary database for each test
- Provides connection pool and SQL execution helpers
- Guarantees cleanup even if test panics or fails

**For multiple databases:**

Tests comparing two database states (like migrations) can nest `with_test_db` calls:

```rust
#[tokio::test]
async fn test_with_multiple_databases() {
    with_test_db(async |initial_db| {
        with_test_db(async |target_db| {
            initial_db.execute("CREATE TABLE users (id INT)").await;
            target_db.execute("CREATE TABLE users (id INT, email TEXT)").await;

            let initial_tables = fetch(initial_db.pool()).await.unwrap();
            let target_tables = fetch(target_db.pool()).await.unwrap();
            // Compare states...
        }).await
    }).await;
}
```

### `MigrationTestHelper`

Simplifies migration testing with the 3-vector pattern:

```rust
let helper = MigrationTestHelper::new().await;

helper.run_migration_test(
    &["CREATE TABLE shared (id INT)"],  // SQL for both databases
    &["CREATE TABLE only_initial (id INT)"],  // SQL for initial database only
    &["CREATE TABLE only_target (id INT)"],   // SQL for target database only
    |steps, final_catalog| {
        // Verify migration steps and final state
        Ok(())
    }
).await
```

### `with_cli_helper`

Provides temporary project for CLI testing:

```rust
use crate::helpers::cli::with_cli_helper;

#[tokio::test]
async fn test_cli() {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        helper.write_schema_file("file.sql", "CREATE TABLE ...")?;
        helper.command().args(["apply"]).assert().success();
        Ok(())
    }).await
}
```

## Writing New Tests

When adding new database object support, create tests for:

1. **Catalog fetch** - Verify object is correctly read from PostgreSQL
2. **Comment support** - Verify comments are fetched and managed
3. **Dependencies** - Verify dependency tracking works
4. **Migration generation** - Verify create/drop/modify operations
5. **Full pipeline** - Test `diff_all()` → `cascade::expand()` → `diff_order()`

See the [contributing guide](contributing.md) for complete testing patterns.

## Test Database Configuration

Tests use Docker containers for PostgreSQL versions 13, 14, 15, 16, 17, and 18. Run `./scripts/test-setup.sh` once to start all containers.

**Test against a specific version:**

```bash
DATABASE_URL=$(./scripts/test-db-url.sh 18) cargo test
```

**Manual setup (if not using Docker):**

```bash
createdb pgmt_test
export DATABASE_URL=postgres://localhost/pgmt_test
cargo test
```

## Version-Gated Tests

Some PostgreSQL features require specific versions (e.g., `security_invoker` for views requires PostgreSQL 15+). Use the `pg_major_version()` helper to skip tests on older versions:

```rust
#[tokio::test]
async fn test_pg15_feature() -> Result<()> {
    with_test_db(async |db| {
        if pg_major_version(db.pool()).await? < 15 {
            return Ok(()); // Skip on PostgreSQL < 15
        }
        // Test code that requires PG 15+ here
        Ok(())
    }).await
}
```

This pattern is used in `tests/catalog/views.rs` and `tests/migrations/views.rs` for features introduced after PostgreSQL 13.
