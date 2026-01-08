# pgmt - PostgreSQL Migration Tool

**For AI Assistants:** This file contains critical patterns, gotchas, and development guidelines. For feature documentation, see `web/src/content/docs/`. For project overview, see @README.md.

## Core Implementation Patterns

### PostgreSQL System Catalogs
- Use `pg_get_expr()` for predicates and expressions, not raw catalog data
- Always include explicit schema filtering to avoid system objects
- Use `ORDER BY` clauses for deterministic results
- For expression indexes, use `pg_get_indexdef()` instead of `pg_attribute`

### Dependency Tracking
- Forward and reverse dependency maps using BTreeMap/BTreeSet for deterministic ordering
- Supports schema → extension → table → sequence → view → function relationships
- Handles custom types in arrays (e.g., `priority[]` depends on `priority`)
- SERIAL columns create table → sequence dependencies
- Function dependencies limited to signature-level (parameters/return types) due to PostgreSQL limitations
- Used for proper migration ordering with circular dependency resolution

**Extension Dependencies:**
- Extensions track schema dependencies (e.g., `CREATE EXTENSION citext SCHEMA app`)
- Tables/views/functions using extension types depend on the **extension**, not the type
- Extension types are detected via `pg_depend` with `deptype = 'e'`
- Extension types are NOT schema-qualified in generated SQL (unlike custom types)
- Views/functions automatically track extension dependencies when using extension types/functions
- Extensions participate in topological sorting (no special phase-based ordering)

**RLS Policy Dependencies:**
- Policies always depend on their parent table
- Policies can reference functions in USING/WITH CHECK expressions (tracked via function signature)
- Policy creation requires table to exist first (enforced by topological sort)
- Policies track comment support like other objects
- Cascade logic filters redundant policy drops when parent table is dropped

### Array Type Handling

PostgreSQL stores arrays with element type OID in `pg_type.typelem`. All catalog queries must use consistent patterns for array handling:

**1. Resolve element types for dependency tracking:**
```sql
-- Get element type name/schema (for arrays, resolve to the base type)
CASE WHEN t.typelem != 0 THEN elem_t.typname ELSE t.typname END AS "type_name",
CASE WHEN t.typelem != 0 THEN elem_tn.nspname ELSE tn.nspname END AS "type_schema",
-- Join to element type
LEFT JOIN pg_type elem_t ON t.typelem = elem_t.oid AND t.typelem != 0
LEFT JOIN pg_namespace elem_tn ON elem_t.typnamespace = elem_tn.oid
```

**2. Extension type detection (must check element type for arrays):**
```sql
LEFT JOIN (...) ext_types ON ext_types.type_oid = COALESCE(NULLIF(t.typelem, 0::oid), t.oid)
```

**3. Track dimensions via `attndims`** where applicable (tables, composite types, views)

**4. Use `format_type()` for display** - preserves array brackets and type modifiers

**Common mistake to avoid:** Stripping underscore prefix (`_typename` → `typename`). This breaks legitimate type names starting with underscore like `_internal_status`. Always use `pg_type.typelem` to detect arrays, never rely on naming conventions.

**Catalog files implementing this pattern:**
- `table.rs` - Column types with multi-dimensional array support
- `function.rs` - Parameter and return types
- `view.rs` - View column types
- `domain.rs` - Base types for domains
- `custom_type.rs` - Composite type attributes
- `aggregate.rs` - State types

### Error Handling
- Uses `anyhow::Result` throughout
- Graceful handling of edge cases (empty tables, missing objects)
- Proper cleanup of database resources

### Migration Safety
- All queries ORDER BY appropriate columns for deterministic results
- Explicit NULL handling in LEFT JOINs
- Schema filtering to avoid system objects
- Proper escaping for dynamic identifiers
- DAG-based ordering prevents circular dependencies
- Safe vs destructive operation classification

### Migration Operations Architecture

**OperationKind vs Safety (as of refactor in late 2024):**

These are SEPARATE concerns that were previously conflated:

**OperationKind** (`Create`, `Drop`, `Alter`) - Used for ordering migrations
- Drops must happen before creates for the same object type
- Defined in `src/diff/operations/mod.rs`
- Every operation type implements `operation_kind()` method
- Used by migration ordering logic in `src/diff/mod.rs`

**Safety** (`Safe`, `Destructive`) - Used for warnings and execution modes
- Indicates risk of data loss to the user
- Defined in `src/render/mod.rs` as part of `RenderedSql`
- Embedded in the rendered SQL, not the operation itself

**Key Insight:** A `Drop` operation can be `Safe`:
- `DROP FUNCTION` is `OperationKind::Drop` but `Safety::Safe` (can be recreated from schema)
- `DROP DOMAIN` is `OperationKind::Drop` but `Safety::Safe` (can be recreated from schema)
- `DROP AGGREGATE` is `OperationKind::Drop` but `Safety::Safe` (can be recreated from schema)
- `DROP TABLE` is `OperationKind::Drop` AND `Safety::Destructive` (loses data permanently)

**Rendering Consolidation:**
- All SQL rendering logic moved from `src/diff/operations/` to `src/render/` module
- Operations define WHAT changes (semantic meaning)
- Rendering defines HOW to execute (SQL generation)
- Each operation implements `SqlRenderer` trait
- Render functions return `Vec<RenderedSql>` with embedded safety classification
- See `src/render/domain.rs`, `src/render/function.rs`, `src/render/policy.rs` for examples

### PostgreSQL Version-Specific Features

Some PostgreSQL features require specific versions:

**View security_invoker (PostgreSQL 15+):**
- Use `pg_major_version()` helper in tests to skip on older versions
- Example: `if pg_major_version(&mut conn).await? < 15 { return Ok(()); }`
- See `tests/catalog/views.rs` and `tests/migrations/views.rs` for patterns

**Pattern for version-gated tests:**
```rust
#[tokio::test]
async fn test_pg15_feature() -> Result<()> {
    with_test_db(async |db| {
        if pg_major_version(db.pool()).await? < 15 {
            return Ok(()); // Skip test on older versions
        }
        // Test code here
        Ok(())
    }).await
}
```

**Testing infrastructure:**
- Test containers run PostgreSQL 13-18
- `pg_major_version()` helper available in TestDatabase
- Use version checks for features introduced after PostgreSQL 13

## Code Organization

### Catalog Module (`src/catalog/`)
Each database object type has its own file with consistent patterns:
- `fetch()` function for retrieving objects from pg_catalog
- Structs representing the object and its metadata
- `DependsOn` trait implementation for dependency tracking
- Uses sqlx with compile-time checked queries

### Diff Module (`src/diff/`)
- `diff()` functions compare old vs new states
- Returns `Vec<MigrationStep>` representing required changes
- Handles complex scenarios like renames, type changes

## Adding New Database Object Support

**CRITICAL CHECKLIST** - Every new database object type MUST implement ALL of these components:

### 1. Catalog Support (`src/catalog/new_object.rs`)
- [ ] Create struct representing the object with ALL metadata fields
- [ ] **MUST include `comment: Option<String>` field for comment support**
- [ ] Implement `fetch()` function using pg_catalog system tables
- [ ] **MUST include comment fetching via LEFT JOIN to pg_description**
- [ ] Implement `DependsOn` trait for dependency tracking
- [ ] **MUST implement `Commentable` trait for comment operations**
- [ ] Use proper ORDER BY clauses for deterministic results
- [ ] Handle array types and custom type dependencies correctly
- [ ] Add the new type to `src/catalog/mod.rs` exports

### 2. Diff Logic (`src/diff/new_object.rs`)
- [ ] Create `diff()` function comparing old vs new states
- [ ] **MUST include comment diff logic using `diff_comments()` utility**
- [ ] Handle CREATE, DROP, ALTER operations as appropriate
- [ ] Handle comment-only changes (SET/DROP comment without structural changes)
- [ ] Import required operations: `CommentOperation`, `{Object}Identifier`
- [ ] Return `Vec<MigrationStep>` with proper operation types
- [ ] Handle complex scenarios (renames, type changes, dependencies)

### 3. Migration Operations (`src/diff/operations/`)
- [ ] Create `{object}_operation.rs` defining operation enums
- [ ] Include `CommentOperation<{Object}Identifier>` in `{Object}Operation`
- [ ] Implement `SqlRenderer` trait for all operation variants
- [ ] Handle CREATE, DROP, ALTER, and COMMENT operations
- [ ] Ensure proper SQL generation for PostgreSQL syntax

### 4. Rendering (`src/render/mod.rs`)
- [ ] Add rendering logic for new `MigrationStep::{Object}` variant
- [ ] Handle all operation types including comments
- [ ] Ensure proper SQL escaping and formatting
- [ ] Test SQL output for correctness

### 5. Testing Requirements

**Catalog Tests** (`tests/catalog/new_object.rs`):
- [ ] `test_fetch_basic_{object}()` - Basic object fetching
- [ ] `test_fetch_{object}_with_comment()` - **Comment fetch verification**
- [ ] `test_fetch_{object}_dependencies()` - Dependency tracking

**Migration Tests** (`tests/migrations/new_object.rs`):
- [ ] `test_{object}_comment_migration()` - **Comment SET operations**
- [ ] `test_drop_{object}_comment_migration()` - **Comment DROP operations**
- [ ] CREATE, DROP, ALTER migration tests
- [ ] Full pipeline tests using `diff_all()` → `cascade::expand()` → `diff_order()`

### 6. Comment Support Requirements
**Every new object MUST support comments:**
- [ ] `comment: Option<String>` field in catalog struct  
- [ ] `Commentable` trait implementation
- [ ] Comment fetching via `LEFT JOIN pg_description d ON d.objoid = {object}.oid AND d.objsubid = 0`
- [ ] Comment diff logic using `diff_comments()` utility

### 7. Validation Checklist
- [ ] All tests pass: `cargo test`
- [ ] Comments fully supported (fetch, diff, migration)
- [ ] Dependencies properly tracked and ordered
- [ ] Follow existing patterns and style guidelines

## Testing Patterns

**Setup:** Run `./scripts/test-setup.sh` once to start PostgreSQL containers (versions 13-18).

### Test Helpers - Always Use These

**Database tests:** Use `with_test_db()` for automatic database creation and cleanup
```rust
use crate::helpers::harness::with_test_db;

#[tokio::test]
async fn test_fetch_tables() {
    with_test_db(async |db| {
        db.execute("CREATE TABLE users (id INT)").await;
        let tables = fetch(db.pool()).await.unwrap();
        assert_eq!(tables.len(), 1);
    }).await;  // Database automatically cleaned up
}
```

**Migration tests:** Use `MigrationTestHelper` with the **3-vector pattern**:
```rust
let helper = MigrationTestHelper::new().await;
helper.run_migration_test(
    &["CREATE TABLE users (id INT)"],  // Both initial and target DBs
    &["INSERT INTO users VALUES (1)"], // Initial DB only (before state)
    &["CREATE INDEX idx ON users(id)"],// Target DB only (after state)
    |steps, final_catalog| {
        // Verify steps and final state
        Ok(())
    }
).await
```

**CLI tests:** Use `with_cli_helper()` for command-line testing
```rust
use crate::helpers::cli::with_cli_helper;

#[tokio::test]
async fn test_baseline_create() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;
        helper.write_schema_file("users.sql", "CREATE TABLE users (id INT);")?;

        helper.command()
            .args(["baseline", "create"])
            .assert()
            .success();

        Ok(())
    }).await
}
```

### Test Organization

Look at existing tests to find patterns:
- `tests/catalog/*` - Object fetching (use `with_test_db`)
- `tests/migrations/*` - Migration generation (use `MigrationTestHelper`)
- `tests/cli/*` - CLI commands (use `with_cli_helper`)
- `tests/helpers/*` - Shared utilities

## Baseline Management

### Critical Migration Reconstruction Logic

**Without baselines (clone repo scenario):**
- pgmt reconstructs state by applying ALL existing migrations chronologically
- **Critical:** Ensures incremental migrations, NOT full schema recreation
- Example: `git clone` → edit schema → `migrate new` → generates ALTER statement (not full CREATE)

**With baselines:**
- Uses latest baseline as starting point for comparison
- Faster than reconstructing full migration chain

**Default behavior:** `migrate new` does NOT create baselines (use `--create-baseline` flag)

**Key functions:**
- `reconstruct_from_migration_chain()` - Apply all migrations for fresh start
- `reconstruct_from_migration_chain_before_version()` - For `migrate update`

## File Dependencies (`-- require:`)

Schema files can declare dependencies using `-- require: path/to/file.sql` comments.

**Example:**
```sql
-- require: schema/types/priority.sql
CREATE TABLE tasks (
    id SERIAL PRIMARY KEY,
    priority priority  -- Uses enum from required file
);
```

**Implementation:** `src/catalog/file_dependencies.rs`
- When file A requires file B, all objects in A depend on all objects in B
- Augments PostgreSQL's introspection-based dependency tracking
- Used during `apply` and `migrate new` for correct ordering

## Configuration System

**Dual Type System:**
- `ConfigInput` - Uses `Option<T>`, enables merging partial configs
- `Config` - Resolved values used at runtime
- `ConfigBuilder` - Handles merging with precedence: CLI args > pgmt.yaml > defaults

**When adding config options:**
1. Add to `ConfigInput` with `Option<T>`
2. Add to `Config` with concrete type
3. Add default in `src/config/defaults.rs`
4. Update `ConfigBuilder::resolve()`
5. Add CLI argument in `src/main.rs`
6. Add tests in `tests/unit/config.rs`

## Code Style Guidelines

- Keep functions focused and under ~50 lines
- Extract helper functions for complex queries
- Use explicit type annotations for HashMap/Vec when helpful
- Prefer `table.columns[0]` over `.find()` in tests to verify ordering
- Always test edge cases: empty tables, custom array types, etc.

### Logging Guidelines

**Critical for UX** - incorrect logging creates noise that confuses users. Default level is `warn`:

- **`println!()`**: User-facing output (commands, results, migration plans) - always visible
- **`info!()`**: Operational details (connection status, final success messages) - visible with `--verbose`
- **`debug!()`**: Implementation details (timing, retries, internal state, temp schemas) - visible with `--debug`
- **`warn!()`**: Potential problems (not false positives like "404 during cleanup")
- **Rule**: Ask "Would this scare a first-time user?" If yes, it's `debug!()` not `println!()`

## Debugging Tips

**Common Issues:**
- **Column ordering**: Always test with explicit indexing (`table.columns[0]`)
- **Custom types**: Check both qualified and unqualified names
- **Dependencies**: Use `depends_on` field to verify relationship detection
- **Deterministic ordering**: Use BTreeMap/BTreeSet instead of HashMap/HashSet

**Useful Commands:**
```bash
cargo test                   # Run all tests
cargo test tables           # Run tests with "tables" in name
cargo test catalog::tables  # Run table catalog tests
cargo check                  # Fast syntax checking

# After modifying sqlx queries:
cargo sqlx prepare --workspace              # Regenerate .sqlx/ metadata
SQLX_OFFLINE=true cargo clippy             # Test offline mode like CI
```

## Discovery & References

**When adding new features, find similar existing code:**
- New object type? Look at `src/catalog/policy.rs` + `tests/catalog/policies.rs` + `tests/migrations/policies.rs`
- New migration logic? Look at `tests/migrations/grants.rs` or `tests/migrations/policies.rs` for patterns
- New CLI command? Look at `src/commands/baseline.rs` + `tests/cli/baseline_commands.rs`

**Documentation:**
- User guides: `web/src/content/docs/docs/guides/`
- CLI reference: `web/src/content/docs/docs/cli/`
- Architecture: `web/src/content/docs/docs/development/architecture.md`

**Before committing:**
1. Check formatting: `cargo fmt --check` (or `cargo fmt` to auto-fix)
2. Check lints: `SQLX_OFFLINE=true cargo clippy --all-targets --all-features -- -D warnings`
3. Run tests: `cargo test`
4. Update this CLAUDE.md if adding new patterns
5. Update user docs in `web/src/content/docs/` if adding features

**Philosophy:** Correctness and safety over rapid development. When in doubt, choose the explicit, testable approach.