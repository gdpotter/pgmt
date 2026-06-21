---
title: Contributing
description: How to contribute to pgmt development.
---

## Setup

```bash
git clone https://github.com/gdpotter/pgmt.git
cd pgmt
./scripts/test-setup.sh    # Start PostgreSQL containers
cargo test                  # Run tests
```

**Prerequisites:** Rust 1.85+, Docker

## Code Style

- `cargo fmt` before committing
- `cargo clippy -- -D warnings` - no warnings
- Use `anyhow::Result` for errors
- All SQL queries use sqlx compile-time verification
- Tests required for new features

## Logging Guidelines

Correct logging is critical for UX. The default level is `warn`:

- `println!()`: User-facing output (commands, results, migration plans) — always visible
- `info!()`: Operational details (connection status, success messages) — visible with `--verbose`
- `debug!()`: Implementation details (timing, retries, temp schemas) — visible with `--debug`
- `warn!()`: Potential problems (not expected behaviors like "404 during cleanup")

**Rule of thumb:** Ask "Would this scare a first-time user?" If yes, use `debug!()` not `println!()`.

## Adding Database Object Support

Each object type needs:

1. **Catalog module** (`src/catalog/object.rs`)
   - `fetch()` function using PostgreSQL system catalogs
   - Include `comment: Option<String>` field
   - Implement `DependsOn` and `Commentable` traits

2. **Diff logic** (`src/diff/object.rs`)
   - Compare old vs new states
   - Generate CREATE, DROP, ALTER operations
   - Handle comment changes with `diff_comments()`

3. **Migration operations** (`src/diff/operations/`)
   - Define operation enums
   - Implement `SqlRenderer` trait

4. **Tests** (`tests/catalog/` and `tests/migrations/`)

Look at `src/catalog/triggers.rs` for a pattern to follow.

## Testing

```rust
// Isolated test database
#[tokio::test]
async fn test_fetch() {
    with_test_db(async |db| {
        db.execute("CREATE TABLE users (id INT)").await;
        let tables = fetch(db.pool()).await.unwrap();
        assert_eq!(tables.len(), 1);
    }).await;
}

// Migration testing
#[tokio::test]
async fn test_migration() {
    let helper = MigrationTestHelper::new().await;
    helper.run_migration_test(
        &[],  // Both databases
        &[],  // Initial only
        &["CREATE TABLE users (id INT)"],  // Target only
        |steps, _| {
            assert!(!steps.is_empty());
            Ok(())
        }
    ).await;
}
```

## Pull Requests

**Before submitting:**

```bash
cargo test
cargo fmt
cargo clippy -- -D warnings
SQLX_OFFLINE=true cargo build
```

**PR requirements:**

- Clear description of changes
- Tests pass
- No clippy warnings
- sqlx metadata committed (`.sqlx/` files)

## Releasing

Releases are automated with [git-cliff](https://git-cliff.org) for the changelog
and [cargo-release](https://github.com/crate-ci/cargo-release) for the version
bump and tag. Pushing a `vX.Y.Z` tag triggers `.github/workflows/release.yml`,
which builds the binaries and publishes to crates.io and npm — the local steps
deliberately do **not** publish.

**1. Curate the changelog.** `CHANGELOG.md` is user-facing, not a commit dump.
The `## Unreleased` section is maintained by hand; draft entries from recent
commits with:

```bash
git cliff --unreleased   # preview entries since the last tag
```

Move the genuinely user-facing items into `## Unreleased` under **Breaking
Changes / Features / Bug Fixes / Performance**, and drop noise (refactors,
tests, CI, dependency bumps) and fixes to still-unreleased work. Writing
[Conventional Commits](https://www.conventionalcommits.org) keeps the draft
clean — git-cliff groups by type and filters chores automatically.

**2. Cut the release** from a green `main`:

```bash
cargo release minor            # preview: patch | minor | major | X.Y.Z
cargo release minor --execute  # bump Cargo.toml + Cargo.lock, stamp the
                               # changelog, commit, tag vX.Y.Z, and push
```

cargo-release promotes `## Unreleased` into a dated version section, then the
pushed tag drives CI, which extracts that section for the GitHub release notes
and publishes everything.

Version bumps are explicit: `cargo release` does exactly the level you name
(pre-1.0, a feature release is `minor`, e.g. 0.4.x → 0.5.0). Configuration lives
in `cliff.toml` and `release.toml`.

## Reporting Issues

Include: pgmt version, PostgreSQL version, OS, full error messages, steps to reproduce.

## Code Organization

| Directory        | Purpose                   |
| ---------------- | ------------------------- |
| `catalog/`       | PostgreSQL introspection  |
| `diff/`          | Schema comparison         |
| `commands/`      | CLI implementations       |
| `config/`        | Configuration             |
| `schema_loader/` | Multi-file schema loading |
| `render/`        | SQL generation            |
