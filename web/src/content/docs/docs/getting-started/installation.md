---
title: Installation
description: Install pgmt via shell script, npm, or cargo. Prerequisites, upgrading, and CI setup.
---

**Shell (macOS/Linux):**

```bash
curl -fsSL https://pgmt.dev/install.sh | sh
```

**npm:**

```bash
npm install -g @pgmt/pgmt
```

**Cargo (builds from source):**

```bash
cargo install pgmt
```

Verify it worked:

```bash
pgmt --version
```

**Windows:** binaries are published via npm and the install script, but not yet
tested on Windows. If you try it, [let us know how it
goes](https://github.com/gdpotter/pgmt/issues).

## Prerequisites

- **PostgreSQL 13+** — a development database you can connect to.
- **Docker** — pgmt validates every change against a disposable
  [shadow database](/docs/concepts/shadow-database) it runs in Docker. If you
  can't run Docker, point `databases.shadow.url` at a dedicated empty database
  instead — pgmt resets it on every run, so never use a database with data you
  care about.

## Upgrading

Same commands as installing: re-run the install script, `npm update -g
@pgmt/pgmt`, or `cargo install pgmt`.

## CI

Don't `cargo install` in CI — that compiles pgmt from source and adds minutes
to every run. Use the GitHub Action or the install script, which fetch a
prebuilt binary:

```yaml
# GitHub Actions
- name: Install pgmt
  uses: gdpotter/pgmt@v0
```

```yaml
# Anywhere else (GitLab CI, etc.)
- curl -fsSL https://pgmt.dev/install.sh | sh
```

See the [CI/CD guide](/docs/guides/ci-cd) for full pipeline examples.
