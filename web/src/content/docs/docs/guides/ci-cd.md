---
title: CI/CD Integration
description: Patterns for integrating pgmt with continuous integration and deployment pipelines.
---

Patterns for integrating pgmt with CI/CD pipelines.

## Available Commands

| Command                 | Purpose                                         | When to Use            |
| ----------------------- | ----------------------------------------------- | ---------------------- |
| `pgmt apply`            | Apply schema to dev database                    | Integration tests      |
| `pgmt config validate`  | Validate configuration syntax                   | Every PR               |
| `pgmt migrate validate` | Ensure migrations produce intended schema       | Every PR               |
| `pgmt migrate diff`     | Detect drift between schema and target database | Scheduled / pre-deploy |
| `pgmt migrate apply`    | Deploy migrations to target database            | On merge to main       |

## Using pgmt apply in CI

For integration tests or setting up a dev database in CI, use `pgmt apply`:

```bash
# Default: fails with exit code 2 if destructive operations exist
pgmt apply

# Force apply everything (use when you know destructive ops are expected)
pgmt apply --force

# Apply only safe operations, skip destructive
pgmt apply --safe-only
```

**How it works:** In non-interactive environments (CI/CD), `pgmt apply` defaults to `--require-approval` behavior, meaning it will:
- Auto-apply all safe operations
- Exit with code 2 if any destructive operations exist

This is ideal for agentic tools and CI pipelines where you want to:
1. Try the safe path first (`pgmt apply`)
2. If it fails with exit code 2, either fix the schema or use `--force`

## Basic Workflow

1. **Development**: Edit schema files, use `pgmt apply` for immediate feedback
2. **CI Pipeline**: Validate schema and migration consistency
3. **Deployment**: Apply reviewed migrations via `pgmt migrate apply`
4. **Monitoring**: Detect drift with scheduled `pgmt migrate diff`

## GitHub Actions Example

### PR Validation

```yaml
# .github/workflows/database-ci.yml
name: Database CI

on:
  pull_request:
    paths: ['schema/**', 'migrations/**', 'pgmt.yaml']
  push:
    branches: [main]

jobs:
  validate:
    runs-on: ubuntu-latest

    services:
      postgres:
        image: postgres:17
        env:
          POSTGRES_PASSWORD: ci_test
        options: --health-cmd pg_isready --health-interval 10s
        ports: [5432:5432]

    steps:
      - uses: actions/checkout@v4

      - name: Install pgmt
        uses: gdpotter/pgmt@v0

      - name: Setup database
        run: createdb testdb
        env:
          PGPASSWORD: ci_test
          PGHOST: localhost
          PGUSER: postgres

      - name: Validate configuration
        run: pgmt config validate

      - name: Validate migrations
        run: pgmt migrate validate
        env:
          DEV_DATABASE_URL: postgres://postgres:ci_test@localhost/testdb

  deploy:
    if: github.ref == 'refs/heads/main'
    needs: [validate]
    runs-on: ubuntu-latest
    environment: production

    steps:
      - uses: actions/checkout@v4

      - name: Install pgmt
        uses: gdpotter/pgmt@v0

      - name: Apply migrations
        run: pgmt migrate apply
        env:
          TARGET_DATABASE_URL: ${{ secrets.PROD_DATABASE_URL }}
```

### Drift Detection

Monitor production for schema drift with a scheduled workflow:

```yaml
# .github/workflows/drift-check.yml
name: Schema Drift Detection

on:
  schedule:
    - cron: '0 9 * * 1-5' # Weekdays at 9am UTC
  workflow_dispatch: # Allow manual trigger

jobs:
  check-drift:
    runs-on: ubuntu-latest

    services:
      postgres:
        image: postgres:17
        env:
          POSTGRES_PASSWORD: ci_test
        options: --health-cmd pg_isready --health-interval 10s
        ports: [5432:5432]

    steps:
      - uses: actions/checkout@v4

      - name: Install pgmt
        uses: gdpotter/pgmt@v0

      - name: Setup shadow database
        run: createdb shadowdb
        env:
          PGPASSWORD: ci_test
          PGHOST: localhost
          PGUSER: postgres

      - name: Check for drift
        id: drift
        run: |
          set +e
          pgmt migrate diff --format json > drift.json
          DRIFT_EXIT_CODE=$?
          set -e

          if [ $DRIFT_EXIT_CODE -eq 1 ]; then
            echo "drift_detected=true" >> $GITHUB_OUTPUT
            echo "::warning::Schema drift detected in production!"
          else
            echo "drift_detected=false" >> $GITHUB_OUTPUT
          fi
        env:
          DEV_DATABASE_URL: postgres://postgres:ci_test@localhost/shadowdb
          TARGET_DATABASE_URL: ${{ secrets.PROD_DATABASE_URL }}

      - name: Create issue on drift
        if: steps.drift.outputs.drift_detected == 'true'
        run: |
          # Check for existing open drift issue
          EXISTING=$(gh issue list --label "drift" --state open --json number --jq '.[0].number // empty')

          CHANGES=$(jq -r '.summary.total_changes' drift.json)
          DESTRUCTIVE=$(jq -r '.summary.destructive_changes' drift.json)

          BODY="Production database has drifted from expected schema.

          **Summary:** $CHANGES changes ($DESTRUCTIVE destructive)

          Run \`pgmt migrate diff --target-url <prod-url>\` locally for details.

          ---
          *Detected on $(date -u +%Y-%m-%d)*"

          if [ -n "$EXISTING" ]; then
            echo "Updating existing issue #$EXISTING"
            gh issue comment "$EXISTING" --body "Drift still present as of $(date -u +%Y-%m-%d): $CHANGES changes"
          else
            echo "Creating new drift issue"
            gh issue create \
              --title "Schema drift detected ($CHANGES changes)" \
              --body "$BODY" \
              --label "drift,database"
          fi
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}

      - name: Close issue if no drift
        if: steps.drift.outputs.drift_detected == 'false'
        run: |
          EXISTING=$(gh issue list --label "drift" --state open --json number --jq '.[0].number // empty')
          if [ -n "$EXISTING" ]; then
            gh issue close "$EXISTING" --comment "Drift resolved as of $(date -u +%Y-%m-%d)"
          fi
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

## GitLab CI Example

```yaml
# .gitlab-ci.yml
stages:
  - validate
  - deploy
  - monitor

validate-schema:
  stage: validate
  services:
    - postgres:17
  variables:
    POSTGRES_PASSWORD: test
    DEV_DATABASE_URL: postgres://postgres:test@postgres/postgres
  script:
    - curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    - source ~/.cargo/env
    - cargo install pgmt
    - pgmt config validate
    - pgmt migrate validate

deploy-migrations:
  stage: deploy
  environment: production
  script:
    - curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    - source ~/.cargo/env
    - cargo install pgmt
    - pgmt migrate apply
  only:
    - main

check-drift:
  stage: monitor
  rules:
    - if: $CI_PIPELINE_SOURCE == "schedule"
  services:
    - postgres:17
  variables:
    POSTGRES_PASSWORD: test
    DEV_DATABASE_URL: postgres://postgres:test@postgres/postgres
  script:
    - curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    - source ~/.cargo/env
    - cargo install pgmt
    - pgmt migrate diff --format summary
  allow_failure: true # Don't block other pipelines
```

## Simple Deployment Script

```bash
#!/bin/bash
# deploy.sh - Simple deployment script

set -e

ENVIRONMENT=${1:-staging}

case $ENVIRONMENT in
  "staging")
    DATABASE_URL="$STAGING_DATABASE_URL"
    ;;
  "production")
    DATABASE_URL="$PRODUCTION_DATABASE_URL"
    ;;
  *)
    echo "Usage: $0 {staging|production}"
    exit 1
    ;;
esac

echo "Deploying to $ENVIRONMENT..."
pgmt migrate apply --target-url "$DATABASE_URL"
echo "Deployment complete"
```

## Output Formats

The `pgmt migrate diff` command supports multiple output formats for CI integration:

| Format   | Flag                | Use Case                        |
| -------- | ------------------- | ------------------------------- |
| Detailed | `--format detailed` | Human review, see exact changes |
| Summary  | `--format summary`  | Quick overview of change counts |
| SQL      | `--format sql`      | Generate remediation scripts    |
| JSON     | `--format json`     | CI/CD parsing and automation    |

Example JSON output:

```json
{
  "has_differences": true,
  "from": "target database",
  "to": "schema files",
  "summary": {
    "total_changes": 3,
    "destructive_changes": 1,
    "safe_changes": 2
  },
  "changes": [...]
}
```

## Exit Codes

Both `pgmt diff` and `pgmt migrate diff` use exit codes for CI integration:

| Exit Code | Meaning              |
| --------- | -------------------- |
| 0         | No differences found |
| 1         | Differences detected |
| Other     | Error occurred       |

## Tips

**Validate before merging:**

```bash
pgmt config validate
pgmt migrate validate
```

**Check for drift before deploying:**

```bash
pgmt migrate diff --format summary
```

**Use separate databases for each environment:**

- Development: Local PostgreSQL
- CI: Ephemeral PostgreSQL container
- Staging: Dedicated database
- Production: Dedicated database

**Store secrets properly:**

- Use GitHub Secrets, GitLab CI variables, or similar
- Never commit database URLs to version control
- Use environment variables or configuration management
