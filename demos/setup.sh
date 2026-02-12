#!/usr/bin/env bash
#
# Creates a demo pgmt project in the given directory.
# Expects DATABASE_URL to be set (by record.sh or CI).
#
# Usage: setup.sh <demo-dir>
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="${1:?Usage: setup.sh <demo-dir>}"

if [ -z "${DATABASE_URL:-}" ]; then
    echo "Error: DATABASE_URL must be set" >&2
    exit 1
fi

cd "$DEMO_DIR"

# Create project structure
mkdir -p schema/{schemas,tables,views,functions,types} migrations baselines

# Generate config from template (substitutes DATABASE_URL)
envsubst < "$SCRIPT_DIR/project/pgmt.yaml" > pgmt.yaml

# Copy schema files using the "before" version of the function
cp "$SCRIPT_DIR"/project/schema/tables/*.sql schema/tables/
cp "$SCRIPT_DIR"/project/schema/views/*.sql schema/views/
cp "$SCRIPT_DIR/project/initial/functions/calculate_score.sql" schema/functions/

# Apply the baseline schema
pgmt apply
pgmt migrate new "initial migration"

# Set up git so we can show a diff in the demo
git init -q
git add -A
git commit -q -m "initial schema"

# Swap in the edited function (adds include_bonus parameter)
cp "$SCRIPT_DIR/project/schema/functions/calculate_score.sql" schema/functions/
