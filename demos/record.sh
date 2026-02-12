#!/usr/bin/env bash
#
# Orchestrates the full demo recording.
# Starts a postgres container, sets up the demo project,
# runs VHS, and cleans up.
#
# Usage:
#   ./demos/record.sh
#
# Requires: docker, pgmt, vhs, git
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PG_VERSION="${PG_VERSION:-17}"

# --- Start postgres in an isolated container ---
echo "Starting postgres ${PG_VERSION}..."
PG_CONTAINER=$(docker run -d --rm \
    -e POSTGRES_DB=pgmt_demo \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=demo \
    -p 0:5432 \
    "postgres:${PG_VERSION}")
trap "docker rm -f $PG_CONTAINER > /dev/null 2>&1" EXIT

# Get the randomly assigned port
PG_PORT=$(docker port "$PG_CONTAINER" 5432/tcp | head -1 | cut -d: -f2)
export DATABASE_URL="postgres://postgres:demo@localhost:${PG_PORT}/pgmt_demo"

# Wait for postgres to accept connections
echo "Waiting for postgres..."
until docker exec "$PG_CONTAINER" pg_isready -q 2>/dev/null; do
    sleep 0.5
done

# --- Set up the demo project ---
echo "Setting up demo project..."
DEMO_DIR=$(mktemp -d)
"$SCRIPT_DIR/setup.sh" "$DEMO_DIR"

# --- Record ---
echo "Recording demo..."
cd "$DEMO_DIR"
vhs "$SCRIPT_DIR/demo.tape"

# Copy outputs from temp dir to demos/out/
mkdir -p "$SCRIPT_DIR/out"
cp "$DEMO_DIR"/demo.gif "$DEMO_DIR"/demo.webm "$SCRIPT_DIR/out/"
cp "$DEMO_DIR"/out.txt "$SCRIPT_DIR/out/" 2>/dev/null || true

echo "Done! Output in demos/out/"
