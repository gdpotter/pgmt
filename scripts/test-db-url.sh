#!/bin/bash

# Get DATABASE_URL for a specific PostgreSQL version
# Usage: ./scripts/test-db-url.sh [version]
# Example: ./scripts/test-db-url.sh 15

set -e

VERSION=${1:-18}
CONTAINER="pgmt-test-pg${VERSION}"

# Check if container is running
if ! docker ps --format "table {{.Names}}" | grep -q "^${CONTAINER}$"; then
    echo "Error: ${CONTAINER} not running." >&2
    echo "Run: docker-compose -f docker-compose.test.yml up -d" >&2
    exit 1
fi

# Check if container is healthy
HEALTH=$(docker inspect --format='{{.State.Health.Status}}' ${CONTAINER} 2>/dev/null || echo "unknown")
if [ "$HEALTH" != "healthy" ]; then
    echo "Error: ${CONTAINER} not healthy (status: ${HEALTH})" >&2
    echo "Wait a moment and try again, or check logs: docker logs ${CONTAINER}" >&2
    exit 1
fi

# Get the dynamically assigned port
PORT=$(docker port ${CONTAINER} 5432/tcp | cut -d: -f2)

if [ -z "$PORT" ]; then
    echo "Error: Could not determine port for ${CONTAINER}" >&2
    exit 1
fi

echo "postgres://postgres:postgres@localhost:${PORT}/postgres"