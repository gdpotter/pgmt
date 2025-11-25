#!/bin/bash

# One-command setup for pgmt test environment
# This script starts all PostgreSQL versions and sets up DATABASE_URL

set -e

echo "🐘 Setting up pgmt test databases..."

# Check if Docker is running
if ! docker info &> /dev/null; then
    echo "❌ Error: Docker is not running. Please start Docker and try again." >&2
    exit 1
fi

# Start all PostgreSQL versions
echo "📦 Starting PostgreSQL containers..."
docker-compose -f docker-compose.test.yml up -d

# Wait for health checks
echo "⏳ Waiting for PostgreSQL containers to be ready..."
TIMEOUT=60
START_TIME=$(date +%s)

while true; do
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))
    
    if [ $ELAPSED -gt $TIMEOUT ]; then
        echo "❌ Timeout waiting for PostgreSQL containers to be ready" >&2
        echo "Check container logs with: docker-compose -f docker-compose.test.yml logs" >&2
        exit 1
    fi
    
    # Check if all containers are healthy
    ALL_HEALTHY=true
    for VERSION in 13 14 15 16 17 18; do
        CONTAINER="pgmt-test-pg${VERSION}"
        HEALTH=$(docker inspect --format='{{.State.Health.Status}}' ${CONTAINER} 2>/dev/null || echo "unknown")
        if [ "$HEALTH" != "healthy" ]; then
            ALL_HEALTHY=false
            break
        fi
    done
    
    if [ "$ALL_HEALTHY" = true ]; then
        break
    fi
    
    echo "  Still waiting... (${ELAPSED}s elapsed)"
    sleep 2
done

# Set up default .env file with PostgreSQL 18
echo "📝 Setting up .env file with PostgreSQL 18 as default..."
DEFAULT_URL=$(./scripts/test-db-url.sh 18)

# Preserve existing DATABASE_URL in .env if it exists and user wants to keep it
if [ -f .env ] && grep -q "^DATABASE_URL=" .env; then
    read -p "📋 .env already contains DATABASE_URL. Overwrite with test database? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "🔄 Keeping existing DATABASE_URL in .env"
        echo "✅ Test setup complete!"
        echo ""
        echo "Available PostgreSQL versions:"
        for VERSION in 13 14 15 16 17 18; do
            URL=$(./scripts/test-db-url.sh ${VERSION})
            echo "  PostgreSQL ${VERSION}: ${URL}"
        done
        exit 0
    fi
fi

# Update or create .env file
if [ -f .env ]; then
    # Remove existing DATABASE_URL line and add new one
    grep -v "^DATABASE_URL=" .env > .env.tmp || true
    echo "DATABASE_URL=${DEFAULT_URL}" >> .env.tmp
    mv .env.tmp .env
else
    echo "DATABASE_URL=${DEFAULT_URL}" > .env
fi

echo "✅ Test setup complete!"
echo ""
echo "📚 Quick start:"
echo "  cargo test                    # Uses PostgreSQL 18 (default)"
echo ""
echo "🔄 Test specific versions:"
echo "  DATABASE_URL=\$(./scripts/test-db-url.sh 13) cargo test  # PostgreSQL 13"
echo "  DATABASE_URL=\$(./scripts/test-db-url.sh 14) cargo test  # PostgreSQL 14"
echo "  DATABASE_URL=\$(./scripts/test-db-url.sh 15) cargo test  # PostgreSQL 15"
echo "  DATABASE_URL=\$(./scripts/test-db-url.sh 16) cargo test  # PostgreSQL 16"
echo "  DATABASE_URL=\$(./scripts/test-db-url.sh 17) cargo test  # PostgreSQL 17"
echo "  DATABASE_URL=\$(./scripts/test-db-url.sh 18) cargo test  # PostgreSQL 18"
echo ""
echo "🧪 Test all versions:"
echo "  ./scripts/test-all-versions.sh"
echo ""
echo "🛑 Stop test databases:"
echo "  docker-compose -f docker-compose.test.yml down"