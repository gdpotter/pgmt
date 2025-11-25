#!/bin/bash
set -e

# Generate sqlx query metadata for offline compilation
# This script assumes the development database is already running

echo "🔍 Preparing sqlx query metadata..."

# Check if DATABASE_URL is set
if [ -z "$DATABASE_URL" ]; then
    echo "❌ DATABASE_URL is not set."
    echo "   Either:"
    echo "   1. Run: export DATABASE_URL=\"postgres://postgres:pgmt_dev_password@localhost:5432/pgmt_dev\""
    echo "   2. Copy .env.example to .env and source it"
    echo "   3. Run scripts/setup-dev-db.sh first"
    exit 1
fi

echo "📡 Using DATABASE_URL: $DATABASE_URL"

# Test database connection
if ! cargo sqlx database create 2>/dev/null; then
    echo "❌ Failed to connect to database. Make sure PostgreSQL is running."
    echo "   Run: ./scripts/setup-dev-db.sh"
    exit 1
fi

echo "✅ Database connection successful"

# Install sqlx-cli if not already installed
if ! command -v cargo-sqlx >/dev/null 2>&1; then
    echo "📦 Installing sqlx-cli..."
    cargo install sqlx-cli --no-default-features --features postgres
fi

# Generate query metadata
echo "🔧 Generating query metadata..."
cargo sqlx prepare --workspace

# Check if .sqlx directory was created
if [ ! -d ".sqlx" ]; then
    echo "❌ Failed to generate .sqlx directory"
    exit 1
fi

echo "✅ Query metadata generated successfully!"
echo ""
echo "📁 Generated files:"
file_count=$(ls -1 .sqlx/ | wc -l)
echo "   $file_count query metadata files in .sqlx/"
echo ""
echo "🧪 Testing offline mode..."
if SQLX_OFFLINE=true cargo check --quiet 2>/dev/null; then
    echo "✅ Offline mode test successful!"
else
    echo "❌ Offline mode test failed"
    exit 1
fi
echo ""
echo "🎉 Next steps:"
echo "1. Commit the .sqlx/ directory: git add .sqlx && git commit -m 'Add sqlx query metadata'"
echo "2. Builds will now work offline without a database!"
echo "3. To update metadata after query changes, run this script again"
echo ""
echo "🌐 For CI/CD builds, use: SQLX_OFFLINE=true cargo build"