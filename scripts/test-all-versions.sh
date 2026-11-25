#!/bin/bash

# Test pgmt against all supported PostgreSQL versions
# This script runs the full test suite against each PostgreSQL version

set -e

VERSIONS=(13 14 15 16 17 18 19)
FAILED_VERSIONS=()

echo "🧪 Testing pgmt against all PostgreSQL versions..."
echo "📊 This will run $(echo ${#VERSIONS[@]}) test suites"
echo ""

START_TIME=$(date +%s)

for VERSION in "${VERSIONS[@]}"; do
    echo "🐘 Testing PostgreSQL ${VERSION}..."
    
    # Get the database URL for this version
    if ! URL=$(./scripts/test-db-url.sh ${VERSION}); then
        echo "❌ Failed to get database URL for PostgreSQL ${VERSION}"
        FAILED_VERSIONS+=($VERSION)
        continue
    fi
    
    # Run the tests with this version
    if DATABASE_URL="${URL}" cargo test --quiet; then
        echo "✅ PostgreSQL ${VERSION} tests passed"
    else
        echo "❌ PostgreSQL ${VERSION} tests failed"
        FAILED_VERSIONS+=($VERSION)
    fi
    echo ""
done

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo "📈 Test Summary"
echo "=============="
echo "⏱️  Total time: ${ELAPSED}s"
echo "✅ Passed: $((${#VERSIONS[@]} - ${#FAILED_VERSIONS[@]}))/${#VERSIONS[@]} PostgreSQL versions"

if [ ${#FAILED_VERSIONS[@]} -eq 0 ]; then
    echo "🎉 All PostgreSQL versions pass!"
    exit 0
else
    echo "❌ Failed: ${FAILED_VERSIONS[*]}"
    echo ""
    echo "💡 To debug a specific version:"
    for VERSION in "${FAILED_VERSIONS[@]}"; do
        echo "   DATABASE_URL=\$(./scripts/test-db-url.sh ${VERSION}) cargo test"
    done
    exit 1
fi
