#!/bin/bash
set -e

# Setup development database for sqlx compile-time verification using Docker
# This creates a PostgreSQL container that sqlx can use to validate queries

CONTAINER_NAME="pgmt-sqlx-dev"
DB_PASSWORD=${DB_PASSWORD:-"pgmt_dev_password"}
DB_NAME=${DB_NAME:-"pgmt_dev"}
POSTGRES_VERSION=${POSTGRES_VERSION:-"18-alpine"}

# Function to find an available port
find_available_port() {
    local port
    # Start from 5433 and find the first available port
    for port in $(seq 5433 5500); do
        if ! netstat -tln 2>/dev/null | grep -q ":$port " && ! ss -tln 2>/dev/null | grep -q ":$port "; then
            echo $port
            return
        fi
    done
    # Fallback to a random high port if nothing in range is available
    python3 -c "import socket; s=socket.socket(); s.bind(('',0)); print(s.getsockname()[1]); s.close()" 2>/dev/null || echo $((RANDOM + 10000))
}

# Use provided port or find an available one
if [ -n "$DB_PORT" ]; then
    # Check if the specified port is available
    if netstat -tln 2>/dev/null | grep -q ":$DB_PORT " || ss -tln 2>/dev/null | grep -q ":$DB_PORT "; then
        echo "⚠️  Port $DB_PORT is already in use. Finding an available port..."
        DB_PORT=$(find_available_port)
    fi
else
    DB_PORT=$(find_available_port)
fi

DATABASE_URL="postgres://postgres:${DB_PASSWORD}@localhost:${DB_PORT}/${DB_NAME}"

echo "🐳 Setting up PostgreSQL development database with Docker..."
echo "Container: $CONTAINER_NAME"
echo "Port: $DB_PORT"
echo "Database: $DB_NAME"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Stop and remove existing container if it exists
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "🧹 Removing existing container: $CONTAINER_NAME"
    docker stop $CONTAINER_NAME > /dev/null 2>&1 || true
    docker rm $CONTAINER_NAME > /dev/null 2>&1 || true
fi

# Start new PostgreSQL container
echo "🚀 Starting PostgreSQL container..."
docker run -d \
    --name $CONTAINER_NAME \
    -p $DB_PORT:5432 \
    -e POSTGRES_PASSWORD=$DB_PASSWORD \
    -e POSTGRES_DB=$DB_NAME \
    -e POSTGRES_USER=postgres \
    postgres:$POSTGRES_VERSION

# Wait for PostgreSQL to be ready
echo "⏳ Waiting for PostgreSQL to be ready..."
timeout=30
while [ $timeout -gt 0 ]; do
    if docker exec $CONTAINER_NAME pg_isready -U postgres > /dev/null 2>&1; then
        echo "✅ PostgreSQL is ready!"
        break
    fi
    sleep 1
    timeout=$((timeout - 1))
done

if [ $timeout -eq 0 ]; then
    echo "❌ PostgreSQL failed to start within 30 seconds"
    docker logs $CONTAINER_NAME
    exit 1
fi

# Create minimal schema that sqlx needs for query validation
echo "🔧 Setting up minimal schema for sqlx query validation..."
docker exec -i $CONTAINER_NAME psql -U postgres -d $DB_NAME << 'EOF'
-- Create minimal test schema for sqlx query validation
-- This provides the basic structure that pgmt's catalog queries expect

-- Ensure we have the basic system catalogs (they should already exist)
-- Create some test data that matches what our queries might encounter

-- Create a test schema (in addition to public)
CREATE SCHEMA IF NOT EXISTS test_schema;

-- Create a simple test table for basic query validation
CREATE TABLE IF NOT EXISTS public._sqlx_test (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create a test table in the test schema
CREATE TABLE IF NOT EXISTS test_schema.test_table (
    id INTEGER PRIMARY KEY,
    data TEXT
);

-- Create a simple test view
CREATE OR REPLACE VIEW public.test_view AS 
SELECT id, name FROM public._sqlx_test;

-- Create a simple test function
CREATE OR REPLACE FUNCTION public.test_function(input_text TEXT) 
RETURNS TEXT AS $$
BEGIN
    RETURN 'test: ' || input_text;
END;
$$ LANGUAGE plpgsql;

-- Create a simple enum type
DO $$ BEGIN
    CREATE TYPE public.test_enum AS ENUM ('active', 'inactive', 'pending');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Create a test trigger function and trigger
CREATE OR REPLACE FUNCTION public.test_trigger_function()
RETURNS TRIGGER AS $$
BEGIN
    NEW.name := UPPER(NEW.name);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS test_trigger ON public._sqlx_test;
CREATE TRIGGER test_trigger 
    BEFORE INSERT OR UPDATE ON public._sqlx_test
    FOR EACH ROW EXECUTE FUNCTION public.test_trigger_function();

-- Insert some test data
INSERT INTO public._sqlx_test (name) VALUES ('test1'), ('test2') 
ON CONFLICT DO NOTHING;

-- Grant some permissions for testing grant queries
CREATE ROLE IF NOT EXISTS test_role;
GRANT SELECT ON public._sqlx_test TO test_role;

EOF

# Update or create .env file with the DATABASE_URL
if [ -f ".env" ]; then
    # Update existing .env file
    if grep -q "^DATABASE_URL=" .env; then
        # Replace existing DATABASE_URL
        if [[ "$OSTYPE" == "darwin"* ]]; then
            sed -i '' "s|^DATABASE_URL=.*|DATABASE_URL=\"$DATABASE_URL\"|" .env
        else
            sed -i "s|^DATABASE_URL=.*|DATABASE_URL=\"$DATABASE_URL\"|" .env
        fi
        echo "📝 Updated DATABASE_URL in .env file"
    else
        # Add DATABASE_URL to existing .env
        echo "DATABASE_URL=\"$DATABASE_URL\"" >> .env
        echo "📝 Added DATABASE_URL to .env file"
    fi
else
    # Create new .env file
    echo "DATABASE_URL=\"$DATABASE_URL\"" > .env
    echo "📝 Created .env file with DATABASE_URL"
fi

echo "✅ Development database is ready for sqlx!"
echo ""
echo "📝 Database connection details:"
echo "  URL: $DATABASE_URL"
echo "  Container: $CONTAINER_NAME"
echo "  Port: $DB_PORT"
echo "  .env file: Updated"
echo ""
echo "🔄 Next steps:"
echo "1. Source the .env file: source .env (or restart your terminal)"
echo "2. Run './scripts/prepare-sqlx.sh' to generate query metadata"
echo "3. Commit the .sqlx/ directory to enable offline builds"
echo ""
echo "🛑 To stop the database:"
echo "  docker stop $CONTAINER_NAME"
echo ""
echo "🗑️  To remove the database:"
echo "  docker rm $CONTAINER_NAME"