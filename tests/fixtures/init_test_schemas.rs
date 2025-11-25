use anyhow::Result;
use std::fs;
use std::path::Path;

/// Test fixtures for init integration tests
/// Provides various schema configurations for testing import functionality
///
/// Creates a comprehensive PostgreSQL schema for testing all object types
pub fn create_comprehensive_schema() -> String {
    r#"-- Comprehensive test schema covering all pgmt-supported object types
-- Generated for pgmt init integration tests

-- Extensions (should be created first)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Schemas
CREATE SCHEMA auth;
CREATE SCHEMA billing;
CREATE SCHEMA analytics;

-- Custom Types
CREATE TYPE auth.user_role AS ENUM ('user', 'admin', 'moderator', 'guest');
CREATE TYPE billing.currency AS ENUM ('USD', 'EUR', 'GBP', 'JPY');
CREATE TYPE analytics.event_type AS ENUM ('click', 'view', 'purchase', 'signup');

-- Domain types
CREATE DOMAIN auth.email AS TEXT
CHECK (VALUE ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$');

CREATE DOMAIN billing.positive_amount AS DECIMAL(10,2)
CHECK (VALUE > 0);

-- Composite types
CREATE TYPE auth.user_profile AS (
    first_name TEXT,
    last_name TEXT,
    bio TEXT,
    avatar_url TEXT
);

CREATE TYPE billing.address AS (
    street TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    country TEXT
);

-- Sequences
CREATE SEQUENCE auth.user_id_seq START 1000;
CREATE SEQUENCE billing.invoice_seq START 10000 INCREMENT 1;
CREATE SEQUENCE analytics.event_id_seq;

-- Tables with comprehensive column types
CREATE TABLE auth.users (
    id INTEGER DEFAULT nextval('auth.user_id_seq') PRIMARY KEY,
    uuid UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
    email auth.email UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role auth.user_role DEFAULT 'user',
    profile auth.user_profile,
    is_active BOOLEAN DEFAULT true,
    login_count INTEGER DEFAULT 0,
    last_login TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB,
    tags TEXT[] DEFAULT '{}'
);

CREATE TABLE billing.customers (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES auth.users(id) ON DELETE CASCADE,
    customer_code TEXT UNIQUE NOT NULL,
    billing_address billing.address,
    preferred_currency billing.currency DEFAULT 'USD',
    credit_limit billing.positive_amount DEFAULT 1000.00,
    is_premium BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE billing.invoices (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES billing.customers(id) ON DELETE SET NULL,
    invoice_number INTEGER DEFAULT nextval('billing.invoice_seq') UNIQUE,
    amount billing.positive_amount NOT NULL,
    currency billing.currency NOT NULL,
    status TEXT DEFAULT 'draft' CHECK (status IN ('draft', 'sent', 'paid', 'cancelled')),
    due_date DATE,
    issued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    paid_at TIMESTAMP,
    notes TEXT
);

CREATE TABLE analytics.events (
    id BIGINT DEFAULT nextval('analytics.event_id_seq') PRIMARY KEY,
    user_id INTEGER REFERENCES auth.users(id) ON DELETE SET NULL,
    event_type analytics.event_type NOT NULL,
    event_data JSONB,
    ip_address INET,
    user_agent TEXT,
    occurred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Generated columns (PostgreSQL 12+)
ALTER TABLE billing.invoices
ADD COLUMN total_with_tax DECIMAL(10,2)
GENERATED ALWAYS AS (amount * 1.1) STORED;

-- Indexes (various types)
CREATE UNIQUE INDEX idx_users_email ON auth.users (email);
CREATE INDEX idx_users_role ON auth.users (role);
CREATE INDEX idx_users_active ON auth.users (is_active) WHERE is_active = true;
CREATE INDEX idx_users_created_at ON auth.users (created_at);

-- Expression indexes
CREATE INDEX idx_users_email_lower ON auth.users (lower(email));
CREATE INDEX idx_customers_code_upper ON billing.customers (upper(customer_code));

-- Multi-column indexes
CREATE INDEX idx_invoices_customer_status ON billing.invoices (customer_id, status);
CREATE INDEX idx_events_user_type_time ON analytics.events (user_id, event_type, occurred_at);

-- Partial indexes
CREATE INDEX idx_invoices_unpaid ON billing.invoices (customer_id)
WHERE status IN ('draft', 'sent');

-- GIN indexes for JSONB
CREATE INDEX idx_users_metadata_gin ON auth.users USING GIN (metadata);
CREATE INDEX idx_events_data_gin ON analytics.events USING GIN (event_data);

-- Array indexes
CREATE INDEX idx_users_tags_gin ON auth.users USING GIN (tags);

-- Views
CREATE VIEW auth.active_users AS
SELECT
    id,
    uuid,
    email,
    role,
    profile,
    login_count,
    last_login,
    created_at
FROM auth.users
WHERE is_active = true;

CREATE VIEW billing.customer_summary AS
SELECT
    c.id,
    c.customer_code,
    u.email as user_email,
    c.preferred_currency,
    c.credit_limit,
    c.is_premium,
    COUNT(i.id) as invoice_count,
    COALESCE(SUM(i.amount), 0) as total_invoiced,
    COUNT(CASE WHEN i.status = 'paid' THEN 1 END) as paid_invoices,
    COALESCE(SUM(CASE WHEN i.status = 'paid' THEN i.amount ELSE 0 END), 0) as total_paid
FROM billing.customers c
JOIN auth.users u ON c.user_id = u.id
LEFT JOIN billing.invoices i ON c.id = i.customer_id
GROUP BY c.id, c.customer_code, u.email, c.preferred_currency, c.credit_limit, c.is_premium;

CREATE VIEW analytics.user_activity_summary AS
SELECT
    u.id as user_id,
    u.email,
    COUNT(e.id) as total_events,
    COUNT(CASE WHEN e.event_type = 'click' THEN 1 END) as clicks,
    COUNT(CASE WHEN e.event_type = 'view' THEN 1 END) as views,
    COUNT(CASE WHEN e.event_type = 'purchase' THEN 1 END) as purchases,
    MAX(e.occurred_at) as last_activity
FROM auth.users u
LEFT JOIN analytics.events e ON u.id = e.user_id
GROUP BY u.id, u.email;

-- Functions and Procedures
CREATE FUNCTION auth.update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION auth.increment_login_count(user_id INTEGER)
RETURNS VOID AS $$
BEGIN
    UPDATE auth.users
    SET login_count = login_count + 1,
        last_login = CURRENT_TIMESTAMP
    WHERE id = user_id;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION billing.calculate_revenue(
    start_date DATE DEFAULT NULL,
    end_date DATE DEFAULT NULL,
    target_currency billing.currency DEFAULT 'USD'
)
RETURNS DECIMAL(15,2) AS $$
    SELECT COALESCE(SUM(amount), 0)
    FROM billing.invoices
    WHERE status = 'paid'
    AND currency = target_currency
    AND (start_date IS NULL OR issued_at::DATE >= start_date)
    AND (end_date IS NULL OR issued_at::DATE <= end_date);
$$ LANGUAGE SQL STABLE;

CREATE FUNCTION analytics.get_user_events(
    p_user_id INTEGER,
    p_event_type analytics.event_type DEFAULT NULL,
    p_limit INTEGER DEFAULT 100
)
RETURNS TABLE(
    event_id BIGINT,
    event_type analytics.event_type,
    event_data JSONB,
    occurred_at TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    SELECT e.id, e.event_type, e.event_data, e.occurred_at
    FROM analytics.events e
    WHERE e.user_id = p_user_id
    AND (p_event_type IS NULL OR e.event_type = p_event_type)
    ORDER BY e.occurred_at DESC
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql STABLE;

-- Stored procedure
CREATE PROCEDURE billing.process_overdue_invoices()
AS $$
BEGIN
    UPDATE billing.invoices
    SET status = 'overdue'
    WHERE status = 'sent'
    AND due_date < CURRENT_DATE;

    -- Log the operation
    INSERT INTO analytics.events (user_id, event_type, event_data)
    VALUES (NULL, 'view', jsonb_build_object('action', 'process_overdue_invoices', 'timestamp', CURRENT_TIMESTAMP));
END;
$$ LANGUAGE plpgsql;

-- Triggers
CREATE TRIGGER trg_users_update_timestamp
    BEFORE UPDATE ON auth.users
    FOR EACH ROW
    EXECUTE FUNCTION auth.update_timestamp();

CREATE TRIGGER trg_customers_update_metadata
    AFTER INSERT OR UPDATE ON billing.customers
    FOR EACH ROW
    EXECUTE FUNCTION auth.update_timestamp();

-- Constraints
ALTER TABLE billing.invoices
ADD CONSTRAINT chk_positive_amount CHECK (amount > 0);

ALTER TABLE billing.invoices
ADD CONSTRAINT chk_valid_due_date CHECK (due_date >= issued_at::DATE);

ALTER TABLE billing.customers
ADD CONSTRAINT chk_valid_customer_code CHECK (length(customer_code) >= 3);

ALTER TABLE analytics.events
ADD CONSTRAINT chk_future_date CHECK (occurred_at <= CURRENT_TIMESTAMP);

-- Exclusion constraints
ALTER TABLE billing.invoices
ADD CONSTRAINT excl_invoice_number_overlap
EXCLUDE (invoice_number WITH =) WHERE (status != 'cancelled');

-- Foreign key constraints with various options
ALTER TABLE billing.invoices
ADD CONSTRAINT fk_invoices_customer
FOREIGN KEY (customer_id) REFERENCES billing.customers(id)
ON DELETE SET NULL ON UPDATE CASCADE;

-- Grants and Permissions
GRANT SELECT ON auth.active_users TO PUBLIC;
GRANT SELECT ON billing.customer_summary TO PUBLIC;
GRANT SELECT ON analytics.user_activity_summary TO PUBLIC;

GRANT EXECUTE ON FUNCTION billing.calculate_revenue TO PUBLIC;
GRANT EXECUTE ON FUNCTION analytics.get_user_events TO PUBLIC;

-- Schema-level grants
GRANT USAGE ON SCHEMA auth TO PUBLIC;
GRANT USAGE ON SCHEMA billing TO PUBLIC;
GRANT USAGE ON SCHEMA analytics TO PUBLIC;

-- Comments on various objects
COMMENT ON SCHEMA auth IS 'User authentication and authorization';
COMMENT ON SCHEMA billing IS 'Customer billing and invoicing';
COMMENT ON SCHEMA analytics IS 'Event tracking and analytics';

COMMENT ON TYPE auth.user_role IS 'Available user roles in the system';
COMMENT ON TYPE billing.currency IS 'Supported currencies for billing';

COMMENT ON TABLE auth.users IS 'System users with authentication and profile information';
COMMENT ON COLUMN auth.users.email IS 'Unique user email address (validated format)';
COMMENT ON COLUMN auth.users.role IS 'User role for authorization and permissions';
COMMENT ON COLUMN auth.users.profile IS 'User profile information stored as composite type';
COMMENT ON COLUMN auth.users.metadata IS 'Additional user metadata in JSON format';

COMMENT ON TABLE billing.customers IS 'Customer information for billing purposes';
COMMENT ON TABLE billing.invoices IS 'Customer invoices and payment tracking';
COMMENT ON TABLE analytics.events IS 'User activity and event tracking';

COMMENT ON VIEW auth.active_users IS 'Currently active users in the system';
COMMENT ON VIEW billing.customer_summary IS 'Summary of customer billing information';

COMMENT ON FUNCTION billing.calculate_revenue IS 'Calculate total revenue for a given period and currency';
COMMENT ON FUNCTION analytics.get_user_events IS 'Retrieve user events with optional filtering';

COMMENT ON INDEX idx_users_email IS 'Unique index for fast email lookups';
COMMENT ON INDEX idx_invoices_customer_status IS 'Composite index for customer invoice queries';

COMMENT ON CONSTRAINT chk_positive_amount ON billing.invoices IS 'Ensure invoice amounts are positive';
COMMENT ON CONSTRAINT fk_invoices_customer ON billing.invoices IS 'Link invoices to customers with cascade rules';
"#.to_string()
}

/// Creates a simple schema for basic testing
pub fn create_simple_schema() -> String {
    r#"-- Simple test schema for basic init testing

CREATE SCHEMA simple;

CREATE TABLE simple.users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE simple.posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES simple.users(id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    content TEXT,
    published BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_posts_user_id ON simple.posts (user_id);
CREATE INDEX idx_posts_published ON simple.posts (published) WHERE published = true;

CREATE VIEW simple.published_posts AS
SELECT
    p.id,
    p.title,
    p.content,
    u.name as author_name,
    p.created_at
FROM simple.posts p
JOIN simple.users u ON p.user_id = u.id
WHERE p.published = true;

CREATE FUNCTION simple.get_user_post_count(user_id INTEGER)
RETURNS INTEGER AS $$
    SELECT COUNT(*)::INTEGER FROM simple.posts WHERE user_id = $1;
$$ LANGUAGE SQL STABLE;

COMMENT ON SCHEMA simple IS 'Simple blog-like schema for testing';
COMMENT ON TABLE simple.users IS 'Blog users';
COMMENT ON TABLE simple.posts IS 'User blog posts';
COMMENT ON VIEW simple.published_posts IS 'Published posts with author information';
"#
    .to_string()
}

/// Creates a multi-file schema structure with dependency relationships
pub fn create_multi_file_schema_structure(base_dir: &Path) -> Result<()> {
    // Create directory structure
    fs::create_dir_all(base_dir.join("01_foundation"))?;
    fs::create_dir_all(base_dir.join("02_types"))?;
    fs::create_dir_all(base_dir.join("03_tables"))?;
    fs::create_dir_all(base_dir.join("04_indexes"))?;
    fs::create_dir_all(base_dir.join("05_views"))?;
    fs::create_dir_all(base_dir.join("06_functions"))?;
    fs::create_dir_all(base_dir.join("07_grants"))?;

    // 01_foundation - Schemas and extensions
    fs::write(
        base_dir.join("01_foundation/schemas.sql"),
        r#"-- Base schemas for the application
CREATE SCHEMA app;
CREATE SCHEMA auth;
CREATE SCHEMA public_api;
"#,
    )?;

    fs::write(
        base_dir.join("01_foundation/extensions.sql"),
        r#"-- Required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
"#,
    )?;

    // 02_types - Custom types
    fs::write(
        base_dir.join("02_types/auth_types.sql"),
        r#"-- require: 01_foundation/schemas.sql
-- Authentication related types
CREATE TYPE auth.user_status AS ENUM ('active', 'inactive', 'pending', 'suspended');
CREATE TYPE auth.permission_level AS ENUM ('read', 'write', 'admin');

CREATE DOMAIN auth.email AS TEXT
CHECK (VALUE ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$');
"#,
    )?;

    fs::write(
        base_dir.join("02_types/app_types.sql"),
        r#"-- require: 01_foundation/schemas.sql
-- Application domain types
CREATE TYPE app.priority AS ENUM ('low', 'medium', 'high', 'urgent');
CREATE TYPE app.category AS ENUM ('feature', 'bug', 'improvement', 'documentation');

CREATE TYPE app.address AS (
    street TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT
);
"#,
    )?;

    // 03_tables - Core tables
    fs::write(
        base_dir.join("03_tables/auth_tables.sql"),
        r#"-- require: 01_foundation/schemas.sql, 02_types/auth_types.sql
-- Authentication tables
CREATE TABLE auth.users (
    id SERIAL PRIMARY KEY,
    uuid UUID DEFAULT uuid_generate_v4() UNIQUE,
    email auth.email UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    status auth.user_status DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE auth.user_permissions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES auth.users(id) ON DELETE CASCADE,
    resource TEXT NOT NULL,
    permission_level auth.permission_level NOT NULL,
    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, resource)
);
"#,
    )?;

    fs::write(
        base_dir.join("03_tables/app_tables.sql"),
        r#"-- require: 01_foundation/schemas.sql, 02_types/app_types.sql, 03_tables/auth_tables.sql
-- Application tables
CREATE TABLE app.projects (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    owner_id INTEGER REFERENCES auth.users(id),
    priority app.priority DEFAULT 'medium',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE app.tasks (
    id SERIAL PRIMARY KEY,
    project_id INTEGER REFERENCES app.projects(id) ON DELETE CASCADE,
    assignee_id INTEGER REFERENCES auth.users(id),
    title TEXT NOT NULL,
    description TEXT,
    category app.category DEFAULT 'feature',
    priority app.priority DEFAULT 'medium',
    completed BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"#,
    )?;

    // 04_indexes
    fs::write(
        base_dir.join("04_indexes/auth_indexes.sql"),
        r#"-- require: 03_tables/auth_tables.sql
-- Authentication table indexes
CREATE UNIQUE INDEX idx_users_email ON auth.users (email);
CREATE INDEX idx_users_status ON auth.users (status);
CREATE INDEX idx_users_created_at ON auth.users (created_at);

CREATE INDEX idx_permissions_user_id ON auth.user_permissions (user_id);
CREATE INDEX idx_permissions_resource ON auth.user_permissions (resource);
"#,
    )?;

    fs::write(
        base_dir.join("04_indexes/app_indexes.sql"),
        r#"-- require: 03_tables/app_tables.sql
-- Application table indexes
CREATE INDEX idx_projects_owner_id ON app.projects (owner_id);
CREATE INDEX idx_projects_priority ON app.projects (priority);

CREATE INDEX idx_tasks_project_id ON app.tasks (project_id);
CREATE INDEX idx_tasks_assignee_id ON app.tasks (assignee_id);
CREATE INDEX idx_tasks_category_priority ON app.tasks (category, priority);
CREATE INDEX idx_tasks_completed ON app.tasks (completed) WHERE completed = true;
"#,
    )?;

    // 05_views
    fs::write(
        base_dir.join("05_views/user_views.sql"),
        r#"-- require: 03_tables/auth_tables.sql, 03_tables/app_tables.sql
-- User-related views
CREATE VIEW public_api.active_users AS
SELECT
    u.id,
    u.uuid,
    u.email,
    u.status,
    u.created_at
FROM auth.users u
WHERE u.status = 'active';

CREATE VIEW public_api.user_project_summary AS
SELECT
    u.id as user_id,
    u.email,
    COUNT(p.id) as project_count,
    COUNT(t.id) as task_count,
    COUNT(CASE WHEN t.completed THEN 1 END) as completed_tasks
FROM auth.users u
LEFT JOIN app.projects p ON u.id = p.owner_id
LEFT JOIN app.tasks t ON u.id = t.assignee_id
GROUP BY u.id, u.email;
"#,
    )?;

    fs::write(
        base_dir.join("05_views/project_views.sql"),
        r#"-- require: 03_tables/app_tables.sql, 03_tables/auth_tables.sql
-- Project-related views
CREATE VIEW public_api.project_overview AS
SELECT
    p.id,
    p.name,
    p.priority,
    u.email as owner_email,
    COUNT(t.id) as task_count,
    COUNT(CASE WHEN t.completed THEN 1 END) as completed_tasks,
    p.created_at
FROM app.projects p
LEFT JOIN auth.users u ON p.owner_id = u.id
LEFT JOIN app.tasks t ON p.id = t.project_id
GROUP BY p.id, p.name, p.priority, u.email, p.created_at;
"#,
    )?;

    // 06_functions
    fs::write(
        base_dir.join("06_functions/auth_functions.sql"),
        r#"-- require: 03_tables/auth_tables.sql
-- Authentication functions
CREATE FUNCTION auth.update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION auth.user_has_permission(p_user_id INTEGER, p_resource TEXT, p_level auth.permission_level)
RETURNS BOOLEAN AS $$
    SELECT EXISTS (
        SELECT 1 FROM auth.user_permissions
        WHERE user_id = p_user_id
        AND resource = p_resource
        AND permission_level >= p_level
    );
$$ LANGUAGE SQL STABLE;

-- Apply update timestamp trigger
CREATE TRIGGER trg_users_update_timestamp
    BEFORE UPDATE ON auth.users
    FOR EACH ROW
    EXECUTE FUNCTION auth.update_timestamp();
"#,
    )?;

    fs::write(
        base_dir.join("06_functions/app_functions.sql"),
        r#"-- require: 03_tables/app_tables.sql
-- Application functions
CREATE FUNCTION app.get_project_progress(p_project_id INTEGER)
RETURNS DECIMAL(5,2) AS $$
DECLARE
    total_tasks INTEGER;
    completed_tasks INTEGER;
BEGIN
    SELECT COUNT(*), COUNT(CASE WHEN completed THEN 1 END)
    INTO total_tasks, completed_tasks
    FROM app.tasks
    WHERE project_id = p_project_id;

    IF total_tasks = 0 THEN
        RETURN 0;
    END IF;

    RETURN (completed_tasks::DECIMAL / total_tasks * 100);
END;
$$ LANGUAGE plpgsql STABLE;

CREATE FUNCTION app.assign_task(p_task_id INTEGER, p_user_id INTEGER)
RETURNS BOOLEAN AS $$
BEGIN
    UPDATE app.tasks
    SET assignee_id = p_user_id
    WHERE id = p_task_id;

    RETURN FOUND;
END;
$$ LANGUAGE plpgsql;
"#,
    )?;

    // 07_grants
    fs::write(
        base_dir.join("07_grants/public_api_grants.sql"),
        r#"-- require: 05_views/user_views.sql, 05_views/project_views.sql
-- Public API grants
GRANT SELECT ON public_api.active_users TO PUBLIC;
GRANT SELECT ON public_api.user_project_summary TO PUBLIC;
GRANT SELECT ON public_api.project_overview TO PUBLIC;

-- Schema usage grants
GRANT USAGE ON SCHEMA public_api TO PUBLIC;
"#,
    )?;

    fs::write(
        base_dir.join("07_grants/function_grants.sql"),
        r#"-- require: 06_functions/auth_functions.sql, 06_functions/app_functions.sql
-- Function execution grants
GRANT EXECUTE ON FUNCTION auth.user_has_permission TO PUBLIC;
GRANT EXECUTE ON FUNCTION app.get_project_progress TO PUBLIC;
GRANT EXECUTE ON FUNCTION app.assign_task TO PUBLIC;
"#,
    )?;

    Ok(())
}

/// Creates a schema with circular dependencies for error testing
pub fn create_circular_dependency_structure(base_dir: &Path) -> Result<()> {
    fs::create_dir_all(base_dir)?;

    // File A depends on File B
    fs::write(
        base_dir.join("file_a.sql"),
        "-- require: file_b.sql\nCREATE TABLE a (id SERIAL, b_id INTEGER);",
    )?;

    // File B depends on File A (creates circular dependency)
    fs::write(
        base_dir.join("file_b.sql"),
        "-- require: file_a.sql\nCREATE TABLE b (id SERIAL, a_id INTEGER);",
    )?;

    Ok(())
}

/// Creates a schema with missing dependencies for error testing
pub fn create_missing_dependency_structure(base_dir: &Path) -> Result<()> {
    fs::create_dir_all(base_dir)?;

    // File depends on non-existent file
    fs::write(
        base_dir.join("dependent.sql"),
        "-- require: does_not_exist.sql\nCREATE TABLE dependent (id SERIAL);",
    )?;

    Ok(())
}
