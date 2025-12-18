/// Test fixtures for init integration tests
/// Provides comprehensive schema for testing all object types
///
/// Creates a comprehensive PostgreSQL schema for testing all object types
pub fn create_comprehensive_schema() -> String {
    r#"-- Comprehensive test schema covering all pgmt-supported object types
-- Generated for pgmt init integration tests

-- Extensions (should be created first)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "btree_gist";

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
    email auth.email NOT NULL,
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

COMMENT ON CONSTRAINT chk_positive_amount ON billing.invoices IS 'Ensure invoice amounts are positive';
COMMENT ON CONSTRAINT fk_invoices_customer ON billing.invoices IS 'Link invoices to customers with cascade rules';
"#.to_string()
}
