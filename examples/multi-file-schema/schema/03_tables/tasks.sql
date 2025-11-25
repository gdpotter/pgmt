-- require: 01_schemas/app.sql, 02_types/priority.sql, 02_types/status.sql, 03_tables/users.sql
CREATE TABLE app.tasks (
    id SERIAL PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    description TEXT,
    priority app.priority DEFAULT 'medium',
    status app.status DEFAULT 'pending',
    assigned_to INTEGER REFERENCES app.users(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

COMMENT ON TABLE app.tasks IS 'User tasks with priority and status';