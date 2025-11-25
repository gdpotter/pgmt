-- require: 01_schemas/app.sql
CREATE TYPE app.status AS ENUM ('pending', 'in_progress', 'completed', 'cancelled');