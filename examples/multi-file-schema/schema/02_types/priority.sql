-- require: 01_schemas/app.sql
CREATE TYPE app.priority AS ENUM ('low', 'medium', 'high', 'urgent');