-- require: 03_tables/users.sql
CREATE VIEW app.active_users AS
SELECT id, email, name, created_at
FROM app.users
WHERE active = true;