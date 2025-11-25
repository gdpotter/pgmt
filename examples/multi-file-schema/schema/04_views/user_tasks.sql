-- require: 03_tables/users.sql, 03_tables/tasks.sql
CREATE VIEW app.user_tasks AS
SELECT 
    t.id,
    t.title,
    t.priority,
    t.status,
    u.name as assigned_user,
    u.email as assigned_email,
    t.created_at,
    t.updated_at
FROM app.tasks t
LEFT JOIN app.users u ON t.assigned_to = u.id;