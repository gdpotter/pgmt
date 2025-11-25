-- require: 03_tables/tasks.sql, 02_types/status.sql
CREATE OR REPLACE FUNCTION app.complete_task(task_id INTEGER)
RETURNS VOID AS $$
BEGIN
    UPDATE app.tasks
    SET status = 'completed', updated_at = NOW()
    WHERE id = task_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION app.get_user_task_count(user_id INTEGER)
RETURNS INTEGER AS $$
BEGIN
    RETURN (
        SELECT COUNT(*)
        FROM app.tasks
        WHERE assigned_to = user_id AND status != 'completed'
    );
END;
$$ LANGUAGE plpgsql;