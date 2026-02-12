-- require: tables/user_activities.sql

CREATE OR REPLACE FUNCTION calculate_score(
    user_id INTEGER
)
RETURNS INTEGER
LANGUAGE SQL
BEGIN ATOMIC
    SELECT COALESCE(SUM(points), 0)
    FROM user_activities ua
    WHERE ua.user_id = calculate_score.user_id;
END;
