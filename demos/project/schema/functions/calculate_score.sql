-- require: tables/user_activities.sql

CREATE OR REPLACE FUNCTION calculate_score(
    user_id INTEGER
  , include_bonus BOOLEAN DEFAULT false
)
RETURNS INTEGER
LANGUAGE SQL
BEGIN ATOMIC
    SELECT COALESCE(SUM(points), 0)
        + CASE WHEN include_bonus THEN COALESCE(SUM(bonus), 0) ELSE 0 END
    FROM user_activities ua
    WHERE ua.user_id = calculate_score.user_id;
END;
