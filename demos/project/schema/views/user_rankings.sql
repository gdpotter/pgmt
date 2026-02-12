-- require: functions/calculate_score.sql
-- require: tables/users.sql

CREATE VIEW user_rankings AS
SELECT
    u.id AS user_id,
    u.name,
    calculate_score(u.id) AS score
FROM users u
ORDER BY score DESC;
