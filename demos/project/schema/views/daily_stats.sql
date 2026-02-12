-- require: views/user_rankings.sql

CREATE VIEW daily_stats AS
SELECT
    score,
    COUNT(*) AS user_count
FROM user_rankings
GROUP BY score;
