-- require: views/daily_stats.sql

CREATE VIEW executive_dashboard AS
SELECT *
FROM daily_stats
WHERE user_count > 1;
