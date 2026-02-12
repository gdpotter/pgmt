-- require: tables/users.sql

CREATE TABLE user_activities (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    points INTEGER NOT NULL DEFAULT 0,
    bonus INTEGER NOT NULL DEFAULT 0
);
