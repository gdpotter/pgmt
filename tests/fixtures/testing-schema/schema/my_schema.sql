CREATE SCHEMA IF NOT EXISTS public;
CREATE EXTENSION "uuid-ossp" SCHEMA public;

CREATE SCHEMA testing_schema;
CREATE SCHEMA another_schema;

create type testing_schema.giveaway_visibility as enum ('VISIBLE', 'HIDDEN', 'VALUE_3', 'ANOTHER_VALUE', 'VALUE_2', 'VALUE_1');

create function testing_schema.jsonb_sum_values(data jsonb) returns numeric
    immutable
  language sql
as
$$
SELECT
    sum(num)
FROM (
         SELECT (JSONB_EACH_TEXT(data)).value::numeric AS num
     ) _
$$;

CREATE TABLE testing_schema.users
    (
  id               uuid                                                             not null
    primary key,
  created_at       timestamp with time zone default CURRENT_TIMESTAMP               not null,
  updated_at       timestamp with time zone                                         not null,
  title            text                                                             not null,
  start_at         date                                                             not null,
  end_at           date                                                             not null,
  categories       text[]                   default '{}'::text[]                    not null,
  prize_costs      jsonb                    default '{}'::jsonb                     not null,
  prize_cost_total numeric(9, 2) generated always as (COALESCE(testing_schema.jsonb_sum_values(prize_costs), (0)::numeric)) stored,
  vip_only         boolean                  default false                           not null,
  image_url        text,
  rules_url        text,
  first_name       text,
  visibility       testing_schema.giveaway_visibility     default 'VISIBLE'::testing_schema.giveaway_visibility not null
);

CREATE VIEW testing_schema.view_1 AS
(
    SELECT id, title, end_at
    FROM testing_schema.users
    WHERE title <> 'test'
);

CREATE VIEW testing_schema.view_2 AS
(
    SELECT id
    FROM testing_schema.view_1
);


CREATE TABLE testing_schema.something (
    id SERIAL PRIMARY KEY
)
;

COMMENT ON TABLE testing_schema.users IS 'This is a comment on the users table!!!!';

COMMENT ON COLUMN testing_schema.users.title IS 'This is a comment on the title column.';
