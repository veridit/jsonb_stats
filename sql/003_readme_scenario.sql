BEGIN;
CREATE EXTENSION jsonb_stats;

-- This test implements the full example scenario from README.md,
-- using materialized views to demonstrate the aggregation pipeline.

-- From README.md: 1. Source Tables
-- The legal_unit table tracks companies and their validity periods.
CREATE TABLE legal_unit (
    legal_unit_id INT,
    name TEXT,
    region TEXT,
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL
);

INSERT INTO legal_unit (legal_unit_id, name, region, valid_from, valid_until) VALUES
(1, 'Company A', 'EU', '2023-01-01', '2024-01-01'),
(2, 'Company B', 'US', '2023-01-01', '2024-01-01'),
(3, 'Company C', 'EU', '2023-01-01', '2024-01-01'),
(1, 'Company A Rev.', 'EU', '2024-01-01', 'infinity'); -- Company A was revised for the next period

-- The stat_for_unit table holds raw statistical data.
CREATE TABLE stat_for_unit (
    legal_unit_id INT,
    code TEXT,
    value_int INT,
    value_float FLOAT,
    value_numeric NUMERIC(10,2),
    value_bool BOOLEAN,
    value_text TEXT,
    value_date DATE,
    stat JSONB GENERATED ALWAYS AS (
        CASE
            WHEN value_int IS NOT NULL THEN stat(value_int)
            WHEN value_float IS NOT NULL THEN stat(value_float)
            WHEN value_numeric IS NOT NULL THEN stat(value_numeric)
            WHEN value_bool IS NOT NULL THEN stat(value_bool)
            WHEN value_text IS NOT NULL THEN stat(value_text)
            WHEN value_date IS NOT NULL THEN stat(value_date)
        END
    ) STORED,
    CONSTRAINT one_value_must_be_set CHECK (
        (value_int IS NOT NULL)::int +
        (value_float IS NOT NULL)::int +
        (value_numeric IS NOT NULL)::int +
        (value_bool IS NOT NULL)::int +
        (value_text IS NOT NULL)::int +
        (value_date IS NOT NULL)::int = 1
    )
);


-- Populate with data for the 2023 period
INSERT INTO stat_for_unit (legal_unit_id, code, value_int) VALUES
(1, 'num_employees', 150),
(2, 'num_employees', 2500),
(3, 'num_employees', 50);

INSERT INTO stat_for_unit (legal_unit_id, code, value_bool) VALUES
(1, 'is_profitable', true),
(2, 'is_profitable', true),
(3, 'is_profitable', false);

INSERT INTO stat_for_unit (legal_unit_id, code, value_text) VALUES
(1, 'industry', 'tech'),
(2, 'industry', 'finance'),
(3, 'industry', 'tech');

INSERT INTO stat_for_unit (legal_unit_id, code, value_float) VALUES
(1, 'turnover', 123456.78),
(2, 'turnover', 987654.32);

-- legal_unit_history (Level 1: stat -> stats)
CREATE MATERIALIZED VIEW legal_unit_history AS
SELECT
    lu.legal_unit_id,
    lu.name,
    lu.region,
    lu.valid_from,
    lu.valid_until,
    (
        SELECT jsonb_stats_agg(sfu.code, sfu.stat)
        FROM stat_for_unit sfu
        WHERE sfu.legal_unit_id = lu.legal_unit_id
    ) AS stats
FROM
    legal_unit lu
WHERE lu.valid_from = '2023-01-01';

-- history_facet (Level 2: stats -> stats_agg)
CREATE MATERIALIZED VIEW history_facet AS
SELECT
    luh.valid_from,
    luh.valid_until,
    luh.region,
    jsonb_stats_agg(luh.stats) as stats_agg
FROM legal_unit_history luh
WHERE luh.valid_from = '2023-01-01'
GROUP BY luh.valid_from, luh.valid_until, luh.region;

-- history (Level 3: stats_agg -> stats_agg)
CREATE MATERIALIZED VIEW history AS
SELECT
    hf.valid_from,
    hf.valid_until,
    jsonb_stats_merge_agg(hf.stats_agg) as stats_agg
FROM history_facet hf
GROUP BY hf.valid_from, hf.valid_until;

-- Test queries to verify the contents of the materialized views.
\t \a \x

-- Test Level 1: stat -> stats from legal_unit_history
SELECT '## Level 1: legal_unit_history' as test_description;
SELECT '--> Expected for legal_unit_id=1:' as " ";
SELECT jsonb_pretty('{"type": "stats", "industry": {"type": "str", "value": "tech"}, "is_profitable": {"type": "bool", "value": true}, "num_employees": {"type": "int", "value": 150}, "turnover": {"type": "float", "value": 123456.78}}');
SELECT legal_unit_id, jsonb_pretty(stats) as stats
FROM legal_unit_history
ORDER BY legal_unit_id;

-- Test Level 2: stats -> stats_agg from history_facet
SELECT '## Level 2: history_facet' as test_description;
SELECT '--> Expected for EU:' as " ";
SELECT jsonb_pretty('{"type": "stats_agg", "industry": {"type": "str_agg", "counts": {"tech": 2}}, "is_profitable": {"type": "bool_agg", "counts": {"false": 1, "true": 1}}, "num_employees": {"coefficient_of_variation_pct": 70.71, "count": 2, "max": 150, "mean": 100.00, "min": 50, "stddev": 70.71, "sum": 200, "sum_sq_diff": 5000.00, "type": "int_agg", "variance": 5000.00}}');
SELECT region, jsonb_pretty(stats_agg) as stats_agg
FROM history_facet
ORDER BY region;

-- Test Level 3: stats_agg -> stats_agg from history
SELECT '## Level 3: history' as test_description;
SELECT '--> Expected global:' as " ";
SELECT jsonb_pretty('{"type": "stats_agg", "industry": {"type": "str_agg", "counts": {"finance": 1, "tech": 2}}, "is_profitable": {"type": "bool_agg", "counts": {"false": 1, "true": 2}}, "num_employees": {"coefficient_of_variation_pct": 154.16, "count": 3, "max": 2500, "mean": 900.00, "min": 50, "stddev": 1387.44, "sum": 2700, "sum_sq_diff": 3845000.00, "type": "int_agg", "variance": 1922500.00}}');
SELECT jsonb_pretty(stats_agg) as stats_agg
FROM history;

ROLLBACK;
