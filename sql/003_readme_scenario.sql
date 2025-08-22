BEGIN;
CREATE EXTENSION IF NOT EXISTS jsonb_stats;

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
    value_bool BOOLEAN,
    value_text TEXT,
    value_date DATE,
    stat JSONB GENERATED ALWAYS AS (
        CASE
            WHEN value_int IS NOT NULL THEN stat(value_int)
            WHEN value_bool IS NOT NULL THEN stat(value_bool)
            WHEN value_text IS NOT NULL THEN stat(value_text)
            WHEN value_date IS NOT NULL THEN stat(value_date)
        END
    ) STORED,
    CONSTRAINT one_value_must_be_set CHECK (
        (value_int IS NOT NULL)::int +
        (value_bool IS NOT NULL)::int +
        (value_text IS NOT NULL)::int +
        (value_date IS NOT NULL)::int = 1
    )
);


-- Populate with data for the 2023 period
INSERT INTO stat_for_unit (legal_unit_id, code, value_int, value_bool, value_text) VALUES
(1, 'num_employees', 150, NULL, NULL),
(1, 'is_profitable', NULL, true, NULL),
(1, 'industry', NULL, NULL, 'tech'),
(2, 'num_employees', 2500, NULL, NULL),
(2, 'is_profitable', NULL, true, NULL),
(2, 'industry', NULL, NULL, 'finance'),
(3, 'num_employees', 50, NULL, NULL),
(3, 'is_profitable', NULL, false, NULL),
(3, 'industry', NULL, NULL, 'tech');

-- From README.md: 2. Aggregation Views

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

-- history_facet (Level 2: stats -> stats_summary)
CREATE MATERIALIZED VIEW history_facet AS
SELECT
    luh.valid_from,
    luh.valid_until,
    luh.region,
    jsonb_stats_summary_agg(luh.stats) as stats_summary
FROM legal_unit_history luh
WHERE luh.valid_from = '2023-01-01'
GROUP BY luh.valid_from, luh.valid_until, luh.region;

-- history (Level 3: stats_summary -> stats_summary)
CREATE MATERIALIZED VIEW history AS
SELECT
    hf.valid_from,
    hf.valid_until,
    jsonb_stats_summary_merge_agg(hf.stats_summary) as stats_summary
FROM history_facet hf
GROUP BY hf.valid_from, hf.valid_until;

-- Test queries to verify the contents of the materialized views.
\t \a \x

-- Test Level 1: stat -> stats from legal_unit_history
SELECT '## Level 1: legal_unit_history' as test_description;
SELECT legal_unit_id, jsonb_pretty(stats) as stats
FROM legal_unit_history
ORDER BY legal_unit_id;

-- Test Level 2: stats -> stats_summary from history_facet
SELECT '## Level 2: history_facet' as test_description;
SELECT region, jsonb_pretty(stats_summary) as stats_summary
FROM history_facet
ORDER BY region;

-- Test Level 3: stats_summary -> stats_summary from history
SELECT '## Level 3: history' as test_description;
SELECT jsonb_pretty(stats_summary) as stats_summary
FROM history;

ROLLBACK;
