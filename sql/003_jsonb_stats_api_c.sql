BEGIN;
CREATE EXTENSION jsonb_stats;

-- Test Setup
-- The legal_unit table tracks companies and their validity periods.
CREATE TABLE legal_unit (
    legal_unit_id INT,
    name TEXT,
    region TEXT,
    valid_from DATE,
    valid_to DATE
);

INSERT INTO legal_unit (legal_unit_id, name, region, valid_from, valid_to) VALUES
(1, 'Company A', 'EU', '2023-01-01', '2023-12-31'),
(2, 'Company B', 'US', '2023-01-01', '2023-12-31'),
(3, 'Company C', 'EU', '2023-01-01', '2023-12-31'),
(1, 'Company A Rev.', 'EU', '2024-01-01', NULL); -- Company A was revised for the next period

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
            WHEN value_int IS NOT NULL THEN stat_c(value_int)
            WHEN value_bool IS NOT NULL THEN stat_c(value_bool)
            WHEN value_text IS NOT NULL THEN stat_c(value_text)
            WHEN value_date IS NOT NULL THEN stat_c(value_date)
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

-- Test cases
-- Show results suitable for jsonb_pretty for easy diffing.
\t
\a
\x

-- Test Level 1: stat -> stats
-- Create `stats` for each legal unit.
WITH legal_unit_history AS (
    SELECT
        lu.legal_unit_id,
        (
            SELECT jsonb_stats_agg_c(sfu.code, sfu.stat)
            FROM stat_for_unit sfu
            WHERE sfu.legal_unit_id = lu.legal_unit_id
        ) AS stats
    FROM
        legal_unit lu
    WHERE lu.valid_from = '2023-01-01'
)
SELECT legal_unit_id, jsonb_pretty(stats) as stats
FROM legal_unit_history
ORDER BY legal_unit_id;


-- Test Level 2: stats -> stats_summary
-- Create `stats_summary` for each region.
WITH legal_unit_history AS (
    SELECT
        lu.legal_unit_id,
        lu.region,
        (
            SELECT jsonb_stats_agg_c(sfu.code, sfu.stat)
            FROM stat_for_unit sfu
            WHERE sfu.legal_unit_id = lu.legal_unit_id
        ) AS stats
    FROM
        legal_unit lu
    WHERE lu.valid_from = '2023-01-01'
),
history_facet AS (
    SELECT
        luh.region,
        jsonb_stats_summary_agg_c(luh.stats) as stats_summary
    FROM legal_unit_history luh
    GROUP BY luh.region
)
SELECT region, jsonb_pretty(stats_summary) as stats_summary
FROM history_facet
ORDER BY region;


-- Test Level 3: stats_summary -> stats_summary
-- Combine regional summaries into a global summary.
WITH legal_unit_history AS (
    SELECT
        lu.legal_unit_id,
        lu.region,
        (
            SELECT jsonb_stats_agg_c(sfu.code, sfu.stat)
            FROM stat_for_unit sfu
            WHERE sfu.legal_unit_id = lu.legal_unit_id
        ) AS stats
    FROM
        legal_unit lu
    WHERE lu.valid_from = '2023-01-01'
),
history_facet AS (
    SELECT
        luh.region,
        jsonb_stats_summary_agg_c(luh.stats) as stats_summary
    FROM legal_unit_history luh
    GROUP BY luh.region
),
history AS (
    SELECT
        jsonb_stats_summary_combine_agg_c(hf.stats_summary) as stats_summary
    FROM history_facet hf
)
SELECT jsonb_pretty(stats_summary)
FROM history;
ROLLBACK;
