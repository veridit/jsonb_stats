BEGIN;
CREATE EXTENSION jsonb_stats;

-- This test drives the implementation of the new, fully-discriminated
-- JSONB structures.

-- Test Data
CREATE TABLE test_data (
    id int,
    stats jsonb
);

INSERT INTO test_data (id, stats) VALUES
(1, '{"num_employees": {"type": "int", "value": 100}, "is_profitable": {"type": "bool", "value": true}}'),
(1, '{"num_employees": {"type": "int", "value": 150}, "is_profitable": {"type": "bool", "value": false}}');

-- Phase 1: Test for top-level type discriminators.
-- This is expected to fail until the C code is updated.
\t \a \x
SELECT '## Phase 1: Test jsonb_stats_agg top-level type' as test_description;
SELECT '--> Expected: has_correct_type: t' as " ";
SELECT (jsonb_stats_agg(stats)->>'type') = 'stats_agg' as has_correct_type
FROM test_data;

-- Phase 1.2: Test for jsonb_stats_agg top-level type
CREATE TABLE test_data_sfunc AS
SELECT 1 as id, 'num_employees' as code, stat(100) as stat
UNION ALL
SELECT 1 as id, 'is_profitable' as code, stat(true) as stat;

SELECT '## Phase 1.2: Test for jsonb_stats_agg(code, stat) top-level type' as test_description;
SELECT '--> Expected: has_correct_type: t' as " ";
SELECT (agg.stats->>'type') = 'stats' as has_correct_type
FROM (
    SELECT jsonb_stats_agg(code, stat) as stats
    FROM test_data_sfunc
    GROUP BY id
) agg;

-- Phase 2: Test for new summary type names
SELECT '## Phase 2: Test for new summary type names' as test_description;
SELECT '--> Expected: has_correct_int_type: t, has_correct_bool_type: t' as " ";
WITH summary AS (
    SELECT jsonb_stats_agg(stats) as s
    FROM test_data
)
SELECT
    (s->'num_employees'->>'type') = 'int_agg' as has_correct_int_type,
    (s->'is_profitable'->>'type') = 'bool_agg' as has_correct_bool_type
FROM summary;

-- Phase 2.2: Test for dec2 type
CREATE TABLE test_data_numeric (
    val numeric(10, 2)
);
INSERT INTO test_data_numeric (val) VALUES (123.45), (678.90);

SELECT '## Phase 2: Test stat() function with numeric' as test_description;
SELECT '--> Expected:' as " ";
SELECT jsonb_pretty('{"type": "dec2", "value": 123.45}');
SELECT stat(val) FROM test_data_numeric LIMIT 1;

SELECT '## Phase 2: Test for dec2_agg summary type' as test_description;
SELECT '--> Expected: has_correct_dec2_type: t' as " ";
WITH summary AS (
    SELECT jsonb_stats_agg(jsonb_build_object('revenue', stat(val))) as s
    FROM test_data_numeric
)
SELECT
    (s->'revenue'->>'type') = 'dec2_agg' as has_correct_dec2_type
FROM summary;

-- Phase 3: Test for removal of elements_count from arr_agg
CREATE TABLE test_data_array (
    val int[]
);
INSERT INTO test_data_array (val) VALUES ('{1,2}'), ('{2,3}');

SELECT '## Phase 3: Test for removal of elements_count' as test_description;
SELECT '--> Expected: is_removed: t, has_correct_type: t' as " ";
WITH summary AS (
    SELECT jsonb_stats_agg(jsonb_build_object('tags', stat(val))) as s
    FROM test_data_array
)
SELECT
    NOT (s->'tags' ? 'elements_count') as is_removed,
    (s->'tags'->>'type') = 'arr_agg' as has_correct_type
FROM summary;

ROLLBACK;
