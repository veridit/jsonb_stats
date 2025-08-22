-- Test Setup
CREATE EXTENSION IF NOT EXISTS jsonb_stats;

CREATE TEMPORARY TABLE test_stats (
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    test_case integer,
    stats jsonb
);

-- Insert test cases, adapted from statbus tests.
INSERT INTO test_stats (test_case, stats) VALUES
    (1, jsonb_build_object('num', stat(1), 'str', stat('a'::text), 'bool', stat(true))),
    (1, jsonb_build_object('num', stat(2), 'str', stat('a'::text), 'bool', stat(false))),
    (1, jsonb_build_object('num', stat(3), 'str', stat('b'::text), 'bool', stat(true))),
    (2, jsonb_build_object('num', stat(0), 'str', stat('a'::text), 'bool', stat(true))),
    (2, jsonb_build_object('num', stat(100), 'str', stat('b'::text), 'bool', stat(false))),
    (2, jsonb_build_object('num', stat(200), 'str', stat('b'::text), 'bool', stat(true))),
    (2, jsonb_build_object('num', stat(300), 'str', stat('c'::text), 'bool', stat(true))),
    (2, jsonb_build_object('num', stat(400), 'str', stat('c'::text), 'bool', stat(false))),
    (2, jsonb_build_object('num', stat(500), 'str', stat('c'::text), 'bool', stat(true))),
    (2, jsonb_build_object('num', stat(600), 'str', stat('d'::text), 'bool', stat(true))),
    (2, jsonb_build_object('num', stat(700), 'str', stat('d'::text), 'bool', stat(true))),
    (2, jsonb_build_object('num', stat(800), 'str', stat('d'::text), 'bool', stat(false))),
    (2, jsonb_build_object('num', stat(900), 'str', stat('d'::text), 'bool', stat(false))),
    (4, jsonb_build_object('arr', stat(ARRAY[1, 2]::int[]))),
    (4, jsonb_build_object('arr', stat(ARRAY[2, 3]::int[]))),
    (5, jsonb_build_object('arr', stat(ARRAY['a', 'b']::text[]))),
    (5, jsonb_build_object('arr', stat(ARRAY['b', 'c']::text[]))),
    (6, jsonb_build_object('arr', stat(ARRAY[1, 2]::int[]), 'str', stat('a'::text))),
    (6, jsonb_build_object('arr', stat(ARRAY[3]::int[]), 'str', stat('b'::text)));

-- Create a reference view using standard SQL aggregates to validate the C implementation.
CREATE TEMPORARY VIEW reference AS
SELECT
    test_case,
    SUM((stats->'num'->>'value')::numeric) AS num_sum,
    COUNT(stats->'num') AS num_count,
    MIN((stats->'num'->>'value')::numeric) AS num_min,
    MAX((stats->'num'->>'value')::numeric) AS num_max,
    AVG((stats->'num'->>'value')::numeric) AS num_mean,
    VARIANCE((stats->'num'->>'value')::numeric) AS num_variance,
    STDDEV((stats->'num'->>'value')::numeric) AS num_stddev,
    SUM(CASE WHEN (stats->'bool'->>'value')::boolean THEN 1 ELSE 0 END) AS bool_true_count,
    SUM(CASE WHEN NOT (stats->'bool'->>'value')::boolean THEN 1 ELSE 0 END) AS bool_false_count,
    COUNT(*) FILTER (WHERE stats->'str'->>'value' = 'a') AS str_a_count,
    COUNT(*) FILTER (WHERE stats->'str'->>'value' = 'b') AS str_b_count,
    COUNT(*) FILTER (WHERE stats->'str'->>'value' = 'c') AS str_c_count,
    COUNT(*) FILTER (WHERE stats->'str'->>'value' = 'd') AS str_d_count
FROM test_stats
GROUP BY test_case
ORDER BY test_case;

-- Test cases
\t \a \x

-- Test 1: Basic iterative case
SELECT '## C Implementation' as test_description, test_case FROM reference WHERE test_case = 1;
SELECT jsonb_pretty(jsonb_stats_summary_agg(stats)) AS computed_stats FROM test_stats WHERE test_case = 1;
SELECT '## SQL Reference' as test_description, * FROM reference WHERE test_case = 1;

-- Test 2: More complex data
SELECT '## C Implementation' as test_description, test_case FROM reference WHERE test_case = 2;
SELECT jsonb_pretty(jsonb_stats_summary_agg(stats)) AS computed_stats FROM test_stats WHERE test_case = 2;
SELECT '## SQL Reference' as test_description, * FROM reference WHERE test_case = 2;

-- Test 4: Array of numeric values
SELECT '## C Implementation' as test_description, test_case FROM reference WHERE test_case = 4;
SELECT jsonb_pretty(jsonb_stats_summary_agg(stats)) FROM test_stats WHERE test_case = 4;

-- Test 5: Array of string values
SELECT '## C Implementation' as test_description, test_case FROM reference WHERE test_case = 5;
SELECT jsonb_pretty(jsonb_stats_summary_agg(stats)) FROM test_stats WHERE test_case = 5;

-- Test 6: Merging summaries with different keys
SELECT '## C Implementation' as test_description, test_case FROM reference WHERE test_case = 6;
SELECT jsonb_pretty(jsonb_stats_summary_agg(stats)) FROM test_stats WHERE test_case = 6;
