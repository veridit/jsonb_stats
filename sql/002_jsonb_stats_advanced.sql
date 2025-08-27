BEGIN;
-- Test Setup
CREATE EXTENSION jsonb_stats;

CREATE TEMPORARY TABLE test_stats_rows (
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    test_case integer,
    code text,
    stat jsonb
);

-- Insert test cases, adapted from statbus tests, using a row-based approach.
INSERT INTO test_stats_rows (test_case, code, stat) VALUES
    (1, 'num', stat(1)), (1, 'str', stat('a'::text)), (1, 'bool', stat(true)),
    (1, 'num', stat(2)), (1, 'str', stat('a'::text)), (1, 'bool', stat(false)),
    (1, 'num', stat(3)), (1, 'str', stat('b'::text)), (1, 'bool', stat(true)),
    (2, 'num', stat(0)), (2, 'str', stat('a'::text)), (2, 'bool', stat(true)),
    (2, 'num', stat(100)), (2, 'str', stat('b'::text)), (2, 'bool', stat(false)),
    (2, 'num', stat(200)), (2, 'str', stat('b'::text)), (2, 'bool', stat(true)),
    (2, 'num', stat(300)), (2, 'str', stat('c'::text)), (2, 'bool', stat(true)),
    (2, 'num', stat(400)), (2, 'str', stat('c'::text)), (2, 'bool', stat(false)),
    (2, 'num', stat(500)), (2, 'str', stat('c'::text)), (2, 'bool', stat(true)),
    (2, 'num', stat(600)), (2, 'str', stat('d'::text)), (2, 'bool', stat(true)),
    (2, 'num', stat(700)), (2, 'str', stat('d'::text)), (2, 'bool', stat(true)),
    (2, 'num', stat(800)), (2, 'str', stat('d'::text)), (2, 'bool', stat(false)),
    (2, 'num', stat(900)), (2, 'str', stat('d'::text)), (2, 'bool', stat(false)),
    (4, 'arr', stat(ARRAY[1, 2]::int[])),
    (4, 'arr', stat(ARRAY[2, 3]::int[])),
    (5, 'arr', stat(ARRAY['a', 'b']::text[])),
    (5, 'arr', stat(ARRAY['b', 'c']::text[])),
    (6, 'arr', stat(ARRAY[1, 2]::int[])), (6, 'str', stat('a'::text)),
    (6, 'arr', stat(ARRAY[3]::int[])), (6, 'str', stat('b'::text));

-- Create a `stats` object for each test case.
CREATE TEMPORARY VIEW test_stats AS
SELECT test_case, jsonb_stats_agg(code, stat) as stats
FROM test_stats_rows
GROUP BY test_case;

-- Create a reference view using standard SQL aggregates to validate the C implementation.
CREATE TEMPORARY VIEW reference AS
SELECT
    test_case,
    SUM((stat->>'value')::numeric) FILTER (WHERE code = 'num') AS num_sum,
    COUNT(stat) FILTER (WHERE code = 'num') AS num_count,
    MIN((stat->>'value')::numeric) FILTER (WHERE code = 'num') AS num_min,
    MAX((stat->>'value')::numeric) FILTER (WHERE code = 'num') AS num_max,
    AVG((stat->>'value')::numeric) FILTER (WHERE code = 'num') AS num_mean,
    VARIANCE((stat->>'value')::numeric) FILTER (WHERE code = 'num') AS num_variance,
    STDDEV((stat->>'value')::numeric) FILTER (WHERE code = 'num') AS num_stddev,
    SUM(CASE WHEN (stat->>'value')::boolean THEN 1 ELSE 0 END) FILTER (WHERE code = 'bool') AS bool_true_count,
    SUM(CASE WHEN NOT (stat->>'value')::boolean THEN 1 ELSE 0 END) FILTER (WHERE code = 'bool') AS bool_false_count,
    COUNT(*) FILTER (WHERE code = 'str' AND stat->>'value' = 'a') AS str_a_count,
    COUNT(*) FILTER (WHERE code = 'str' AND stat->>'value' = 'b') AS str_b_count,
    COUNT(*) FILTER (WHERE code = 'str' AND stat->>'value' = 'c') AS str_c_count,
    COUNT(*) FILTER (WHERE code = 'str' AND stat->>'value' = 'd') AS str_d_count
FROM test_stats_rows
GROUP BY test_case
ORDER BY test_case;

-- Test cases
\t \a \x

-- Test 1: Basic iterative case
SELECT '## C Implementation' as test_description, test_case FROM reference WHERE test_case = 1;
SELECT jsonb_pretty(jsonb_stats_agg(stats)) AS computed_stats FROM test_stats WHERE test_case = 1;
SELECT '## SQL Reference' as test_description, * FROM reference WHERE test_case = 1;

-- Test 2: More complex data
SELECT '## C Implementation' as test_description, test_case FROM reference WHERE test_case = 2;
SELECT jsonb_pretty(jsonb_stats_agg(stats)) AS computed_stats FROM test_stats WHERE test_case = 2;
SELECT '## SQL Reference' as test_description, * FROM reference WHERE test_case = 2;

-- Test 4: Array of numeric values
SELECT '## C Implementation' as test_description, test_case FROM reference WHERE test_case = 4;
SELECT jsonb_pretty(jsonb_stats_agg(stats)) FROM test_stats WHERE test_case = 4;

-- Test 5: Array of string values
SELECT '## C Implementation' as test_description, test_case FROM reference WHERE test_case = 5;
SELECT jsonb_pretty(jsonb_stats_agg(stats)) FROM test_stats WHERE test_case = 5;

-- Test 6: Merging summaries with different keys
SELECT '## C Implementation' as test_description, test_case FROM reference WHERE test_case = 6;
SELECT jsonb_pretty(jsonb_stats_agg(stats)) FROM test_stats WHERE test_case = 6;

ROLLBACK;
