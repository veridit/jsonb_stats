BEGIN;
-- Create PL/pgSQL reference implementations from dev/
\include dev/reference_plpgsql.sql

-- Test Setup
CREATE EXTENSION jsonb_stats;

CREATE TABLE test_data (stats jsonb);
CREATE TEMPORARY TABLE benchmark (
  timestamp TIMESTAMPTZ DEFAULT clock_timestamp(),
  event TEXT,
  row_count INTEGER
);

INSERT INTO benchmark (event, row_count) VALUES ('BEGIN', 0);

-- Generate 10k rows of test data
INSERT INTO test_data (stats)
SELECT jsonb_build_object(
    'type', 'stats',
    'num', stat(floor(random() * 1000)::int),
    'str', stat(substr(md5(random()::text), 1, 5)),
    'bool', stat(random() > 0.5)
)
FROM generate_series(1, 10000);
INSERT INTO benchmark (event, row_count) VALUES ('Data generated', (SELECT COUNT(*) FROM test_data));

-- Benchmark C implementation
INSERT INTO benchmark (event, row_count) VALUES ('C implementation start', 0);
SELECT jsonb_stats_agg(stats) INTO TEMP TABLE c_result FROM test_data;
INSERT INTO benchmark (event, row_count) VALUES ('C implementation end', (SELECT COUNT(*) FROM test_data));

-- Benchmark PL/pgSQL implementation
INSERT INTO benchmark (event, row_count) VALUES ('PL/pgSQL implementation start', 0);
SELECT jsonb_stats_agg_plpgsql(stats) INTO TEMP TABLE plpgsql_result FROM test_data;
INSERT INTO benchmark (event, row_count) VALUES ('PL/pgSQL implementation end', (SELECT COUNT(*) FROM test_data));

-- Correctness check: ensure C and PL/pgSQL produce same counts.
CREATE TEMPORARY VIEW comparison AS
SELECT
    (c_result.jsonb_stats_agg->'num'->>'count')::int AS c_num_count,
    (plpgsql_result.jsonb_stats_agg_plpgsql->'num'->>'count')::int AS p_num_count
FROM c_result, plpgsql_result;

-- This output is stable and goes into the regression test's expected file.
\t \a \x
SELECT 'Correctness check' as test_description;
SELECT 'C and PL/pgSQL implementations produced identical counts' AS result
FROM comparison
WHERE c_num_count = p_num_count;

INSERT INTO benchmark (event, row_count) VALUES ('Tear down', 0);
DROP VIEW comparison;
DROP TABLE test_data;
DROP TABLE c_result;
DROP TABLE plpgsql_result;


-- Capture performance metrics to a separate file for manual review.
\o expected/004_benchmark_performance.out

WITH timings AS (
    SELECT
        event,
        row_count,
        EXTRACT(EPOCH FROM (timestamp - lag(timestamp) OVER (ORDER BY timestamp))) as duration
    FROM benchmark
)
SELECT
    event,
    to_char(duration, 'FM999.00 "secs"') AS duration,
    CASE
        WHEN duration > 0 THEN '~' || round(row_count / duration / 100) * 100 || ' rows/s'
        ELSE 'N/A'
    END as throughput
FROM timings
WHERE event IN ('C implementation end', 'PL/pgSQL implementation end')
ORDER BY event;

\o

ROLLBACK;
