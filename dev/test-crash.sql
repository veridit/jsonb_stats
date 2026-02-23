-- =============================================================================
-- REGRESSION TEST: jsonb_stats_merge_agg CTE-inlining segfault
-- =============================================================================
--
-- This script exercises the real crashing code path: PL/pgSQL RETURN QUERY
-- with a non-MATERIALIZED CTE containing jsonb_stats_merge_agg, joined to a
-- sibling CTE. The planner inlines the aggregate CTE, causing the finalfunc
-- result to be allocated in a per-tuple memory context that gets reset.
--
-- Run via: make test-crash
-- Success: prints "ALL TESTS PASSED"
-- Failure: SIGSEGV (signal 11) — psql connection lost
--
-- The fix (TxnJsonB newtype) allocates the finalfunc result in
-- CurTransactionContext, which survives per-tuple resets.
-- =============================================================================

-- Setup
DROP FUNCTION IF EXISTS test_crash() CASCADE;
DROP FUNCTION IF EXISTS test_workaround() CASCADE;
DROP TYPE IF EXISTS test_result CASCADE;
DROP TABLE IF EXISTS test_unit CASCADE;

CREATE TABLE test_unit (
    unit_id int NOT NULL,
    unit_type text NOT NULL,
    valid_from date NOT NULL DEFAULT '2025-01-01',
    valid_to date NOT NULL DEFAULT 'infinity',
    used_for_counting boolean NOT NULL DEFAULT true,
    category int,
    stats_summary jsonb NOT NULL DEFAULT '{"type": "stats_agg"}'::jsonb
);

INSERT INTO test_unit (unit_id, unit_type, category)
SELECT i, CASE WHEN i % 2 = 0 THEN 'a' ELSE 'b' END, i % 5
FROM generate_series(1, 100) AS i;

ANALYZE test_unit;

CREATE TYPE test_result AS (
    unit_type text,
    category int,
    cnt int,
    stats_summary jsonb
);

-- WORKAROUND: MATERIALIZED prevents inlining (always works)
CREATE FUNCTION test_workaround() RETURNS SETOF test_result
LANGUAGE plpgsql AS $test_workaround$
BEGIN
    RETURN QUERY
    WITH
    filtered AS (
        SELECT * FROM test_unit su
        WHERE daterange(su.valid_from, su.valid_to, '[)') && daterange('2024-01-01', '2026-01-01', '[)')
    ),
    stats_agg AS MATERIALIZED (
        SELECT f.unit_type, f.category,
            COALESCE(public.jsonb_stats_merge_agg(f.stats_summary), '{}'::jsonb) AS stats_summary
        FROM filtered f
        GROUP BY 1, 2
    ),
    counts AS (
        SELECT f.unit_type, f.category, count(*)::integer AS cnt
        FROM filtered f
        GROUP BY 1, 2
    )
    SELECT c.unit_type, c.category, c.cnt,
        COALESCE(s.stats_summary, '{}'::jsonb)
    FROM counts c
    LEFT JOIN stats_agg s ON s.unit_type = c.unit_type
        AND s.category IS NOT DISTINCT FROM c.category;
END;
$test_workaround$;

-- BUG: Without the TxnJsonB fix, this crashes with SIGSEGV
CREATE FUNCTION test_crash() RETURNS SETOF test_result
LANGUAGE plpgsql AS $test_crash$
BEGIN
    RETURN QUERY
    WITH
    filtered AS (
        SELECT * FROM test_unit su
        WHERE daterange(su.valid_from, su.valid_to, '[)') && daterange('2024-01-01', '2026-01-01', '[)')
    ),
    stats_agg AS (
        SELECT f.unit_type, f.category,
            COALESCE(public.jsonb_stats_merge_agg(f.stats_summary), '{}'::jsonb) AS stats_summary
        FROM filtered f
        GROUP BY 1, 2
    ),
    counts AS (
        SELECT f.unit_type, f.category, count(*)::integer AS cnt
        FROM filtered f
        GROUP BY 1, 2
    )
    SELECT c.unit_type, c.category, c.cnt,
        COALESCE(s.stats_summary, '{}'::jsonb)
    FROM counts c
    LEFT JOIN stats_agg s ON s.unit_type = c.unit_type
        AND s.category IS NOT DISTINCT FROM c.category;
END;
$test_crash$;

-- Run tests
\echo '=== TEST 1: Plain SQL (control) ==='
WITH
filtered AS (
    SELECT * FROM test_unit su
    WHERE daterange(su.valid_from, su.valid_to, '[)') && daterange('2024-01-01', '2026-01-01', '[)')
),
stats_agg AS (
    SELECT f.unit_type, f.category,
        COALESCE(public.jsonb_stats_merge_agg(f.stats_summary), '{}'::jsonb) AS stats_summary
    FROM filtered f
    GROUP BY 1, 2
),
counts AS (
    SELECT f.unit_type, f.category, count(*)::integer AS cnt
    FROM filtered f
    GROUP BY 1, 2
)
SELECT count(*) AS row_count FROM counts c
LEFT JOIN stats_agg s ON s.unit_type = c.unit_type
    AND s.category IS NOT DISTINCT FROM c.category;

\echo '=== TEST 2: PL/pgSQL with MATERIALIZED (workaround) ==='
SELECT count(*) AS row_count FROM test_workaround();

\echo '=== TEST 3: PL/pgSQL without MATERIALIZED (previously crashed) ==='
SELECT count(*) AS row_count FROM test_crash();

-- Cleanup
DROP FUNCTION test_crash() CASCADE;
DROP FUNCTION test_workaround() CASCADE;
DROP TYPE test_result CASCADE;
DROP TABLE test_unit CASCADE;

\echo '=== ALL TESTS PASSED ==='
