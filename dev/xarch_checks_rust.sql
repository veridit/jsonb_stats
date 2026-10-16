-- Rust-side cross-architecture checks (requires the extension installed).
-- One line per check: <label>|<matches oracle>|<md5 of output text>
CREATE EXTENSION jsonb_stats;

SELECT 'rust_agg_asc', a::text = e.agg::text, md5(a::text)
FROM (SELECT jsonb_stats_agg(stats ORDER BY i) a FROM exact_fixture) q, exact_fixture_expected e;

SELECT 'rust_agg_shuffled', a::text = e.agg::text, md5(a::text)
FROM (SELECT jsonb_stats_agg(stats ORDER BY md5(i::text)) a FROM exact_fixture) q, exact_fixture_expected e;

SELECT 'rust_merge_mod7_desc', a::text = e.agg::text, md5(a::text)
FROM (SELECT jsonb_stats_merge_agg(a ORDER BY g DESC) a
      FROM (SELECT i % 7 g, jsonb_stats_agg(stats) a FROM exact_fixture GROUP BY 1) t) q, exact_fixture_expected e;

SELECT 'rust_scalar_merge', jsonb_stats_merge(o.a, v.a)::text = e.agg::text, md5(jsonb_stats_merge(o.a, v.a)::text)
FROM (SELECT jsonb_stats_agg(stats) a FROM exact_fixture WHERE i % 2 = 1) o,
     (SELECT jsonb_stats_agg(stats) a FROM exact_fixture WHERE i % 2 = 0) v, exact_fixture_expected e;

SELECT 'rust_edge_cases_mismatching', count(*)::text, ''
FROM exact_edge_cases e
WHERE (SELECT jsonb_stats_agg(
               jsonb_build_object('v', jsonb_build_object('type', e.kind, 'value', u.v::numeric))) -> 'v'
       FROM unnest(e.vals) AS u(v))::text IS DISTINCT FROM e.expected::text;

CREATE TEMP TABLE exact_x300 AS
SELECT e.i * 1000 + r AS i, e.stats FROM exact_fixture e, generate_series(1, 300) AS r;
ANALYZE exact_x300;
SET parallel_setup_cost = 0; SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0; SET max_parallel_workers_per_gather = 4;
SET debug_parallel_query = regress;
SELECT 'rust_parallel_x300', a::text = e.agg::text, md5(a::text)
FROM (SELECT jsonb_stats_agg(stats) a FROM exact_x300) q, exact_fixture_x300_expected e;
