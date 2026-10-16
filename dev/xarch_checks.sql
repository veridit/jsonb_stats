-- Cross-architecture determinism checks. Prints one line per check:
--   <label>|<matches oracle>|<md5 of output text>
-- Run on each architecture and diff the outputs.
SELECT 'ref_agg_desc',
       jsonb_stats_agg_plpgsql(stats ORDER BY i DESC)::text = (SELECT agg::text FROM exact_fixture_expected),
       md5(jsonb_stats_agg_plpgsql(stats ORDER BY i DESC)::text)
FROM exact_fixture;

SELECT 'ref_merge_mod7',
       r::text = (SELECT agg::text FROM exact_fixture_expected), md5(r::text)
FROM (SELECT jsonb_stats_merge_agg_plpgsql(a) AS r
      FROM (SELECT i % 7, jsonb_stats_agg_plpgsql(stats) AS a FROM exact_fixture GROUP BY 1) t) q;

SELECT 'ref_edge_cases_mismatching', count(*)::text, ''
FROM exact_edge_cases e
WHERE (SELECT jsonb_stats_agg_plpgsql(
               jsonb_build_object('v', jsonb_build_object('type', e.kind, 'value', u.v::numeric))) -> 'v'
       FROM unnest(e.vals) AS u(v))::text IS DISTINCT FROM e.expected::text;

SELECT 'oracle_expected', true, md5(agg::text) FROM exact_fixture_expected;
