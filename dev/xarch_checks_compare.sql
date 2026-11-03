-- jsonb_stats_values / jsonb_stats_compare cross-architecture checks.
-- <label>|<matches oracle>|<md5 of output text>
SELECT 'compare_oracle_mismatching', count(*) FILTER (WHERE got::text IS DISTINCT FROM expected::text)::text,
       md5(string_agg(got::text, '|' ORDER BY name))
FROM (SELECT c.name, c.expected, jsonb_stats_compare(
        (SELECT jsonb_stats_agg(jsonb_build_object('type', 'stats', 'v', jsonb_build_object('type', c.kind, 'value', x::numeric))) FROM unnest(c.a_vals) x),
        (SELECT jsonb_stats_agg(jsonb_build_object('type', 'stats', 'v', jsonb_build_object('type', c.kind, 'value', x::numeric))) FROM unnest(c.b_vals) x)) -> 'v' AS got
      FROM compare_cases c) q;

SELECT 'values_exact_fixture', true,
       md5(string_agg(concat_ws('|', e.i, v.key, v.stat_type, v.value, v.text_value, v.count), ';' ORDER BY e.i, v.ord))
FROM exact_fixture e, jsonb_stats_values(e.stats) WITH ORDINALITY AS v(key, stat_type, value, text_value, count, ord);
