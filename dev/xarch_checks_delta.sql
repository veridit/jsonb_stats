-- stats_delta cross-architecture checks (requires the extension installed).
-- <label>|<matches oracle>|<md5 of output text>
CREATE TEMP TABLE delta_events AS
SELECT month_from AS month, unit, jsonb_stats_to_delta(stats) AS d FROM delta_units
UNION ALL
SELECT month_to, unit, jsonb_stats_negate(stats) FROM delta_units WHERE month_to IS NOT NULL;

SELECT 'delta_running_months_mismatching', count(*) FILTER (WHERE g.summary::text IS DISTINCT FROM e.summary::text)::text,
       md5(string_agg(g.summary::text, '|' ORDER BY g.month))
FROM delta_expected e
JOIN (WITH p AS (SELECT month, jsonb_stats_delta_agg(d) AS d FROM delta_events GROUP BY month),
           m AS (SELECT g AS month, coalesce(p.d, '{"type": "stats_delta"}') AS d
                 FROM generate_series(1, 24) AS g LEFT JOIN p ON p.month = g)
      SELECT month, jsonb_stats_delta_summary(jsonb_stats_delta_agg(d) OVER (ORDER BY month), d) AS summary FROM m) g
  USING (month);

SELECT 'delta_agg_all', true, md5(jsonb_stats_delta_agg(d ORDER BY md5(unit::text || month))::text) FROM delta_events;

SELECT 'delta_reference_running_mismatching', count(*)::text, ''
FROM delta_expected e
JOIN (WITH p AS (SELECT month, jsonb_stats_delta_agg_plpgsql(d) AS d FROM delta_events GROUP BY month),
           m AS (SELECT g AS month, coalesce(p.d, '{"type": "stats_delta"}') AS d
                 FROM generate_series(1, 24) AS g LEFT JOIN p ON p.month = g)
      SELECT month, jsonb_stats_delta_summary_plpgsql(jsonb_stats_delta_agg_plpgsql(d) OVER (ORDER BY month), d) AS s FROM m) g
  USING (month)
WHERE g.s::text IS DISTINCT FROM e.summary::text;
