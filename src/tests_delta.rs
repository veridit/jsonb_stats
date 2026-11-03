// Difference summaries (stats_delta) tests, jsonb_stats 3.1.
//
// Included into `mod tests` in lib.rs. Expected values come from
// dev/delta_oracle.py (independent, exact rational arithmetic) via
// dev/fixtures/delta_fixture.sql: 60 units that enter, change value and leave
// over 24 months. Every comparison is on ::text.

fn load_delta_fixture() {
    Spi::run(include_str!("../dev/fixtures/delta_fixture.sql")).expect("Failed to load delta fixture");
    // One +delta when a unit version starts, one -delta when it ends.
    Spi::run(
        "CREATE TEMP TABLE delta_events AS
         SELECT month_from AS month, unit, jsonb_stats_to_delta(stats) AS d FROM delta_units
         UNION ALL
         SELECT month_to, unit, jsonb_stats_negate(stats) FROM delta_units WHERE month_to IS NOT NULL;
         ANALYZE delta_events",
    )
    .unwrap();
}

/// Months whose summary differs from the oracle, for a query yielding (month, summary).
fn delta_mismatches(query: &str) -> String {
    sql_text(&format!(
        "SELECT coalesce(string_agg(e.month::text, ',' ORDER BY e.month), '')
         FROM delta_expected AS e LEFT JOIN ({query}) AS g(month, summary) USING (month)
         WHERE g.summary::text IS DISTINCT FROM e.summary::text"
    ))
}

const RUNNING_SUMMARY: &str = "
    WITH p AS (SELECT month, jsonb_stats_delta_agg(d) AS d FROM delta_events GROUP BY month),
    m AS (SELECT g AS month, coalesce(p.d, '{\"type\": \"stats_delta\"}') AS d
          FROM generate_series(1, 24) AS g LEFT JOIN p ON p.month = g)
    SELECT month, jsonb_stats_delta_to_agg(jsonb_stats_delta_agg(d) OVER (ORDER BY month), d) FROM m";

#[pg_test]
fn test_delta_running_sum_matches_oracle() {
    load_delta_fixture();
    assert_eq!(delta_mismatches(RUNNING_SUMMARY), "", "running window vs oracle");
    // Same stock built without a window: all events up to the month.
    let direct = "SELECT g, jsonb_stats_delta_to_agg(
            (SELECT jsonb_stats_delta_agg(d ORDER BY md5(unit::text || month)) FROM delta_events WHERE month <= g),
            coalesce((SELECT jsonb_stats_delta_agg(d) FROM delta_events WHERE month = g), '{\"type\": \"stats_delta\"}'))
        FROM generate_series(1, 24) AS g";
    assert_eq!(delta_mismatches(direct), "", "direct sums vs oracle");
}

#[pg_test]
fn test_delta_stock_equals_alive_units() {
    load_delta_fixture();
    // Numeric fields of the stock equal jsonb_stats_agg over the units alive
    // at that month, without min/max.
    let diffs = sql_text(
        "WITH s AS (
            SELECT g AS month, jsonb_stats_delta_to_agg(
                (SELECT jsonb_stats_delta_agg(d) FROM delta_events WHERE month <= g)) AS stock,
                (SELECT jsonb_stats_agg(stats) FROM delta_units
                  WHERE month_from <= g AND (month_to IS NULL OR g < month_to)) AS alive
            FROM generate_series(1, 24) AS g)
         SELECT count(*)::text FROM s, LATERAL jsonb_each(s.alive) AS a(key, v)
         WHERE a.key <> 'type'
           AND (CASE WHEN a.v->>'type' = 'str_agg' THEN a.v ELSE a.v - 'min' - 'max' END)::text
               IS DISTINCT FROM (s.stock->a.key)::text",
    );
    assert_eq!(diffs, "0");
}

#[pg_test]
fn test_delta_round_trip_and_order() {
    load_delta_fixture();
    // Entry then exit returns exactly to the empty stock, for every unit.
    let not_empty = sql_text(
        "SELECT count(*)::text FROM delta_units
         WHERE jsonb_stats_delta_to_agg(jsonb_stats_delta_merge(
                   jsonb_stats_to_delta(stats), jsonb_stats_negate(stats)))::text
               <> '{\"type\": \"stats_agg\", \"min_max\": false}'",
    );
    assert_eq!(not_empty, "0");
    // negate(negate(x)) = to_delta(x); to_delta is idempotent.
    let bad = sql_text(
        "SELECT count(*)::text FROM delta_units
         WHERE jsonb_stats_negate(jsonb_stats_negate(stats))::text <> jsonb_stats_to_delta(stats)::text
            OR jsonb_stats_to_delta(jsonb_stats_to_delta(stats))::text <> jsonb_stats_to_delta(stats)::text
            OR jsonb_stats_to_delta(jsonb_stats_to_agg(stats))::text <> jsonb_stats_to_delta(stats)::text",
    );
    assert_eq!(bad, "0");
    // Any order and grouping: identical bytes.
    let expected = sql_text("SELECT jsonb_stats_delta_agg(d ORDER BY month, unit)::text FROM delta_events");
    for q in [
        "SELECT jsonb_stats_delta_agg(d ORDER BY md5(unit::text || month::text))::text FROM delta_events",
        "SELECT jsonb_stats_delta_agg(d ORDER BY month DESC, unit DESC)::text FROM delta_events",
        "SELECT jsonb_stats_delta_agg(p ORDER BY g DESC)::text FROM (
             SELECT unit % 7 AS g, jsonb_stats_delta_agg(d) AS p FROM delta_events GROUP BY 1) t",
        "SELECT jsonb_stats_delta_merge(
             (SELECT jsonb_stats_delta_agg(d) FROM delta_events WHERE unit % 2 = 0),
             (SELECT jsonb_stats_delta_agg(d) FROM delta_events WHERE unit % 2 = 1))::text",
    ] {
        assert_eq!(sql_text(q), expected, "{q}");
    }
}

#[pg_test]
fn test_delta_parallel_matches_serial() {
    load_delta_fixture();
    Spi::run(
        "CREATE TEMP TABLE delta_x200 AS
         SELECT e.month, e.unit * 1000 + r AS unit, e.d FROM delta_events e, generate_series(1, 200) AS r;
         ANALYZE delta_x200",
    )
    .unwrap();
    Spi::run("SET max_parallel_workers_per_gather = 0").unwrap();
    let serial = sql_text("SELECT jsonb_stats_delta_agg(d)::text FROM delta_x200");
    let serial_g = sql_text(
        "SELECT string_agg(s, '|' ORDER BY month) FROM (
           SELECT month, jsonb_stats_delta_agg(d)::text AS s FROM delta_x200 GROUP BY month) t",
    );
    Spi::run(
        "SET parallel_setup_cost = 0; SET parallel_tuple_cost = 0;
         SET min_parallel_table_scan_size = 0; SET max_parallel_workers_per_gather = 4;
         SET debug_parallel_query = regress",
    )
    .unwrap();
    assert_eq!(sql_text("SELECT jsonb_stats_delta_agg(d)::text FROM delta_x200"), serial);
    assert_eq!(
        sql_text(
            "SELECT string_agg(s, '|' ORDER BY month) FROM (
               SELECT month, jsonb_stats_delta_agg(d)::text AS s FROM delta_x200 GROUP BY month) t"
        ),
        serial_g
    );
    // 200 copies of every event: the stock is 200 times the original.
    let x1 = sql_text(
        "SELECT jsonb_stats_delta_to_agg(jsonb_stats_delta_agg(d))->'emp'->>'sum' FROM delta_events",
    );
    let x200 = sql_text(
        "SELECT jsonb_stats_delta_to_agg(jsonb_stats_delta_agg(d))->'emp'->>'sum' FROM delta_x200",
    );
    assert_eq!(
        x200,
        (x1.parse::<i64>().unwrap() * 200).to_string()
    );
}

#[pg_test]
fn test_delta_moving_window_matches_plain() {
    load_delta_fixture();
    // Rolling 3 months uses the inverse transition (minvfunc); it must give
    // exactly what a fresh sum over the same rows gives.
    let bad = sql_text(
        "WITH p AS (SELECT month, jsonb_stats_delta_agg(d) AS d FROM delta_events GROUP BY month),
         w AS (SELECT month, jsonb_stats_delta_agg(d) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS r
               FROM p)
         SELECT count(*)::text FROM w
         WHERE r::text IS DISTINCT FROM (
             SELECT jsonb_stats_delta_agg(p2.d)::text FROM p AS p2
             WHERE p2.month IN (SELECT month FROM p AS p3 WHERE p3.month <= w.month ORDER BY month DESC LIMIT 3))",
    );
    assert_eq!(bad, "0");
    // The plan really is a moving aggregate.
    let inv = sql_text(
        "SELECT (aggminvtransfn <> 0 AND aggmtransfn <> 0 AND aggcombinefn <> 0)::text
         FROM pg_aggregate WHERE aggfnoid = 'jsonb_stats_delta_agg'::regproc",
    );
    assert_eq!(inv, "true");
}

#[pg_test]
fn test_delta_reference_parity() {
    load_plpgsql_reference();
    load_delta_fixture();
    let reference = RUNNING_SUMMARY
        .replace("jsonb_stats_delta_agg(", "jsonb_stats_delta_agg_plpgsql(")
        .replace("jsonb_stats_delta_to_agg(", "jsonb_stats_delta_to_agg_plpgsql(");
    assert_eq!(delta_mismatches(&reference), "", "reference running window vs oracle");
    for (rust, refr) in [
        ("jsonb_stats_to_delta(stats)", "jsonb_stats_to_delta_plpgsql(stats)"),
        ("jsonb_stats_negate(stats)", "jsonb_stats_negate_plpgsql(stats)"),
        ("jsonb_stats_to_delta(jsonb_stats_to_agg(stats))", "jsonb_stats_to_delta_plpgsql(jsonb_stats_to_agg(stats))"),
        (
            "jsonb_stats_delta_merge(jsonb_stats_to_delta(stats), jsonb_stats_negate(stats))",
            "jsonb_stats_delta_merge_plpgsql(jsonb_stats_to_delta_plpgsql(stats), jsonb_stats_negate_plpgsql(stats))",
        ),
        (
            "jsonb_stats_delta_to_agg(jsonb_stats_to_delta(stats), jsonb_stats_to_delta(stats))",
            "jsonb_stats_delta_to_agg_plpgsql(jsonb_stats_to_delta_plpgsql(stats), jsonb_stats_to_delta_plpgsql(stats))",
        ),
    ] {
        let diffs = sql_text(&format!(
            "SELECT count(*)::text FROM delta_units WHERE ({rust})::text IS DISTINCT FROM ({refr})::text"
        ));
        assert_eq!(diffs, "0", "{rust} vs {refr}");
    }
    assert_eq!(
        sql_text("SELECT jsonb_stats_delta_agg_plpgsql(d ORDER BY unit DESC, month)::text FROM delta_events"),
        sql_text("SELECT jsonb_stats_delta_agg(d)::text FROM delta_events"),
        "delta_agg reference parity"
    );
    let moving = "SELECT string_agg(r::text, '|' ORDER BY month) FROM (
        SELECT month, AGG(d) OVER (ORDER BY month ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS r
        FROM (SELECT month, jsonb_stats_delta_agg(d) AS d FROM delta_events GROUP BY month) p) w";
    assert_eq!(
        sql_text(&moving.replace("AGG", "jsonb_stats_delta_agg_plpgsql")),
        sql_text(&moving.replace("AGG", "jsonb_stats_delta_agg")),
        "moving window reference parity"
    );
}

#[pg_test]
fn test_delta_shapes_and_rates() {
    // The example from todo.md: stock 50,000 employees; 30 births bring 600,
    // 20 deaths take 900, size changes move 2,000 -> 2,100.
    let s = sql_text(
        r#"SELECT ((jsonb_stats_delta_to_agg(
            '{"type": "stats_delta", "emp": {"type": "int_delta",
               "in": {"count": 1080, "sum": 52700, "sum_sq": 52700000},
               "out": {"count": 70, "sum": 2900, "sum_sq": 300000}}}'::jsonb,
            '{"type": "stats_delta", "emp": {"type": "int_delta",
               "in": {"count": 80, "sum": 2700, "sum_sq": 200000},
               "out": {"count": 70, "sum": 2900, "sum_sq": 300000}}}'::jsonb)->'emp')
           - '{sum_sq,variance,stddev,sum_sq_diff,coefficient_of_variation_pct}'::text[])::text"#,
    );
    assert_eq!(
        s,
        r#"{"in": {"sum": 2700, "count": 80}, "out": {"sum": 2900, "count": 70}, "sum": 49800, "mean": 49.31, "type": "int_agg", "count": 1010, "churn_pct": 11.20, "change_pct": -0.40}"#
    );
    // Shapes: empty flows are omitted, float trailing zeros trimmed, dec2 scale kept.
    assert_eq!(
        sql_text(
            r#"SELECT jsonb_stats_delta_merge(
                jsonb_stats_to_delta(stats(jsonb_build_object('f', stat(1.50::float8), 'd', stat(1.5::numeric(10,2))))),
                jsonb_stats_negate(stats(jsonb_build_object('f', stat(0.5::float8)))))::text"#
        ),
        r#"{"d": {"in": {"sum": 1.50, "count": 1, "sum_sq": 2.2500}, "type": "dec2_delta"}, "f": {"in": {"sum": 1.5, "count": 1, "sum_sq": 2.25}, "out": {"sum": 0.5, "count": 1, "sum_sq": 0.25}, "type": "float_delta"}, "type": "stats_delta"}"#
    );
    // Empty inputs
    assert_eq!(sql_text("SELECT jsonb_stats_delta_agg(NULL::jsonb)::text"), r#"{"type": "stats_delta"}"#);
    assert_eq!(
        sql_text(r#"SELECT jsonb_stats_delta_to_agg('{"type": "stats_delta"}')::text"#),
        r#"{"type": "stats_agg", "min_max": false}"#
    );
}

#[pg_test]
fn test_delta_invalid_input_fails_fast() {
    load_plpgsql_reference();
    let cases = [
        // A stock that is only an exit is not a valid stock.
        (
            r#"SELECT jsonb_stats_delta_to_agg(jsonb_stats_negate('{"type": "stats", "e": {"type": "int", "value": 3}}'))"#,
            "jsonb_stats: int_delta for key 'e' has an invalid 'stock' flow: count must be an integer >= 0",
        ),
        // The previous stock (stock - period) must be valid too: an entry in
        // the period that is not in the stock.
        (
            r#"SELECT jsonb_stats_delta_to_agg('{"type": "stats_delta"}', jsonb_stats_to_delta('{"type": "stats", "e": {"type": "int", "value": 3}}'))"#,
            "jsonb_stats: int_delta for key 'e' has an invalid 'previous stock' flow: count must be an integer >= 0",
        ),
        (
            r#"SELECT jsonb_stats_to_delta('{"type": "stats_delta", "e": {"type": "int_delta", "in": {"count": -1, "sum": 0, "sum_sq": 0}}}')"#,
            "jsonb_stats: int_delta for key 'e' has an invalid 'in' flow: count must be an integer >= 0",
        ),
        (
            r#"SELECT jsonb_stats_to_delta('{"type": "stats_delta", "e": {"type": "int_delta", "in": {"count": 2, "sum": 10, "sum_sq": 1}}}')"#,
            "jsonb_stats: int_delta for key 'e' has an invalid 'in' flow: inconsistent (count * sum_sq < sum^2)",
        ),
        (
            r#"SELECT jsonb_stats_to_delta('{"type": "stats_delta", "e": {"type": "int_delta", "in": {"count": 1, "sum": 1.5, "sum_sq": 2.25}}}')"#,
            "jsonb_stats: int_delta for key 'e' has an invalid 'in' flow: sum and sum_sq must be integers",
        ),
        (
            r#"SELECT jsonb_stats_to_delta('{"type": "stats_delta", "e": {"type": "str_delta", "out": {"counts": {"a": -1}}}}')"#,
            "jsonb_stats: str_delta for key 'e' has an invalid 'out' flow: counts must be integers >= 0",
        ),
        (
            r#"SELECT jsonb_stats_to_delta('{"type": "stats_delta", "e": {"type": "foo_delta"}}')"#,
            "jsonb_stats: unknown summary type 'foo_delta' for key 'e'. Expected a *_agg or *_delta of: int, float, dec2, nat, str, bool, arr, date",
        ),
        (
            r#"SELECT jsonb_stats_to_delta('{"type": "stats_agg", "min_max": false}')"#,
            "jsonb_stats: a stats_agg without min/max (from jsonb_stats_delta_to_agg) is a result, not an input",
        ),
        (
            r#"SELECT jsonb_stats_to_delta('{"type": "stats", "min_max": {"type": "int", "value": 1}}')"#,
            "jsonb_stats: key 'min_max' is reserved in stats_delta",
        ),
        (
            r#"SELECT jsonb_stats_to_delta('{"type": "nope"}')"#,
            "jsonb_stats: expected stats, stats_agg or stats_delta, got type \"nope\"",
        ),
        (
            r#"SELECT jsonb_stats_delta_merge(jsonb_stats_to_delta('{"type": "stats", "e": {"type": "int", "value": 3}}'), jsonb_stats_to_delta('{"type": "stats", "e": {"type": "str", "value": "x"}}'))"#,
            "jsonb_stats: type mismatch for key 'e': existing int_delta vs incoming str_delta",
        ),
    ];
    for (q, msg) in cases {
        assert_eq!(sql_error(q), msg, "{q}");
        let refq = q
            .replace("jsonb_stats_delta_to_agg(", "jsonb_stats_delta_to_agg_plpgsql(")
            .replace("jsonb_stats_to_delta(", "jsonb_stats_to_delta_plpgsql(")
            .replace("jsonb_stats_negate(", "jsonb_stats_negate_plpgsql(")
            .replace("jsonb_stats_delta_merge(", "jsonb_stats_delta_merge_plpgsql(");
        assert_eq!(sql_error(&refq), msg, "reference: {refq}");
    }
}
