// jsonb_stats_values and jsonb_stats_compare tests (3.1).
//
// Included into `mod tests` in lib.rs. Expected compare values come from
// dev/compare_oracle.py (textbook definitions with exact rationals, variance
// from the values themselves) via dev/fixtures/compare_fixture.sql.

fn load_compare_fixture() {
    Spi::run(include_str!("../dev/fixtures/compare_fixture.sql")).expect("Failed to load compare fixture");
}

/// SQL building one stats_agg with key 'v' from a text[] of values of `kind`.
const AGG_OF: &str = "(SELECT jsonb_stats_agg(jsonb_build_object('type', 'stats', 'v',
    jsonb_build_object('type', c.kind, 'value', x::numeric))) FROM unnest(ARR) AS x)";

fn compare_mismatches(func: &str) -> String {
    let (a, b) = (AGG_OF.replace("ARR", "c.a_vals"), AGG_OF.replace("ARR", "c.b_vals"));
    sql_text(&format!(
        "SELECT coalesce(string_agg(name, ', ' ORDER BY name), '') FROM (
           SELECT c.name, c.expected, ({func}({a}, {b}) -> 'v') AS got FROM compare_cases c) q
         WHERE got::text IS DISTINCT FROM expected::text"
    ))
}

#[pg_test]
fn test_compare_matches_oracle() {
    load_compare_fixture();
    assert_eq!(compare_mismatches("jsonb_stats_compare"), "", "Rust vs oracle");
    assert_eq!(sql_text("SELECT count(*)::text FROM compare_cases"), "53");
}

#[pg_test]
fn test_compare_reference_parity() {
    load_plpgsql_reference();
    load_compare_fixture();
    assert_eq!(compare_mismatches("jsonb_stats_compare_plpgsql"), "", "PL/pgSQL vs oracle");
    // Byte parity on whole objects with several keys, both directions, and
    // mixed types (counted keys skipped, missing keys empty).
    let (a, b) = (AGG_OF.replace("ARR", "c.a_vals"), AGG_OF.replace("ARR", "c.b_vals"));
    let diffs = sql_text(&format!(
        "WITH p AS (SELECT {a} AS a, {b} AS b FROM compare_cases c),
         q AS (SELECT a || jsonb_build_object('only_a', a->'v', 'tag', jsonb_stats_to_agg(stats('t', 'x'::text))->'t') AS a,
                      b || jsonb_build_object('only_b', b->'v') AS b FROM p)
         SELECT count(*)::text FROM q
         WHERE jsonb_stats_compare(a, b)::text IS DISTINCT FROM jsonb_stats_compare_plpgsql(a, b)::text
            OR jsonb_stats_compare(b, a)::text IS DISTINCT FROM jsonb_stats_compare_plpgsql(b, a)::text"
    ));
    assert_eq!(diffs, "0");
}

#[pg_test]
fn test_compare_shapes() {
    // Missing on one side counts as empty; counted keys are skipped.
    let got = sql_text(
        "SELECT jsonb_stats_compare(
            jsonb_stats_to_agg(stats(jsonb_build_object('e', stat(10), 'r', stat('Oslo'::text)))),
            jsonb_stats_to_agg(stats(jsonb_build_object('t', stat(2.5::float8)))))::text",
    );
    assert_eq!(
        got,
        r#"{"e": {"z": null, "type": "int", "count_a": 1, "count_b": 0, "change_pct": -100.00, "mean_change_pct": null}, "t": {"z": null, "type": "float", "count_a": 0, "count_b": 1, "change_pct": null, "mean_change_pct": null}, "type": "stats_compare"}"#
    );
    // Works on stocks read from deltas (min_max: false), including empty stocks.
    let got = sql_text(
        "SELECT jsonb_stats_compare(
            jsonb_stats_delta_to_agg(jsonb_stats_to_delta((SELECT jsonb_stats_agg(stats('e', g)) FROM generate_series(1, 10) g))),
            jsonb_stats_delta_to_agg(jsonb_stats_delta_merge(jsonb_stats_to_delta(stats('e', 3)), jsonb_stats_negate(stats('e', 3))),
                                     jsonb_stats_negate(stats('e', 3))))::text",
    );
    assert_eq!(
        got,
        r#"{"e": {"z": null, "type": "int", "count_a": 10, "count_b": 0, "change_pct": -100.00, "mean_change_pct": null}, "type": "stats_compare"}"#
    );
}

#[pg_test]
fn test_compare_delta_stocks() {
    load_plpgsql_reference();
    load_delta_fixture();
    // Non-empty stocks read from deltas (no min/max) compare exactly like the
    // same units aggregated directly: months 12 vs 24 of the delta fixture.
    let stock = |m: i32| {
        format!(
            "(SELECT jsonb_stats_delta_to_agg(jsonb_stats_delta_agg(d)) FROM delta_events WHERE month <= {m})"
        )
    };
    let alive = |m: i32| {
        format!(
            "(SELECT jsonb_stats_agg(stats) FROM delta_units WHERE month_from <= {m} AND (month_to IS NULL OR {m} < month_to))"
        )
    };
    let via_delta = sql_text(&format!("SELECT jsonb_stats_compare({}, {})::text", stock(12), stock(24)));
    let direct = sql_text(&format!("SELECT jsonb_stats_compare({}, {})::text", alive(12), alive(24)));
    assert_eq!(via_delta, direct, "delta stocks vs direct aggregation");
    assert!(via_delta.contains(r#""z": "#) && !via_delta.contains("region"), "{via_delta}");
    let reference = sql_text(&format!(
        "SELECT jsonb_stats_compare_plpgsql({}, {})::text",
        stock(12),
        stock(24)
    ));
    assert_eq!(via_delta, reference, "reference on delta stocks");
    // min/max are only optional when the stats_agg is marked min_max: false.
    let msg = sql_error(
        r#"SELECT jsonb_stats_compare('{"type": "stats_agg", "e": {"type": "int_agg", "count": 2, "sum": 3, "sum_sq": 5}}', '{"type": "stats_agg"}')"#,
    );
    assert_eq!(msg, "jsonb_stats: int_agg for key 'e' has missing or non-numeric 'min'");
    let ok = sql_text(
        r#"SELECT jsonb_stats_compare('{"type": "stats_agg", "min_max": false, "e": {"type": "int_agg", "count": 2, "sum": 3, "sum_sq": 5}}', '{"type": "stats_agg"}')::text"#,
    );
    assert_eq!(
        ok,
        r#"{"e": {"z": null, "type": "int", "count_a": 2, "count_b": 0, "change_pct": -100.00, "mean_change_pct": null}, "type": "stats_compare"}"#
    );
    let msg = sql_error(
        r#"SELECT jsonb_stats_compare('{"type": "stats_agg", "min_max": false, "e": {"type": "int_agg", "count": 2, "sum": 10, "sum_sq": 1}}', '{"type": "stats_agg"}')"#,
    );
    assert_eq!(msg, "jsonb_stats: int_agg for key 'e' is inconsistent (count * sum_sq < sum^2)");
}

#[pg_test]
fn test_compare_fails_fast() {
    load_plpgsql_reference();
    let cases = [
        (
            "SELECT jsonb_stats_compare(stats('e', 1), jsonb_stats_to_agg(stats('e', 1)))",
            "jsonb_stats: jsonb_stats_compare expects two stats_agg, got \"stats\" and \"stats_agg\"",
        ),
        (
            "SELECT jsonb_stats_compare(jsonb_stats_to_agg(stats('e', 1)), jsonb_stats_to_agg(stats('e', 1.5::float8)))",
            "jsonb_stats: type mismatch for key 'e': existing int_agg vs incoming float_agg",
        ),
        (
            r#"SELECT jsonb_stats_compare('{"type": "stats_agg", "e": {"type": "int_agg", "count": 0, "sum": 5, "sum_sq": 25}}', jsonb_stats_to_agg(stats('e', 1)))"#,
            "jsonb_stats: int_agg for key 'e' has count 0 but a non-zero or missing sum/sum_sq",
        ),
        (
            r#"SELECT jsonb_stats_compare('{"type": "stats_agg", "e": {"type": "foo_agg"}}', '{"type": "stats_agg"}')"#,
            "jsonb_stats: unknown aggregate type 'foo_agg'. Expected: int_agg, float_agg, dec2_agg, nat_agg, str_agg, bool_agg, arr_agg, date_agg",
        ),
    ];
    for (q, msg) in cases {
        assert_eq!(sql_error(q), msg, "{q}");
        let refq = q.replace("jsonb_stats_compare(", "jsonb_stats_compare_plpgsql(");
        assert_eq!(sql_error(&refq), msg, "reference: {refq}");
    }
}

fn values_text(func: &str, x: &str) -> String {
    sql_text(&format!(
        "SELECT coalesce(string_agg(concat_ws('|', key, stat_type, coalesce(value::text, '~'), coalesce(text_value, '~'), count::text), ';' ORDER BY ord), '')
         FROM {func}({x}) WITH ORDINALITY AS t(key, stat_type, value, text_value, count, ord)"
    ))
}

#[pg_test]
fn test_values_shapes() {
    let s = "stats(jsonb_build_object('n', stat(7), 'f', stat(1.50::float8), 'd', stat(2.5::numeric),
               't', stat('x'::text), 'a', stat(ARRAY['p', 'p', 'q']), 'b', stat(true), 'dt', stat('2025-03-01'::date)))";
    assert_eq!(
        values_text("jsonb_stats_values", s),
        "a|arr|~|p|2;a|arr|~|q|1;b|bool|~|true|1;d|dec2|2.50|~|1;f|float|1.5|~|1;n|int|7|~|1;t|str|~|x|1;dt|date|~|2025-03-01|1"
    );
    // A stats_agg gives the sum and count; values(stats) = values(to_agg(stats)).
    assert_eq!(
        values_text("jsonb_stats_values", "(SELECT jsonb_stats_agg(stats('e', g)) FROM generate_series(1, 4) g)"),
        "e|int|10|~|4"
    );
    assert_eq!(
        values_text("jsonb_stats_values", &format!("jsonb_stats_to_agg({s})")),
        values_text("jsonb_stats_values", s)
    );
    // Empty stock from a delta: count 0, sum at its scale.
    assert_eq!(
        values_text(
            "jsonb_stats_values",
            r#"'{"type": "stats_agg", "min_max": false, "x": {"type": "dec2_agg", "count": 0, "sum": 0.00, "sum_sq": 0.0000}}'"#
        ),
        "x|dec2|0.00|~|0"
    );
    // Typed numeric: the median works directly.
    assert_eq!(
        sql_text(
            "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v.value)::text
             FROM generate_series(1, 5) g, jsonb_stats_values(stats('e', g * 10)) v"
        ),
        "30"
    );
}

#[pg_test]
fn test_values_reference_parity() {
    load_plpgsql_reference();
    load_delta_fixture();
    load_exact_fixture();
    for (src, x) in [
        ("delta_units", "stats"),
        ("delta_units", "jsonb_stats_to_agg(stats)"),
        ("exact_fixture", "stats"),
        ("(SELECT jsonb_stats_agg(stats) AS agg FROM exact_fixture) t", "agg"),
        (
            "(SELECT jsonb_stats_delta_to_agg(jsonb_stats_delta_agg(d)) AS agg FROM delta_events) t",
            "agg",
        ),
    ] {
        let q = |f: &str| {
            format!(
                "SELECT coalesce(string_agg(r, '#'), '') FROM (SELECT (SELECT string_agg(concat_ws('|', key, stat_type, coalesce(value::text, '~'), coalesce(text_value, '~'), count::text), ';' ORDER BY ord)
                   FROM {f}({x}) WITH ORDINALITY AS v(key, stat_type, value, text_value, count, ord)) AS r FROM {src}) s"
            )
        };
        assert_eq!(
            sql_text(&q("jsonb_stats_values")),
            sql_text(&q("jsonb_stats_values_plpgsql")),
            "{src} {x}"
        );
    }
}

#[pg_test]
fn test_values_fails_fast() {
    load_plpgsql_reference();
    let cases = [
        (
            "SELECT * FROM jsonb_stats_values(jsonb_stats_to_delta(stats('e', 1)))",
            "jsonb_stats: jsonb_stats_values cannot read a stats_delta; read it with jsonb_stats_delta_to_agg first",
        ),
        (
            r#"SELECT * FROM jsonb_stats_values('{"type": "nope"}')"#,
            "jsonb_stats: expected stats or stats_agg, got type \"nope\"",
        ),
        (
            r#"SELECT * FROM jsonb_stats_values('{"type": "stats_agg", "e": {"type": "str_agg", "counts": {"x": -1}}}')"#,
            "jsonb_stats: str_agg for key 'e' has an invalid count for 'x'",
        ),
        (
            r#"SELECT * FROM jsonb_stats_values('{"type": "stats", "e": {"type": "int", "value": 1.5}}')"#,
            "jsonb_stats: int value must be an integer, got 1.5",
        ),
        (
            "SELECT * FROM jsonb_stats_values('[1]')",
            "jsonb_stats: jsonb_stats_values input must be an object",
        ),
    ];
    for (q, msg) in cases {
        assert_eq!(sql_error(q), msg, "{q}");
        let refq = q.replace("jsonb_stats_values(", "jsonb_stats_values_plpgsql(");
        assert_eq!(sql_error(&refq), msg, "reference: {refq}");
    }
}
