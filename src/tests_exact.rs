// Exactness and determinism tests for numeric aggregates (jsonb_stats 3.0).
//
// Included into `mod tests` in lib.rs (pgrx runs pg_tests from the `tests`
// schema). Expected values come from dev/exact_oracle.py, an independent
// Python rational-arithmetic implementation, via dev/fixtures/exact_fixture.sql.
// Regenerate with: python3 dev/exact_oracle.py > dev/fixtures/exact_fixture.sql
//
// Every comparison is on `::text`, so number scale (5000 vs 5000.00) and key
// set are part of the contract, not just numeric equality.

fn load_exact_fixture() {
    Spi::run(include_str!("../dev/fixtures/exact_fixture.sql"))
        .expect("Failed to load exact fixture");
}

fn sql_text(query: &str) -> String {
    Spi::get_one::<String>(query)
        .unwrap_or_else(|e| panic!("query failed: {e}\n{query}"))
        .unwrap_or_else(|| panic!("query returned NULL\n{query}"))
}

/// Run `query`, return the error message it raised (or "<no error>").
/// Lets us assert errors raised from inside SQL-level aggregate paths.
fn sql_error(query: &str) -> String {
    Spi::run(
        "CREATE OR REPLACE FUNCTION pg_temp.capture_error(q text) RETURNS text
     LANGUAGE plpgsql AS $capture_error$
     BEGIN
         EXECUTE q;
         RETURN '<no error>';
     EXCEPTION WHEN OTHERS THEN
         RETURN SQLERRM;
     END
     $capture_error$",
    )
    .unwrap();
    let quoted = query.replace('\'', "''");
    sql_text(&format!("SELECT pg_temp.capture_error('{quoted}')"))
}

fn expected_fixture_text() -> String {
    sql_text("SELECT agg::text FROM exact_fixture_expected")
}

// ── Order independence: same rows, any order, identical bytes ──

#[pg_test]
fn test_exact_agg_order_independent() {
    load_exact_fixture();
    let expected = expected_fixture_text();
    for order in ["i", "i DESC", "md5(i::text)", "md5(i::text) DESC"] {
        let got = sql_text(&format!(
            "SELECT jsonb_stats_agg(stats ORDER BY {order})::text FROM exact_fixture"
        ));
        assert_eq!(got, expected, "jsonb_stats_agg ORDER BY {order}");
    }
}

// ── Grouping / merge-order independence ──

#[pg_test]
fn test_exact_merge_agg_grouping_independent() {
    load_exact_fixture();
    let expected = expected_fixture_text();
    for grouping in ["i % 7", "i % 3", "i", "1", "md5(i::text) < '8'"] {
        for order in ["grp", "grp DESC"] {
            let got = sql_text(&format!(
                "WITH g AS (
                 SELECT ({grouping})::text AS grp, jsonb_stats_agg(stats ORDER BY i) AS a
                 FROM exact_fixture GROUP BY 1
             )
             SELECT jsonb_stats_merge_agg(a ORDER BY {order})::text FROM g"
            ));
            assert_eq!(
                got, expected,
                "merge_agg GROUP BY {grouping} ORDER BY {order}"
            );
        }
    }
}

#[pg_test]
fn test_exact_two_level_merge() {
    // stats -> facet aggs -> regional merges -> global merge
    load_exact_fixture();
    let expected = expected_fixture_text();
    let got = sql_text(
        "WITH facet AS (
         SELECT i % 11 AS f, jsonb_stats_agg(stats) AS a FROM exact_fixture GROUP BY 1
     ), region AS (
         SELECT f % 3 AS r, jsonb_stats_merge_agg(a ORDER BY f DESC) AS a FROM facet GROUP BY 1
     )
     SELECT jsonb_stats_merge_agg(a ORDER BY r)::text FROM region",
    );
    assert_eq!(got, expected);
}

#[pg_test]
fn test_exact_scalar_merge_and_to_agg() {
    // README incremental pattern: jsonb_stats_merge(existing, jsonb_stats_to_agg(row))
    load_exact_fixture();
    let expected = expected_fixture_text();
    let halves = "WITH odd AS (SELECT jsonb_stats_agg(stats) AS a FROM exact_fixture WHERE i % 2 = 1),
                  even AS (SELECT jsonb_stats_agg(stats) AS a FROM exact_fixture WHERE i % 2 = 0)";
    let ab = sql_text(&format!(
        "{halves} SELECT jsonb_stats_merge(odd.a, even.a)::text FROM odd, even"
    ));
    assert_eq!(ab, expected, "jsonb_stats_merge(odd, even)");
    let ba = sql_text(&format!(
        "{halves} SELECT jsonb_stats_merge(even.a, odd.a)::text FROM odd, even"
    ));
    assert_eq!(ba, expected, "jsonb_stats_merge(even, odd)");

    let per_row = sql_text(
    "SELECT jsonb_stats_merge_agg(jsonb_stats_to_agg(stats) ORDER BY i DESC)::text FROM exact_fixture",
);
    assert_eq!(
        per_row, expected,
        "merge_agg over jsonb_stats_to_agg per row"
    );

    // Fold rows one at a time with the scalar merge, as an incremental UPDATE would.
    let folded = sql_text(
        "WITH RECURSIVE fold(i, a) AS (
         SELECT 1, jsonb_stats_to_agg(stats) FROM exact_fixture WHERE i = 1
         UNION ALL
         SELECT f.i + 1, jsonb_stats_merge(f.a, jsonb_stats_to_agg(e.stats))
         FROM fold f JOIN exact_fixture e ON e.i = f.i + 1
     )
     SELECT a::text FROM fold ORDER BY i DESC LIMIT 1",
    );
    assert_eq!(folded, expected, "scalar jsonb_stats_merge fold");
}

#[pg_test]
fn test_exact_low_level_accum_final() {
    load_exact_fixture();
    let expected = expected_fixture_text();
    let got = sql_text(
        "WITH RECURSIVE acc(i, s) AS (
         SELECT 0, '{}'::jsonb
         UNION ALL
         SELECT a.i + 1, jsonb_stats_accum(a.s, e.stats)
         FROM acc a JOIN exact_fixture e ON e.i = a.i + 1
     )
     SELECT jsonb_stats_final(s)::text FROM acc ORDER BY i DESC LIMIT 1",
    );
    assert_eq!(got, expected);
}

// ── Parallel plans: serial and parallel give identical bytes ──

#[pg_test]
fn test_exact_parallel_matches_serial() {
    load_exact_fixture();
    Spi::run(
        "CREATE TEMP TABLE exact_x300 AS
     SELECT e.i * 1000 + r AS i, e.stats FROM exact_fixture e, generate_series(1, 300) AS r;
     ANALYZE exact_x300",
    )
    .unwrap();
    let expected = sql_text("SELECT agg::text FROM exact_fixture_x300_expected");

    Spi::run("SET max_parallel_workers_per_gather = 0").unwrap();
    let serial = sql_text("SELECT jsonb_stats_agg(stats)::text FROM exact_x300");
    assert_eq!(serial, expected, "serial");

    Spi::run(
        "SET parallel_setup_cost = 0;
     SET parallel_tuple_cost = 0;
     SET min_parallel_table_scan_size = 0;
     SET max_parallel_workers_per_gather = 4;
     SET debug_parallel_query = regress",
    )
    .unwrap();
    let parallel = sql_text("SELECT jsonb_stats_agg(stats)::text FROM exact_x300");
    assert_eq!(parallel, expected, "parallel jsonb_stats_agg");

    let parallel_merge = sql_text(
        "SELECT jsonb_stats_merge_agg(a)::text FROM (
         SELECT i % 97 AS g, jsonb_stats_agg(stats) AS a FROM exact_x300 GROUP BY 1
     ) t",
    );
    assert_eq!(parallel_merge, expected, "parallel jsonb_stats_merge_agg");
}

// ── Exact rounding and NULL rules (oracle edge cases, both directions) ──

#[pg_test]
fn test_exact_edge_cases() {
    load_exact_fixture();
    for direction in ["ASC", "DESC"] {
        let mismatches = sql_text(&format!(
        "SELECT coalesce(string_agg(name || E':\\n  got  ' || coalesce(got::text, 'NULL')
                                       || E'\\n  want ' || expected::text, E'\\n' ORDER BY name), '')
         FROM (
             SELECT e.name, e.expected,
                    (SELECT jsonb_stats_agg(
                                jsonb_build_object('v', jsonb_build_object('type', e.kind, 'value', u.v::numeric))
                                ORDER BY u.o {direction}) -> 'v'
                     FROM unnest(e.vals) WITH ORDINALITY AS u(v, o)) AS got
             FROM exact_edge_cases e
         ) q
         WHERE got::text IS DISTINCT FROM expected::text"
    ));
        assert_eq!(mismatches, "", "edge cases ({direction})");
    }
}

#[pg_test]
fn test_exact_float_is_decimal_exact() {
    // 0.1 + 0.2 must be 0.3, not 0.30000000000000004
    let got = sql_text(
        "SELECT (jsonb_stats_agg(stats(jsonb_build_object('f', stat(v))))->'f'->>'sum')
     FROM (VALUES (0.1::float8), (0.2::float8)) AS t(v)",
    );
    assert_eq!(got, "0.3");
}

// ── Schema: sum_sq field, state shape, legacy input ──

#[pg_test]
fn test_exact_accum_state_shape() {
    let got = sql_text(
        "SELECT jsonb_stats_accum(jsonb_stats_accum('{}'::jsonb,
                '{\"x\": {\"type\": \"int\", \"value\": 3}}'),
                '{\"x\": {\"type\": \"int\", \"value\": 4}}')::text",
    );
    assert_eq!(
        got,
        r#"{"x": {"max": 4, "min": 3, "sum": 7, "type": "int_agg", "count": 2, "sum_sq": 25}}"#
    );
}

#[pg_test]
fn test_exact_dec2_state_scales() {
    let got = sql_text(
        "SELECT jsonb_stats_accum(jsonb_stats_accum('{}'::jsonb,
                '{\"x\": {\"type\": \"dec2\", \"value\": 1}}'),
                '{\"x\": {\"type\": \"dec2\", \"value\": 2.5}}')::text",
    );
    assert_eq!(
        got,
        r#"{"x": {"max": 2.50, "min": 1.00, "sum": 3.50, "type": "dec2_agg", "count": 2, "sum_sq": 7.2500}}"#
    );
}

#[pg_test]
fn test_exact_scalar_merge_emits_full_summary() {
    let got = sql_text(
        "SELECT jsonb_stats_merge(
         jsonb_stats_to_agg('{\"x\": {\"type\": \"int\", \"value\": 1}}'),
         jsonb_stats_to_agg('{\"x\": {\"type\": \"int\", \"value\": 2}}'))::text",
    );
    assert_eq!(
        got,
        r#"{"x": {"max": 2, "min": 1, "sum": 3, "mean": 1.50, "type": "int_agg", "count": 2, "stddev": 0.71, "sum_sq": 5, "variance": 0.50, "sum_sq_diff": 0.50, "coefficient_of_variation_pct": 47.14}, "type": "stats_agg"}"#
    );
}

#[pg_test]
fn test_exact_derived_input_fields_ignored() {
    // A summary whose derived fields are stale/wrong must be recomputed from state.
    let got = sql_text(
    "SELECT jsonb_stats_final('{\"x\": {\"type\": \"int_agg\", \"count\": 2, \"sum\": 3, \"sum_sq\": 5,
             \"min\": 1, \"max\": 2, \"mean\": 999, \"sum_sq_diff\": 999, \"variance\": 999}}')::text",
);
    assert_eq!(
        got,
        r#"{"x": {"max": 2, "min": 1, "sum": 3, "mean": 1.50, "type": "int_agg", "count": 2, "stddev": 0.71, "sum_sq": 5, "variance": 0.50, "sum_sq_diff": 0.50, "coefficient_of_variation_pct": 47.14}, "type": "stats_agg"}"#
    );
}

// ── Legacy (2.x) summaries: upgraded on read ──
//
// 2.x summaries have no sum_sq. It is rebuilt as well as possible from the
// stored count, sum and sum_sq_diff:
//   sum_sq = round(sum_sq_diff + sum^2 / count) at the kind's sum_sq scale,
//   clamped up to ceil(sum^2 / count) so the state stays consistent.
// For int/nat the true sum_sq is integral, so it is recovered exactly whenever
// the stored error is < 0.5. The next recompute from source data is exact.

const LEGACY_INT_AGG: &str = r#"{"x": {"type": "int_agg", "count": 2, "sum": 200, "min": 50, "max": 150, "mean": 100.00, "sum_sq_diff": 5000.00}}"#;

/// jsonb_stats_accum(summary, '{}') returns the parsed exact state.
fn legacy_state(summary: &str) -> String {
    sql_text(&format!(
        "SELECT jsonb_stats_accum('{summary}'::jsonb, '{{}}'::jsonb)::text"
    ))
}

#[pg_test]
fn test_exact_legacy_sum_sq_rebuilt() {
    let cases = [
        // exact recovery: 50, 150
        (
            LEGACY_INT_AGG,
            r#"{"x": {"max": 150, "min": 50, "sum": 200, "type": "int_agg", "count": 2, "sum_sq": 25000}}"#,
        ),
        // exact recovery despite the rounded sum_sq_diff: 1, 2, 2 -> sum_sq 9
        (
            r#"{"x": {"type": "int_agg", "count": 3, "sum": 5, "min": 1, "max": 2, "mean": 1.67, "sum_sq_diff": 0.67}}"#,
            r#"{"x": {"max": 2, "min": 1, "sum": 5, "type": "int_agg", "count": 3, "sum_sq": 9}}"#,
        ),
        // nat behaves like int
        (
            r#"{"x": {"type": "nat_agg", "count": 3, "sum": 15, "min": 0, "max": 10, "mean": 5.00, "sum_sq_diff": 50.00}}"#,
            r#"{"x": {"max": 10, "min": 0, "sum": 15, "type": "nat_agg", "count": 3, "sum_sq": 125}}"#,
        ),
        // dec2: best effort (true sum_sq is 7.2500; 2.x stored sum_sq_diff rounded to 1.13)
        (
            r#"{"x": {"type": "dec2_agg", "count": 2, "sum": 3.5, "min": 1, "max": 2.5, "mean": 1.75, "sum_sq_diff": 1.13}}"#,
            r#"{"x": {"max": 2.50, "min": 1.00, "sum": 3.50, "type": "dec2_agg", "count": 2, "sum_sq": 7.2550}}"#,
        ),
        // float: f64 noise in the old sum is kept as is
        (
            r#"{"x": {"type": "float_agg", "count": 2, "sum": 0.30000000000000004, "min": 0.1, "max": 0.2, "mean": 0.15, "sum_sq_diff": 0.01}}"#,
            r#"{"x": {"max": 0.2, "min": 0.1, "sum": 0.30000000000000004, "type": "float_agg", "count": 2, "sum_sq": 0.0550000000000000120000000000000008}}"#,
        ),
        // clamp: a slightly negative stored sum_sq_diff cannot make the state inconsistent
        (
            r#"{"x": {"type": "float_agg", "count": 2, "sum": 2, "min": 1, "max": 1, "mean": 1, "sum_sq_diff": -0.0000001}}"#,
            r#"{"x": {"max": 1, "min": 1, "sum": 2, "type": "float_agg", "count": 2, "sum_sq": 2}}"#,
        ),
        // single value: sum_sq = sum^2 exactly
        (
            r#"{"x": {"type": "int_agg", "count": 1, "sum": 7, "min": 7, "max": 7, "mean": 7.00, "sum_sq_diff": 0.00}}"#,
            r#"{"x": {"max": 7, "min": 7, "sum": 7, "type": "int_agg", "count": 1, "sum_sq": 49}}"#,
        ),
    ];
    for (legacy, want) in cases {
        assert_eq!(legacy_state(legacy), want, "{legacy}");
    }
}

#[pg_test]
fn test_exact_legacy_accepted_everywhere() {
    let fresh =
        r#"{"x": {"type": "int_agg", "count": 1, "sum": 2500, "sum_sq": 6250000, "min": 2500, "max": 2500}}"#;
    // 50, 150 (legacy) + 2500 (fresh) == the exact 3-row summary
    let want = r#"{"x": {"max": 2500, "min": 50, "sum": 2700, "mean": 900.00, "type": "int_agg", "count": 3, "stddev": 1386.54, "sum_sq": 6275000, "variance": 1922500.00, "sum_sq_diff": 3845000.00, "coefficient_of_variation_pct": 154.06}, "type": "stats_agg"}"#;
    for query in [
        format!("SELECT jsonb_stats_merge('{LEGACY_INT_AGG}', '{fresh}')::text"),
        format!("SELECT jsonb_stats_merge('{fresh}', '{LEGACY_INT_AGG}')::text"),
        format!("SELECT jsonb_stats_merge_agg(a)::text FROM (VALUES ('{fresh}'::jsonb), ('{LEGACY_INT_AGG}'::jsonb)) t(a)"),
        format!(
            "SELECT jsonb_stats_final(jsonb_stats_accum('{LEGACY_INT_AGG}', '{{\"x\": {{\"type\": \"int\", \"value\": 2500}}}}'))::text"
        ),
    ] {
        assert_eq!(sql_text(&query), want, "{query}");
    }
    // Once rewritten, the summary carries sum_sq and is stable.
    let once = sql_text(&format!("SELECT jsonb_stats_final('{LEGACY_INT_AGG}')::text"));
    let twice = sql_text(&format!(
        "SELECT jsonb_stats_final(jsonb_stats_final('{LEGACY_INT_AGG}'))::text"
    ));
    assert_eq!(once, twice);
    assert!(once.contains(r#""sum_sq": 25000"#), "{once}");
}

#[pg_test]
fn test_exact_legacy_without_sum_sq_diff_rejected() {
    let bad = r#"{"x": {"type": "int_agg", "count": 2, "sum": 200, "min": 50, "max": 150}}"#;
    assert_eq!(
        sql_error(&format!("SELECT jsonb_stats_final('{bad}')")),
        "jsonb_stats: int_agg for key 'x' has neither 'sum_sq' nor 'sum_sq_diff'"
    );
}

#[pg_test]
fn test_exact_legacy_reference_parity() {
    load_plpgsql_reference();
    let fresh =
        r#"{"x": {"type": "int_agg", "count": 1, "sum": 2500, "sum_sq": 6250000, "min": 2500, "max": 2500}}"#;
    let legacies = [
        LEGACY_INT_AGG,
        r#"{"x": {"type": "int_agg", "count": 73, "sum": 3554078835, "min": 0, "max": 90000000, "mean": 48686011.44, "sum_sq_diff": 72475317361765168.00}}"#,
        r#"{"x": {"type": "dec2_agg", "count": 2, "sum": 3.5, "min": 1, "max": 2.5, "mean": 1.75, "sum_sq_diff": 1.13}}"#,
        r#"{"x": {"type": "float_agg", "count": 2, "sum": 0.30000000000000004, "min": 0.1, "max": 0.2, "mean": 0.15, "sum_sq_diff": 0.01}}"#,
        r#"{"x": {"type": "float_agg", "count": 2, "sum": 2, "min": 1, "max": 1, "mean": 1, "sum_sq_diff": -0.0000001}}"#,
    ];
    for legacy in legacies {
        for (rust, reference) in [
            (
                format!("jsonb_stats_final('{legacy}')"),
                format!("jsonb_stats_final_plpgsql('{legacy}')"),
            ),
            (
                format!("jsonb_stats_merge('{legacy}', '{legacy}')"),
                format!("jsonb_stats_merge_plpgsql('{legacy}', '{legacy}')"),
            ),
        ] {
            assert_eq!(
                sql_text(&format!("SELECT ({rust})::text")),
                sql_text(&format!("SELECT ({reference})::text")),
                "{rust}"
            );
        }
    }
    let r = sql_text(&format!(
        "SELECT jsonb_stats_merge('{LEGACY_INT_AGG}', '{fresh}')::text"
    ));
    let p = sql_text(&format!(
        "SELECT jsonb_stats_merge_plpgsql('{LEGACY_INT_AGG}', '{fresh}')::text"
    ));
    assert_eq!(r, p);
}

// ── Fail-fast validation of numeric values (both accum paths) ──

#[pg_test]
fn test_exact_value_validation() {
    let cases = [
        (
            r#"{"type": "int", "value": 1.5}"#,
            "jsonb_stats: int value must be an integer, got 1.5",
        ),
        (
            r#"{"type": "nat", "value": 1.5}"#,
            "jsonb_stats: nat value must be an integer, got 1.5",
        ),
        (
            r#"{"type": "nat", "value": -1}"#,
            "jsonb_stats: nat value must be >= 0, got -1",
        ),
        (
            r#"{"type": "dec2", "value": 1.234}"#,
            "jsonb_stats: dec2 value must have at most 2 decimal places, got 1.234",
        ),
        (
            r#"{"type": "int", "value": "12"}"#,
            "jsonb_stats: stat of type 'int' has missing or non-numeric 'value'",
        ),
        (
            r#"{"type": "float", "value": null}"#,
            "jsonb_stats: stat of type 'float' has missing or non-numeric 'value'",
        ),
        (
            r#"{"type": "dec2"}"#,
            "jsonb_stats: stat of type 'dec2' has missing or non-numeric 'value'",
        ),
    ];
    for (stat, want) in cases {
        let stats = format!(r#"{{"x": {stat}}}"#);
        // init path, scalar
        assert_eq!(
            sql_error(&format!("SELECT jsonb_stats_accum('{{}}', '{stats}')")),
            want,
            "accum init {stat}"
        );
        // init path, aggregate (internal state)
        assert_eq!(
            sql_error(&format!("SELECT jsonb_stats_agg('{stats}'::jsonb)")),
            want,
            "agg init {stat}"
        );
        // update path, aggregate: first a valid value of the same type
        let kind = if stat.contains("\"nat\"") {
            "nat"
        } else if stat.contains("\"dec2\"") {
            "dec2"
        } else if stat.contains("\"float\"") {
            "float"
        } else {
            "int"
        };
        let ok = format!(r#"{{"x": {{"type": "{kind}", "value": 1}}}}"#);
        assert_eq!(
        sql_error(&format!("SELECT jsonb_stats_agg(s ORDER BY o) FROM (VALUES (1, '{ok}'::jsonb), (2, '{stats}'::jsonb)) t(o, s)")),
        want,
        "agg update {stat}"
    );
    }
}

#[pg_test]
fn test_exact_summary_validation() {
    let cases = [
        (
            r#"{"x": {"type": "int_agg", "count": 1.5, "sum": 1, "sum_sq": 1, "min": 1, "max": 1}}"#,
            "jsonb_stats: int_agg for key 'x' has invalid 'count' (must be an integer >= 1)",
        ),
        (
            r#"{"x": {"type": "int_agg", "count": 0, "sum": 1, "sum_sq": 1, "min": 1, "max": 1}}"#,
            "jsonb_stats: int_agg for key 'x' has invalid 'count' (must be an integer >= 1)",
        ),
        (
            r#"{"x": {"type": "int_agg", "count": 2, "sum": 10, "sum_sq": 1, "min": 1, "max": 9}}"#,
            "jsonb_stats: int_agg for key 'x' is inconsistent (count * sum_sq < sum^2)",
        ),
        (
            r#"{"x": {"type": "float_agg", "count": 1, "sum": "1", "sum_sq": 1, "min": 1, "max": 1}}"#,
            "jsonb_stats: float_agg for key 'x' has missing or non-numeric 'sum'",
        ),
    ];
    for (agg, want) in cases {
        assert_eq!(
            sql_error(&format!("SELECT jsonb_stats_final('{agg}')")),
            want,
            "final {agg}"
        );
        assert_eq!(
            sql_error(&format!("SELECT jsonb_stats_merge_agg('{agg}'::jsonb)")),
            want,
            "merge_agg {agg}"
        );
    }
}

// ── stat() type mapping ──

#[pg_test]
fn test_exact_stat_type_mapping() {
    let cases = [
        ("5::smallint", r#"{"type": "int", "value": 5}"#),
        ("5::integer", r#"{"type": "int", "value": 5}"#),
        (
            "9223372036854775807::bigint",
            r#"{"type": "int", "value": 9223372036854775807}"#,
        ),
        ("0.1::real", r#"{"type": "float", "value": 0.1}"#),
        ("0.1::float8", r#"{"type": "float", "value": 0.1}"#),
        (
            "1e308::float8",
            r#"{"type": "float", "value": 100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000}"#,
        ),
        ("5::numeric", r#"{"type": "dec2", "value": 5.00}"#),
        ("1.234::numeric", r#"{"type": "dec2", "value": 1.23}"#),
        ("1.235::numeric", r#"{"type": "dec2", "value": 1.24}"#),
        ("(-1.235)::numeric", r#"{"type": "dec2", "value": -1.24}"#),
        ("12.5::numeric(10,1)", r#"{"type": "dec2", "value": 12.50}"#),
    ];
    for (expr, want) in cases {
        assert_eq!(
            sql_text(&format!("SELECT stat({expr})::text")),
            want,
            "stat({expr})"
        );
    }
}

#[pg_test]
fn test_exact_stat_rejects_non_finite() {
    for expr in [
        "'NaN'::float8",
        "'Infinity'::float8",
        "'-Infinity'::real",
        "'NaN'::numeric",
        "'Infinity'::numeric",
    ] {
        let err = sql_error(&format!("SELECT stat({expr})"));
        assert!(
            err.starts_with("jsonb_stats: stat() cannot represent non-finite value"),
            "stat({expr}) gave: {err}"
        );
    }
}

#[pg_test]
fn test_exact_stat_arrays_and_fallback() {
    // Arrays map to arr (README type map); other types fall back to their text output.
    let cases = [
        (
            "ARRAY['a','b']::text[]",
            r#"{"type": "arr", "value": "{a,b}"}"#,
        ),
        ("ARRAY[1,2,3]", r#"{"type": "arr", "value": "{1,2,3}"}"#),
        (
            "'2024-01-15 10:30:00'::timestamp",
            r#"{"type": "str", "value": "2024-01-15 10:30:00"}"#,
        ),
        (
            "'f47ac10b-58cc-4372-a567-0e02b2c3d479'::uuid",
            r#"{"type": "str", "value": "f47ac10b-58cc-4372-a567-0e02b2c3d479"}"#,
        ),
    ];
    for (expr, want) in cases {
        assert_eq!(
            sql_text(&format!("SELECT stat({expr})::text")),
            want,
            "stat({expr})"
        );
    }
    let got = sql_text(
        "SELECT (jsonb_stats_agg(jsonb_build_object('tags', stat(t)))->'tags')::text
     FROM (VALUES (ARRAY['a','b']), (ARRAY['b'])) AS v(t)",
    );
    assert_eq!(
        got,
        r#"{"type": "arr_agg", "count": 2, "counts": {"a": 1, "b": 2}}"#
    );
}

#[pg_test]
fn test_exact_accum_rejects_type_change_for_key() {
    let int_then_float = r#"(VALUES (1, '{"x": {"type": "int", "value": 1}}'::jsonb), (2, '{"x": {"type": "float", "value": 1.5}}'::jsonb)) t(o, s)"#;
    let want = "jsonb_stats: type mismatch for key 'x': existing int_agg vs incoming float";
    assert_eq!(
        sql_error(&format!(
            "SELECT jsonb_stats_agg(s ORDER BY o) FROM {int_then_float}"
        )),
        want,
        "agg"
    );
    assert_eq!(
        sql_error(
            r#"SELECT jsonb_stats_accum(jsonb_stats_accum('{}', '{"x": {"type": "int", "value": 1}}'), '{"x": {"type": "float", "value": 1.5}}')"#
        ),
        want,
        "accum"
    );
    assert_eq!(
        sql_error(
            r#"SELECT jsonb_stats_accum(jsonb_stats_accum('{}', '{"x": {"type": "int", "value": 1}}'), '{"x": {"type": "str", "value": "a"}}')"#
        ),
        "jsonb_stats: type mismatch for key 'x': existing int_agg vs incoming str",
        "accum int then str"
    );
}

// ── Reference parity: Rust and PL/pgSQL produce identical bytes ──

#[pg_test]
fn test_exact_reference_parity() {
    load_plpgsql_reference();
    load_exact_fixture();
    let expected = expected_fixture_text();

    let reference_agg = sql_text(
        "SELECT jsonb_stats_agg_plpgsql(stats ORDER BY i DESC)::text FROM exact_fixture",
    );
    assert_eq!(reference_agg, expected, "reference agg vs oracle");

    let reference_merge = sql_text(
    "SELECT jsonb_stats_merge_agg_plpgsql(a)::text FROM (
         SELECT i % 7 AS g, jsonb_stats_agg_plpgsql(stats) AS a FROM exact_fixture GROUP BY 1) t",
);
    assert_eq!(reference_merge, expected, "reference merge_agg vs oracle");

    // Low-level functions, step by step
    let pairs = [
    ("jsonb_stats_accum('{}'::jsonb, stats)", "jsonb_stats_accum_plpgsql('{}'::jsonb, stats)"),
    (
        "jsonb_stats_final(jsonb_stats_accum(jsonb_stats_accum('{}'::jsonb, stats), stats))",
        "jsonb_stats_final_plpgsql(jsonb_stats_accum_plpgsql(jsonb_stats_accum_plpgsql('{}'::jsonb, stats), stats))",
    ),
    (
        "jsonb_stats_merge(jsonb_stats_to_agg(stats), jsonb_stats_to_agg(stats))",
        "jsonb_stats_merge_plpgsql(jsonb_stats_agg_plpgsql_one(stats), jsonb_stats_agg_plpgsql_one(stats))",
    ),
];
    Spi::run(
    "CREATE OR REPLACE FUNCTION pg_temp.jsonb_stats_agg_plpgsql_one(s jsonb) RETURNS jsonb
     LANGUAGE sql AS $one$ SELECT jsonb_stats_final_plpgsql(jsonb_stats_accum_plpgsql('{}'::jsonb, s)) $one$",
)
.unwrap();
    for (rust, reference) in pairs {
        let reference = reference.replace(
            "jsonb_stats_agg_plpgsql_one",
            "pg_temp.jsonb_stats_agg_plpgsql_one",
        );
        let diffs = sql_text(&format!(
        "SELECT count(*)::text FROM exact_fixture WHERE ({rust})::text IS DISTINCT FROM ({reference})::text"
    ));
        assert_eq!(diffs, "0", "{rust} vs {reference}");
    }

    // Reference on the edge cases
    let mismatches = sql_text(
    "SELECT coalesce(string_agg(name, ', ' ORDER BY name), '') FROM (
         SELECT e.name, e.expected,
                (SELECT jsonb_stats_agg_plpgsql(
                            jsonb_build_object('v', jsonb_build_object('type', e.kind, 'value', u.v::numeric))
                            ORDER BY u.o) -> 'v'
                 FROM unnest(e.vals) WITH ORDINALITY AS u(v, o)) AS got
         FROM exact_edge_cases e) q
     WHERE got::text IS DISTINCT FROM expected::text",
);
    assert_eq!(mismatches, "", "reference edge cases");
}
