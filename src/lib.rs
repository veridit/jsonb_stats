use pgrx::prelude::*;

pg_module_magic!();

mod accum;
mod final_fn;
mod helpers;
mod merge;
mod stat;
mod state;

// Re-export all pg_extern functions so pgrx can discover them
pub use accum::{jsonb_stats_accum, jsonb_stats_accum_sfunc};
pub use final_fn::{jsonb_stats_final, jsonb_stats_final_internal};
pub use merge::{jsonb_stats_merge, jsonb_stats_merge_sfunc};
pub use stat::{jsonb_stats_sfunc, stat, stats_from_jsonb};

// Aggregate definitions using extension_sql!
// These must come after all function definitions (enforced by `requires`).
extension_sql!(
    r#"
-- stats -> stats_agg (Internal state avoids serde_json round-trip per row)
CREATE AGGREGATE jsonb_stats_agg(jsonb) (
    sfunc = jsonb_stats_accum_sfunc,
    stype = internal,
    finalfunc = jsonb_stats_final_internal
);

-- stats_agg -> stats_agg (Internal state avoids serde_json round-trip per row)
CREATE AGGREGATE jsonb_stats_merge_agg(jsonb) (
    sfunc = jsonb_stats_merge_sfunc,
    stype = internal,
    finalfunc = jsonb_stats_final_internal
);

-- (code, stat) -> stats (convenience aggregate)
CREATE AGGREGATE jsonb_stats_agg(text, jsonb) (
    sfunc = jsonb_stats_sfunc,
    stype = jsonb,
    initcond = '{}'
);

-- Overloaded stats(code, val) helper — wraps stat() + stats()
CREATE FUNCTION stats(code text, val anyelement)
RETURNS jsonb
AS $$ SELECT stats(jsonb_build_object(code, stat(val))) $$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
"#,
    name = "aggregates",
    requires = [
        jsonb_stats_accum,
        jsonb_stats_accum_sfunc,
        jsonb_stats_merge,
        jsonb_stats_merge_sfunc,
        jsonb_stats_final,
        jsonb_stats_final_internal,
        jsonb_stats_sfunc,
        stats_from_jsonb,
        stat
    ]
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    fn load_plpgsql_reference() {
        Spi::run(include_str!("../dev/reference_plpgsql.sql"))
            .expect("Failed to load PL/pgSQL reference");
    }

    #[pg_test]
    fn test_extension_loads() {
        // Verify the extension loaded successfully
        let result = Spi::get_one::<bool>("SELECT true");
        assert_eq!(result, Ok(Some(true)));
    }

    // ── stat() tests ──

    #[pg_test]
    fn test_stat_int() {
        let result = Spi::get_one::<pgrx::JsonB>("SELECT stat(150)");
        let val = result.unwrap().unwrap().0;
        assert_eq!(val["type"], "int");
        assert_eq!(val["value"], 150);
    }

    #[pg_test]
    fn test_stat_text() {
        let result = Spi::get_one::<pgrx::JsonB>("SELECT stat('tech'::text)");
        let val = result.unwrap().unwrap().0;
        assert_eq!(val["type"], "str");
        assert_eq!(val["value"], "tech");
    }

    #[pg_test]
    fn test_stat_bool() {
        let result = Spi::get_one::<pgrx::JsonB>("SELECT stat(true)");
        let val = result.unwrap().unwrap().0;
        assert_eq!(val["type"], "bool");
        assert_eq!(val["value"], true);
    }

    // ── stats() tests ──

    #[pg_test]
    fn test_stats_adds_type() {
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT stats('{\"foo\": {\"type\": \"int\", \"value\": 1}}'::jsonb)",
        );
        let val = result.unwrap().unwrap().0;
        assert_eq!(val["type"], "stats");
        assert_eq!(val["foo"]["type"], "int");
    }

    // ── jsonb_stats_sfunc tests ──

    #[pg_test]
    fn test_sfunc_builds_stats() {
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT jsonb_stats_sfunc(
                jsonb_stats_sfunc('{}'::jsonb, 'a', stat(1)),
                'b', stat('x'::text)
            )",
        );
        let val = result.unwrap().unwrap().0;
        assert_eq!(val["type"], "stats");
        assert_eq!(val["a"]["type"], "int");
        assert_eq!(val["b"]["type"], "str");
    }

    // ── jsonb_stats_agg(text, jsonb) tests ──

    #[pg_test]
    fn test_stats_agg_text_jsonb() {
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT jsonb_stats_agg(code, stat(val))
             FROM (VALUES ('a', 1), ('b', 2)) AS t(code, val)",
        );
        let val = result.unwrap().unwrap().0;
        assert_eq!(val["type"], "stats");
        assert_eq!(val["a"]["value"], 1);
        assert_eq!(val["b"]["value"], 2);
    }

    // ── jsonb_stats_accum tests ──

    #[pg_test]
    fn test_accum_init_int() {
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT jsonb_stats_accum(
                '{}'::jsonb,
                '{\"num\": {\"type\": \"int\", \"value\": 150}}'::jsonb
            )",
        );
        let val = result.unwrap().unwrap().0;
        let num = &val["num"];
        assert_eq!(num["type"], "int_agg");
        assert_eq!(num["count"], 1);
        assert_eq!(num["sum"], 150);
        assert_eq!(num["mean"], 150);
        assert_eq!(num["sum_sq_diff"], 0);
    }

    #[pg_test]
    fn test_accum_init_str() {
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT jsonb_stats_accum(
                '{}'::jsonb,
                '{\"ind\": {\"type\": \"str\", \"value\": \"tech\"}}'::jsonb
            )",
        );
        let val = result.unwrap().unwrap().0;
        let ind = &val["ind"];
        assert_eq!(ind["type"], "str_agg");
        assert_eq!(ind["counts"]["tech"], 1);
    }

    #[pg_test]
    fn test_accum_update_int() {
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT jsonb_stats_accum(
                jsonb_stats_accum(
                    '{}'::jsonb,
                    '{\"num\": {\"type\": \"int\", \"value\": 150}}'::jsonb
                ),
                '{\"num\": {\"type\": \"int\", \"value\": 50}}'::jsonb
            )",
        );
        let val = result.unwrap().unwrap().0;
        let num = &val["num"];
        assert_eq!(num["count"], 2);
        assert_eq!(num["sum"], 200);
        assert_eq!(num["min"], 50);
        assert_eq!(num["max"], 150);
    }

    #[pg_test]
    fn test_accum_matches_plpgsql() {
        load_plpgsql_reference();
        let ok = Spi::get_one::<bool>(
            "SELECT jsonb_stats_accum(
                '{}'::jsonb,
                '{\"num\": {\"type\": \"int\", \"value\": 150}}'::jsonb
            ) = jsonb_stats_accum_plpgsql(
                '{}'::jsonb,
                '{\"num\": {\"type\": \"int\", \"value\": 150}}'::jsonb
            )",
        );
        assert_eq!(ok, Ok(Some(true)));
    }

    #[pg_test]
    fn test_accum_two_values_matches_plpgsql() {
        load_plpgsql_reference();
        let ok = Spi::get_one::<bool>(
            "WITH step1_rust AS (
                SELECT jsonb_stats_accum('{}'::jsonb,
                    '{\"num\": {\"type\": \"int\", \"value\": 150}}'::jsonb) AS s
            ),
            step2_rust AS (
                SELECT jsonb_stats_accum(s,
                    '{\"num\": {\"type\": \"int\", \"value\": 50}}'::jsonb) AS s
                FROM step1_rust
            ),
            step1_plpgsql AS (
                SELECT jsonb_stats_accum_plpgsql('{}'::jsonb,
                    '{\"num\": {\"type\": \"int\", \"value\": 150}}'::jsonb) AS s
            ),
            step2_plpgsql AS (
                SELECT jsonb_stats_accum_plpgsql(s,
                    '{\"num\": {\"type\": \"int\", \"value\": 50}}'::jsonb) AS s
                FROM step1_plpgsql
            )
            SELECT r.s = p.s FROM step2_rust r, step2_plpgsql p",
        );
        assert_eq!(ok, Ok(Some(true)));
    }

    // ── jsonb_stats_merge tests ──

    #[pg_test]
    fn test_merge_int_agg() {
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT jsonb_stats_merge(
                '{\"num\": {\"type\": \"int_agg\", \"count\": 2, \"sum\": 200, \"min\": 50, \"max\": 150, \"mean\": 100, \"sum_sq_diff\": 5000}}'::jsonb,
                '{\"num\": {\"type\": \"int_agg\", \"count\": 1, \"sum\": 2500, \"min\": 2500, \"max\": 2500, \"mean\": 2500, \"sum_sq_diff\": 0}}'::jsonb
            )",
        );
        let val = result.unwrap().unwrap().0;
        let num = &val["num"];
        assert_eq!(num["count"], 3);
        assert_eq!(num["sum"], 2700);
        assert_eq!(num["min"], 50);
        assert_eq!(num["max"], 2500);
    }

    #[pg_test]
    fn test_merge_str_agg() {
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT jsonb_stats_merge(
                '{\"ind\": {\"type\": \"str_agg\", \"counts\": {\"tech\": 2}}}'::jsonb,
                '{\"ind\": {\"type\": \"str_agg\", \"counts\": {\"finance\": 1}}}'::jsonb
            )",
        );
        let val = result.unwrap().unwrap().0;
        assert_eq!(val["ind"]["counts"]["tech"], 2);
        assert_eq!(val["ind"]["counts"]["finance"], 1);
    }

    #[pg_test]
    fn test_merge_adopts_new_keys() {
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT jsonb_stats_merge(
                '{\"a\": {\"type\": \"str_agg\", \"counts\": {\"x\": 1}}}'::jsonb,
                '{\"b\": {\"type\": \"str_agg\", \"counts\": {\"y\": 1}}}'::jsonb
            )",
        );
        let val = result.unwrap().unwrap().0;
        assert_eq!(val["a"]["counts"]["x"], 1);
        assert_eq!(val["b"]["counts"]["y"], 1);
    }

    #[pg_test]
    fn test_merge_matches_plpgsql() {
        load_plpgsql_reference();
        let ok = Spi::get_one::<bool>(
            "SELECT jsonb_stats_merge(
                '{\"num\": {\"type\": \"int_agg\", \"count\": 2, \"sum\": 200, \"min\": 50, \"max\": 150, \"mean\": 100, \"sum_sq_diff\": 5000}}'::jsonb,
                '{\"num\": {\"type\": \"int_agg\", \"count\": 1, \"sum\": 2500, \"min\": 2500, \"max\": 2500, \"mean\": 2500, \"sum_sq_diff\": 0}}'::jsonb
            ) = jsonb_stats_merge_plpgsql(
                '{\"num\": {\"type\": \"int_agg\", \"count\": 2, \"sum\": 200, \"min\": 50, \"max\": 150, \"mean\": 100, \"sum_sq_diff\": 5000}}'::jsonb,
                '{\"num\": {\"type\": \"int_agg\", \"count\": 1, \"sum\": 2500, \"min\": 2500, \"max\": 2500, \"mean\": 2500, \"sum_sq_diff\": 0}}'::jsonb
            )",
        );
        assert_eq!(ok, Ok(Some(true)));
    }

    // ── jsonb_stats_final tests ──

    #[pg_test]
    fn test_final_int_agg() {
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT jsonb_stats_final(
                '{\"num\": {\"type\": \"int_agg\", \"count\": 2, \"sum\": 200, \"min\": 50, \"max\": 150, \"mean\": 100, \"sum_sq_diff\": 5000}}'::jsonb
            )",
        );
        let val = result.unwrap().unwrap().0;
        assert_eq!(val["type"], "stats_agg");
        let num = &val["num"];
        // sum_sq_diff / (count-1) = 5000 / 1 = 5000
        assert_eq!(num["variance"].to_string(), "5000.00");
        // sqrt(5000) = 70.710678...
        assert_eq!(num["stddev"].to_string(), "70.71");
        // (70.71 / 100) * 100 = 70.71
        assert_eq!(
            num["coefficient_of_variation_pct"].to_string(),
            "70.71"
        );
        assert_eq!(num["mean"].to_string(), "100.00");
    }

    #[pg_test]
    fn test_final_single_count_nulls() {
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT jsonb_stats_final(
                '{\"num\": {\"type\": \"int_agg\", \"count\": 1, \"sum\": 100, \"min\": 100, \"max\": 100, \"mean\": 100, \"sum_sq_diff\": 0}}'::jsonb
            )",
        );
        let val = result.unwrap().unwrap().0;
        assert!(val["num"]["variance"].is_null());
        assert!(val["num"]["stddev"].is_null());
        assert!(val["num"]["coefficient_of_variation_pct"].is_null());
    }

    #[pg_test]
    fn test_final_matches_plpgsql() {
        load_plpgsql_reference();
        let ok = Spi::get_one::<bool>(
            "SELECT jsonb_stats_final(
                '{\"num\": {\"type\": \"int_agg\", \"count\": 2, \"sum\": 200, \"min\": 50, \"max\": 150, \"mean\": 100, \"sum_sq_diff\": 5000}}'::jsonb
            ) = jsonb_stats_final_plpgsql(
                '{\"num\": {\"type\": \"int_agg\", \"count\": 2, \"sum\": 200, \"min\": 50, \"max\": 150, \"mean\": 100, \"sum_sq_diff\": 5000}}'::jsonb
            )",
        );
        assert_eq!(ok, Ok(Some(true)));
    }

    // ── Full pipeline: jsonb_stats_agg (accum + final) ──

    #[pg_test]
    fn test_full_pipeline_agg() {
        load_plpgsql_reference();
        let ok = Spi::get_one::<bool>(
            "WITH data(stats) AS (
                VALUES
                    ('{\"num\": {\"type\": \"int\", \"value\": 150}, \"ind\": {\"type\": \"str\", \"value\": \"tech\"}, \"ok\": {\"type\": \"bool\", \"value\": true}}'::jsonb),
                    ('{\"num\": {\"type\": \"int\", \"value\": 50}, \"ind\": {\"type\": \"str\", \"value\": \"tech\"}, \"ok\": {\"type\": \"bool\", \"value\": false}}'::jsonb)
            )
            SELECT jsonb_stats_agg(stats) = jsonb_stats_agg_plpgsql(stats)
            FROM data",
        );
        assert_eq!(ok, Ok(Some(true)));
    }

    // ── Full pipeline: jsonb_stats_merge_agg ──

    #[pg_test]
    fn test_full_pipeline_merge_agg() {
        load_plpgsql_reference();
        let ok = Spi::get_one::<bool>(
            "WITH agg_data(stats_agg) AS (
                VALUES
                    ('{\"num\": {\"type\": \"int_agg\", \"count\": 2, \"sum\": 200, \"min\": 50, \"max\": 150, \"mean\": 100, \"sum_sq_diff\": 5000},
                      \"ind\": {\"type\": \"str_agg\", \"counts\": {\"tech\": 2}},
                      \"ok\": {\"type\": \"bool_agg\", \"counts\": {\"true\": 1, \"false\": 1}}}'::jsonb),
                    ('{\"num\": {\"type\": \"int_agg\", \"count\": 1, \"sum\": 2500, \"min\": 2500, \"max\": 2500, \"mean\": 2500, \"sum_sq_diff\": 0},
                      \"ind\": {\"type\": \"str_agg\", \"counts\": {\"finance\": 1}},
                      \"ok\": {\"type\": \"bool_agg\", \"counts\": {\"true\": 1}}}'::jsonb)
            )
            SELECT jsonb_stats_merge_agg(stats_agg) = jsonb_stats_merge_agg_plpgsql(stats_agg)
            FROM agg_data",
        );
        assert_eq!(ok, Ok(Some(true)));
    }

    // ── End-to-end test matching sql/001 scenario ──

    #[pg_test]
    fn test_end_to_end_three_companies() {
        load_plpgsql_reference();

        // Build stats for 3 companies, aggregate by region, then merge globally
        let ok = Spi::get_one::<bool>(
            "WITH
            -- Company stats as JSONB
            company_stats(region, stats) AS (
                VALUES
                    ('EU', '{\"num_employees\": {\"type\": \"int\", \"value\": 150}, \"industry\": {\"type\": \"str\", \"value\": \"tech\"}, \"is_profitable\": {\"type\": \"bool\", \"value\": true}}'::jsonb),
                    ('US', '{\"num_employees\": {\"type\": \"int\", \"value\": 2500}, \"industry\": {\"type\": \"str\", \"value\": \"finance\"}, \"is_profitable\": {\"type\": \"bool\", \"value\": true}}'::jsonb),
                    ('EU', '{\"num_employees\": {\"type\": \"int\", \"value\": 50}, \"industry\": {\"type\": \"str\", \"value\": \"tech\"}, \"is_profitable\": {\"type\": \"bool\", \"value\": false}}'::jsonb)
            ),
            -- Level 2: aggregate by region
            by_region_rust AS (
                SELECT region, jsonb_stats_agg(stats) AS agg FROM company_stats GROUP BY region
            ),
            by_region_plpgsql AS (
                SELECT region, jsonb_stats_agg_plpgsql(stats) AS agg FROM company_stats GROUP BY region
            ),
            -- Level 3: merge regions into global
            global_rust AS (
                SELECT jsonb_stats_merge_agg(agg) AS agg FROM by_region_rust
            ),
            global_plpgsql AS (
                SELECT jsonb_stats_merge_agg_plpgsql(agg) AS agg FROM by_region_plpgsql
            )
            SELECT r.agg = p.agg FROM global_rust r, global_plpgsql p",
        );
        assert_eq!(ok, Ok(Some(true)));
    }

    // ── float type tests ──

    #[pg_test]
    fn test_accum_init_float() {
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT jsonb_stats_accum(
                '{}'::jsonb,
                '{\"price\": {\"type\": \"float\", \"value\": 3.14}}'::jsonb
            )",
        );
        let val = result.unwrap().unwrap().0;
        let price = &val["price"];
        assert_eq!(price["type"], "float_agg");
        assert_eq!(price["count"], 1);
    }

    #[pg_test]
    fn test_merge_float_agg() {
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT jsonb_stats_merge(
                '{\"price\": {\"type\": \"float_agg\", \"count\": 2, \"sum\": 6.28, \"min\": 2.14, \"max\": 4.14, \"mean\": 3.14, \"sum_sq_diff\": 2}}'::jsonb,
                '{\"price\": {\"type\": \"float_agg\", \"count\": 1, \"sum\": 9.99, \"min\": 9.99, \"max\": 9.99, \"mean\": 9.99, \"sum_sq_diff\": 0}}'::jsonb
            )",
        );
        let val = result.unwrap().unwrap().0;
        let price = &val["price"];
        assert_eq!(price["count"], 3);
        assert_eq!(price["min"].to_string(), "2.14");
        assert_eq!(price["max"].to_string(), "9.99");
    }

    #[pg_test]
    fn test_final_float_agg() {
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT jsonb_stats_final(
                '{\"price\": {\"type\": \"float_agg\", \"count\": 2, \"sum\": 200, \"min\": 50, \"max\": 150, \"mean\": 100, \"sum_sq_diff\": 5000}}'::jsonb
            )",
        );
        let val = result.unwrap().unwrap().0;
        assert_eq!(val["type"], "stats_agg");
        let price = &val["price"];
        assert_eq!(price["type"], "float_agg");
        assert_eq!(price["variance"].to_string(), "5000.00");
        assert_eq!(price["stddev"].to_string(), "70.71");
    }

    // ── dec2 type tests ──

    #[pg_test]
    fn test_accum_init_dec2() {
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT jsonb_stats_accum(
                '{}'::jsonb,
                '{\"amount\": {\"type\": \"dec2\", \"value\": 99.99}}'::jsonb
            )",
        );
        let val = result.unwrap().unwrap().0;
        let amount = &val["amount"];
        assert_eq!(amount["type"], "dec2_agg");
        assert_eq!(amount["count"], 1);
    }

    // ── nat type tests ──

    #[pg_test]
    fn test_accum_init_nat() {
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT jsonb_stats_accum(
                '{}'::jsonb,
                '{\"headcount\": {\"type\": \"nat\", \"value\": 42}}'::jsonb
            )",
        );
        let val = result.unwrap().unwrap().0;
        let headcount = &val["headcount"];
        assert_eq!(headcount["type"], "nat_agg");
        assert_eq!(headcount["count"], 1);
        assert_eq!(headcount["sum"], 42);
    }

    #[pg_test(error = "jsonb_stats: nat value must be >= 0, got -1")]
    fn test_accum_nat_rejects_negative() {
        crate::jsonb_stats_accum(
            pgrx::JsonB(serde_json::json!({})),
            pgrx::JsonB(serde_json::json!({"headcount": {"type": "nat", "value": -1}})),
        );
    }

    #[pg_test(error = "jsonb_stats: nat value must be >= 0, got -5")]
    fn test_accum_nat_rejects_negative_update() {
        let first = crate::jsonb_stats_accum(
            pgrx::JsonB(serde_json::json!({})),
            pgrx::JsonB(serde_json::json!({"headcount": {"type": "nat", "value": 10}})),
        );
        crate::jsonb_stats_accum(
            first,
            pgrx::JsonB(serde_json::json!({"headcount": {"type": "nat", "value": -5}})),
        );
    }

    // ── date type tests ──

    #[pg_test]
    fn test_accum_init_date() {
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT jsonb_stats_accum(
                '{}'::jsonb,
                '{\"founded\": {\"type\": \"date\", \"value\": \"2024-01-15\"}}'::jsonb
            )",
        );
        let val = result.unwrap().unwrap().0;
        let founded = &val["founded"];
        assert_eq!(founded["type"], "date_agg");
        assert_eq!(founded["counts"]["2024-01-15"], 1);
        assert_eq!(founded["min"], "2024-01-15");
        assert_eq!(founded["max"], "2024-01-15");
    }

    #[pg_test]
    fn test_accum_update_date() {
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT jsonb_stats_accum(
                jsonb_stats_accum(
                    '{}'::jsonb,
                    '{\"founded\": {\"type\": \"date\", \"value\": \"2024-01-15\"}}'::jsonb
                ),
                '{\"founded\": {\"type\": \"date\", \"value\": \"2023-06-01\"}}'::jsonb
            )",
        );
        let val = result.unwrap().unwrap().0;
        let founded = &val["founded"];
        assert_eq!(founded["counts"]["2024-01-15"], 1);
        assert_eq!(founded["counts"]["2023-06-01"], 1);
        assert_eq!(founded["min"], "2023-06-01");
        assert_eq!(founded["max"], "2024-01-15");
    }

    #[pg_test]
    fn test_merge_date_agg() {
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT jsonb_stats_merge(
                '{\"founded\": {\"type\": \"date_agg\", \"counts\": {\"2024-01-15\": 2}, \"min\": \"2024-01-15\", \"max\": \"2024-01-15\"}}'::jsonb,
                '{\"founded\": {\"type\": \"date_agg\", \"counts\": {\"2023-06-01\": 1, \"2024-01-15\": 1}, \"min\": \"2023-06-01\", \"max\": \"2024-01-15\"}}'::jsonb
            )",
        );
        let val = result.unwrap().unwrap().0;
        let founded = &val["founded"];
        assert_eq!(founded["counts"]["2024-01-15"], 3);
        assert_eq!(founded["counts"]["2023-06-01"], 1);
        assert_eq!(founded["min"], "2023-06-01");
        assert_eq!(founded["max"], "2024-01-15");
    }

    #[pg_test]
    fn test_final_date_agg() {
        // date_agg should pass through unchanged (no derived stats)
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT jsonb_stats_final(
                '{\"founded\": {\"type\": \"date_agg\", \"counts\": {\"2024-01-15\": 2}, \"min\": \"2024-01-15\", \"max\": \"2024-01-15\"}}'::jsonb
            )",
        );
        let val = result.unwrap().unwrap().0;
        assert_eq!(val["type"], "stats_agg");
        let founded = &val["founded"];
        assert_eq!(founded["type"], "date_agg");
        assert_eq!(founded["counts"]["2024-01-15"], 2);
    }

    // ── Error handling: fail fast on bad input ──
    //
    // These tests call functions directly (not through SPI) so that
    // pgrx::error!() propagates to the #[pg_test(error)] handler.
    // SPI catches PG ERRORs in subtransactions, hiding them from the handler.

    #[pg_test(error = "jsonb_stats: unknown stat type 'foo'. Expected: int, float, dec2, nat, str, bool, arr, date")]
    fn test_accum_rejects_unknown_type() {
        crate::jsonb_stats_accum(
            pgrx::JsonB(serde_json::json!({})),
            pgrx::JsonB(serde_json::json!({"x": {"type": "foo", "value": 1}})),
        );
    }

    #[pg_test(error = "jsonb_stats: stat of type 'str' has missing or invalid 'value'")]
    fn test_accum_rejects_missing_value() {
        crate::jsonb_stats_accum(
            pgrx::JsonB(serde_json::json!({})),
            pgrx::JsonB(serde_json::json!({"x": {"type": "str"}})),
        );
    }

    #[pg_test(error = "jsonb_stats: date stat requires a string 'value'")]
    fn test_accum_rejects_date_with_numeric_value() {
        crate::jsonb_stats_accum(
            pgrx::JsonB(serde_json::json!({})),
            pgrx::JsonB(serde_json::json!({"x": {"type": "date", "value": 42}})),
        );
    }

    #[pg_test(error = "jsonb_stats: type mismatch in merge: 'int_agg' vs 'str_agg'")]
    fn test_merge_rejects_type_mismatch() {
        crate::jsonb_stats_merge(
            pgrx::JsonB(serde_json::json!({"x": {"type": "int_agg", "count": 1, "sum": 1, "min": 1, "max": 1, "mean": 1, "sum_sq_diff": 0}})),
            pgrx::JsonB(serde_json::json!({"x": {"type": "str_agg", "counts": {"a": 1}}})),
        );
    }

    #[pg_test(error = "jsonb_stats: unknown aggregate type 'foo_agg'. Expected: int_agg, float_agg, dec2_agg, nat_agg, str_agg, bool_agg, arr_agg, date_agg")]
    fn test_merge_rejects_unknown_agg_type() {
        crate::jsonb_stats_merge(
            pgrx::JsonB(serde_json::json!({"x": {"type": "foo_agg", "count": 1}})),
            pgrx::JsonB(serde_json::json!({"x": {"type": "foo_agg", "count": 1}})),
        );
    }

    // ── Full pipeline with mixed types ──

    #[pg_test]
    fn test_full_pipeline_mixed_types() {
        let result = Spi::get_one::<pgrx::JsonB>(
            "WITH data(stats) AS (
                VALUES
                    ('{\"emp\": {\"type\": \"int\", \"value\": 150}, \"price\": {\"type\": \"float\", \"value\": 3.14}, \"founded\": {\"type\": \"date\", \"value\": \"2024-01-15\"}, \"ind\": {\"type\": \"str\", \"value\": \"tech\"}}'::jsonb),
                    ('{\"emp\": {\"type\": \"int\", \"value\": 50}, \"price\": {\"type\": \"float\", \"value\": 2.72}, \"founded\": {\"type\": \"date\", \"value\": \"2023-06-01\"}, \"ind\": {\"type\": \"str\", \"value\": \"finance\"}}'::jsonb)
            )
            SELECT jsonb_stats_agg(stats) FROM data",
        );
        let val = result.unwrap().unwrap().0;
        assert_eq!(val["type"], "stats_agg");
        // int
        assert_eq!(val["emp"]["type"], "int_agg");
        assert_eq!(val["emp"]["count"], 2);
        // float
        assert_eq!(val["price"]["type"], "float_agg");
        assert_eq!(val["price"]["count"], 2);
        // date
        assert_eq!(val["founded"]["type"], "date_agg");
        assert_eq!(val["founded"]["min"], "2023-06-01");
        assert_eq!(val["founded"]["max"], "2024-01-15");
        // str
        assert_eq!(val["ind"]["type"], "str_agg");
        assert_eq!(val["ind"]["counts"]["tech"], 1);
        assert_eq!(val["ind"]["counts"]["finance"], 1);
    }

    // ── Benchmarks: Rust vs PL/pgSQL ──
    //
    // pgrx tests run inside the PostgreSQL server process, so eprintln/warning
    // output goes to PG's stderr (invisible to the test runner). We write
    // benchmark results to /tmp/jsonb_stats_benchmarks.txt so they survive.

    const BENCHMARK_FILE: &str = "/tmp/jsonb_stats_benchmarks.txt";

    /// Time a SQL statement by running clock_timestamp() before/after via separate SPI calls.
    /// Uses SELECT INTO to force materialization of aggregate results.
    fn time_sql(query: &str) -> f64 {
        let t1 = Spi::get_one::<f64>(
            "SELECT extract(epoch from clock_timestamp())::float8",
        )
        .unwrap()
        .unwrap();
        Spi::run(query).unwrap();
        let t2 = Spi::get_one::<f64>(
            "SELECT extract(epoch from clock_timestamp())::float8",
        )
        .unwrap()
        .unwrap();
        (t2 - t1) * 1000.0
    }

    fn log_benchmark(msg: &str) {
        use std::io::Write;
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(BENCHMARK_FILE)
        {
            let _ = writeln!(f, "{}", msg);
        }
    }

    #[pg_test]
    fn test_benchmark_accum_10k() {
        load_plpgsql_reference();
        Spi::run(
            "CREATE TEMP TABLE bench_data AS
             SELECT jsonb_build_object(
                 'num', jsonb_build_object('type', 'int', 'value', floor(random() * 1000)::int),
                 'str', jsonb_build_object('type', 'str', 'value', substr(md5(random()::text), 1, 5)),
                 'ok',  jsonb_build_object('type', 'bool', 'value', random() > 0.5)
             ) AS stats
             FROM generate_series(1, 10000)",
        )
        .unwrap();

        let rust_ms = time_sql(
            "SELECT jsonb_stats_agg(stats) INTO TEMP TABLE accum_rust FROM bench_data",
        );
        let plpgsql_ms = time_sql(
            "SELECT jsonb_stats_agg_plpgsql(stats) INTO TEMP TABLE accum_plpgsql FROM bench_data",
        );

        let speedup = plpgsql_ms / rust_ms;
        let msg = format!(
            "BENCHMARK accum 10K rows: Rust={:.0}ms, PL/pgSQL={:.0}ms, speedup={:.1}x",
            rust_ms, plpgsql_ms, speedup
        );
        log_benchmark(&msg);

        // Verify correctness: both produce same count
        let ok = Spi::get_one::<bool>(
            "SELECT (r.jsonb_stats_agg->'num'->>'count')::int
                  = (p.jsonb_stats_agg_plpgsql->'num'->>'count')::int
             FROM accum_rust r, accum_plpgsql p",
        );
        assert_eq!(ok, Ok(Some(true)), "Rust and PL/pgSQL counts must match");

        assert!(
            rust_ms < plpgsql_ms,
            "{msg} — Rust should be faster"
        );
    }

    #[pg_test]
    fn test_benchmark_merge_1k_groups() {
        load_plpgsql_reference();

        // Create 1000 pre-aggregated stats_agg objects (simulating regional summaries).
        // Each group has ~100 rows, yielding large count maps for str_agg.
        Spi::run(
            "CREATE TEMP TABLE bench_agg_data AS
             WITH raw AS (
                 SELECT
                     (i % 1000) AS grp,
                     jsonb_build_object(
                         'num', jsonb_build_object('type', 'int', 'value', floor(random() * 1000)::int),
                         'str', jsonb_build_object('type', 'str', 'value', substr(md5(random()::text), 1, 5)),
                         'ok',  jsonb_build_object('type', 'bool', 'value', random() > 0.5)
                     ) AS stats
                 FROM generate_series(1, 100000) i
             )
             SELECT jsonb_stats_agg(stats) AS agg
             FROM raw
             GROUP BY grp",
        )
        .unwrap();

        let rust_ms = time_sql(
            "SELECT jsonb_stats_merge_agg(agg) INTO TEMP TABLE merge_rust FROM bench_agg_data",
        );
        let plpgsql_ms = time_sql(
            "SELECT jsonb_stats_merge_agg_plpgsql(agg) INTO TEMP TABLE merge_plpgsql FROM bench_agg_data",
        );

        let speedup = plpgsql_ms / rust_ms;
        let msg = format!(
            "BENCHMARK merge 1K groups: Rust={:.0}ms, PL/pgSQL={:.0}ms, speedup={:.1}x",
            rust_ms, plpgsql_ms, speedup
        );
        log_benchmark(&msg);

        assert!(
            rust_ms < plpgsql_ms,
            "{msg} — Rust should be faster"
        );
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
