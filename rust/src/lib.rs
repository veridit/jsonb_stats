use pgrx::prelude::*;

pg_module_magic!();

mod accum;
mod final_fn;
mod helpers;
mod merge;
mod stat;

// Re-export all pg_extern functions so pgrx can discover them
pub use accum::jsonb_stats_accum;
pub use final_fn::jsonb_stats_final;
pub use merge::jsonb_stats_merge;
pub use stat::{jsonb_stats_sfunc, stat, stats_from_jsonb};

// Aggregate definitions using extension_sql!
// These must come after all function definitions (enforced by `requires`).
extension_sql!(
    r#"
-- stats -> stats_agg (with final and parallel merge)
CREATE AGGREGATE jsonb_stats_agg(jsonb) (
    sfunc = jsonb_stats_accum,
    stype = jsonb,
    initcond = '{}',
    finalfunc = jsonb_stats_final,
    combinefunc = jsonb_stats_merge,
    parallel = safe
);

-- stats_agg -> stats_agg (merge aggregates with final)
CREATE AGGREGATE jsonb_stats_merge_agg(jsonb) (
    sfunc = jsonb_stats_merge,
    stype = jsonb,
    initcond = '{}',
    finalfunc = jsonb_stats_final,
    combinefunc = jsonb_stats_merge,
    parallel = safe
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
        jsonb_stats_merge,
        jsonb_stats_final,
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
        Spi::run(include_str!("../../dev/reference_plpgsql.sql"))
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
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
