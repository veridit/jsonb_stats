--
-- Reference PL/pgSQL implementation of the jsonb_stats aggregates, adapted
-- from the StatBus project. This file is the authoritative spec: every Rust
-- function must produce byte-identical output (compare ::text).
--
-- Numeric semantics (int, nat, dec2, float), jsonb_stats 3.0:
--
--   * State is exact: count, sum, sum_sq (sum of squares), min, max, kept as
--     PostgreSQL numeric. Accumulating and merging only add, so the result
--     does not depend on row order, grouping, merge order or parallel plan.
--   * Values: int/nat must be integral (nat >= 0) and are kept at scale 0;
--     dec2 may have at most 2 decimals and is kept at scale 2; float keeps the
--     exact decimal text it was given (jsonb stores numbers as numeric).
--   * Derived fields are computed once from the state, with exact rational
--     arithmetic, and rounded once to 2 decimals, half away from zero.
--     N = count * sum_sq - sum^2
--       mean        = sum / count
--       sum_sq_diff = N / count
--       variance    = N / (count * (count - 1))          NULL when count <= 1
--       stddev      = sqrt(variance)                     NULL when count <= 1
--       coefficient_of_variation_pct = 100 * stddev / mean
--                                    NULL when count <= 1 or sum = 0
--   * min/max ties between equal values with different scales keep the larger
--     scale, so even the printed form is order independent.
--

-- ── Exact helpers ──

-- Round the rational a / b (b <> 0) to 2 decimals, half away from zero.
CREATE OR REPLACE FUNCTION jsonb_stats_round2_ratio_plpgsql(a numeric, b numeric)
RETURNS numeric
LANGUAGE sql IMMUTABLE STRICT
AS $jsonb_stats_round2_ratio_plpgsql$
    SELECT (sign(a) * sign(b) * div(200 * abs(a) + abs(b), 2 * abs(b))) * 0.01
$jsonb_stats_round2_ratio_plpgsql$;

-- Exact floor(sqrt(n)) for an integral numeric n >= 0 (Newton's method).
CREATE OR REPLACE FUNCTION jsonb_stats_isqrt_plpgsql(n numeric)
RETURNS numeric
LANGUAGE plpgsql IMMUTABLE STRICT
AS $jsonb_stats_isqrt_plpgsql$
DECLARE
    x numeric;
    y numeric;
BEGIN
    IF n < 2 THEN
        RETURN n;
    END IF;
    x := trunc(sqrt(n)) + 1;  -- any start >= floor(sqrt(n)) converges downward
    LOOP
        y := div(x + div(n, x), 2);
        EXIT WHEN y >= x;
        x := y;
    END LOOP;
    WHILE x * x > n LOOP
        x := x - 1;
    END LOOP;
    WHILE (x + 1) * (x + 1) <= n LOOP
        x := x + 1;
    END LOOP;
    RETURN x;
END;
$jsonb_stats_isqrt_plpgsql$;

-- sqrt(a / b) rounded to 2 decimals, half away from zero (a >= 0, b > 0).
-- floor(100*sqrt(q) + 1/2) = (isqrt(floor(40000 q)) + 1) div 2
CREATE OR REPLACE FUNCTION jsonb_stats_sqrt_round2_ratio_plpgsql(a numeric, b numeric)
RETURNS numeric
LANGUAGE sql IMMUTABLE STRICT
AS $jsonb_stats_sqrt_round2_ratio_plpgsql$
    SELECT div(jsonb_stats_isqrt_plpgsql(div(40000 * a, b)) + 1, 2) * 0.01
$jsonb_stats_sqrt_round2_ratio_plpgsql$;

-- Pick min (want_less) or max of two values; equal values keep the larger scale.
CREATE OR REPLACE FUNCTION jsonb_stats_pick_plpgsql(a numeric, b numeric, want_less boolean)
RETURNS numeric
LANGUAGE sql IMMUTABLE STRICT
AS $jsonb_stats_pick_plpgsql$
    SELECT CASE
        WHEN a = b THEN CASE WHEN scale(a) >= scale(b) THEN a ELSE b END
        WHEN (a < b) = want_less THEN a
        ELSE b
    END
$jsonb_stats_pick_plpgsql$;

-- Parse and validate a numeric stat value; returns it at its canonical scale.
CREATE OR REPLACE FUNCTION jsonb_stats_num_value_plpgsql(stat_type text, stat_obj jsonb)
RETURNS numeric
LANGUAGE plpgsql IMMUTABLE STRICT
AS $jsonb_stats_num_value_plpgsql$
DECLARE
    v numeric;
BEGIN
    IF jsonb_typeof(stat_obj->'value') IS DISTINCT FROM 'number' THEN
        RAISE EXCEPTION 'jsonb_stats: stat of type ''%'' has missing or non-numeric ''value''', stat_type;
    END IF;
    v := (stat_obj->'value')::numeric;
    IF stat_type IN ('int', 'nat') THEN
        IF v <> trunc(v) THEN
            RAISE EXCEPTION 'jsonb_stats: % value must be an integer, got %', stat_type, stat_obj->'value';
        END IF;
        IF stat_type = 'nat' AND v < 0 THEN
            RAISE EXCEPTION 'jsonb_stats: nat value must be >= 0, got %', stat_obj->'value';
        END IF;
        RETURN trunc(v);
    ELSIF stat_type = 'dec2' THEN
        IF v <> round(v, 2) THEN
            RAISE EXCEPTION 'jsonb_stats: dec2 value must have at most 2 decimal places, got %', stat_obj->'value';
        END IF;
        RETURN round(v, 2);
    END IF;
    RETURN v;  -- float: exact decimal as given
END;
$jsonb_stats_num_value_plpgsql$;

-- Read the exact state fields of a numeric *_agg summary, validating them.
CREATE OR REPLACE FUNCTION jsonb_stats_num_state_plpgsql(
    key text, summary jsonb,
    OUT count_val numeric, OUT sum_val numeric, OUT sum_sq_val numeric,
    OUT min_val numeric, OUT max_val numeric)
LANGUAGE plpgsql IMMUTABLE STRICT
AS $jsonb_stats_num_state_plpgsql$
DECLARE
    agg_type text := summary->>'type';
    field text;
BEGIN
    IF NOT summary ? 'sum_sq' THEN
        RAISE EXCEPTION 'jsonb_stats: % for key ''%'' lacks ''sum_sq'' (created by jsonb_stats < 3.0). Recompute it from the source stats.', agg_type, key;
    END IF;
    IF jsonb_typeof(summary->'count') IS DISTINCT FROM 'number'
       OR (summary->'count')::numeric < 1
       OR (summary->'count')::numeric <> trunc((summary->'count')::numeric) THEN
        RAISE EXCEPTION 'jsonb_stats: % for key ''%'' has invalid ''count'' (must be an integer >= 1)', agg_type, key;
    END IF;
    FOREACH field IN ARRAY ARRAY['sum', 'sum_sq', 'min', 'max'] LOOP
        IF jsonb_typeof(summary->field) IS DISTINCT FROM 'number' THEN
            RAISE EXCEPTION 'jsonb_stats: % for key ''%'' has missing or non-numeric ''%''', agg_type, key, field;
        END IF;
    END LOOP;
    count_val := trunc((summary->'count')::numeric);
    sum_val := (summary->'sum')::numeric;
    sum_sq_val := (summary->'sum_sq')::numeric;
    min_val := (summary->'min')::numeric;
    max_val := (summary->'max')::numeric;
    IF count_val * sum_sq_val < sum_val * sum_val THEN
        RAISE EXCEPTION 'jsonb_stats: % for key ''%'' is inconsistent (count * sum_sq < sum^2)', agg_type, key;
    END IF;
END;
$jsonb_stats_num_state_plpgsql$;

-- Exact state object for a numeric summary.
CREATE OR REPLACE FUNCTION jsonb_stats_num_state_obj_plpgsql(
    agg_type text, count_val numeric, sum_val numeric, sum_sq_val numeric,
    min_val numeric, max_val numeric)
RETURNS jsonb
LANGUAGE sql IMMUTABLE STRICT
AS $jsonb_stats_num_state_obj_plpgsql$
    SELECT jsonb_build_object(
        'type', agg_type, 'count', count_val, 'sum', sum_val, 'sum_sq', sum_sq_val,
        'min', min_val, 'max', max_val)
$jsonb_stats_num_state_obj_plpgsql$;

-- Full numeric summary: exact state plus derived fields.
CREATE OR REPLACE FUNCTION jsonb_stats_num_summary_plpgsql(key text, summary jsonb)
RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE STRICT
AS $jsonb_stats_num_summary_plpgsql$
DECLARE
    st record;
    n_big numeric;
    variance numeric;
    stddev numeric;
    cv_pct numeric;
    cv_mag numeric;
BEGIN
    st := jsonb_stats_num_state_plpgsql(key, summary);
    n_big := st.count_val * st.sum_sq_val - st.sum_val * st.sum_val;
    IF st.count_val > 1 THEN
        variance := jsonb_stats_round2_ratio_plpgsql(n_big, st.count_val * (st.count_val - 1));
        stddev := jsonb_stats_sqrt_round2_ratio_plpgsql(n_big, st.count_val * (st.count_val - 1));
        IF st.sum_val <> 0 THEN
            -- cv^2 = 10000 * N * count / ((count - 1) * sum^2)
            cv_mag := jsonb_stats_sqrt_round2_ratio_plpgsql(
                10000 * n_big * st.count_val, (st.count_val - 1) * st.sum_val * st.sum_val);
            cv_pct := sign(st.sum_val) * cv_mag;
        END IF;
    END IF;
    RETURN jsonb_stats_num_state_obj_plpgsql(
               summary->>'type', st.count_val, st.sum_val, st.sum_sq_val, st.min_val, st.max_val)
        || jsonb_build_object(
               'mean', jsonb_stats_round2_ratio_plpgsql(st.sum_val, st.count_val),
               'sum_sq_diff', jsonb_stats_round2_ratio_plpgsql(n_big, st.count_val),
               'variance', variance,
               'stddev', stddev,
               'coefficient_of_variation_pct', cv_pct);
END;
$jsonb_stats_num_summary_plpgsql$;

-- ── Accumulation (Level 2: stats -> running state) ──

CREATE OR REPLACE FUNCTION jsonb_stats_accum_plpgsql(state jsonb, stats jsonb)
RETURNS jsonb
LANGUAGE plpgsql
AS $jsonb_stats_accum_plpgsql$
DECLARE
    new_state jsonb := state;
    stat_key text;
    stat_obj jsonb;
    stat_type text;
    stat_val_str text;
    v numeric;
    st record;
    current_summary jsonb;
    new_summary jsonb;
    current_count int;
    element text;
BEGIN
    FOR stat_key, stat_obj IN SELECT * FROM jsonb_each(stats) LOOP
        IF stat_key = 'type' THEN CONTINUE; END IF;

        current_summary := new_state->stat_key;
        stat_type := stat_obj->>'type';
        stat_val_str := stat_obj->>'value';

        IF current_summary IS NOT NULL
           AND current_summary->>'type' IS DISTINCT FROM stat_type || '_agg' THEN
            RAISE EXCEPTION 'jsonb_stats: type mismatch for key ''%'': existing % vs incoming %',
                stat_key, current_summary->>'type', stat_type;
        END IF;

        IF stat_type IN ('int', 'nat', 'dec2', 'float') THEN
            v := jsonb_stats_num_value_plpgsql(stat_type, stat_obj);
            IF current_summary IS NULL THEN
                new_summary := jsonb_stats_num_state_obj_plpgsql(stat_type || '_agg', 1, v, v * v, v, v);
            ELSE
                st := jsonb_stats_num_state_plpgsql(stat_key, current_summary);
                new_summary := jsonb_stats_num_state_obj_plpgsql(
                    stat_type || '_agg',
                    st.count_val + 1, st.sum_val + v, st.sum_sq_val + v * v,
                    jsonb_stats_pick_plpgsql(st.min_val, v, true),
                    jsonb_stats_pick_plpgsql(st.max_val, v, false));
            END IF;
        ELSIF stat_type IN ('str', 'bool') THEN
            IF stat_val_str IS NULL THEN
                RAISE EXCEPTION 'jsonb_stats: stat of type ''%'' has missing or invalid ''value''', stat_type;
            END IF;
            IF current_summary IS NULL THEN
                new_summary := jsonb_build_object('type', stat_type || '_agg', 'counts', jsonb_build_object(stat_val_str, 1));
            ELSE
                current_count := COALESCE((current_summary->'counts'->>stat_val_str)::int, 0);
                new_summary := jsonb_set(current_summary, ARRAY['counts', stat_val_str], to_jsonb(current_count + 1));
            END IF;
        ELSIF stat_type = 'arr' THEN
            IF current_summary IS NULL THEN
                new_summary := jsonb_build_object('type', 'arr_agg', 'count', 1, 'counts', '{}'::jsonb);
            ELSE
                new_summary := current_summary || jsonb_build_object('count', (current_summary->>'count')::int + 1);
            END IF;
            IF stat_val_str != '{}' THEN
                FOR element IN SELECT unnest(string_to_array(trim(stat_val_str, '{}'), ',')) LOOP
                    current_count := COALESCE((new_summary->'counts'->>element)::int, 0);
                    new_summary := jsonb_set(new_summary, ARRAY['counts', element], to_jsonb(current_count + 1));
                END LOOP;
            END IF;
        ELSIF stat_type = 'date' THEN
            IF jsonb_typeof(stat_obj->'value') IS DISTINCT FROM 'string' THEN
                RAISE EXCEPTION 'jsonb_stats: date stat requires a string ''value''';
            END IF;
            IF current_summary IS NULL THEN
                new_summary := jsonb_build_object('type', 'date_agg', 'counts', jsonb_build_object(stat_val_str, 1),
                                                  'min', stat_val_str, 'max', stat_val_str);
            ELSE
                current_count := COALESCE((current_summary->'counts'->>stat_val_str)::int, 0);
                new_summary := jsonb_set(current_summary, ARRAY['counts', stat_val_str], to_jsonb(current_count + 1))
                    || jsonb_build_object(
                           'min', LEAST(current_summary->>'min' COLLATE "C", stat_val_str COLLATE "C"),
                           'max', GREATEST(current_summary->>'max' COLLATE "C", stat_val_str COLLATE "C"));
            END IF;
        ELSE
            RAISE EXCEPTION 'jsonb_stats: unknown stat type ''%''. Expected: int, float, dec2, nat, str, bool, arr, date', stat_type;
        END IF;
        new_state := jsonb_set(new_state, ARRAY[stat_key], new_summary, true);
    END LOOP;
    RETURN new_state;
END;
$jsonb_stats_accum_plpgsql$;

-- ── Merge (Level 3: stats_agg -> stats_agg) ──
--
-- jsonb_stats_merge_state_plpgsql combines two summaries' exact state (the
-- aggregate transition). jsonb_stats_merge_plpgsql is the scalar merge: it
-- also finalizes, so its result can be stored directly as a stats_agg.

CREATE OR REPLACE FUNCTION jsonb_stats_merge_state_plpgsql(a jsonb, b jsonb)
RETURNS jsonb
LANGUAGE plpgsql
AS $jsonb_stats_merge_state_plpgsql$
DECLARE
    merged_state jsonb := a;
    summary_key text; summary_a jsonb; summary_b jsonb; merged_summary jsonb; type_a text;
    sa record; sb record;
    counts_a jsonb; k text; v jsonb;
BEGIN
    FOR summary_key, summary_b IN SELECT * FROM jsonb_each(b) LOOP
        IF summary_key = 'type' OR jsonb_typeof(summary_b) <> 'object' THEN CONTINUE; END IF;
        summary_a := merged_state->summary_key;
        IF jsonb_typeof(summary_a) IS DISTINCT FROM 'object' THEN
            merged_summary := summary_b;
        ELSE
            type_a := summary_a->>'type';
            IF type_a IS DISTINCT FROM summary_b->>'type' THEN
                RAISE EXCEPTION 'jsonb_stats: type mismatch for key ''%'': existing % vs incoming %', summary_key, type_a, summary_b->>'type';
            END IF;
            IF type_a IN ('int_agg', 'nat_agg', 'dec2_agg', 'float_agg') THEN
                sa := jsonb_stats_num_state_plpgsql(summary_key, summary_a);
                sb := jsonb_stats_num_state_plpgsql(summary_key, summary_b);
                merged_summary := jsonb_stats_num_state_obj_plpgsql(
                    type_a,
                    sa.count_val + sb.count_val, sa.sum_val + sb.sum_val, sa.sum_sq_val + sb.sum_sq_val,
                    jsonb_stats_pick_plpgsql(sa.min_val, sb.min_val, true),
                    jsonb_stats_pick_plpgsql(sa.max_val, sb.max_val, false));
            ELSIF type_a IN ('str_agg', 'bool_agg', 'arr_agg', 'date_agg') THEN
                merged_summary := summary_a;
                IF type_a = 'arr_agg' THEN
                    merged_summary := merged_summary || jsonb_build_object('count', (summary_a->>'count')::int + (summary_b->>'count')::int);
                END IF;
                counts_a := summary_a->'counts';
                FOR k, v IN SELECT * FROM jsonb_each(summary_b->'counts') LOOP
                    counts_a := jsonb_set(counts_a, ARRAY[k], to_jsonb(COALESCE((counts_a->>k)::int, 0) + (v->>0)::int));
                END LOOP;
                merged_summary := jsonb_set(merged_summary, '{counts}', counts_a);
                IF type_a = 'date_agg' THEN
                    merged_summary := merged_summary || jsonb_build_object(
                        'min', LEAST(summary_a->>'min' COLLATE "C", summary_b->>'min' COLLATE "C"),
                        'max', GREATEST(summary_a->>'max' COLLATE "C", summary_b->>'max' COLLATE "C"));
                END IF;
            ELSE
                RAISE EXCEPTION 'jsonb_stats: unknown aggregate type ''%''. Expected: int_agg, float_agg, dec2_agg, nat_agg, str_agg, bool_agg, arr_agg, date_agg', type_a;
            END IF;
        END IF;
        merged_state := jsonb_set(merged_state, ARRAY[summary_key], merged_summary, true);
    END LOOP;
    RETURN merged_state - 'type';
END;
$jsonb_stats_merge_state_plpgsql$;

-- ── Final (adds derived numeric fields and "type": "stats_agg") ──

CREATE OR REPLACE FUNCTION jsonb_stats_final_plpgsql(state jsonb)
RETURNS jsonb
LANGUAGE plpgsql
AS $jsonb_stats_final_plpgsql$
DECLARE
    key text; summary jsonb; result jsonb;
BEGIN
    result := jsonb_build_object('type', 'stats_agg');
    FOR key, summary IN SELECT * FROM jsonb_each(state) LOOP
        IF key = 'type' THEN CONTINUE; END IF;
        IF summary->>'type' IN ('int_agg', 'nat_agg', 'dec2_agg', 'float_agg') THEN
            summary := jsonb_stats_num_summary_plpgsql(key, summary);
        END IF;
        result := jsonb_set(result, ARRAY[key], summary);
    END LOOP;
    RETURN result;
END;
$jsonb_stats_final_plpgsql$;

CREATE OR REPLACE FUNCTION jsonb_stats_merge_plpgsql(a jsonb, b jsonb)
RETURNS jsonb
LANGUAGE sql
AS $jsonb_stats_merge_plpgsql$
    SELECT jsonb_stats_final_plpgsql(jsonb_stats_merge_state_plpgsql(a, b))
$jsonb_stats_merge_plpgsql$;

CREATE AGGREGATE jsonb_stats_agg_plpgsql(jsonb) (
    sfunc = jsonb_stats_accum_plpgsql,
    stype = jsonb,
    initcond = '{}',
    finalfunc = jsonb_stats_final_plpgsql,
    combinefunc = jsonb_stats_merge_state_plpgsql,
    parallel = safe
);

CREATE AGGREGATE jsonb_stats_merge_agg_plpgsql(jsonb) (
    sfunc = jsonb_stats_merge_state_plpgsql,
    stype = jsonb,
    initcond = '{}',
    finalfunc = jsonb_stats_final_plpgsql,
    combinefunc = jsonb_stats_merge_state_plpgsql,
    parallel = safe
);
