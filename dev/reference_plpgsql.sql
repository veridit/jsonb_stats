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

-- Round the rational a / b (b <> 0) to k decimals, half away from zero, at scale k.
CREATE OR REPLACE FUNCTION jsonb_stats_round_ratio_plpgsql(a numeric, b numeric, k int)
RETURNS numeric
LANGUAGE sql IMMUTABLE STRICT
AS $jsonb_stats_round_ratio_plpgsql$
    SELECT round((sign(a) * sign(b) * div(2 * abs(a) * 10::numeric ^ k + abs(b), 2 * abs(b)))
                 * ('1e-' || k)::numeric, k)  -- multiply: numeric division would round
$jsonb_stats_round_ratio_plpgsql$;

-- The rational a / b (b > 0) rounded up (toward +inf) to k decimals, at scale k.
CREATE OR REPLACE FUNCTION jsonb_stats_ceil_ratio_plpgsql(a numeric, b numeric, k int)
RETURNS numeric
LANGUAGE sql IMMUTABLE STRICT
AS $jsonb_stats_ceil_ratio_plpgsql$
    -- ceil = floor + 1 when there is a remainder. Scale back by multiplying
    -- (numeric division would round at a limited scale).
    SELECT round((div(a * 10::numeric ^ k, b)
                  + CASE WHEN mod(a * 10::numeric ^ k, b) > 0 THEN 1 ELSE 0 END)
                 * ('1e-' || k)::numeric, k)
$jsonb_stats_ceil_ratio_plpgsql$;

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
    legacy boolean := NOT summary ? 'sum_sq';
    k int;
    sq_est numeric;
    sq_floor numeric;
BEGIN
    IF legacy AND NOT summary ? 'sum_sq_diff' THEN
        RAISE EXCEPTION 'jsonb_stats: % for key ''%'' has neither ''sum_sq'' nor ''sum_sq_diff''', agg_type, key;
    END IF;
    IF jsonb_typeof(summary->'count') IS DISTINCT FROM 'number'
       OR (summary->'count')::numeric < 1
       OR (summary->'count')::numeric <> trunc((summary->'count')::numeric) THEN
        RAISE EXCEPTION 'jsonb_stats: % for key ''%'' has invalid ''count'' (must be an integer >= 1)', agg_type, key;
    END IF;
    FOREACH field IN ARRAY CASE WHEN legacy THEN ARRAY['sum', 'sum_sq_diff', 'min', 'max']
                                ELSE ARRAY['sum', 'sum_sq', 'min', 'max'] END LOOP
        IF jsonb_typeof(summary->field) IS DISTINCT FROM 'number' THEN
            RAISE EXCEPTION 'jsonb_stats: % for key ''%'' has missing or non-numeric ''%''', agg_type, key, field;
        END IF;
    END LOOP;
    count_val := trunc((summary->'count')::numeric);
    sum_val := (summary->'sum')::numeric;
    min_val := (summary->'min')::numeric;
    max_val := (summary->'max')::numeric;
    IF NOT legacy THEN
        sum_sq_val := (summary->'sum_sq')::numeric;
    ELSE
        -- jsonb_stats 2.x summary: upgrade on read. Normalize to the kind's
        -- scale, then sum_sq = max(round_k(ssd + sum^2/n), ceil_k(sum^2/n)).
        IF agg_type IN ('int_agg', 'nat_agg') THEN
            sum_val := round(sum_val); min_val := round(min_val); max_val := round(max_val);
        ELSIF agg_type = 'dec2_agg' THEN
            sum_val := round(sum_val, 2); min_val := round(min_val, 2); max_val := round(max_val, 2);
        END IF;
        k := 2 * scale(sum_val);
        sq_est := jsonb_stats_round_ratio_plpgsql(
            (summary->'sum_sq_diff')::numeric * count_val + sum_val * sum_val, count_val, k);
        sq_floor := jsonb_stats_ceil_ratio_plpgsql(sum_val * sum_val, count_val, k);
        sum_sq_val := CASE WHEN sq_floor > sq_est THEN sq_floor ELSE sq_est END;
    END IF;
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
    -- Numeric entries carry exact state only (also upgrades 2.x summaries
    -- and drops stale derived fields from a finalized input state).
    FOR stat_key, current_summary IN SELECT * FROM jsonb_each(new_state) LOOP
        IF jsonb_typeof(current_summary) = 'object'
           AND current_summary->>'type' IN ('int_agg', 'nat_agg', 'dec2_agg', 'float_agg') THEN
            st := jsonb_stats_num_state_plpgsql(stat_key, current_summary);
            new_state := jsonb_set(new_state, ARRAY[stat_key], jsonb_stats_num_state_obj_plpgsql(
                current_summary->>'type', st.count_val, st.sum_val, st.sum_sq_val, st.min_val, st.max_val));
        END IF;
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

-- ════════════════════════════════════════════════════════════════════════
-- Difference summaries: stats_delta (jsonb_stats 3.1)
-- ════════════════════════════════════════════════════════════════════════
--
-- A stats_delta records changes to a stock: a unit's summary is added when it
-- enters (jsonb_stats_to_delta) and subtracted when it leaves
-- (jsonb_stats_negate). Each entry keeps the two flows apart:
--
--   {"type": "int_delta", "in": {"count", "sum", "sum_sq"}, "out": {...}}
--   {"type": "str_delta", "in": {"counts": {...}}, "out": {...}}   (arr: + "count")
--
-- Each flow is a sum of real summaries, so it is a valid nonnegative state;
-- the stock is in - out. Adding deltas only adds flows, so the result does not
-- depend on order, grouping or parallel plan. min/max are not kept (they
-- cannot be subtracted).
--
-- Canonical form (identical bytes on every path): int/nat sums at scale 0,
-- dec2 sum at scale 2 and sum_sq at scale 4, float trimmed of trailing zeros;
-- zero counts, empty flows and entries without flows are omitted.
--
-- In PL/pgSQL the running state is a "loose" map: every entry carries both
-- flows, empty ones included.

-- Validate one numeric flow and return it canonical: {count, sum, sum_sq}.
-- A NULL flow is empty.
CREATE OR REPLACE FUNCTION jsonb_stats_delta_num_flow_plpgsql(
    delta_type text, key text, side text, flow jsonb)
RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE
AS $jsonb_stats_delta_num_flow_plpgsql$
DECLARE
    kind text := split_part(delta_type, '_', 1);
    c numeric; s numeric; q numeric;
    reason text;
BEGIN
    IF flow IS NULL THEN
        c := 0; s := 0; q := 0;
    ELSIF jsonb_typeof(flow) IS DISTINCT FROM 'object' THEN
        reason := 'must be an object';
    ELSIF jsonb_typeof(flow->'count') IS DISTINCT FROM 'number'
          OR (flow->'count')::numeric < 0
          OR (flow->'count')::numeric <> trunc((flow->'count')::numeric) THEN
        reason := 'count must be an integer >= 0';
    ELSIF jsonb_typeof(flow->'sum') IS DISTINCT FROM 'number'
          OR jsonb_typeof(flow->'sum_sq') IS DISTINCT FROM 'number' THEN
        reason := 'sum and sum_sq must be numbers';
    ELSE
        c := trunc((flow->'count')::numeric);
        s := (flow->'sum')::numeric;
        q := (flow->'sum_sq')::numeric;
    END IF;
    IF reason IS NULL THEN
        IF kind IN ('int', 'nat') AND (s <> trunc(s) OR q <> trunc(q)) THEN
            reason := 'sum and sum_sq must be integers';
        ELSIF kind = 'dec2' AND (s <> round(s, 2) OR q <> round(q, 4)) THEN
            reason := 'sum must have at most 2 and sum_sq at most 4 decimals';
        ELSIF kind = 'nat' AND s < 0 THEN
            reason := 'sum must be >= 0';
        ELSIF c * q < s * s OR (c = 0 AND q <> 0) THEN
            reason := 'inconsistent (count * sum_sq < sum^2)';
        END IF;
    END IF;
    IF reason IS NOT NULL THEN
        RAISE EXCEPTION 'jsonb_stats: % for key ''%'' has an invalid ''%'' flow: %',
            delta_type, key, side, reason;
    END IF;
    IF kind IN ('int', 'nat') THEN
        s := trunc(s); q := trunc(q);
    ELSIF kind = 'dec2' THEN
        s := round(s, 2); q := round(q, 4);
    ELSE
        s := trim_scale(s); q := trim_scale(q);
    END IF;
    RETURN jsonb_build_object('count', c, 'sum', s, 'sum_sq', q);
END;
$jsonb_stats_delta_num_flow_plpgsql$;

-- Validate one counted flow and return it canonical: {counts} (arr: + count),
-- zero counts removed. A NULL flow is empty.
CREATE OR REPLACE FUNCTION jsonb_stats_delta_cat_flow_plpgsql(
    delta_type text, key text, side text, flow jsonb)
RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE
AS $jsonb_stats_delta_cat_flow_plpgsql$
DECLARE
    counts jsonb := '{}';
    k text; v jsonb;
    n numeric := 0;
    reason text;
BEGIN
    IF flow IS NOT NULL THEN
        IF jsonb_typeof(flow) IS DISTINCT FROM 'object' THEN
            reason := 'must be an object';
        ELSIF jsonb_typeof(flow->'counts') IS DISTINCT FROM 'object' THEN
            reason := 'counts must be an object';
        ELSIF delta_type = 'arr_delta'
              AND (jsonb_typeof(flow->'count') IS DISTINCT FROM 'number'
                   OR (flow->'count')::numeric < 0
                   OR (flow->'count')::numeric <> trunc((flow->'count')::numeric)) THEN
            reason := 'count must be an integer >= 0';
        ELSE
            IF delta_type = 'arr_delta' THEN
                n := trunc((flow->'count')::numeric);
            END IF;
            FOR k, v IN SELECT * FROM jsonb_each(flow->'counts') LOOP
                IF jsonb_typeof(v) IS DISTINCT FROM 'number' OR v::numeric < 0
                   OR v::numeric <> trunc(v::numeric) THEN
                    reason := 'counts must be integers >= 0';
                    EXIT;
                END IF;
                IF v::numeric <> 0 THEN
                    counts := counts || jsonb_build_object(k, trunc(v::numeric));
                END IF;
            END LOOP;
        END IF;
    END IF;
    IF reason IS NOT NULL THEN
        RAISE EXCEPTION 'jsonb_stats: % for key ''%'' has an invalid ''%'' flow: %',
            delta_type, key, side, reason;
    END IF;
    IF delta_type = 'arr_delta' THEN
        RETURN jsonb_build_object('count', n, 'counts', counts);
    END IF;
    RETURN jsonb_build_object('counts', counts);
END;
$jsonb_stats_delta_cat_flow_plpgsql$;

-- One summary (*_agg or *_delta) as a loose delta entry.
CREATE OR REPLACE FUNCTION jsonb_stats_delta_entry_plpgsql(key text, summary jsonb)
RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE
AS $jsonb_stats_delta_entry_plpgsql$
DECLARE
    t text := summary->>'type';
    kind text;
    dt text;
    st record;
    in_flow jsonb;
BEGIN
    IF key = 'min_max' THEN
        RAISE EXCEPTION 'jsonb_stats: key ''min_max'' is reserved in stats_delta';
    END IF;
    IF jsonb_typeof(summary) IS DISTINCT FROM 'object' THEN
        RAISE EXCEPTION 'jsonb_stats: summary for key ''%'' must be an object', key;
    END IF;
    kind := regexp_replace(t, '_(agg|delta)$', '');
    IF t IS NULL OR kind = t
       OR kind NOT IN ('int', 'nat', 'dec2', 'float', 'str', 'bool', 'arr', 'date') THEN
        RAISE EXCEPTION 'jsonb_stats: unknown summary type ''%'' for key ''%''. Expected a *_agg or *_delta of: int, float, dec2, nat, str, bool, arr, date', t, key;
    END IF;
    dt := kind || '_delta';
    IF t = dt THEN
        IF kind IN ('int', 'nat', 'dec2', 'float') THEN
            RETURN jsonb_build_object('type', dt,
                'in', jsonb_stats_delta_num_flow_plpgsql(dt, key, 'in', summary->'in'),
                'out', jsonb_stats_delta_num_flow_plpgsql(dt, key, 'out', summary->'out'));
        END IF;
        RETURN jsonb_build_object('type', dt,
            'in', jsonb_stats_delta_cat_flow_plpgsql(dt, key, 'in', summary->'in'),
            'out', jsonb_stats_delta_cat_flow_plpgsql(dt, key, 'out', summary->'out'));
    END IF;
    -- A *_agg: everything flows in.
    IF kind IN ('int', 'nat', 'dec2', 'float') THEN
        st := jsonb_stats_num_state_plpgsql(key, summary);
        in_flow := jsonb_build_object('count', st.count_val, 'sum', st.sum_val, 'sum_sq', st.sum_sq_val);
        RETURN jsonb_build_object('type', dt,
            'in', jsonb_stats_delta_num_flow_plpgsql(dt, key, 'in', in_flow),
            'out', jsonb_stats_delta_num_flow_plpgsql(dt, key, 'out', NULL));
    END IF;
    in_flow := jsonb_build_object('counts', COALESCE(summary->'counts', '{}'));
    IF kind = 'arr' THEN
        in_flow := in_flow || jsonb_build_object('count', COALESCE(summary->'count', '0'));
    END IF;
    RETURN jsonb_build_object('type', dt,
        'in', jsonb_stats_delta_cat_flow_plpgsql(dt, key, 'in', in_flow),
        'out', jsonb_stats_delta_cat_flow_plpgsql(dt, key, 'out', NULL));
END;
$jsonb_stats_delta_entry_plpgsql$;

-- a + sign * b for two loose flows of the same kind (no validation: the
-- inverse transition may pass through partial states).
CREATE OR REPLACE FUNCTION jsonb_stats_delta_flow_add_plpgsql(a jsonb, b jsonb, sign int)
RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE STRICT
AS $jsonb_stats_delta_flow_add_plpgsql$
DECLARE
    counts jsonb;
    k text; v jsonb; n numeric;
BEGIN
    IF a ? 'sum' THEN
        RETURN jsonb_build_object(
            'count', (a->>'count')::numeric + sign * (b->>'count')::numeric,
            'sum', (a->>'sum')::numeric + sign * (b->>'sum')::numeric,
            'sum_sq', (a->>'sum_sq')::numeric + sign * (b->>'sum_sq')::numeric);
    END IF;
    counts := a->'counts';
    FOR k, v IN SELECT * FROM jsonb_each(b->'counts') LOOP
        n := COALESCE((counts->>k)::numeric, 0) + sign * v::numeric;
        IF n = 0 THEN
            counts := counts - k;
        ELSE
            counts := counts || jsonb_build_object(k, n);
        END IF;
    END LOOP;
    IF a ? 'count' THEN
        RETURN jsonb_build_object('counts', counts,
            'count', (a->>'count')::numeric + sign * (b->>'count')::numeric);
    END IF;
    RETURN jsonb_build_object('counts', counts);
END;
$jsonb_stats_delta_flow_add_plpgsql$;

-- Loose state + sign * input. Input: stats_delta, stats_agg or stats.
-- sign = -1 is the moving-aggregate inverse: it removes an input added before.
CREATE OR REPLACE FUNCTION jsonb_stats_delta_state_add_plpgsql(state jsonb, d jsonb, sign int)
RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE
AS $jsonb_stats_delta_state_add_plpgsql$
DECLARE
    result jsonb := COALESCE(state, '{}');
    key text; summary jsonb; e jsonb; cur jsonb;
BEGIN
    IF d IS NULL THEN
        RETURN result;
    END IF;
    IF jsonb_typeof(d) IS DISTINCT FROM 'object' THEN
        RAISE EXCEPTION 'jsonb_stats: stats_delta input must be an object';
    END IF;
    IF d->>'type' = 'stats' THEN
        d := jsonb_stats_final_plpgsql(jsonb_stats_accum_plpgsql('{}', d));
    ELSIF d ? 'type' AND d->>'type' NOT IN ('stats_agg', 'stats_delta') THEN
        RAISE EXCEPTION 'jsonb_stats: expected stats, stats_agg or stats_delta, got type %', d->'type';
    END IF;
    IF d->'min_max' = 'false' THEN
        RAISE EXCEPTION 'jsonb_stats: a stats_agg without min/max (from jsonb_stats_delta_to_agg) is a result, not an input';
    END IF;
    FOR key, summary IN SELECT * FROM jsonb_each(d) LOOP
        IF key = 'type' THEN CONTINUE; END IF;
        e := jsonb_stats_delta_entry_plpgsql(key, summary);
        cur := result->key;
        IF cur IS NULL THEN
            IF sign <> 1 THEN
                RAISE EXCEPTION 'jsonb_stats: cannot remove key ''%'' that was never added', key;
            END IF;
            result := result || jsonb_build_object(key, e);
        ELSE
            IF cur->>'type' IS DISTINCT FROM e->>'type' THEN
                RAISE EXCEPTION 'jsonb_stats: type mismatch for key ''%'': existing % vs incoming %',
                    key, cur->>'type', e->>'type';
            END IF;
            result := result || jsonb_build_object(key, jsonb_build_object('type', e->>'type',
                'in', jsonb_stats_delta_flow_add_plpgsql(cur->'in', e->'in', sign),
                'out', jsonb_stats_delta_flow_add_plpgsql(cur->'out', e->'out', sign)));
        END IF;
    END LOOP;
    RETURN result;
END;
$jsonb_stats_delta_state_add_plpgsql$;

-- The empty flow of a delta type.
CREATE OR REPLACE FUNCTION jsonb_stats_delta_flow_zero_plpgsql(dt text)
RETURNS jsonb
LANGUAGE sql IMMUTABLE STRICT
AS $jsonb_stats_delta_flow_zero_plpgsql$
    SELECT CASE
        WHEN dt IN ('int_delta', 'nat_delta', 'dec2_delta', 'float_delta')
            THEN '{"count": 0, "sum": 0, "sum_sq": 0}'::jsonb
        WHEN dt = 'arr_delta' THEN '{"count": 0, "counts": {}}'::jsonb
        ELSE '{"counts": {}}'::jsonb
    END
$jsonb_stats_delta_flow_zero_plpgsql$;

-- Is a loose flow empty?
CREATE OR REPLACE FUNCTION jsonb_stats_delta_flow_empty_plpgsql(flow jsonb)
RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT
AS $jsonb_stats_delta_flow_empty_plpgsql$
    SELECT COALESCE((flow->>'count')::numeric, 0) = 0 AND (flow->'counts' IS NULL OR flow->'counts' = '{}')
$jsonb_stats_delta_flow_empty_plpgsql$;

-- Loose state -> canonical stats_delta (validates every flow).
CREATE OR REPLACE FUNCTION jsonb_stats_delta_out_plpgsql(state jsonb)
RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE
AS $jsonb_stats_delta_out_plpgsql$
DECLARE
    result jsonb := jsonb_build_object('type', 'stats_delta');
    key text; e jsonb; dt text; entry jsonb; side text; flow jsonb;
BEGIN
    FOR key, e IN SELECT * FROM jsonb_each(COALESCE(state, '{}')) LOOP
        dt := e->>'type';
        entry := jsonb_build_object('type', dt);
        FOREACH side IN ARRAY ARRAY['in', 'out'] LOOP
            IF dt IN ('int_delta', 'nat_delta', 'dec2_delta', 'float_delta') THEN
                flow := jsonb_stats_delta_num_flow_plpgsql(dt, key, side, e->side);
            ELSE
                flow := jsonb_stats_delta_cat_flow_plpgsql(dt, key, side, e->side);
            END IF;
            IF NOT jsonb_stats_delta_flow_empty_plpgsql(flow) THEN
                entry := entry || jsonb_build_object(side, flow);
            END IF;
        END LOOP;
        IF entry ? 'in' OR entry ? 'out' THEN
            result := result || jsonb_build_object(key, entry);
        END IF;
    END LOOP;
    RETURN result;
END;
$jsonb_stats_delta_out_plpgsql$;

-- Swap in and out of every entry of a loose state.
CREATE OR REPLACE FUNCTION jsonb_stats_delta_swap_plpgsql(state jsonb)
RETURNS jsonb
LANGUAGE sql IMMUTABLE STRICT
AS $jsonb_stats_delta_swap_plpgsql$
    SELECT COALESCE(jsonb_object_agg(key, jsonb_build_object('type', value->'type', 'in', value->'out', 'out', value->'in')), '{}')
    FROM jsonb_each(state)
$jsonb_stats_delta_swap_plpgsql$;

-- Entry event: a stats_agg (or stats, or stats_delta) as a stats_delta.
CREATE OR REPLACE FUNCTION jsonb_stats_to_delta_plpgsql(x jsonb)
RETURNS jsonb
LANGUAGE sql IMMUTABLE STRICT
AS $jsonb_stats_to_delta_plpgsql$
    SELECT jsonb_stats_delta_out_plpgsql(jsonb_stats_delta_state_add_plpgsql('{}', x, 1))
$jsonb_stats_to_delta_plpgsql$;

-- Exit event: the negation (in and out swapped).
CREATE OR REPLACE FUNCTION jsonb_stats_negate_plpgsql(x jsonb)
RETURNS jsonb
LANGUAGE sql IMMUTABLE STRICT
AS $jsonb_stats_negate_plpgsql$
    SELECT jsonb_stats_delta_out_plpgsql(jsonb_stats_delta_swap_plpgsql(
        jsonb_stats_delta_state_add_plpgsql('{}', x, 1)))
$jsonb_stats_negate_plpgsql$;

CREATE OR REPLACE FUNCTION jsonb_stats_delta_merge_plpgsql(a jsonb, b jsonb)
RETURNS jsonb
LANGUAGE sql IMMUTABLE STRICT
AS $jsonb_stats_delta_merge_plpgsql$
    SELECT jsonb_stats_delta_out_plpgsql(jsonb_stats_delta_state_add_plpgsql(
        jsonb_stats_delta_state_add_plpgsql('{}', a, 1), b, 1))
$jsonb_stats_delta_merge_plpgsql$;

CREATE OR REPLACE FUNCTION jsonb_stats_delta_sfunc_plpgsql(state jsonb, d jsonb)
RETURNS jsonb LANGUAGE sql IMMUTABLE
AS $jsonb_stats_delta_sfunc_plpgsql$ SELECT jsonb_stats_delta_state_add_plpgsql(state, d, 1) $jsonb_stats_delta_sfunc_plpgsql$;

CREATE OR REPLACE FUNCTION jsonb_stats_delta_minvfunc_plpgsql(state jsonb, d jsonb)
RETURNS jsonb LANGUAGE sql IMMUTABLE
AS $jsonb_stats_delta_minvfunc_plpgsql$ SELECT jsonb_stats_delta_state_add_plpgsql(state, d, -1) $jsonb_stats_delta_minvfunc_plpgsql$;

CREATE OR REPLACE FUNCTION jsonb_stats_delta_combine_plpgsql(a jsonb, b jsonb)
RETURNS jsonb LANGUAGE sql IMMUTABLE
AS $jsonb_stats_delta_combine_plpgsql$
    SELECT jsonb_stats_delta_state_add_plpgsql(a, jsonb_stats_delta_out_plpgsql(b), 1)
$jsonb_stats_delta_combine_plpgsql$;

CREATE AGGREGATE jsonb_stats_delta_agg_plpgsql(jsonb) (
    sfunc = jsonb_stats_delta_sfunc_plpgsql,
    stype = jsonb,
    initcond = '{}',
    finalfunc = jsonb_stats_delta_out_plpgsql,
    combinefunc = jsonb_stats_delta_combine_plpgsql,
    msfunc = jsonb_stats_delta_sfunc_plpgsql,
    minvfunc = jsonb_stats_delta_minvfunc_plpgsql,
    mstype = jsonb,
    minitcond = '{}',
    mfinalfunc = jsonb_stats_delta_out_plpgsql,
    parallel = safe
);

-- ── Reading a stock: jsonb_stats_delta_to_agg ──
--
-- jsonb_stats_delta_to_agg(stock, period) turns a running sum of deltas into
-- a stats_agg marked "min_max": false. stock is the sum of all changes up to
-- a date; period (optional) is the sum of the changes inside the last period,
-- so the stock before the period is stock - period.
--
-- Numeric keys: count, sum, sum_sq and the derived fields as in stats_agg
-- (mean and sum_sq_diff NULL when count = 0). With a period also
--   "in" / "out": {count, sum} of the period's flows,
--   change_pct = 100 * (sum - sum_prev) / |sum_prev|
-- rounded to 2 decimals half away from zero, NULL when sum_prev = 0.
-- Counted keys: net counts (arr: net count); with a period also in/out flows.
-- A key is shown when its stock is not empty or the period has flows for it.
-- Fails fast when the stock or the previous stock is not a valid state.

CREATE OR REPLACE FUNCTION jsonb_stats_delta_to_agg_plpgsql(stock jsonb, period jsonb DEFAULT NULL)
RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE
AS $jsonb_stats_delta_to_agg_plpgsql$
DECLARE
    st jsonb := jsonb_stats_delta_state_add_plpgsql('{}', stock, 1);
    pe jsonb := jsonb_stats_delta_state_add_plpgsql('{}', period, 1);
    result jsonb := jsonb_build_object('type', 'stats_agg', 'min_max', false);
    key text; dt text; kind text; se jsonb; pe_e jsonb;
    net jsonb; pnet jsonb; prev jsonb; pin jsonb; pout jsonb;
    out_e jsonb;
    c numeric; s numeric; q numeric; n_big numeric; s0 numeric;
    variance numeric; stddev numeric; cv numeric;
    k text; v jsonb; empty_period boolean;
BEGIN
    FOR key IN SELECT jsonb_object_keys(st) UNION SELECT jsonb_object_keys(pe) LOOP
        se := st->key;
        pe_e := pe->key;
        IF se IS NOT NULL AND pe_e IS NOT NULL AND se->>'type' <> pe_e->>'type' THEN
            RAISE EXCEPTION 'jsonb_stats: type mismatch for key ''%'': existing % vs incoming %',
                key, se->>'type', pe_e->>'type';
        END IF;
        dt := COALESCE(se->>'type', pe_e->>'type');
        kind := split_part(dt, '_', 1);
        IF se IS NULL THEN
            se := jsonb_build_object('type', dt,
                'in', jsonb_stats_delta_flow_zero_plpgsql(dt), 'out', jsonb_stats_delta_flow_zero_plpgsql(dt));
        END IF;
        net := jsonb_stats_delta_flow_add_plpgsql(se->'in', se->'out', -1);
        IF kind IN ('int', 'nat', 'dec2', 'float') THEN
            net := jsonb_stats_delta_num_flow_plpgsql(dt, key, 'stock', net);
        ELSE
            net := jsonb_stats_delta_cat_flow_plpgsql(dt, key, 'stock', net);
        END IF;
        empty_period := pe_e IS NULL
            OR (jsonb_stats_delta_flow_empty_plpgsql(pe_e->'in') AND jsonb_stats_delta_flow_empty_plpgsql(pe_e->'out'));
        IF period IS NOT NULL THEN
            IF pe_e IS NULL THEN
                pin := jsonb_stats_delta_flow_zero_plpgsql(dt);
                pout := pin;
            ELSE
                pin := pe_e->'in';
                pout := pe_e->'out';
            END IF;
            pnet := jsonb_stats_delta_flow_add_plpgsql(pin, pout, -1);
            prev := jsonb_stats_delta_flow_add_plpgsql(net, pnet, -1);
            IF kind IN ('int', 'nat', 'dec2', 'float') THEN
                prev := jsonb_stats_delta_num_flow_plpgsql(dt, key, 'previous stock', prev);
                pin := jsonb_stats_delta_num_flow_plpgsql(dt, key, 'in', pin);
                pout := jsonb_stats_delta_num_flow_plpgsql(dt, key, 'out', pout);
            ELSE
                prev := jsonb_stats_delta_cat_flow_plpgsql(dt, key, 'previous stock', prev);
                pin := jsonb_stats_delta_cat_flow_plpgsql(dt, key, 'in', pin);
                pout := jsonb_stats_delta_cat_flow_plpgsql(dt, key, 'out', pout);
            END IF;
        END IF;
        IF jsonb_stats_delta_flow_empty_plpgsql(net) AND empty_period THEN
            CONTINUE;
        END IF;
        IF kind IN ('int', 'nat', 'dec2', 'float') THEN
            c := (net->>'count')::numeric;
            s := (net->>'sum')::numeric;
            q := (net->>'sum_sq')::numeric;
            n_big := c * q - s * s;
            variance := NULL; stddev := NULL; cv := NULL;
            IF c > 1 THEN
                variance := jsonb_stats_round2_ratio_plpgsql(n_big, c * (c - 1));
                stddev := jsonb_stats_sqrt_round2_ratio_plpgsql(n_big, c * (c - 1));
                IF s <> 0 THEN
                    cv := sign(s) * jsonb_stats_sqrt_round2_ratio_plpgsql(10000 * n_big * c, (c - 1) * s * s);
                END IF;
            END IF;
            out_e := jsonb_build_object(
                'type', kind || '_agg', 'count', c, 'sum', net->'sum', 'sum_sq', net->'sum_sq',
                'mean', CASE WHEN c > 0 THEN jsonb_stats_round2_ratio_plpgsql(s, c) END,
                'sum_sq_diff', CASE WHEN c > 0 THEN jsonb_stats_round2_ratio_plpgsql(n_big, c) END,
                'variance', variance, 'stddev', stddev,
                'coefficient_of_variation_pct', cv);
            IF period IS NOT NULL THEN
                s0 := (prev->>'sum')::numeric;
                out_e := out_e || jsonb_build_object(
                    'in', jsonb_build_object('count', pin->'count', 'sum', pin->'sum'),
                    'out', jsonb_build_object('count', pout->'count', 'sum', pout->'sum'),
                    'change_pct', CASE WHEN s0 <> 0 THEN jsonb_stats_round2_ratio_plpgsql(100 * (s - s0), abs(s0)) END);
            END IF;
        ELSE
            out_e := jsonb_build_object('type', kind || '_agg') || net;
            IF period IS NOT NULL THEN
                out_e := out_e || jsonb_build_object('in', pin, 'out', pout);
            END IF;
        END IF;
        result := result || jsonb_build_object(key, out_e);
    END LOOP;
    RETURN result;
END;
$jsonb_stats_delta_to_agg_plpgsql$;

-- ════════════════════════════════════════════════════════════════════════
-- Finding what stands out: jsonb_stats_values, jsonb_stats_compare (3.1)
-- ════════════════════════════════════════════════════════════════════════

-- Variables as rows, so PostgreSQL's own aggregates (percentile_cont, ...)
-- and joins work on any variable. stats is read as jsonb_stats_to_agg(stats).
-- Numeric keys: value = sum, count = count. Counted keys: one row per
-- category (text_value) with its count. stats_delta is rejected.
CREATE OR REPLACE FUNCTION jsonb_stats_values_plpgsql(x jsonb)
RETURNS TABLE(key text, stat_type text, value numeric, text_value text, count bigint)
LANGUAGE plpgsql IMMUTABLE STRICT
AS $jsonb_stats_values_plpgsql$
DECLARE
    agg jsonb;
    k text; s jsonb; t text; c text; n jsonb;
    st record;
BEGIN
    IF jsonb_typeof(x) IS DISTINCT FROM 'object' THEN
        RAISE EXCEPTION 'jsonb_stats: jsonb_stats_values input must be an object';
    END IF;
    IF x->>'type' = 'stats' THEN
        agg := jsonb_stats_final_plpgsql(jsonb_stats_accum_plpgsql('{}', x));
    ELSIF x->>'type' = 'stats_delta' THEN
        RAISE EXCEPTION 'jsonb_stats: jsonb_stats_values cannot read a stats_delta; read it with jsonb_stats_delta_to_agg first';
    ELSIF x ? 'type' AND x->>'type' IS DISTINCT FROM 'stats_agg' THEN
        RAISE EXCEPTION 'jsonb_stats: expected stats or stats_agg, got type %', x->'type';
    ELSE
        agg := x;
    END IF;
    -- jsonb_each returns keys in jsonb order (length, then bytes)
    FOR k, s IN SELECT e.key, e.value FROM jsonb_each(agg) AS e LOOP
        CONTINUE WHEN k IN ('type', 'min_max');
        IF jsonb_typeof(s) IS DISTINCT FROM 'object' THEN
            RAISE EXCEPTION 'jsonb_stats: summary for key ''%'' must be an object', k;
        END IF;
        t := s->>'type';
        IF t IN ('int_agg', 'nat_agg', 'dec2_agg', 'float_agg') THEN
            st := jsonb_stats_num_state_or_empty_plpgsql(k, s);
            key := k; stat_type := split_part(t, '_', 1);
            value := st.sum_val; text_value := NULL; count := st.count_val;
            RETURN NEXT;
        ELSIF t IN ('str_agg', 'bool_agg', 'arr_agg', 'date_agg') THEN
            IF jsonb_typeof(s->'counts') IS DISTINCT FROM 'object' THEN
                RAISE EXCEPTION 'jsonb_stats: % for key ''%'' has no ''counts'' object', t, k;
            END IF;
            FOR c, n IN SELECT e.key, e.value FROM jsonb_each(s->'counts') AS e LOOP
                IF jsonb_typeof(n) IS DISTINCT FROM 'number' OR n::numeric < 0
                   OR n::numeric <> trunc(n::numeric) THEN
                    RAISE EXCEPTION 'jsonb_stats: % for key ''%'' has an invalid count for ''%''', t, k, c;
                END IF;
                key := k; stat_type := split_part(t, '_', 1);
                value := NULL; text_value := c; count := n::numeric;
                RETURN NEXT;
            END LOOP;
        ELSE
            RAISE EXCEPTION 'jsonb_stats: unknown aggregate type ''%''. Expected: int_agg, float_agg, dec2_agg, nat_agg, str_agg, bool_agg, arr_agg, date_agg', coalesce(t, '');
        END IF;
    END LOOP;
END;
$jsonb_stats_values_plpgsql$;

-- Numeric state of a summary that may be an empty stock (count 0, from
-- jsonb_stats_delta_to_agg): then sum and sum_sq must be 0, and sum keeps
-- its printed scale. NULL summary = empty. Otherwise as num_state.
CREATE OR REPLACE FUNCTION jsonb_stats_num_state_or_empty_plpgsql(
    key text, summary jsonb,
    OUT count_val numeric, OUT sum_val numeric, OUT sum_sq_val numeric)
LANGUAGE plpgsql IMMUTABLE
AS $jsonb_stats_num_state_or_empty_plpgsql$
DECLARE
    st record;
BEGIN
    IF summary IS NULL THEN
        count_val := 0; sum_val := 0; sum_sq_val := 0;
    ELSIF jsonb_typeof(summary->'count') = 'number' AND (summary->'count')::numeric = 0 THEN
        IF jsonb_typeof(summary->'sum') IS DISTINCT FROM 'number'
           OR jsonb_typeof(summary->'sum_sq') IS DISTINCT FROM 'number'
           OR (summary->'sum')::numeric <> 0 OR (summary->'sum_sq')::numeric <> 0 THEN
            RAISE EXCEPTION 'jsonb_stats: % for key ''%'' has count 0 but a non-zero or missing sum/sum_sq', summary->>'type', key;
        END IF;
        count_val := 0; sum_val := (summary->'sum')::numeric; sum_sq_val := (summary->'sum_sq')::numeric;
    ELSE
        st := jsonb_stats_num_state_plpgsql(key, summary);
        count_val := st.count_val; sum_val := st.sum_val; sum_sq_val := st.sum_sq_val;
    END IF;
END;
$jsonb_stats_num_state_or_empty_plpgsql$;

-- Compare two stats_agg, a = reference (previous period or peers),
-- b = subject. Per numeric key present in either (a missing side is empty):
--   change_pct      = 100 (sum_b - sum_a) / |sum_a|          NULL if sum_a = 0
--   mean_change_pct = 100 (mean_b - mean_a) / |mean_a|       NULL if a count is 0 or sum_a = 0
--   z = (mean_b - mean_a) / sqrt(var_a/n_a + var_b/n_b)      NULL if n_a < 2 or n_b < 2
--                                                            or both variances are 0
-- With N = n sum_sq - sum^2 (var = N / (n (n-1))):
--   z^2 = (S_b n_a - S_a n_b)^2 (n_a-1)(n_b-1) / (N_a n_b^2 (n_b-1) + N_b n_a^2 (n_a-1))
-- All rounded once to 2 decimals, half away from zero. Counted keys are skipped.
CREATE OR REPLACE FUNCTION jsonb_stats_compare_plpgsql(a jsonb, b jsonb)
RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE STRICT
AS $jsonb_stats_compare_plpgsql$
DECLARE
    result jsonb := jsonb_build_object('type', 'stats_compare');
    k text; sa jsonb; sb jsonb; t text;
    x record; y record;
    na numeric; nb numeric; s_a numeric; s_b numeric; big_a numeric; big_b numeric;
    d numeric; den numeric; z numeric; mean_change numeric;
BEGIN
    IF jsonb_typeof(a) IS DISTINCT FROM 'object' OR jsonb_typeof(b) IS DISTINCT FROM 'object' THEN
        RAISE EXCEPTION 'jsonb_stats: jsonb_stats_compare inputs must be objects';
    END IF;
    IF a->>'type' IS DISTINCT FROM 'stats_agg' OR b->>'type' IS DISTINCT FROM 'stats_agg' THEN
        RAISE EXCEPTION 'jsonb_stats: jsonb_stats_compare expects two stats_agg, got % and %', a->'type', b->'type';
    END IF;
    FOR k IN SELECT e.key FROM jsonb_each(a) AS e UNION SELECT e.key FROM jsonb_each(b) AS e ORDER BY 1 LOOP
        CONTINUE WHEN k IN ('type', 'min_max');
        sa := a->k; sb := b->k;
        IF sa IS NOT NULL AND jsonb_typeof(sa) IS DISTINCT FROM 'object' THEN
            RAISE EXCEPTION 'jsonb_stats: summary for key ''%'' must be an object', k;
        END IF;
        IF sb IS NOT NULL AND jsonb_typeof(sb) IS DISTINCT FROM 'object' THEN
            RAISE EXCEPTION 'jsonb_stats: summary for key ''%'' must be an object', k;
        END IF;
        IF sa IS NOT NULL AND sb IS NOT NULL AND sa->>'type' IS DISTINCT FROM sb->>'type' THEN
            RAISE EXCEPTION 'jsonb_stats: type mismatch for key ''%'': existing % vs incoming %', k, sa->>'type', sb->>'type';
        END IF;
        t := COALESCE(sa->>'type', sb->>'type');
        IF t IN ('str_agg', 'bool_agg', 'arr_agg', 'date_agg') THEN
            CONTINUE;
        ELSIF t NOT IN ('int_agg', 'nat_agg', 'dec2_agg', 'float_agg') OR t IS NULL THEN
            RAISE EXCEPTION 'jsonb_stats: unknown aggregate type ''%''. Expected: int_agg, float_agg, dec2_agg, nat_agg, str_agg, bool_agg, arr_agg, date_agg', coalesce(t, '');
        END IF;
        x := jsonb_stats_num_state_or_empty_plpgsql(k, sa);
        y := jsonb_stats_num_state_or_empty_plpgsql(k, sb);
        na := x.count_val; nb := y.count_val; s_a := x.sum_val; s_b := y.sum_val;
        big_a := na * x.sum_sq_val - s_a * s_a;
        big_b := nb * y.sum_sq_val - s_b * s_b;
        mean_change := NULL;
        IF na > 0 AND nb > 0 AND s_a <> 0 THEN
            -- (S_b/n_b - S_a/n_a) / |S_a/n_a| = (S_b n_a - S_a n_b) / (n_b |S_a|)
            mean_change := jsonb_stats_round2_ratio_plpgsql(100 * (s_b * na - s_a * nb), nb * abs(s_a));
        END IF;
        z := NULL;
        IF na >= 2 AND nb >= 2 THEN
            d := s_b * na - s_a * nb;
            den := big_a * nb * nb * (nb - 1) + big_b * na * na * (na - 1);
            IF den > 0 THEN
                z := sign(d) * jsonb_stats_sqrt_round2_ratio_plpgsql(d * d * (na - 1) * (nb - 1), den);
            END IF;
        END IF;
        result := result || jsonb_build_object(k, jsonb_build_object(
            'type', split_part(t, '_', 1),
            'count_a', na,
            'count_b', nb,
            'change_pct', CASE WHEN s_a <> 0 THEN jsonb_stats_round2_ratio_plpgsql(100 * (s_b - s_a), abs(s_a)) END,
            'mean_change_pct', mean_change,
            'z', z));
    END LOOP;
    RETURN result;
END;
$jsonb_stats_compare_plpgsql$;
