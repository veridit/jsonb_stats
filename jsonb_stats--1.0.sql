-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION jsonb_stats" to load this file. \quit

--------------------------------------------------------------------------------
-- PL/pgSQL Implementation
--------------------------------------------------------------------------------

-- Helper to create a stat object
CREATE FUNCTION stat(value anyelement)
RETURNS jsonb AS $$
BEGIN
    RETURN jsonb_build_object(
        'type', pg_typeof(value)::text,
        'value', to_jsonb(value)
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

-- Level 1: stat -> stats
CREATE FUNCTION jsonb_stats_sfunc(state jsonb, code text, stat_val jsonb)
RETURNS jsonb AS $$
BEGIN
    RETURN state || jsonb_build_object(code, stat_val);
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

CREATE AGGREGATE jsonb_stats_agg(text, jsonb) (
    sfunc = jsonb_stats_sfunc,
    stype = jsonb,
    initcond = '{}'
);

-- Level 2: stats -> stats_summary
CREATE FUNCTION jsonb_stats_summary_accum(state jsonb, stats jsonb)
RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE
    stat_key text;
    stat_val jsonb;
    stat_type text;
    stat_raw_val jsonb;
    prev_stat_state jsonb;
    prev_stat_type text;
    next_stat_state jsonb;
BEGIN
    FOR stat_key, stat_val IN SELECT * FROM jsonb_each(stats) LOOP
        stat_type := stat_val->>'type';
        stat_raw_val := stat_val->'value';

        IF state ? stat_key THEN
            prev_stat_state := state->stat_key;
            prev_stat_type := prev_stat_state->>'type';

            IF stat_type <> split_part(prev_stat_type, '_', 1) THEN
                RAISE EXCEPTION 'Type mismatch for key "%": % vs %', stat_key, stat_type, split_part(prev_stat_type, '_', 1);
            END IF;

            CASE stat_type
                WHEN 'integer', 'numeric', 'real', 'double precision' THEN
                    DECLARE
                        sum numeric := (prev_stat_state->'sum')::numeric + stat_raw_val::numeric;
                        count integer := (prev_stat_state->'count')::integer + 1;
                        delta numeric := stat_raw_val::numeric - (prev_stat_state->'mean')::numeric;
                        mean numeric := (prev_stat_state->'mean')::numeric + delta / count;
                        min numeric := LEAST((prev_stat_state->'min')::numeric, stat_raw_val::numeric);
                        max numeric := GREATEST((prev_stat_state->'max')::numeric, stat_raw_val::numeric);
                        sum_sq_diff numeric := (prev_stat_state->'sum_sq_diff')::numeric + delta * (stat_raw_val::numeric - mean);
                    BEGIN
                        next_stat_state := prev_stat_state || jsonb_build_object(
                            'sum', sum, 'count', count, 'mean', mean, 'min', min, 'max', max, 'sum_sq_diff', sum_sq_diff
                        );
                    END;
                WHEN 'text' THEN
                    next_stat_state := jsonb_set(
                        prev_stat_state,
                        ARRAY['counts', stat_raw_val->>0],
                        to_jsonb(COALESCE((prev_stat_state->'counts'->(stat_raw_val->>0))::integer, 0) + 1)
                    );
                WHEN 'boolean' THEN
                    next_stat_state := jsonb_set(
                        prev_stat_state,
                        ARRAY['counts', stat_raw_val::text],
                        to_jsonb(COALESCE((prev_stat_state->'counts'->(stat_raw_val::text))::integer, 0) + 1)
                    );
                ELSE
                    next_stat_state := prev_stat_state;
            END CASE;
        ELSE
            -- Initialize new entry in state
            next_stat_state := jsonb_build_object('type', stat_type || '_summary');
            CASE stat_type
                WHEN 'integer', 'numeric', 'real', 'double precision' THEN
                    next_stat_state := next_stat_state || jsonb_build_object(
                        'sum', stat_raw_val::numeric, 'count', 1, 'mean', stat_raw_val::numeric,
                        'min', stat_raw_val::numeric, 'max', stat_raw_val::numeric, 'sum_sq_diff', 0
                    );
                WHEN 'text' THEN
                    next_stat_state := next_stat_state || jsonb_build_object('counts', jsonb_build_object(stat_raw_val->>0, 1));
                WHEN 'boolean' THEN
                    next_stat_state := next_stat_state || jsonb_build_object('counts', jsonb_build_object(stat_raw_val::text, 1));
                ELSE
                    -- Unsupported type, store minimally
            END CASE;
        END IF;
        state := state || jsonb_build_object(stat_key, next_stat_state);
    END LOOP;
    RETURN state;
END;
$$;

-- Level 3: stats_summary -> stats_summary
CREATE FUNCTION jsonb_stats_summary_merge(a jsonb, b jsonb)
RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE
    key text;
    val_a jsonb;
    val_b jsonb;
    type_a text;
    type_b text;
    merged_val jsonb;
BEGIN
    -- Handle keys in a
    FOR key, val_a IN SELECT * FROM jsonb_each(a) LOOP
        IF b ? key THEN
            val_b := b->key;
            type_a := val_a->>'type';
            type_b := val_b->>'type';

            IF type_a <> type_b THEN
                RAISE EXCEPTION 'Type mismatch for key "%": % vs %', key, type_a, type_b;
            END IF;

            CASE split_part(type_a, '_', 1)
                WHEN 'integer', 'numeric', 'real', 'double precision' THEN
                    DECLARE
                        count_a INTEGER := (val_a->'count')::INTEGER;
                        count_b INTEGER := (val_b->'count')::INTEGER;
                        total_count INTEGER := count_a + count_b;
                        mean_a NUMERIC := (val_a->'mean')::NUMERIC;
                        mean_b NUMERIC := (val_b->'mean')::NUMERIC;
                        delta NUMERIC := mean_b - mean_a;
                    BEGIN
                        merged_val := val_a || jsonb_build_object(
                            'sum', (val_a->'sum')::numeric + (val_b->'sum')::numeric,
                            'count', total_count,
                            'mean', (mean_a * count_a + mean_b * count_b) / total_count,
                            'min', LEAST((val_a->'min')::numeric, (val_b->'min')::numeric),
                            'max', GREATEST((val_a->'max')::numeric, (val_b->'max')::numeric),
                            'sum_sq_diff', (val_a->'sum_sq_diff')::numeric + (val_b->'sum_sq_diff')::numeric + delta * delta * count_a * count_b / total_count
                        );
                    END;
                WHEN 'text', 'boolean' THEN
                    DECLARE
                        merged_counts jsonb := val_a->'counts';
                        k text;
                        v jsonb;
                    BEGIN
                        FOR k, v IN SELECT * FROM jsonb_each(val_b->'counts') LOOP
                            merged_counts := jsonb_set(
                                merged_counts,
                                ARRAY[k],
                                to_jsonb(COALESCE((merged_counts->k)::integer, 0) + v::integer)
                            );
                        END LOOP;
                        merged_val := jsonb_set(val_a, ARRAY['counts'], merged_counts);
                    END;
                ELSE
                    merged_val := val_a;
            END CASE;
            a := jsonb_set(a, ARRAY[key], merged_val);
        END IF;
    END LOOP;

    -- Add keys only in b
    FOR key, val_b IN SELECT b_data.key, b_data.value FROM jsonb_each(b) AS b_data WHERE NOT (a ? b_data.key) LOOP
        a := a || jsonb_build_object(key, val_b);
    END LOOP;

    RETURN a;
END;
$$;

-- Final function to round numeric fields
CREATE FUNCTION jsonb_stats_to_summary_round(state jsonb)
RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE
    key text;
    val jsonb;
    type text;
    result jsonb := '{}';
BEGIN
    FOR key, val IN SELECT * FROM jsonb_each(state) LOOP
        type := val->>'type';
        IF type LIKE '%_summary' THEN
            CASE split_part(type, '_', 1)
                WHEN 'integer', 'numeric', 'real', 'double precision' THEN
                    val := val || jsonb_build_object(
                        'mean', round((val->'mean')::numeric, 2),
                        'sum_sq_diff', round((val->'sum_sq_diff')::numeric, 2)
                    );
                ELSE
                    -- No rounding for other types
            END CASE;
        END IF;
        result := result || jsonb_build_object(key, val);
    END LOOP;
    RETURN result;
END;
$$;


CREATE AGGREGATE jsonb_stats_summary_agg(jsonb) (
    sfunc = jsonb_stats_summary_accum,
    stype = jsonb,
    initcond = '{}',
    finalfunc = jsonb_stats_to_summary_round
);

CREATE AGGREGATE jsonb_stats_summary_merge_agg(jsonb) (
    sfunc = jsonb_stats_summary_merge,
    stype = jsonb,
    initcond = '{}',
    finalfunc = jsonb_stats_to_summary_round
);


--------------------------------------------------------------------------------
-- C Implementation (Stubs)
--------------------------------------------------------------------------------

CREATE FUNCTION stat_c(anyelement)
RETURNS jsonb
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION jsonb_stats_sfunc_c(jsonb, text, jsonb)
RETURNS jsonb
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE AGGREGATE jsonb_stats_agg_c(text, jsonb) (
    sfunc = jsonb_stats_sfunc_c,
    stype = jsonb,
    initcond = '{}'
);

CREATE FUNCTION jsonb_stats_summary_accum_c(jsonb, jsonb)
RETURNS jsonb
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION jsonb_stats_to_summary_round_c(jsonb)
RETURNS jsonb
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE AGGREGATE jsonb_stats_summary_agg_c(jsonb) (
    sfunc = jsonb_stats_summary_accum_c,
    stype = jsonb,
    initcond = '{}',
    finalfunc = jsonb_stats_to_summary_round_c
);

CREATE FUNCTION jsonb_stats_summary_merge_c(jsonb, jsonb)
RETURNS jsonb
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE AGGREGATE jsonb_stats_summary_merge_agg_c(jsonb) (
    sfunc = jsonb_stats_summary_merge_c,
    stype = jsonb,
    initcond = '{}',
    finalfunc = jsonb_stats_to_summary_round_c
);
