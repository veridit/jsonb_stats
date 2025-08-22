--
-- This file contains the reference PL/pgSQL implementation for jsonb_stats aggregates,
-- adapted from the statbus project. These functions serve as the "gold standard"
-- for testing the C implementation.
--

-- Accumulation function (Level 2: stats -> stats_summary)
CREATE OR REPLACE FUNCTION jsonb_stats_summary_accum_plpgsql(state jsonb, stats jsonb)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
    new_state jsonb := state;
    stat_key text;
    stat_obj jsonb;
    stat_type text;
    stat_val_str text;
    stat_val_numeric numeric;
    current_summary jsonb;
    new_summary jsonb;
    -- For integer summary
    sum_val numeric; count_val int; mean_val numeric; min_val numeric; max_val numeric; sum_sq_diff_val numeric; delta numeric;
    -- For text/boolean summary
    counts jsonb; val_key text; current_count int;
    -- For array summary
    elements_count int; element text;
BEGIN
    FOR stat_key, stat_obj IN SELECT * FROM jsonb_each(stats) LOOP
        current_summary := new_state->stat_key;
        stat_type := stat_obj->>'type';
        stat_val_str := stat_obj->>'value';

        IF current_summary IS NULL THEN
            -- INIT PATH
            IF stat_type = 'integer' THEN
                stat_val_numeric := stat_val_str::numeric;
                new_summary := jsonb_build_object(
                    'type', 'integer_summary', 'count', 1, 'sum', stat_val_numeric, 'min', stat_val_numeric, 'max', stat_val_numeric,
                    'mean', stat_val_numeric, 'sum_sq_diff', 0
                );
            ELSIF stat_type = 'text' OR stat_type = 'boolean' THEN
                new_summary := jsonb_build_object(
                    'type', stat_type || '_summary', 'counts', jsonb_build_object(stat_val_str, 1)
                );
            ELSIF stat_type LIKE '%[]' THEN
                new_summary := jsonb_build_object(
                    'type', 'array_summary', 'count', 1, 'elements_count', 0, 'counts', '{}'::jsonb
                );
                IF stat_val_str != '{}' THEN
                    FOR element IN SELECT unnest(string_to_array(trim(stat_val_str, '{}'), ',')) LOOP
                        new_summary := jsonb_set(new_summary, '{counts, '||element||'}', to_jsonb(1));
                        new_summary := jsonb_set(new_summary, '{elements_count}', to_jsonb((new_summary->>'elements_count')::int + 1));
                    END LOOP;
                END IF;
            END IF;
        ELSE
            -- UPDATE PATH
            new_summary := current_summary;
            IF stat_type = 'integer' THEN
                stat_val_numeric := stat_val_str::numeric;
                sum_val := (current_summary->>'sum')::numeric + stat_val_numeric;
                count_val := (current_summary->>'count')::int + 1;
                mean_val := (current_summary->>'mean')::numeric;
                delta := stat_val_numeric - mean_val;
                mean_val := mean_val + delta / count_val;
                min_val := LEAST((current_summary->>'min')::numeric, stat_val_numeric);
                max_val := GREATEST((current_summary->>'max')::numeric, stat_val_numeric);
                sum_sq_diff_val := (current_summary->>'sum_sq_diff')::numeric + delta * (stat_val_numeric - mean_val);
                new_summary := jsonb_build_object(
                    'type', 'integer_summary', 'count', count_val, 'sum', sum_val, 'min', min_val, 'max', max_val,
                    'mean', mean_val, 'sum_sq_diff', sum_sq_diff_val
                );
            ELSIF stat_type = 'text' OR stat_type = 'boolean' THEN
                counts := current_summary->'counts';
                current_count := COALESCE((counts->>stat_val_str)::int, 0);
                new_summary := jsonb_set(new_summary, '{counts, '||stat_val_str||'}', to_jsonb(current_count + 1));
            ELSIF stat_type LIKE '%[]' THEN
                new_summary := new_summary || jsonb_build_object('count', (current_summary->>'count')::int + 1);
                IF stat_val_str != '{}' THEN
                    FOR element IN SELECT unnest(string_to_array(trim(stat_val_str, '{}'), ',')) LOOP
                        current_count := COALESCE((new_summary->'counts'->>element)::int, 0);
                        new_summary := jsonb_set(new_summary, '{counts, '||element||'}', to_jsonb(current_count + 1));
                        new_summary := jsonb_set(new_summary, '{elements_count}', to_jsonb((new_summary->>'elements_count')::int + 1));
                    END LOOP;
                END IF;
            END IF;
        END IF;
        new_state := jsonb_set(new_state, ARRAY[stat_key], new_summary, true);
    END LOOP;
    RETURN new_state;
END;
$$;

-- Merge function (Level 3: stats_summary -> stats_summary)
CREATE OR REPLACE FUNCTION jsonb_stats_summary_merge_plpgsql(a jsonb, b jsonb)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
    merged_state jsonb := a;
    summary_key text; summary_a jsonb; summary_b jsonb; merged_summary jsonb; type_a text;
    -- Integer summary
    count_a numeric; count_b numeric; total_count numeric; mean_a numeric; mean_b numeric; delta numeric;
    -- Text/boolean/array summary
    counts_a jsonb; counts_b jsonb; k text; v jsonb;
BEGIN
    FOR summary_key, summary_b IN SELECT * FROM jsonb_each(b) LOOP
        summary_a := merged_state->summary_key;
        IF summary_a IS NULL THEN
            merged_summary := summary_b;
        ELSE
            type_a := summary_a->>'type'; merged_summary := summary_a;
            IF type_a = 'integer_summary' THEN
                count_a := (summary_a->>'count')::numeric; count_b := (summary_b->>'count')::numeric; total_count := count_a + count_b;
                mean_a := (summary_a->>'mean')::numeric; mean_b := (summary_b->>'mean')::numeric; delta := mean_b - mean_a;
                merged_summary := jsonb_build_object(
                    'type', type_a, 'count', total_count,
                    'sum', (summary_a->>'sum')::numeric + (summary_b->>'sum')::numeric,
                    'min', LEAST((summary_a->>'min')::numeric, (summary_b->>'min')::numeric),
                    'max', GREATEST((summary_a->>'max')::numeric, (summary_b->>'max')::numeric),
                    'mean', (mean_a * count_a + mean_b * count_b) / total_count,
                    'sum_sq_diff', (summary_a->>'sum_sq_diff')::numeric + (summary_b->>'sum_sq_diff')::numeric + (delta^2 * count_a * count_b) / total_count
                );
            ELSIF type_a IN ('text_summary', 'boolean_summary', 'array_summary') THEN
                merged_summary := summary_a || jsonb_build_object('count', (summary_a->>'count')::int + (summary_b->>'count')::int);
                IF type_a = 'array_summary' THEN
                    merged_summary := merged_summary || jsonb_build_object('elements_count', (summary_a->>'elements_count')::int + (summary_b->>'elements_count')::int);
                END IF;
                counts_a := summary_a->'counts'; counts_b := summary_b->'counts';
                FOR k, v IN SELECT * FROM jsonb_each(counts_b) LOOP
                    counts_a := jsonb_set(counts_a, ARRAY[k], to_jsonb(COALESCE((counts_a->>k)::int, 0) + (v->>0)::int));
                END LOOP;
                merged_summary := jsonb_set(merged_summary, '{counts}', counts_a);
            END IF;
        END IF;
        merged_state := jsonb_set(merged_state, ARRAY[summary_key], merged_summary, true);
    END LOOP;
    RETURN merged_state;
END;
$$;

-- Final function to add derived stats and round numerics
CREATE OR REPLACE FUNCTION jsonb_stats_summary_final_plpgsql(state jsonb)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
    key text; summary jsonb; result jsonb := '{}';
    -- For integer summary
    count_val numeric; mean_val numeric; sum_sq_diff_val numeric; variance numeric; stddev numeric;
BEGIN
    FOR key, summary IN SELECT * FROM jsonb_each(state) LOOP
        IF summary->>'type' = 'integer_summary' THEN
            count_val := (summary->>'count')::numeric;
            mean_val := (summary->>'mean')::numeric;
            sum_sq_diff_val := (summary->>'sum_sq_diff')::numeric;
            variance := CASE WHEN count_val > 1 THEN sum_sq_diff_val / (count_val - 1) ELSE NULL END;
            stddev := CASE WHEN variance IS NOT NULL THEN sqrt(variance) ELSE NULL END;
            summary := summary
                || jsonb_build_object('variance', variance, 'stddev', stddev)
                || jsonb_build_object('mean', round(mean_val, 2))
                || jsonb_build_object('sum_sq_diff', round(sum_sq_diff_val, 2))
                || jsonb_build_object('variance', round(variance, 2))
                || jsonb_build_object('stddev', round(stddev, 2));
        END IF;
        result := result || jsonb_build_object(key, summary);
    END LOOP;
    RETURN result;
END;
$$;

CREATE AGGREGATE jsonb_stats_summary_agg_plpgsql(jsonb) (
    sfunc = jsonb_stats_summary_accum_plpgsql,
    stype = jsonb,
    initcond = '{}',
    finalfunc = jsonb_stats_summary_final_plpgsql
);

CREATE AGGREGATE jsonb_stats_summary_merge_agg_plpgsql(jsonb) (
    sfunc = jsonb_stats_summary_merge_plpgsql,
    stype = jsonb,
    initcond = '{}',
    finalfunc = jsonb_stats_summary_final_plpgsql
);
