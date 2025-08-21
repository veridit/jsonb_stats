You are making a new postgres extension called `jsonb_stats`.

The purpose is to support a `stat jsonb` column that contains a single statistical value,
with its corresponding database type.
The `stat` can be aggregated into `stats jsonb` to support keeping a dynamic number of statistical
variables for a row, without having to change the schema.
These can again be aggregated over time into `stats_summary` that no longer keep the individual stats,
but provide summarised information in a way that is particularly suited to combination calculations.

## Example scenario

This example demonstrates how `jsonb_stats` can be used to track statistics for legal units over time.

### Source Tables

First, let's define our source tables and populate them with some data.

*   `legal_unit`: Holds basic information about a company.
*   `stat_for_unit`: A generic table to hold different statistical values for each company. This is where we will use our new `stat` type.

```sql
-- The legal_unit table tracks companies and their validity periods.
CREATE TABLE legal_unit (
    legal_unit_id INT,
    name TEXT,
    region TEXT,
    valid_from DATE,
    valid_to DATE
);

INSERT INTO legal_unit (legal_unit_id, name, region, valid_from, valid_to) VALUES
(1, 'Company A', 'EU', '2023-01-01', '2023-12-31'),
(2, 'Company B', 'US', '2023-01-01', '2023-12-31'),
(3, 'Company C', 'EU', '2023-01-01', '2023-12-31'),
(1, 'Company A Rev.', 'EU', '2024-01-01', NULL); -- Company A was revised for the next period

-- The stat_for_unit table holds raw statistical data.
-- We imagine a `stat` column that would be created by the extension.
CREATE TABLE stat_for_unit (
    legal_unit_id INT,
    code TEXT,
    value_int INT,
    value_bool BOOLEAN,
    value_text TEXT,
    value_date DATE,
    stat JSONB GENERATED ALWAYS AS (
        CASE
            WHEN value_int IS NOT NULL THEN to_stat(value_int)
            WHEN value_bool IS NOT NULL THEN to_stat(value_bool)
            WHEN value_text IS NOT NULL THEN to_stat(value_text)
            WHEN value_date IS NOT NULL THEN to_stat(value_date)
        END
    ) STORED,
    CONSTRAINT one_value_must_be_set CHECK (
        (value_int IS NOT NULL)::int +
        (value_bool IS NOT NULL)::int +
        (value_text IS NOT NULL)::int +
        (value_date IS NOT NULL)::int = 1
    )
);

-- This helper function simulates how the extension would create a `stat` object.
-- The real extension would provide a C-function `stat(value)`.
CREATE OR REPLACE FUNCTION to_stat(value anyelement) RETURNS jsonb AS $$
BEGIN
    RETURN jsonb_build_object(
        'type', pg_typeof(value)::text,
        'value', to_jsonb(value)
    );
END;
$$ LANGUAGE plpgsql;


-- Populate with data for the 2023 period
INSERT INTO stat_for_unit (legal_unit_id, code, value_int, value_bool, value_text) VALUES
(1, 'num_employees', 150, NULL, NULL),
(1, 'is_profitable', NULL, true, NULL),
(1, 'industry', NULL, NULL, 'tech'),
(2, 'num_employees', 2500, NULL, NULL),
(2, 'is_profitable', NULL, true, NULL),
(2, 'industry', NULL, NULL, 'finance'),
(3, 'num_employees', 50, NULL, NULL),
(3, 'is_profitable', NULL, false, NULL),
(3, 'industry', NULL, NULL, 'tech');
```

### Materialised Views for Aggregation

Now, we'll create materialised views to show how the aggregation functions would work.

#### `legal_unit_history`

This view aggregates all the individual `stat` values for a legal unit into a single `stats` jsonb object. This is the first level of aggregation.

```sql
-- This view would use a `jsonb_stats_agg(stat)` function from the extension.
-- For this example, we'll simulate its behavior using `jsonb_object_agg`.
CREATE MATERIALIZED VIEW legal_unit_history AS
SELECT
    lu.legal_unit_id,
    lu.name,
    lu.region,
    lu.valid_from,
    lu.valid_to,
    (
        SELECT jsonb_object_agg(sfu.code, sfu.stat)
        FROM stat_for_unit sfu
        WHERE sfu.legal_unit_id = lu.legal_unit_id
    ) AS stats -- `stats` is an aggregation of `stat` objects for one unit
FROM
    legal_unit lu
WHERE lu.valid_from = '2023-01-01';

-- After running this, the `stats` column for 'Company A' would look like:
-- {
--   "num_employees": {"type": "integer", "value": 150},
--   "is_profitable": {"type": "boolean", "value": true},
--   "industry":      {"type": "text",    "value": "tech"}
-- }
```

#### `history_facet`

This view provides a summary faceted by region. This allows for drilling down into specific regions or comparing them. The `stats_summary` is structured to be further aggregatable, for example, to get a global summary by combining regional summaries.

```sql
-- This view aggregates stats at the region level.
-- This would use a `jsonb_stats_summary_agg(stats)` function.
CREATE MATERIALIZED VIEW history_facet AS
SELECT
    luh.valid_from,
    luh.valid_to,
    luh.region,
    jsonb_stats_summary_agg(luh.stats) as stats_summary
FROM legal_unit_history luh
WHERE luh.valid_from = '2023-01-01'
GROUP BY luh.valid_from, luh.valid_to, luh.region;

-- We will simulate the output for this view.
-- The real extension would provide `jsonb_stats_summary_agg`.
-- For now, we will show the expected results per region.

-- EU region summary for 2023:
-- {
--   "num_employees": {"type": "integer_summary", "count": 2, "sum": 200, "min": 50, "max": 150},
--   "is_profitable": {"type": "boolean_summary", "true_count": 1, "false_count": 1},
--   "industry":      {"type": "text_summary", "distinct_values": 1, "values": ["tech"]}
-- }

-- US region summary for 2023:
-- {
--   "num_employees": {"type": "integer_summary", "count": 1, "sum": 2500, "min": 2500, "max": 2500},
--   "is_profitable": {"type": "boolean_summary", "true_count": 1, "false_count": 0},
--   "industry":      {"type": "text_summary", "distinct_values": 1, "values": ["finance"]}
-- }
```

#### `history`

This view takes the aggregation one step further to a global `stats_summary`. This summary doesn't keep individual values but provides summary statistics, like counts, sums, and distinct values, making it ideal for analytics. Importantly, this global summary can be calculated by combining the regional summaries from the `history_facet` view, which is a key feature of the `jsonb_stats` extension.

```sql
-- This view would use a `jsonb_stats_summary_agg(stats)` function on all stats,
-- or a `jsonb_stats_summary_combine_agg(stats_summary)` on the faceted summaries.
-- This is a more complex aggregation that computes summaries across many units.
-- For example, for numeric types it might calculate min, max, sum, and count.
-- For boolean types, it might count true/false values.
-- For text types, it might count distinct values.

-- We'll simulate the output of this aggregation for the 2023 period.
CREATE MATERIALIZED VIEW history AS
SELECT
    '2023-01-01'::DATE AS valid_from,
    '2023-12-31'::DATE AS valid_to,
    jsonb_build_object(
        'num_employees', jsonb_build_object(
            'type', 'integer_summary',
            'count', 3,
            'sum', 2700,
            'min', 50,
            'max', 2500
        ),
        'is_profitable', jsonb_build_object(
            'type', 'boolean_summary',
            'true_count', 2,
            'false_count', 1
        ),
        'industry', jsonb_build_object(
            'type', 'text_summary',
            'distinct_values', 2,
            'values', jsonb_build_array('tech', 'finance')
        )
    ) as stats_summary
FROM legal_unit_history
WHERE valid_from = '2023-01-01'
GROUP BY valid_from, valid_to;

-- The resulting `stats_summary` provides a high-level overview
-- without storing all the individual data points, making it
-- efficient for reporting on large datasets.
```

### Relevant Code from `statbus`

The `statbus` project contains a PL/pgSQL implementation of the aggregation logic that can serve as a reference for the C implementation in this extension.

#### Core Logic

The core logic for the second-level (`stats` -> `stats_summary`) and third-level (`stats_summary` -> combined `stats_summary`) aggregations can be found in these files:

-   `statbus/migrations/20240229000000_create_jsonb_stats_to_summary.up.sql`: Provides `jsonb_stats_to_summary`, which will become `jsonb_stats_summary_agg`.
-   `statbus/migrations/20240301000000_create_jsonb_stats_summary_merge.up.sql`: Provides `jsonb_stats_summary_merge`, which will become `jsonb_stats_summary_combine_agg`.

Here is the reference code:

```sql
-- From ...create_jsonb_stats_to_summary.up.sql
CREATE OR REPLACE FUNCTION public.jsonb_stats_to_summary(state jsonb, stats jsonb)
RETURNS jsonb
LANGUAGE plpgsql
STABLE STRICT
PARALLEL SAFE
COST 100
AS $$
DECLARE
    prev_stat_state jsonb;
    stat_key text;
    stat_value jsonb;
    stat_type text;
    prev_stat_type text;
    next_stat_state jsonb;
    state_type text;
    stats_type text;
BEGIN
    IF state IS NULL OR stats IS NULL THEN
        RAISE EXCEPTION 'Logic error: STRICT function should never be called with NULL';
    END IF;

    state_type := jsonb_typeof(state);
    IF state_type <> 'object' THEN
        RAISE EXCEPTION 'Type mismatch for state "%": % <> object', state, state_type;
    END IF;

    stats_type := jsonb_typeof(stats);
    IF stats_type <> 'object' THEN
        RAISE EXCEPTION 'Type mismatch for stats "%": % <> object', stats, stats_type;
    END IF;

    -- Update state with data from `value`
    FOR stat_key, stat_value IN SELECT * FROM jsonb_each(stats) LOOP
        stat_type := jsonb_typeof(stat_value);

        IF state ? stat_key THEN
            prev_stat_state := state->stat_key;
            prev_stat_type := prev_stat_state->>'type';
            IF stat_type <> prev_stat_type THEN
                RAISE EXCEPTION 'Type mismatch between values for key "%" was "%" became "%"', stat_key, prev_stat_type, stat_type;
            END IF;
            next_stat_state = jsonb_build_object('type', stat_type);

            CASE stat_type
                -- Handle numeric values with iterative mean, variance, standard deviation, and coefficient of variation.
                WHEN 'number' THEN
                    DECLARE
                        sum numeric := (prev_stat_state->'sum')::numeric + stat_value::numeric;
                        count integer := (prev_stat_state->'count')::integer + 1;
                        delta numeric := stat_value::numeric - (prev_stat_state->'mean')::numeric;
                        mean numeric := (prev_stat_state->'mean')::numeric + delta / count;
                        min numeric := LEAST((prev_stat_state->'min')::numeric, stat_value::numeric);
                        max numeric := GREATEST((prev_stat_state->'max')::numeric, stat_value::numeric);
                        sum_sq_diff numeric := (prev_stat_state->'sum_sq_diff')::numeric + delta * (stat_value::numeric - mean);

                        -- Calculate variance and standard deviation
                        variance numeric := CASE WHEN count > 1 THEN sum_sq_diff / (count - 1) ELSE NULL END;
                        stddev numeric := CASE WHEN variance IS NOT NULL THEN sqrt(variance) ELSE NULL END;

                        -- Calculate Coefficient of Variation (CV)
                        coefficient_of_variation_pct numeric := CASE
                            WHEN mean IS NULL OR mean = 0 THEN NULL
                            ELSE (stddev / mean) * 100
                        END;
                    BEGIN
                        next_stat_state :=  next_stat_state ||
                            jsonb_build_object(
                                'sum', sum,
                                'count', count,
                                'mean', mean,
                                'min', min,
                                'max', max,
                                'sum_sq_diff', sum_sq_diff,
                                'variance', variance,
                                'stddev', stddev,
                                'coefficient_of_variation_pct', coefficient_of_variation_pct
                            );
                    END;

                -- Handle string values
                WHEN 'string' THEN
                    next_stat_state :=  next_stat_state ||
                        jsonb_build_object(
                            'counts',
                            -- The previous dictionary with count for each key.
                            (prev_stat_state->'counts')
                            -- Appending to it
                            ||
                            -- The updated count for this particular key.
                            jsonb_build_object(
                                -- Notice that `->>0` extracts the non-quoted string,
                                -- otherwise the key would be double quoted.
                                stat_value->>0,
                                COALESCE((prev_stat_state->'counts'->(stat_value->>0))::integer, 0) + 1
                            )
                        );

                -- Handle boolean types
                WHEN 'boolean' THEN
                    next_stat_state :=  next_stat_state ||
                        jsonb_build_object(
                            'counts', jsonb_build_object(
                                'true', COALESCE((prev_stat_state->'counts'->'true')::integer, 0) + (stat_value::boolean)::integer,
                                'false', COALESCE((prev_stat_state->'counts'->'false')::integer, 0) + (NOT stat_value::boolean)::integer
                            )
                        );

                -- Handle array types
                WHEN 'array' THEN
                    DECLARE
                        element text;
                        element_count integer;
                        count integer;
                    BEGIN
                        -- Start with the previous state, to preserve previous counts.
                        next_stat_state := prev_stat_state;

                        FOR element IN SELECT jsonb_array_elements_text(stat_value) LOOP
                            -- Retrieve the old count for this element, defaulting to 0 if not present
                            count := COALESCE((next_stat_state->'counts'->element)::integer, 0) + 1;

                            -- Update the next state with the incremented count
                            next_stat_state := jsonb_set(
                                next_stat_state,
                                ARRAY['counts',element],
                                to_jsonb(count)
                            );
                        END LOOP;
                    END;

                -- Handle object (nested JSON)
                WHEN 'object' THEN
                    next_stat_state := public.jsonb_stats_to_summary(prev_stat_state, stat_value);

                ELSE
                    RAISE EXCEPTION 'Unsupported type "%" for %', stat_type, stat_value;
            END CASE;
        ELSE
            -- Initialize new entry in state
            next_stat_state = jsonb_build_object('type', stat_type);
            CASE stat_type
                WHEN 'number' THEN
                    next_stat_state := next_stat_state ||
                        jsonb_build_object(
                            'sum', stat_value::numeric,
                            'count', 1,
                            'mean', stat_value::numeric,
                            'min', stat_value::numeric,
                            'max', stat_value::numeric,
                            'sum_sq_diff', 0,
                            'variance', 0,
                            'stddev', 0,
                            'coefficient_of_variation_pct', 0
                        );

                WHEN 'string' THEN
                    next_stat_state :=  next_stat_state ||
                        jsonb_build_object(
                            -- Notice that `->>0` extracts the non-quoted string,
                            -- otherwise the key would be double quoted.
                            'counts', jsonb_build_object(stat_value->>0, 1)
                        );

                WHEN 'boolean' THEN
                    next_stat_state :=  next_stat_state ||
                            jsonb_build_object(
                            'counts', jsonb_build_object(
                                'true', (stat_value::boolean)::integer,
                                'false', (NOT stat_value::boolean)::integer
                            )
                        );

                WHEN 'array' THEN
                    -- Initialize array with counts of each unique value
                    next_stat_state :=  next_stat_state ||
                        jsonb_build_object(
                            'counts',
                            (
                            SELECT jsonb_object_agg(element,1)
                            FROM jsonb_array_elements_text(stat_value) AS element
                            )
                        );

                WHEN 'object' THEN
                    next_stat_state := public.jsonb_stats_to_summary(next_stat_state, stat_value);

                ELSE
                    RAISE EXCEPTION 'Unsupported type "%" for %', stat_type, stat_value;
            END CASE;
        END IF;

        state := state || jsonb_build_object(stat_key, next_stat_state);
    END LOOP;

    RETURN state;
END;
$$;

-- From ...create_jsonb_stats_summary_merge.up.sql
CREATE OR REPLACE FUNCTION public.jsonb_stats_summary_merge(a jsonb, b jsonb)
RETURNS jsonb
LANGUAGE plpgsql
STABLE STRICT
PARALLEL SAFE
COST 100
AS $$
DECLARE
    key_a text;
    key_b text;
    val_a jsonb;
    val_b jsonb;
    merged_val jsonb;
    type_a text;
    type_b text;
    result jsonb := '{}';
BEGIN
    -- Ensure both a and b are objects
    IF jsonb_typeof(a) <> 'object' OR jsonb_typeof(b) <> 'object' THEN
        RAISE EXCEPTION 'Both arguments must be JSONB objects';
    END IF;

    -- Iterate over keys in both JSONB objects
    FOR key_a, val_a IN SELECT * FROM jsonb_each(a) LOOP
        IF b ? key_a THEN
            val_b := b->key_a;
            type_a := val_a->>'type';
            type_b := val_b->>'type';

            -- Ensure the types are the same for the same key
            IF type_a <> type_b THEN
                RAISE EXCEPTION 'Type mismatch for key "%": % vs %', key_a, type_a, type_b;
            END IF;

            -- Merge the values based on their type
            CASE type_a
                WHEN 'number' THEN
                    DECLARE
                        count_a INTEGER := (val_a->'count')::INTEGER;
                        count_b INTEGER := (val_b->'count')::INTEGER;
                        total_count INTEGER := count_a + count_b;

                        mean_a NUMERIC := (val_a->'mean')::NUMERIC;
                        mean_b NUMERIC := (val_b->'mean')::NUMERIC;
                        merged_mean NUMERIC := (mean_a * count_a + mean_b * count_b) / total_count;

                        sum_sq_diff_a NUMERIC := (val_a->'sum_sq_diff')::NUMERIC;
                        sum_sq_diff_b NUMERIC := (val_b->'sum_sq_diff')::NUMERIC;
                        delta NUMERIC := mean_b - mean_a;

                        merged_sum_sq_diff NUMERIC :=
                            sum_sq_diff_a + sum_sq_diff_b + delta * delta * count_a * count_b / total_count;
                        merged_variance NUMERIC :=
                            CASE WHEN total_count > 1
                            THEN merged_sum_sq_diff / (total_count - 1)
                            ELSE NULL
                            END;
                        merged_stddev NUMERIC :=
                            CASE WHEN merged_variance IS NOT NULL
                            THEN sqrt(merged_variance)
                            ELSE NULL
                            END;

                        -- Calculate Coefficient of Variation (CV)
                        coefficient_of_variation_pct NUMERIC :=
                            CASE WHEN merged_mean <> 0
                            THEN (merged_stddev / merged_mean) * 100
                            ELSE NULL
                            END;
                    BEGIN
                        merged_val := jsonb_build_object(
                            'sum', (val_a->'sum')::numeric + (val_b->'sum')::numeric,
                            'count', total_count,
                            'mean', merged_mean,
                            'min', LEAST((val_a->'min')::numeric, (val_b->'min')::numeric),
                            'max', GREATEST((val_a->'max')::numeric, (val_b->'max')::numeric),
                            'sum_sq_diff', merged_sum_sq_diff,
                            'variance', merged_variance,
                            'stddev', merged_stddev,
                            'coefficient_of_variation_pct', coefficient_of_variation_pct
                        );
                    END;

                WHEN 'string' THEN
                    merged_val := jsonb_build_object(
                        'counts', (
                            SELECT jsonb_object_agg(key, value)
                            FROM (
                                SELECT key, SUM(value) AS value
                                FROM (
                                    SELECT key, value::integer FROM jsonb_each(val_a->'counts')
                                    UNION ALL
                                    SELECT key, value::integer FROM jsonb_each(val_b->'counts')
                                ) AS enumerated
                                GROUP BY key
                            ) AS merged_counts
                        )
                    );

                WHEN 'boolean' THEN
                    merged_val := jsonb_build_object(
                        'counts', jsonb_build_object(
                            'true', (val_a->'counts'->>'true')::integer + (val_b->'counts'->>'true')::integer,
                            'false', (val_a->'counts'->>'false')::integer + (val_b->'counts'->>'false')::integer
                        )
                    );

                WHEN 'array' THEN
                    merged_val := jsonb_build_object(
                        'counts', (
                            SELECT jsonb_object_agg(key, value)
                            FROM (
                                SELECT key, SUM(value) AS value
                                FROM (
                                    SELECT key, value::integer FROM jsonb_each(val_a->'counts')
                                    UNION ALL
                                    SELECT key, value::integer FROM jsonb_each(val_b->'counts')
                                ) AS enumerated
                                GROUP BY key
                            ) AS merged_counts
                        )
                    );

                WHEN 'object' THEN
                    merged_val := public.jsonb_stats_summary_merge(val_a, val_b);

                ELSE
                    RAISE EXCEPTION 'Unsupported type "%" for key "%"', type_a, key_a;
            END CASE;

            -- Add the merged value to the result
            result := result || jsonb_build_object(key_a, jsonb_build_object('type', type_a) || merged_val);
        ELSE
            -- Key only in a
            result := result || jsonb_build_object(key_a, val_a);
        END IF;
    END LOOP;

    -- Add keys only in b
    FOR key_b, val_b IN SELECT key, value FROM jsonb_each(b) WHERE NOT (a ? key) LOOP
        result := result || jsonb_build_object(key_b, val_b);
    END LOOP;

    RETURN result;
END;
$$;
```

#### Testing

The file `statbus/test/sql/004_jsonb_stats_to_summary.sql` contains a comprehensive set of test cases for the aggregation logic. This can be adapted for the extension's regression tests.

#### Extension Boilerplate

The `sql_saga` directory serves as an excellent template for the necessary boilerplate for a PostgreSQL extension, including the `Makefile`, `sql_saga.control` file, and `META.json`.

### TODO

-   [ ] Define the C structures for `stat`, `stats`, and `stats_summary`.
-   [ ] Implement the input/output functions for the new types.
-   [ ] Implement the `stat` constructor function (e.g., `stat(anyelement)`).
-   [ ] Implement the first-level aggregate function `jsonb_stats_agg(stat)`.
-   [ ] Implement the second-level aggregate function `jsonb_stats_summary_agg(stats)`.
-   [ ] Implement the summary combine aggregate function `jsonb_stats_summary_combine_agg(stats_summary)`.
-   [ ] Write tests for all functions and aggregations.
-   [ ] Create the SQL script for extension installation (`.sql` and `control` file).
-   [ ] Add documentation.
