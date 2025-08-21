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
