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
            WHEN value_int IS NOT NULL THEN stat(value_int)
            WHEN value_bool IS NOT NULL THEN stat(value_bool)
            WHEN value_text IS NOT NULL THEN stat(value_text)
            WHEN value_date IS NOT NULL THEN stat(value_date)
        END
    ) STORED,
    CONSTRAINT one_value_must_be_set CHECK (
        (value_int IS NOT NULL)::int +
        (value_bool IS NOT NULL)::int +
        (value_text IS NOT NULL)::int +
        (value_date IS NOT NULL)::int = 1
    )
);



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
-- This view would use a `jsonb_stats_agg(code, stat)` function from the extension.
CREATE MATERIALIZED VIEW legal_unit_history AS
SELECT
    lu.legal_unit_id,
    lu.name,
    lu.region,
    lu.valid_from,
    lu.valid_to,
    (
        SELECT jsonb_stats_agg(sfu.code, sfu.stat)
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

-- The real extension would provide `jsonb_stats_summary_agg`.
-- The expected results per region are shown below.

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
-- This view would use a `jsonb_stats_summary_merge_agg(stats_summary)` on the faceted summaries.
CREATE MATERIALIZED VIEW history AS
SELECT
    hf.valid_from,
    hf.valid_to,
    jsonb_stats_summary_merge_agg(hf.stats_summary) as stats_summary
FROM history_facet hf
GROUP BY hf.valid_from, hf.valid_to;

-- The resulting `stats_summary` provides a high-level overview
-- without storing all the individual data points, making it
-- efficient for reporting on large datasets.
```


### TODO

-   [x] Standardize `stats_summary` structure and function names.
-   [x] Define API and create SQL stubs (`jsonb_stats--1.0.sql`).
-   [x] Port PL/pgSQL implementation into the extension.
-   [x] Create regression test for PL/pgSQL API (`sql/002_jsonb_stats_api_plpgsql.sql`).
-   [x] Create regression test for C API (`sql/003_jsonb_stats_api_c.sql`).
-   [ ] Implement `stat_c(anyelement)`.
-   [ ] Implement `jsonb_stats_agg_c(text, jsonb)`.
-   [ ] Implement `jsonb_stats_summary_agg_c(jsonb)`.
-   [ ] Implement `jsonb_stats_summary_merge_agg_c(jsonb)`.
-   [ ] Define C structures for aggregate states.
-   [ ] Add documentation for C implementation details.
