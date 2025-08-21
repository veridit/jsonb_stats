# jsonb_stats

`jsonb_stats` is a PostgreSQL extension designed for efficient handling of statistical data. It provides a structured, multi-level approach to storing and aggregating statistical values within `jsonb` columns, making it ideal for analytics on dynamic variables without schema modifications.

## Core Concepts

The extension revolves around three hierarchical JSONB structures:

1.  **`stat`**: Represents a single statistical value, preserving its original data type.
    ```json
    {"type": "integer", "value": 150}
    ```
2.  **`stats`**: A collection of named `stat` objects for a single entity (e.g., a company at a point in time). This is the result of the first level of aggregation.
    ```json
    {
      "num_employees": {"type": "integer", "value": 150},
      "is_profitable": {"type": "boolean", "value": true},
      "industry":      {"type": "text",    "value": "tech"}
    }
    ```
3.  **`stats_summary`**: An aggregate summary of multiple `stats` objects. It computes statistics like count, sum, min, and max, and is designed to be efficiently combined with other summaries. This is the result of the second and third levels of aggregation.
    ```json
    {
      "num_employees": {"type": "integer_summary", "count": 3, "sum": 2700, "min": 50, "max": 2500},
      "is_profitable": {"type": "boolean_summary", "true_count": 2, "false_count": 1}
    }
    ```

## Example Scenario

This example demonstrates how `jsonb_stats` can be used to track statistics for legal units over time and aggregate them for reporting.

### 1. Source Tables

First, we define our source tables. `stat_for_unit` uses a generated `stat` column, which would be created by the extension's `to_stat()` function.

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

### 2. Aggregation Views

Next, we create materialized views to demonstrate the aggregation pipeline.

#### `legal_unit_history` (Level 1: `stat` -> `stats`)

This view uses `jsonb_stats_agg(stat)` to aggregate all individual `stat` values for a legal unit into a single `stats` object.

```sql
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
    ) AS stats
FROM
    legal_unit lu
WHERE lu.valid_from = '2023-01-01';
```

#### `history_facet` (Level 2: `stats` -> `stats_summary`)

This view uses `jsonb_stats_summary_agg(stats)` to create a statistical summary for each region, allowing for drill-down analysis.

```sql
-- This view would use the extension's jsonb_stats_summary_agg(stats) function.
CREATE MATERIALIZED VIEW history_facet AS
SELECT
    luh.valid_from,
    luh.valid_to,
    luh.region,
    jsonb_stats_summary_agg(luh.stats) as stats_summary
FROM legal_unit_history luh
WHERE luh.valid_from = '2023-01-01'
GROUP BY luh.valid_from, luh.valid_to, luh.region;

-- Expected output for EU region:
-- {
--   "num_employees": {"type": "integer_summary", "count": 2, "sum": 200, "min": 50, "max": 150},
--   "is_profitable": {"type": "boolean_summary", "true_count": 1, "false_count": 1},
--   "industry":      {"type": "text_summary", "distinct_values": 1, "values": ["tech"]}
-- }
```

#### `history` (Level 3: `stats_summary` -> `stats_summary`)

This final view creates a global summary. It can be generated either from the `stats` objects directly or, more efficiently, by combining the regional `stats_summary` objects using `jsonb_stats_summary_combine_agg(stats_summary)`.

```sql
-- This view demonstrates combining faceted summaries into a global summary.
CREATE MATERIALIZED VIEW history AS
SELECT
    hf.valid_from,
    hf.valid_to,
    jsonb_stats_summary_combine_agg(hf.stats_summary) as stats_summary
FROM history_facet hf
GROUP BY hf.valid_from, hf.valid_to;

-- The resulting global stats_summary:
-- {
--   "num_employees": {"type": "integer_summary", "count": 3, "sum": 2700, "min": 50, "max": 2500},
--   "is_profitable": {"type": "boolean_summary", "true_count": 2, "false_count": 1},
--   "industry":      {"type": "text_summary", "distinct_values": 2, "values": ["tech", "finance"]}
-- }
```

## API

The extension will provide the following core functions and aggregates:

*   **Constructor Function**:
    *   `to_stat(anyelement)`: Creates a `stat` JSONB object from any scalar value.
*   **Aggregate Functions**:
    *   `jsonb_stats_agg(stat)`: Aggregates `stat` objects into a single `stats` object.
    *   `jsonb_stats_summary_agg(stats)`: Aggregates `stats` objects into a `stats_summary` object.
    *   `jsonb_stats_summary_combine_agg(stats_summary)`: Combines multiple `stats_summary` objects into a single, higher-level summary.

## Installation

```sh
# TBD:
make && make install
psql -c "CREATE EXTENSION jsonb_stats;"
```

## Development

This extension is developed as a standard PostgreSQL C extension.

```sh
# To run tests:
make && make installcheck
```
