# jsonb_stats

`jsonb_stats` is a PostgreSQL extension for efficient statistical aggregation. Its primary purpose is to enable analytics on a dynamic number of variables without requiring schema modifications, using a structured, multi-level approach to storing and aggregating data within `jsonb` columns.

## Purpose-Driven Design

The extension is built on two core principles that enable powerful, hierarchical analytics:

1.  **Mergeable Summaries**: The statistical summaries (`stats_summary`) are designed to be efficiently combined. This is achieved by using online algorithms for calculating metrics like mean and variance (e.g., Welford's method). This feature is critical for building multi-level reports, such as aggregating daily data into monthly summaries, or regional data into a global summary, without reprocessing the raw data. This allows for the creation of faceted histories (`history_facet`) that can be drilled down into or rolled up.

2.  **Normalized Change Detection**: The `integer_summary` includes the `coefficient_of_variation_pct`. This metric provides a standardized, unit-less measure of variability relative to the mean. It allows data analysts to quickly identify significant changes or volatility in a statistic, regardless of the actual scale of the underlying numbers, making it easier to pinpoint areas of interest in large datasets.

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
        "num_employees": {
            "type": "integer_summary",
            "count": 3,
            "sum": 2700,
            "min": 50,
            "max": 2500,
            "mean": 900.00,
            "sum_sq_diff": 3845000.00,
            "variance": 1922500.00,
            "stddev": 1386.54,
            "coefficient_of_variation_pct": 154.06
        },
        "is_profitable": {
            "type": "boolean_summary",
            "counts": { "false": 1, "true": 2 }
        },
        "industry": {
            "type": "text_summary",
            "counts": { "finance": 1, "tech": 2 }
        }
    }
    ```

## Example Scenario

This example demonstrates how `jsonb_stats` can be used to track statistics for legal units over time and aggregate them for reporting.

### 1. Source Tables

First, we define our source tables. `stat_for_unit` uses a generated `stat` column, which is created by the extension's `stat()` function.

```sql
-- The legal_unit table tracks companies and their validity periods.
CREATE TABLE legal_unit (
    legal_unit_id INT,
    name TEXT,
    region TEXT,
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL
);

INSERT INTO legal_unit (legal_unit_id, name, region, valid_from, valid_until) VALUES
(1, 'Company A', 'EU', '2023-01-01', '2024-01-01'),
(2, 'Company B', 'US', '2023-01-01', '2024-01-01'),
(3, 'Company C', 'EU', '2023-01-01', '2024-01-01'),
(1, 'Company A Rev.', 'EU', '2024-01-01', 'infinity'); -- Company A was revised for the next period

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

### 2. Aggregation Views

Next, we create materialized views to demonstrate the aggregation pipeline.

#### `legal_unit_history` (Level 1: `stat` -> `stats`)

This view uses `jsonb_stats_agg(code, stat)` to aggregate all individual `stat` values for a legal unit into a single `stats` object.

```sql
CREATE MATERIALIZED VIEW legal_unit_history AS
SELECT
    lu.legal_unit_id,
    lu.name,
    lu.region,
    lu.valid_from,
    lu.valid_until,
    (
        SELECT jsonb_stats_agg(sfu.code, sfu.stat)
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
    luh.valid_until,
    luh.region,
    jsonb_stats_summary_agg(luh.stats) as stats_summary
FROM legal_unit_history luh
WHERE luh.valid_from = '2023-01-01'
GROUP BY luh.valid_from, luh.valid_until, luh.region;

-- Expected output for EU region:
-- {
--   "num_employees": {
--     "type": "integer_summary", "count": 2, "sum": 200, "min": 50, "max": 150,
--     "mean": 100.00, "sum_sq_diff": 5000.00, "variance": 5000.00, "stddev": 70.71,
--     "coefficient_of_variation_pct": 70.71
--   },
--   "is_profitable": { "type": "boolean_summary", "counts": { "false": 1, "true": 1 } },
--   "industry":      { "type": "text_summary", "counts": { "tech": 2 } }
-- }
```

#### `history` (Level 3: `stats_summary` -> `stats_summary`)

This final view creates a global summary. It can be generated either from the `stats` objects directly or, more efficiently, by combining the regional `stats_summary` objects using `jsonb_stats_summary_merge_agg(stats_summary)`.

```sql
-- This view demonstrates combining faceted summaries into a global summary.
CREATE MATERIALIZED VIEW history AS
SELECT
    hf.valid_from,
    hf.valid_until,
    jsonb_stats_summary_merge_agg(hf.stats_summary) as stats_summary
FROM history_facet hf
GROUP BY hf.valid_from, hf.valid_until;

-- The resulting global stats_summary is shown in the Core Concepts section.
```

### Structures in Detail

The `stats_summary` object contains different summary structures depending on the data type being aggregated. The logic for these summaries is documented in `dev/reference_plpgsql.sql`.

#### `integer_summary`
Aggregates numeric values. The calculation methods are chosen specifically to support efficient, online aggregation and merging of summaries.
- `count`: Number of values.
- `sum`: The sum of all values.
- `min`/`max`: The minimum and maximum values.
- `mean`: The arithmetic mean, updated iteratively. ([Calculation Reference](https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm))
- `sum_sq_diff`: The sum of squared differences from the mean, calculated using [Welford's online algorithm](https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm) to ensure numerical stability and mergeability.
- `variance`: The sample variance.
- `stddev`: The sample standard deviation.
- `coefficient_of_variation_pct`: The coefficient of variation (CV), expressed as a percentage (`stddev / mean * 100`). This provides a standardized measure of dispersion.

**Example:**
Given three `stats` objects:
`{"reading": stat(10)}`
`{"reading": stat(5)}`
`{"reading": stat(20)}`

The resulting `integer_summary` would be:
```json
{
    "reading": {
        "type": "integer_summary",
        "count": 3,
        "sum": 35,
        "min": 5,
        "max": 20,
        "mean": 11.67,
        "sum_sq_diff": 116.67,
        "variance": 58.33,
        "stddev": 7.64,
        "coefficient_of_variation_pct": 65.47
    }
}
```

#### `text_summary` / `boolean_summary`
Aggregates string or boolean values.
- `counts`: A JSONB object where keys are the distinct values and values are their frequencies.

**Example (`text_summary`):**
Given three `stats` objects:
`{"category": stat('apple'::text)}`
`{"category": stat('banana'::text)}`
`{"category": stat('apple'::text)}`

The resulting `text_summary` would be:
```json
{
    "category": {
        "type": "text_summary",
        "counts": {
            "apple": 2,
            "banana": 1
        }
    }
}
```

**Example (`boolean_summary`):**
Given three `stats` objects:
`{"is_active": stat(true)}`
`{"is_active": stat(false)}`
`{"is_active": stat(true)}`

The resulting `boolean_summary` would be:
```json
{
    "is_active": {
        "type": "boolean_summary",
        "counts": {
            "false": 1,
            "true": 2
        }
    }
}
```

#### `array_summary`
Aggregates array values.
- `count`: The number of arrays that have been processed. For example, aggregating two separate arrays results in `count: 2`.
- `elements_count`: The sum of the number of elements from all processed arrays.
- `counts`: A JSONB object tracking the frequency of each unique element across all arrays.

**Example:**
Given three `stats` objects:
`{"tags": stat(ARRAY[1, 2])}`
`{"tags": stat(ARRAY[2, 3])}`
`{"tags": stat(ARRAY[3, 4])}`

The resulting `array_summary` would be:
```json
{
    "tags": {
        "type": "array_summary",
        "count": 3,
        "elements_count": 6,
        "counts": {
            "1": 1,
            "2": 2,
            "3": 2,
            "4": 1
        }
    }
}
```

## API

The extension provides the following core functions and aggregates:

*   **Constructor Function**:
    *   `stat(anyelement)`: Creates a `stat` JSONB object from any scalar value.
*   **Aggregate Functions**:
    *   `jsonb_stats_agg(code text, stat jsonb)`: Aggregates `(code, stat)` pairs into a single `stats` object.
    *   `jsonb_stats_summary_agg(stats jsonb)`: Aggregates `stats` objects into a `stats_summary` object.
    *   `jsonb_stats_summary_merge_agg(stats_summary jsonb)`: Merges multiple `stats_summary` objects into a single, higher-level summary.

## Installation

To build and install the extension from source, run:
```sh
make install
```

Then, connect to your PostgreSQL database and enable the extension:
```sql
CREATE EXTENSION jsonb_stats;
```

## Development

This extension is developed as a standard PostgreSQL C extension. To run the full regression test suite:

```sh
make install && make test
```
