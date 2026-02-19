# jsonb_stats

`jsonb_stats` is a PostgreSQL extension for efficient statistical aggregation. Its primary purpose is to enable analytics on a dynamic number of variables without requiring schema modifications, using a structured, multi-level approach to storing and aggregating data within `jsonb` columns.

## Purpose-Driven Design

The extension is built on two core principles that enable powerful, hierarchical analytics:

1.  **Mergeable Summaries**: The statistical summaries (`stats_agg`) are designed to be efficiently combined. This is achieved by using online algorithms for calculating metrics like mean and variance (e.g., Welford's method). This feature is critical for building multi-level reports, such as aggregating daily data into monthly summaries, or regional data into a global summary, without reprocessing the raw data. This allows for the creation of faceted histories (`history_facet`) that can be drilled down into or rolled up.

2.  **Normalized Change Detection**: All numeric summaries (`int_agg`, `float_agg`, `dec2_agg`, `nat_agg`) include the `coefficient_of_variation_pct`. This metric provides a standardized, unit-less measure of variability relative to the mean. It allows data analysts to quickly identify significant changes or volatility in a statistic, regardless of the actual scale of the underlying numbers, making it easier to pinpoint areas of interest in large datasets.

## Core Concepts

The extension revolves around three hierarchical JSONB structures:

1.  **`stat`**: Represents a single statistical value, preserving its original data type.
    ```json
    {"type": "int", "value": 150}
    ```
2.  **`stats`**: A collection of named `stat` objects for a single entity, identified by `type: "stats"`.
    ```json
    {
      "type": "stats",
      "num_employees": {"type": "int", "value": 150},
      "is_profitable": {"type": "bool", "value": true},
      "industry":      {"type": "str",    "value": "tech"}
    }
    ```
3.  **`stats_agg`**: An aggregate summary of `stats` objects, identified by `type: "stats_agg"`. It is designed to be efficiently combined with other summaries.
    ```json
    {
        "type": "stats_agg",
        "num_employees": {
            "coefficient_of_variation_pct": 154.16,
            "count": 3,
            "max": 2500,
            "mean": 900.00,
            "min": 50,
            "stddev": 1387.44,
            "sum": 2700,
            "sum_sq_diff": 3845000.00,
            "type": "int_agg",
            "variance": 1922500.00
        },
        "is_profitable": {
            "type": "bool_agg",
            "counts": { "false": 1, "true": 2 }
        },
        "industry": {
            "type": "str_agg",
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
    value_float FLOAT,
    value_numeric NUMERIC(10,2),
    value_bool BOOLEAN,
    value_text TEXT,
    value_date DATE,
    -- stat() is STRICT (returns NULL for NULL input), so COALESCE picks the non-null result
    stat JSONB GENERATED ALWAYS AS (
        COALESCE(
            stat(value_int), stat(value_float), stat(value_numeric),
            stat(value_bool), stat(value_text), stat(value_date)
        )
    ) STORED,
    CONSTRAINT one_value_must_be_set CHECK (
        (value_int IS NOT NULL)::int +
        (value_float IS NOT NULL)::int +
        (value_numeric IS NOT NULL)::int +
        (value_bool IS NOT NULL)::int +
        (value_text IS NOT NULL)::int +
        (value_date IS NOT NULL)::int = 1
    )
);


-- Populate with data for the 2023 period
INSERT INTO stat_for_unit (legal_unit_id, code, value_int) VALUES
(1, 'num_employees', 150),
(2, 'num_employees', 2500),
(3, 'num_employees', 50);

INSERT INTO stat_for_unit (legal_unit_id, code, value_bool) VALUES
(1, 'is_profitable', true),
(2, 'is_profitable', true),
(3, 'is_profitable', false);

INSERT INTO stat_for_unit (legal_unit_id, code, value_text) VALUES
(1, 'industry', 'tech'),
(2, 'industry', 'finance'),
(3, 'industry', 'tech');
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

#### `history_facet` (Level 2: `stats` -> `stats_agg`)

This view uses `jsonb_stats_agg(stats)` to create a statistical summary for each region, allowing for drill-down analysis.

```sql
-- This view would use the extension's jsonb_stats_agg(stats) function.
CREATE MATERIALIZED VIEW history_facet AS
SELECT
    luh.valid_from,
    luh.valid_until,
    luh.region,
    jsonb_stats_agg(luh.stats) as stats_agg
FROM legal_unit_history luh
WHERE luh.valid_from = '2023-01-01'
GROUP BY luh.valid_from, luh.valid_until, luh.region;

-- Expected output for EU region:
-- {
--     "type": "stats_agg",
--     "industry": {
--         "type": "str_agg",
--         "counts": {
--             "tech": 2
--         }
--     },
--     "turnover": {
--         "max": 123456.78,
--         "min": 123456.78,
--         "sum": 123456.78,
--         "mean": 123456.78,
--         "type": "float_agg",
--         "count": 1,
--         "stddev": null,
--         "variance": null,
--         "sum_sq_diff": 0.00,
--         "coefficient_of_variation_pct": null
--     },
--     "is_profitable": {
--         "type": "bool_agg",
--         "counts": {
--             "true": 1,
--             "false": 1
--         }
--     },
--     "num_employees": {
--         "coefficient_of_variation_pct": 70.71,
--         "count": 2,
--         "max": 150,
--         "mean": 100.00,
--         "min": 50,
--         "stddev": 70.71,
--         "sum": 200,
--         "sum_sq_diff": 5000.00,
--         "type": "int_agg",
--         "variance": 5000.00
--     }
-- }
```

#### `history` (Level 3: `stats_agg` -> `stats_agg`)

This final view creates a global summary. It can be generated either from the `stats` objects directly or, more efficiently, by combining the regional `stats_agg` objects using `jsonb_stats_merge_agg(stats_agg)`.

```sql
-- This view demonstrates combining faceted summaries into a global summary.
CREATE MATERIALIZED VIEW history AS
SELECT
    hf.valid_from,
    hf.valid_until,
    jsonb_stats_merge_agg(hf.stats_agg) as stats_agg
FROM history_facet hf
GROUP BY hf.valid_from, hf.valid_until;

-- The resulting global stats_agg is shown in the Core Concepts section.
```

### Converting Individual `stats` for Merging

When rolling up data incrementally, you may need to merge an individual `stats` object with an existing `stats_agg`. Since `jsonb_stats_merge` operates on two `stats_agg` objects, the individual `stats` must first be converted. `jsonb_stats_to_agg` handles this conversion:

```sql
-- A new company arrives — convert its stats to stats_agg, then merge
-- with the existing regional aggregate
UPDATE history_facet
SET stats_agg = jsonb_stats_merge(
    stats_agg,
    jsonb_stats_to_agg(new_company.stats)
)
WHERE region = new_company.region;
```

Without `jsonb_stats_to_agg`, you would need a subquery to wrap the aggregate call:
```sql
jsonb_stats_merge(stats_agg, (SELECT jsonb_stats_agg(s) FROM (VALUES (new_company.stats)) t(s)))
```

### Structures in Detail

The `stats_agg` object contains different summary structures depending on the data type being aggregated. The logic for these summaries is documented in `dev/reference_plpgsql.sql`.

#### Numeric Summaries (`int_agg`, `float_agg`, `dec2_agg`, `nat_agg`)
Aggregates numeric values, providing a trade-off between performance and precision. All calculated fields are stored as JSON `number`s.

-   **`int_agg`**: For `bigint` values. Uses fast `int64` arithmetic.
-   **`float_agg`**: For `float8` values. Uses fast `double` arithmetic.
-   **`dec2_agg`**: For values with two decimal places. Uses fast, scaled `int64` arithmetic internally to guarantee precision while representing values as standard JSON `number`s in the output.
-   **`nat_agg`**: For natural numbers (non-negative integers). Same Welford accumulation as `int_agg`, but validates that values are >= 0. Negative values are silently skipped. Created manually via `jsonb_build_object('type','nat','value',42)` (no PG OID maps to it automatically).

All numeric summaries share the following fields:
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

The resulting `int_agg` would be:
```json
{
    "reading": {
        "type": "int_agg",
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

#### Categorical Summaries (`str_agg`, `bool_agg`)
Aggregates string or boolean values.
- `counts`: A JSONB object where keys are the distinct values and values are their frequencies.

**Example (`str_agg`):**
Given three `stats` objects:
`{"category": stat('apple'::text)}`
`{"category": stat('banana'::text)}`
`{"category": stat('apple'::text)}`

The resulting `str_agg` would be:
```json
{
    "category": {
        "type": "str_agg",
        "counts": {
            "apple": 2,
            "banana": 1
        }
    }
}
```

**Example (`bool_agg`):**
Given three `stats` objects:
`{"is_active": stat(true)}`
`{"is_active": stat(false)}`
`{"is_active": stat(true)}`

The resulting `bool_agg` would be:
```json
{
    "is_active": {
        "type": "bool_agg",
        "counts": {
            "false": 1,
            "true": 2
        }
    }
}
```

#### Date Summary (`date_agg`)
Aggregates date values with a hybrid approach: a count map (like `str_agg`) plus min/max date tracking.
- `counts`: A JSONB object where keys are ISO date strings and values are their frequencies.
- `min`: The earliest date observed (ISO format string comparison is correct for dates).
- `max`: The latest date observed.

**Example:**
Given three `stats` objects:
`{"founded": stat('2024-01-15'::date)}`
`{"founded": stat('2023-06-01'::date)}`
`{"founded": stat('2024-01-15'::date)}`

The resulting `date_agg` would be:
```json
{
    "founded": {
        "type": "date_agg",
        "counts": {
            "2023-06-01": 1,
            "2024-01-15": 2
        },
        "min": "2023-06-01",
        "max": "2024-01-15"
    }
}
```

#### Array Summary (`arr_agg`)
Aggregates array values.
- `count`: The number of arrays that have been processed. For example, aggregating two separate arrays results in `count: 2`. This is consistent with `count` for numeric summaries.
- `counts`: A JSONB object tracking the frequency of each unique element across all arrays.

**Example:**
Given three `stats` objects:
`{"tags": stat(ARRAY[1, 2])}`
`{"tags": stat(ARRAY[2, 3])}`
`{"tags": stat(ARRAY[3, 4])}`

The resulting `arr_agg` would be:
```json
{
    "tags": {
        "type": "arr_agg",
        "count": 3,
        "counts": {
            "1": 1,
            "2": 2,
            "3": 2,
            "4": 1
        }
    }
}
```

## Performance

The Rust implementation (via pgrx) uses native `HashMap` state with `Box` heap allocation, avoiding JSONB serialization on every row. Benchmarks compare against the PL/pgSQL reference implementation:

| Benchmark | Rust | PL/pgSQL | Speedup |
|-----------|------|----------|---------|
| `jsonb_stats_agg` — 10K rows, 3 types | 27ms | 14,015ms | **515x** |
| `jsonb_stats_merge_agg` — 1K groups | 108ms | 797,240ms | **7,380x** |

The merge speedup is larger because PL/pgSQL performs full JSONB serialization round-trips per group, while Rust merges native structs and only serializes once in the finalfunc.

### Parallel aggregation

Both `jsonb_stats_agg` and `jsonb_stats_merge_agg` declare `parallel = safe` with `combinefunc`, `serialfunc`, and `deserialfunc`. This means PostgreSQL can automatically split aggregation across multiple parallel workers on large tables — **no client changes required**.

The planner enables parallelism based on table size and cost estimates. To verify a parallel plan is being used:

```sql
EXPLAIN (COSTS OFF)
SELECT jsonb_stats_agg(stats) FROM large_table;
-- Look for "Partial Aggregate" + "Gather" nodes
```

To tune parallelism (rarely needed — defaults work well):

```sql
SET max_parallel_workers_per_gather = 4;  -- default is 2
```

Benchmarks run as part of the test suite (`cargo pgrx test`). Results are written to `/tmp/jsonb_stats_benchmarks.txt`.

## Design Philosophy

A key design feature of `jsonb_stats` is its use of a `stat` object (`{"type": "int", "value": 10}`). This structure is used to explicitly preserve the original SQL data type of a value (`bigint`, `float8`, `numeric(x,2)`, etc.) before it is aggregated.

While this adds a level of nesting compared to working with raw `jsonb` values, it is a deliberate choice to enable performance and correctness. By capturing the intended type in a `stat` object, `jsonb_stats` can use the most performant internal representation (e.g., `int64` for `int`, scaled integers for `decimal2`) to perform fast and accurate calculations. This avoids the performance overhead and precision issues associated with PostgreSQL's generic `numeric` type inside a high-volume aggregation loop.

## API

### Constructor Functions

| Function | Description |
|----------|-------------|
| `stat(anyelement)` | Creates a typed `stat` JSONB from any scalar value |
| `stats(jsonb)` | Adds `"type":"stats"` to a JSONB object containing stat entries |
| `stats(code text, val anyelement)` | Shorthand: wraps `stat(val)` into a named stats object |

### Type Mapping

`stat()` automatically maps PostgreSQL types to stat types:

| PostgreSQL type | Stat type | Aggregate type |
|-----------------|-----------|----------------|
| `integer` | `int` | `int_agg` |
| `float8` | `float` | `float_agg` |
| `numeric` | `dec2` | `dec2_agg` |
| `date` | `date` | `date_agg` |
| `text` / `varchar` | `str` | `str_agg` |
| `boolean` | `bool` | `bool_agg` |
| `array` | `arr` | `arr_agg` |
| _(manual)_ | `nat` | `nat_agg` |

`nat` has no automatic mapping — create manually: `jsonb_build_object('type','nat','value',42)`.

### Aggregate Functions

| Function | Description |
|----------|-------------|
| `jsonb_stats_agg(code text, stat jsonb)` | Pairs → `stats` (convenience for building stats row by row) |
| `jsonb_stats_agg(stats jsonb)` | `stats` → `stats_agg` (accumulate + finalize with Welford statistics) |
| `jsonb_stats_merge_agg(stats_agg jsonb)` | `stats_agg` → `stats_agg` (parallel merge of pre-aggregated summaries) |

### Scalar Functions

| Function | Description |
|----------|-------------|
| `jsonb_stats_to_agg(stats jsonb)` | Convert a single `stats` → `stats_agg` (for merging with existing aggregates) |
| `jsonb_stats_merge(a jsonb, b jsonb)` | Binary merge of two `stats_agg` objects (no aggregate context needed) |
| `jsonb_stats_accum(state jsonb, stats jsonb)` | Low-level: accumulate one `stats` into running state |
| `jsonb_stats_final(state jsonb)` | Low-level: compute derived stats (variance, stddev, cv_pct) on accumulated state |

### Error Handling

The extension follows a **fail-fast** strategy. Invalid input raises a PostgreSQL `ERROR` (aborting the transaction) rather than silently producing wrong results:

- **Unknown stat type** (e.g., `"type":"foo"`) → `ERROR: unknown stat type 'foo'`
- **Missing or invalid value** (e.g., str stat with no `"value"` key) → `ERROR: missing or invalid 'value'`
- **Negative nat value** → `ERROR: nat value must be >= 0`
- **Type mismatch in merge** (e.g., merging `int_agg` with `str_agg` for the same key) → `ERROR: type mismatch`
- **Unknown aggregate type** → `ERROR: unknown aggregate type`

## Installation

The extension is built with [pgrx](https://github.com/pgcentralfoundation/pgrx) (Rust).

```sh
make install          # Install into system PostgreSQL
```

Then, connect to your PostgreSQL database and enable the extension:
```sql
CREATE EXTENSION jsonb_stats;
```

### Git Hooks and Scratch Directories

This project uses a scratch directory (`tmp/`) for local experiments and AI tool interaction. Files in this directory can be locally staged to view changes with `git diff`, but a pre-commit hook will prevent them from ever being committed.

**One-Time Setup:** To enable this and other project conventions, all developers must configure Git to use our shared hooks path after cloning:

```bash
git config core.hooksPath devops/githooks
```

## Development

The extension is implemented in Rust via [pgrx](https://github.com/pgcentralfoundation/pgrx). To run the test suite:

```sh
make test             # Run all tests (including benchmarks)
make run              # Launch psql with extension loaded
```
