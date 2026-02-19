# Architecture

## Data Hierarchy

Three JSONB structure levels, each building on the previous:

```
stat  →  stats  →  stats_agg
```

**stat** — A single typed value. The `type` field preserves the original SQL data type.
```json
{"type": "int", "value": 150}
```

**stats** — A named collection of stats for one entity. Has `"type": "stats"`.
```json
{
    "type": "stats",
    "num_employees": {"type": "int", "value": 150},
    "industry": {"type": "str", "value": "tech"}
}
```

**stats_agg** — An aggregate summary combining multiple stats. Has `"type": "stats_agg"`.
```json
{
    "type": "stats_agg",
    "num_employees": {"type": "int_agg", "count": 3, "sum": 2700, "mean": 900.00, ...},
    "industry": {"type": "str_agg", "counts": {"tech": 2, "finance": 1}}
}
```

This forms a fully self-describing discriminated union at every level — the `type` field unambiguously determines the shape of each object.

## Type System

| stat type | agg type | Internal repr | Description |
|-----------|----------|---------------|-------------|
| `int` | `int_agg` | i64 | Integer values (bigint) |
| `float` | `float_agg` | f64 | Floating-point values (float8) |
| `dec2` | `dec2_agg` | scaled i64 | Fixed two-decimal (numeric). Scaled ×100 internally for exact arithmetic |
| `nat` | `nat_agg` | i64 | Natural numbers (non-negative integers). Validated >= 0 |
| `str` | `str_agg` | count map | String values → frequency counts |
| `bool` | `bool_agg` | count map | Boolean values → frequency counts |
| `date` | `date_agg` | count map + min/max | Date values → frequency counts with min/max tracking |
| `arr` | `arr_agg` | count map + count | Array elements → frequency counts with array count |

All types form a strict discriminated union: the `type` field determines the exact shape of the object. This enables type-safe consumption in client languages (e.g., TypeScript).

## Aggregate Pipeline

```
stat() → stats() → jsonb_stats_agg (accum + final) → jsonb_stats_merge_agg
```

The pipeline uses two different state strategies:

**Internal state (native HashMap)** — Used by `jsonb_stats_accum_sfunc`, `jsonb_stats_merge_sfunc`, and the parallel functions (`jsonb_stats_combine`, `jsonb_stats_serial`, `jsonb_stats_deserial`). State is a Rust `StatsState` struct allocated on the Rust heap (`Box::new`). This avoids JSONB serialization per row — the critical optimization that makes Rust ~500x faster than PL/pgSQL for accumulation.

**JSONB state** — Used by the scalar `jsonb_stats_merge` function. Parses JSONB via serde_json, merges, serializes back. This is fine because merge is called O(groups) not O(rows).

The `jsonb_stats_final_internal` finalfunc converts the Internal `StatsState` to a JSONB `stats_agg`, computing derived statistics (variance, stddev, coefficient of variation) in the process.

## Welford's Online Algorithm

All numeric types (`int_agg`, `float_agg`, `dec2_agg`, `nat_agg`) use Welford's method for numerically stable statistics:

**Single-value update** (accumulate):
```
count += 1
delta = value - mean
mean += delta / count
delta2 = value - mean
sum_sq_diff += delta * delta2
```

**Parallel merge** (combine two partial aggregates):
```
combined_count = a.count + b.count
delta = b.mean - a.mean
combined_mean = a.mean + delta * (b.count / combined_count)
combined_sum_sq_diff = a.sum_sq_diff + b.sum_sq_diff + delta² * a.count * b.count / combined_count
```

**Derived stats** (finalfunc, computed once at the end):
- `variance = sum_sq_diff / (count - 1)` — sample variance (NULL if count <= 1)
- `stddev = sqrt(variance)`
- `coefficient_of_variation_pct = (stddev / mean) * 100` — normalized dispersion (NULL if mean = 0)

All derived numeric fields are rounded to 2 decimal places.

## Parallel Aggregation

Both `jsonb_stats_agg` and `jsonb_stats_merge_agg` are `parallel = safe` with three supporting functions in `src/parallel.rs`:

- **`jsonb_stats_combine(internal, internal) → internal`** — Merges two partial `StatsState` structs using `merge_agg_entries`. Non-STRICT (handles NULL inputs from empty partitions). Borrows state1, takes ownership of state2 (freed after merge).
- **`jsonb_stats_serial(internal) → bytea`** — Serializes `StatsState` to JSON bytes via `serde_json::to_vec`. Borrows state (does not free — PG may call multiple times). Called O(workers) not O(rows).
- **`jsonb_stats_deserial(bytea, internal) → internal`** — Deserializes JSON bytes back to a `Box<StatsState>`. The second `internal` argument is required by PG but unused.

`StatsState`, `AggEntry`, and `NumFields` all derive `Serialize`/`Deserialize` for this purpose.

PostgreSQL automatically uses parallel plans when beneficial — no client changes required. The planner considers table size, `max_parallel_workers_per_gather`, and cost estimates.

## Error Handling

Fail fast with `pgrx::error!()` — unknown types, invalid values, type mismatches all raise a PostgreSQL ERROR that aborts the transaction. No silent skips or default fallbacks.

Error messages follow the pattern: `jsonb_stats: <description>`, e.g.:
- `jsonb_stats: unknown stat type 'foo'. Expected: int, float, dec2, nat, str, bool, arr, date`
- `jsonb_stats: nat value must be >= 0, got -1`
- `jsonb_stats: type mismatch in merge: 'int_agg' vs 'str_agg'`

## Performance

Internal state avoids serde_json serialization per row (the O(n²) trap that made an earlier JSONB-state approach 1.4x *slower* than PL/pgSQL).

Benchmarks (Rust vs PL/pgSQL reference):

| Benchmark | Rust | PL/pgSQL | Speedup |
|-----------|------|----------|---------|
| `jsonb_stats_agg` — 10K rows, 3 types | 27ms | 14,015ms | **515x** |
| `jsonb_stats_merge_agg` — 1K groups | 108ms | 797,240ms | **7,380x** |

The merge speedup is larger because PL/pgSQL performs full JSONB serialization round-trips per group, while Rust merges native structs and only serializes once in the finalfunc.
