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
| `int` | `int_agg` | exact decimal, scale 0 | Integer values. Must be integral |
| `float` | `float_agg` | exact decimal | Float values, taken as the exact decimal their shortest round-trip text denotes |
| `dec2` | `dec2_agg` | exact decimal, scale 2 | Fixed two-decimal values. At most 2 decimals |
| `nat` | `nat_agg` | exact decimal, scale 0 | Natural numbers (non-negative integers). Validated >= 0 |
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

**JSONB state** — Used by the scalar functions (`jsonb_stats_accum`, `jsonb_stats_merge`, `jsonb_stats_final`). They parse JSONB into the same `StatsState`, apply the same operations, and serialize back. This is fine because they are called O(groups) not O(rows).

All paths share one implementation in `src/state.rs`: `StatsState::accumulate`, `StatsState::merge_summaries`, `AggEntry::merge` / `NumFields::merge` (the single combine), and `NumFields::summary_json` (the single summary builder).

The `jsonb_stats_final_internal` finalfunc converts the Internal `StatsState` to a JSONB `stats_agg`, computing derived statistics (mean, sum_sq_diff, variance, stddev, coefficient of variation) in the process.

## Exact Numeric State

All numeric types (`int_agg`, `float_agg`, `dec2_agg`, `nat_agg`) keep an exact state of decimals (`src/exact.rs`: integer mantissa + decimal scale). The mantissa is an `i128` while it fits (38 digits), and a `BigInt` beyond that. Every operation is overflow-checked and promotes to `BigInt` (or demotes back) by magnitude alone, so the representation never changes a result and never appears in JSON:

```
count, sum = Σx, sum_sq = Σx², min, max
```

**Accumulate** one value: `count += 1; sum += x; sum_sq += x²; min/max`.

**Merge** two states: add `count`, `sum`, `sum_sq`; take `min`/`max`.

Both operations are exact additions, so they are associative and commutative: the state, and therefore the output, is identical for every row order, grouping, merge order, parallel plan and CPU. (The 2.x implementation used f64 Welford/Chan updates and merged using the rounded `mean`, which made results depend on all of these; see todo.md / git history.)

Min/max ties between equal values printed with different scales (e.g. `2.5` vs `2.50`) keep the larger scale, so even the printed form is order independent.

**Derived stats** (finalfunc, and every function returning a `stats_agg`), with `N = count · sum_sq − sum²`:
- `mean = sum / count`
- `sum_sq_diff = N / count`
- `variance = N / (count · (count − 1))` — sample variance (NULL if count <= 1)
- `stddev = sqrt(variance)` (NULL if count <= 1)
- `coefficient_of_variation_pct = 100 · stddev / mean` (NULL if count <= 1 or sum = 0)

Each is computed from exact integers and rounded once to 2 decimals, half away from zero. Square roots use integer square roots: `round(100·sqrt(q)) = (isqrt(floor(40000·q)) + 1) div 2`, which is exact. The PL/pgSQL reference implements the same formulas in `numeric` with `div()` and a Newton `isqrt`.

The parallel serialfunc encodes decimals as strings, so worker state crosses process boundaries without loss.

**2.x summaries** (no `sum_sq`) are upgraded when read, in `NumFields::from_summary` (the only parser for stored summaries): `sum_sq = max(round_k(sum_sq_diff + sum²/count), ceil_k(sum²/count))` at the kind's `sum_sq` scale `k`. This is exact for int/nat when the stored error is < 0.5, best effort otherwise, and the state stays consistent. The PL/pgSQL reference implements the same rule.

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
- `jsonb_stats: type mismatch for key 'x': existing int_agg vs incoming str_agg`
- `jsonb_stats: int_agg for key 'x' has neither 'sum_sq' nor 'sum_sq_diff'`

## Performance

Internal state avoids serde_json serialization per row (the O(n²) trap that made an earlier JSONB-state approach 1.4x *slower* than PL/pgSQL).

Benchmarks (Rust vs PL/pgSQL reference):

| Benchmark | Rust | PL/pgSQL | Speedup |
|-----------|------|----------|---------|
| `jsonb_stats_agg` — 10K rows, 3 types | 33–45ms | 16,701–17,855ms | **393–511x** |
| `jsonb_stats_merge_agg` — 1K groups | 110–120ms | 1,090,917–1,110,045ms | **9,098–10,116x** |

The merge speedup is larger because PL/pgSQL performs full JSONB serialization round-trips per group, while Rust merges native structs and only serializes once in the finalfunc.
