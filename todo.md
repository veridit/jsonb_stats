# todo

## Numeric aggregate output differs between machines (float rounding in f64 state)

### Problem

The same rows produce different `sum_sq_diff`, `variance`, `stddev` and
`coefficient_of_variation_pct` on different machines and in different merge
orders. Found by StatBus test 306 (`test/sql/306_load_demo_data.sql`), which
merges `float_agg` turnover statistics for 73 enterprises:

| Machine | Merge order | `variance` | `sum_sq_diff` |
|---|---|---|---|
| CI, amd64 | scan order | 168582257517595.41 | 12137922541266870.00 |
| macOS, arm64 | scan order | 168582257517595.38 | 12137922541266868.00 |
| macOS, arm64 | `ORDER BY unit_id` | 168582257517595.38 | |
| macOS, arm64 | `ORDER BY unit_id DESC` | 168582257517595.44 | |

Declaring a merge order made one machine deterministic, but amd64 and arm64
still disagreed, so order alone does not fix it.

### Cause: a property of the number type, not of the stat types

All four numeric aggregates (`int_agg`, `float_agg`, `dec2_agg`, `nat_agg`)
keep their state in `f64`: `NumFields` in `src/state.rs` (`sum`, `mean`,
`sum_sq_diff`), merged by `NumFields::merge` and by `merge_num_agg` in
`src/merge.rs:75-101` (Chan's parallel formula). `f64` has a 53-bit mantissa,
about 15-16 significant decimal digits:

- At `variance` ≈ 1.7e14 the gap between adjacent f64 values is 0.03125.
  `round2` (`src/helpers.rs:45`) prints it to the cent, so the last two
  digits are rounding noise.
- At `sum_sq_diff` ≈ 1.2e16 the gap is 2.0. The printed `.00` is invented
  precision, and the units digit itself varies.
- f64 addition is not associative, so merge order changes the last bits.
  Different CPUs and compilers (FMA contraction, instruction choice) round
  intermediate results differently, so amd64 and arm64 differ even in the
  same order.

This contradicts the repo's own contract in two places:

- `CLAUDE.md`: every Rust function must give **identical output** to the
  PL/pgSQL reference (`dev/reference_plpgsql.sql`). That reference computes in
  PostgreSQL `numeric` (exact decimal; see lines 144-168), so it gives one
  answer per input, on every machine, in any order. The Rust f64 path does not.
- `README.md` (Numeric Summaries): `dec2_agg` "uses fast, scaled int64
  arithmetic internally to guarantee precision". It does not: `Dec2Agg` holds
  the same f64 `NumFields` as the others (`src/state.rs:73-77`).

### Possible fix

Keep the accumulator state in exact arithmetic for the types whose inputs are
exact, and only produce a derived value as an f64 when it is final.

1. **Exact state for `int_agg`, `nat_agg` and `dec2_agg`.** Their inputs are
   exact (integers, or scaled integers for dec2). Hold
   `count`, `sum` and `sum of squares` as `i128` (dec2 scaled by 100, so its
   squares scaled by 10000). Then every merge is exact and associative:
   `sum = a.sum + b.sum`, `sumsq = a.sumsq + b.sumsq`, with no order or CPU
   dependence. Derive at finalization, in exact integer arithmetic where
   possible:
   - `mean = sum / count`
   - `sum_sq_diff = sumsq - sum² / count`, computed as
     `(count·sumsq - sum²) / count` so the subtraction is exact before the
     one division.
   - `variance = sum_sq_diff / (count - 1)`, `stddev = sqrt(variance)`.
   This matches the PL/pgSQL `numeric` reference exactly for these types, and
   it is what the README already promises for dec2.
   Check `i128` overflow bounds for the expected data: `sumsq` of n values up
   to |v| needs n·v² < 1.7e38. Error clearly on overflow, as `round2` already
   does for non-finite values.

2. **`float_agg` keeps f64**, because its input is f64. Make its output stable
   anyway:
   - Use a compensated, order-independent combine for `sum` and the Chan
     terms (for example a two-sum/Neumaier pair kept in the state), so a
     merge does not lose low bits; and
   - **print only the digits an f64 carries.** Replace `round2` for derived
     float fields with rounding to a fixed number of significant digits.
     For the values above, 15, 14 and 12 significant digits all print the
     three variances identically (`168582257517595`, `1.685822575176e+14`,
     `1.68582257518e+14`). This is not a full guarantee: two results that
     straddle a rounding boundary still print differently, however few digits
     are kept. Only exact or compensated arithmetic removes the difference;
     rounding makes it rare. `round2` stays right for exact types and for small
     magnitudes.

3. **One combine path.** `NumFields::merge` (`src/state.rs:46`) and
   `merge_num_agg` (`src/merge.rs:75`) implement the same formula twice. Make
   one call the other, so the exact and compensated arithmetic is written once.

4. **Correct the README** for whatever is chosen: say which types are exact,
   and that `float_agg` derived fields carry float precision.

### How to know it is fixed

- A test that merges the same values in ascending, descending and shuffled
  order returns byte-identical output for `int_agg`, `nat_agg` and
  `dec2_agg`, and output identical to the PL/pgSQL reference.
- For `float_agg`, the same test returns identical printed output across
  orders.
- Run that test on both amd64 and arm64 (CI and a Mac) and record the results.
- The values from the table above, rebuilt as a test fixture (73 values,
  turnover 0 to 90000000), print the same on both machines.
- Benchmarks still beat the PL/pgSQL reference (`CLAUDE.md`).

### Maintainer findings (2026-10-16)

Checked against PG 18 with `dev/reference_plpgsql.sql` loaded, 73 ints in 0..90M:

| Path | `sum_sq_diff` | `variance` |
|---|---|---|
| exact (`n·Σx² - (Σx)²) / n`) | 44786159048564926.00 | 622029986785623.97 |
| reference `agg`, asc / desc / shuffled | 44786159048564925.95 | 622029986785623.97 |
| reference `merge_agg` over 7 groups | 44786159046192521.34 | 622029986752673.91 |

1. **The reference is not exact either.** `numeric` division has finite scale,
   so Welford's `mean += delta / count` drifts (`…925.95` vs exact `…926.00`).
   It is deterministic, not exact. "Match the reference" and "be exact"
   conflict, so the reference has to change too.
2. **The biggest error is in `merge_agg`, not f64.** It reads the *finalized*
   `mean`, which is rounded to 2 decimals, and uses it in Chan's delta term.
   That is off by 2.4e9 here, in both the reference and Rust. Fix: derive
   the mean from `sum / count` when merging. `sum` is exact for int/nat/dec2.
3. **The FMA explanation does not hold for Rust.** rustc never contracts to FMA
   without an explicit `mul_add`, and IEEE `+ - * / sqrt` are correctly rounded.
   Same order gives the same bits on amd64 and arm64. The CI/Mac difference is
   almost certainly scan/worker order differing between machines. (CI was never
   run with `ORDER BY`.) Order-independence is still the goal.
4. **For int/nat, exact merges need no schema change.** Σx² is an integer and
   `n·Σx² = n·sum_sq_diff + sum²`. If the new code prints `sum_sq_diff`
   correctly rounded (error ≤ 0.005), rounding gives back Σx² exactly. This
   only works for output from the new code. Old output (f64 error about 2,
   reference merge error about 1e9) cannot be recovered. For dec2 (Σx² in
   1e-4 units) a 2-decimal `sum_sq_diff` is not enough.
   Alternative: add an exact `sum_sq` field to `*_agg`. This is an additive
   schema change, and the merge reads it when it is present.
5. Derived values can be correctly rounded without floats: variance is the
   rational `(n·S2 - S1²) / (n(n-1))`. stddev and cv are square roots of
   rationals, rounded with integer sqrt. Use checked i128 and `error!` on
   overflow.
6. Side findings: `stat()` maps only INT4 to `int`, so `bigint` falls through to
   `str`. README says negative `nat` values are skipped, but the code errors.
   `stat(numeric)` emits any scale as `dec2`, e.g. `1.234`.

### Decisions (agreed with maintainer 2026-10-16)

The schema change is accepted, the reference changes, and the side bugs get
fixed. Work test-first: commit the failing tests, then make them pass.

**State.** Every numeric `*_agg` (`int`, `nat`, `dec2`, `float`) holds exact
decimals: `count`, `sum`, `sum_sq` (new: Σx²), `min`, `max`. Rust uses BigInt
mantissa + scale, the reference uses `numeric`. Accumulate and merge only add.
They never divide, so every order, grouping, parallel plan and CPU gives the
same state.

**`float` is exact too.** jsonb stores every JSON number as `numeric`, so the
value text a `float` stat carries (shortest round-trip text of the float8) is
taken as an exact decimal. The old "float carries f64 precision" caveat goes
away, and `0.1 + 0.2` sums to `0.3`.

**Derived fields** are computed from the exact state and rounded once to 2
decimals, half away from zero (PG `round()`). Let `N = n·sum_sq - sum²`:
- `mean = sum / n`
- `sum_sq_diff = N / n`
- `variance = N / (n(n-1))`
- `stddev = sqrt(variance)`
- `cv_pct = 100·stddev/mean`
`stddev` and `cv_pct` are correctly rounded square roots of exact rationals,
via integer sqrt. NULL rules: `count <= 1` gives NULL variance/stddev/cv, and
`sum = 0` gives NULL cv. `N < 0` means a corrupt input, which is an error.

**Scales** (as PG `numeric` would print them):
- `int`/`nat`: values must be integral and are normalized to scale 0.
- `dec2`: values may have at most 2 decimals and are normalized to scale 2.
  `sum`/`min`/`max` print at scale 2 and `sum_sq` at scale 4.
- `float`: natural scale. `sum` has the max input scale and `sum_sq` twice that.
- Ties in `min`/`max` (equal value, different scale) keep the larger scale,
  so the result does not depend on order.

**Summary shapes.**
- `jsonb_stats_accum` (low-level running state) emits state fields only:
  `type, count, sum, sum_sq, min, max`.
- Everything that returns a `stats_agg` emits full summaries from one shared
  builder, with the state fields plus `mean, sum_sq_diff, variance, stddev,
  coefficient_of_variation_pct`. That covers `jsonb_stats_final`,
  `jsonb_stats_merge`, the aggregates and `jsonb_stats_to_agg`.
- A scalar `jsonb_stats_merge`, as used in the README's incremental `UPDATE`
  example, therefore no longer stores a summary with variance missing or
  `mean` unrounded.
- Derived fields on input are ignored and recomputed.

**Fail fast.**
- A numeric `*_agg` without `sum_sq` (written by jsonb_stats < 3.0) raises an
  error telling the user to recompute it from source stats.
- Errors for: non-number values, int/nat not integral, nat < 0, dec2 with more
  than 2 decimals, count not an integer >= 1.

**`stat()`.**
- `smallint`/`integer`/`bigint` map to `int`.
- `real` maps to `float` (shortest f32 text) and `float8` to `float`.
- `numeric` maps to `dec2`, rounded to 2 decimals like a `numeric(_,2)` cast.
- NaN and ±Infinity raise errors.

**Version** 3.0.0: the new required field is a breaking change.

**Oracle.** `dev/exact_oracle.py` (Python `fractions`) computes the expected
summaries independently of both implementations.

### Downstream

StatBus pins this extension by commit (`postgres/Dockerfile`,
`jsonb_stats_release`). After a release, StatBus bumps the pin and
re-records its expected test outputs once (ticket STATBUS-512). Stored
statistics change in their last digits, so StatBus will recompute its derived
tables on upgrade.
