# Changelog

## 3.0.0

Numeric aggregates are now exact and deterministic. Previously the same rows
could produce different `sum_sq_diff`, `variance`, `stddev` and
`coefficient_of_variation_pct` on different machines and in different merge
orders.

Root causes (both in 2.x Rust and the 2.x PL/pgSQL reference):
- state was `mean` + `sum_sq_diff` in f64 (Rust) or division-rounded
  numeric (reference), updated with Welford/Chan formulas that are not
  associative;
- merges read the *finalized* `mean`, rounded to 2 decimals, so
  `jsonb_stats_merge_agg` was off by orders of magnitude more than f64 noise
  (2.4e9 in `sum_sq_diff` on a 73-row test set).

### Breaking

Upgrade with `ALTER EXTENSION jsonb_stats UPDATE` (SQL interface unchanged),
then recompute stored aggregates. See README "Upgrading".

- Numeric `*_agg` summaries carry a new required field `sum_sq` (sum of
  squares). Summaries from 2.x are rejected; recompute them from source stats.
- State is exact (`count, sum, sum_sq, min, max` as arbitrary-precision
  decimals); every derived field is computed exactly and rounded once to
  2 decimals, half away from zero. Stored statistics change in their last
  digits.
- `float` values are aggregated as the exact decimal their JSON text denotes
  (`0.1 + 0.2 = 0.3`).
- `dec2_agg` prints `sum`/`min`/`max` at scale 2 and `sum_sq` at scale 4.
- `jsonb_stats_merge` returns a finalized `stats_agg`; `jsonb_stats_accum`
  returns exact state only for numeric keys.
- New validation errors: non-integral `int`/`nat`, `dec2` with more than 2
  decimals, non-numeric numeric values, malformed numeric summaries, a key
  changing stat type between rows.
- Type-mismatch message is now
  `type mismatch for key '<k>': existing <a> vs incoming <b>`.
- The PL/pgSQL reference was rewritten with the same exact semantics.

### Fixed

- `stat(smallint)` crashed the backend (segfault); `stat(bigint)` produced a
  string. Both now map to `int`.
- `stat(real)` maps to `float`; `stat(numeric)` rounds to 2 decimals;
  `stat()` of arrays maps to `arr`; other types use their text output.
- `stat()` of NaN / Infinity raises an error instead of producing `null`.
- README example statistics corrected (stddev of 150/2500/50 is 1386.54).

### Verification

- `dev/exact_oracle.py`: independent Python rational-arithmetic oracle and
  fixture generator (73 rows, values 0..90,000,000, plus
  negatives, near-i64::MAX, dec2 and float columns; 25 rounding, NULL and i128-boundary edge
  cases). Cross-checked against PostgreSQL numeric with exact integer division.
- `src/tests_exact.rs`: byte-identical output across row orders, groupings,
  merge orders, two-level merges, scalar folds and forced parallel plans,
  plus reference parity. Full suite: 96 pg_tests + unit tests pass.
- Cross-architecture (`make xarch`, `dev/xarch/`, `dev/xarch_checks*.sql`):
  the same 10 checks give identical output and md5 on macOS arm64,
  Linux arm64 and Linux amd64 (x86_64 build cross-compiled, run under
  emulation). Fixture output md5 `6a5ccf0d8485ceaf6debc2d615874145`.
- `make fixture-check` fails if the committed oracle fixture is stale.
- Benchmarks still far ahead of the reference (393-511x accumulate,
  9,098-10,116x merge). Against 2.x per 100K rows: `str` unchanged, `int`
  about 10% slower, `float` about 25% slower (the exactness cost).
