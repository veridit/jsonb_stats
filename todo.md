A living document of upcoming tasks.
Tasks are checked when done and made brief.
Keep a journal.md that tracks the state of the current ongoing task and relevant details.

# `jsonb_stats` Development Tasks

## Next

- [ ] Add `combinefunc` + `parallel = safe` on aggregates (enables PostgreSQL parallel aggregation)
- [ ] Add comparison regression test: Rust vs PL/pgSQL output side-by-side for all functions

<details>
<summary>Completed Tasks</summary>

## Completed

### Rust (pgrx) Implementation
- [x] **Phase 1**: Scaffold pgrx project in `rust/`, `cargo pgrx init`, extension `jsonb_stats` v2.0
- [x] **Phase 2**: Implement `jsonb_stats_merge` — Welford parallel merge for int_agg, count map merge for str/bool/arr_agg
- [x] **Phase 3**: Implement `jsonb_stats_final` — derived stats (variance, stddev, cv_pct), rounding
- [x] **Phase 4**: Implement `jsonb_stats_accum` — init + update paths for all types
- [x] **Phase 5**: Port `stat()` and `jsonb_stats_sfunc`
- [x] Optimize accum with Internal state (Box<StatsState> on Rust heap, avoids serde_json per row)
- [x] Optimize merge_agg with Internal state (same approach)
- [x] Expand type system: add float_agg, dec2_agg, nat_agg, date_agg through full pipeline
- [x] Extract NumFields struct for shared Welford logic across all numeric types
- [x] 36 tests passing (correctness + PL/pgSQL comparison + benchmarks)
- [x] Update README with new types and Rust build instructions

### Legacy (C extension)
-   [x] Define API and create SQL stubs (`jsonb_stats--1.0.sql`).
-   [x] Port PL/pgSQL implementation into the extension.
-   [x] Create regression test for PL/pgSQL API (`sql/002_jsonb_stats_api_plpgsql.sql`).
-   [x] Create regression test for C API (`sql/003_jsonb_stats_api_c.sql`).
-   [x] Implement `stat_c(anyelement)`.
-   [x] Implement `jsonb_stats_agg_c(text, jsonb)`.
-   [x] Implement `jsonb_stats_summary_agg_c(jsonb)`.
-   [x] Implement `jsonb_stats_summary_merge_agg_c(jsonb)`.
-   [x] Define C structures for aggregate states.
-   [x] Add documentation for C implementation details.
-   [x] Add advanced regression tests for edge cases and arrays.
-   [x] Implement full support for array types in `jsonb_stats_summary_agg`.
-   [x] Add a reference PL/pgSQL implementation and a regression test to compare it against the C version.
-   [x] Enhance C implementation to match `statbus` reference (variance, stddev, etc.).
-   [x] Implement `coefficient_of_variation_pct` in C implementation.
-   [x] Enhance C array summary to match `statbus` reference (unique element counts).
-   [x] Refactor tests to use a `reference` view for validation, based on `statbus`.
-   [x] Create a new regression test (`003`) that implements the full example scenario from the `README.md`.
-   [x] Add benchmark test comparing C and PL/pgSQL performance.
-   [x] Improve `Makefile` with `fast`/`TESTS` targets and robust test execution.
-   [x] Align examples and tests with `sql_saga` temporal conventions.
-   [x] Improve test isolation using transactions and a dedicated regression database.
-   [x] Set up `pgzx` Zig project (abandoned in favor of Rust/pgrx).

</details>
