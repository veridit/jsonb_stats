A living document of upcoming tasks.
Tasks are checked ✅ when done and made brief.
Keep a journal.md that tracks the state of the current ongoing task and relevant details.

# `jsonb_stats` Development Tasks

This file tracks the major development tasks for the `jsonb_stats` extension.

## High Priority
- [ ] Spike: Evaluate `pgzx` (Zig) as an alternative to the C implementation.
    - [ ] Set up a new `pgzx` project based on their template.
    - [ ] Port a single function (e.g., `stat(anyelement)`) to Zig as a proof-of-concept.
    - [ ] Assess the development experience and feasibility of porting the entire extension.

<details>
<summary>On Hold: C Implementation</summary>

- [ ] Fix all C-level bugs and restore full functionality by incrementally rebuilding the aggregate functions.
    - [x] Stabilize C code by stubbing aggregate functions.
    - [ ] Re-implement `jsonb_stats_accum` (initialization path).
    - [ ] Re-implement `jsonb_stats_accum` (update path for all summary types).
    - [ ] Re-implement `jsonb_stats_merge`.
    - [ ] Re-implement `jsonb_stats_final` (calculation of derived statistics).
    - [ ] Update all tests to match new API and verify correctness.
- [ ] Finalize documentation and clean up test files.

</details>

<details>
<summary>Completed Tasks</summary>

## Completed
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
-   [x] Implement full support for array types in `jsonb_stats_summary_agg` (in progress, basic counting done).
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
